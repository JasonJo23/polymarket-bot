"""
=============================================================================
edge_detector.py – EdgeDetector  (v2.0)
=============================================================================
KORJAUKSET v1.0 → v2.0:

  BUG #1  Sama kysymys+outcome analysoitu 20+ kertaa per päivä
          → Barcelona analysoitu 20x, Girona 15x — turhat API-kulut
          → KORJAUS: analyysicache per sykli — sama kysymys+outcome
            analysoidaan vain KERRAN per sykli

  BUG #2  MIN_EDGE_THRESHOLD=0.05 liian matala
          → 0.025 edge hyväksyttiin vaikka se on lähes satunnainen
          → KORJAUS: oletusarvo nostettu 0.08:aan
            Vain selkeät edget (>8%) johtavat ostoon

  BUG #3  confidence="low" esti ostamisen vaikka edge oli suuri
          → US-Iran NO +0.195 edgellä ostettu oikein
          → Mutta NaVi +0.185 conf=medium hyväksytty — OK
          → KORJAUS: jos edge >= 0.15 ja conf != "low" → osta
            jos edge >= 0.20 → osta vaikka conf="low" (selkeä virhe markkinassa)
=============================================================================
"""

import os
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional

log = logging.getLogger("Scout.EdgeDetector")


class EdgeDetector:

    def __init__(self):
        self.min_edge         = float(os.getenv("MIN_EDGE_THRESHOLD", 0.08))
        self.min_data_quality = float(os.getenv("MIN_DATA_QUALITY", 0.3))
        self.min_token_price  = float(os.getenv("MIN_TOKEN_PRICE", 0.25))
        self.max_token_price  = float(os.getenv("MAX_TOKEN_PRICE", 0.80))
        self.enabled          = os.getenv("EDGE_DETECTOR_ENABLED", "true").lower() == "true"

        # BUG #1 KORJAUS: analyysicache per sykli
        # key = "question|outcome" → result dict
        self._analysis_cache: Dict[str, Dict] = {}
        self._cache_created = datetime.now(timezone.utc)

        self._probability_engine = None

    def _get_probability_engine(self):
        if self._probability_engine is None:
            from probability_engine import ProbabilityEngine
            self._probability_engine = ProbabilityEngine()
        return self._probability_engine

    def clear_cache(self):
        """Tyhjentää analyysicachen — kutsutaan jokaisen syklin alussa."""
        old_size = len(self._analysis_cache)
        self._analysis_cache.clear()
        self._cache_created = datetime.now(timezone.utc)
        # Tyhjennä myös market context cache
        try:
            from market_context import clear_context_cache
            clear_context_cache()
        except Exception:
            pass
        if old_size > 0:
            log.debug(f"Analyysicache tyhjennetty ({old_size} merkintää)")

    def should_buy(
        self,
        signal:      Dict[str, Any],
        token_price: float
    ) -> Dict[str, Any]:
        question = signal.get("question", "")
        outcome  = signal.get("outcome", "")

        if not self.enabled:
            return self._approve("EdgeDetector ei käytössä")

        # Hintatarkistus
        if token_price < self.min_token_price:
            return self._reject(f"Hinta liian matala ({token_price:.3f} < {self.min_token_price})")
        if token_price > self.max_token_price:
            return self._reject(f"Hinta liian korkea ({token_price:.3f} > {self.max_token_price})")

        # BUG #1 KORJAUS: tarkista cache ensin
        cache_key = f"{question}|{outcome}"
        if cache_key in self._analysis_cache:
            cached = self._analysis_cache[cache_key]
            log.debug(f"Cache hit: {question[:40]} → edge={cached.get('edge',0):+.2f}")
            return cached

        # Hae reaaliaikainen konteksti Polymarket API:sta
        market_ctx = {}
        try:
            from market_context import get_market_context
            market_ctx = get_market_context(
                question=question,
                token_price=token_price,
            )
        except Exception as e:
            log.debug(f"Market context haku epäonnistui: {e}")

        context = {
            "sport":        self._detect_sport(question),
            "home_team":    "",
            "away_team":    "",
            "injuries":     [],
            "recent_form":  [],
            "h2h":          [],
            "news":         [],
            "lineup_notes": [],
            "data_quality": market_ctx.get("data_quality", 0.5),
            "fetched_at":   datetime.now(timezone.utc).isoformat(),
            # Lisää Polymarket-konteksti
            "opponents":    market_ctx.get("opponents", ""),
            "tournament":   market_ctx.get("tournament", ""),
            "description":  market_ctx.get("description", ""),
            "crypto_price": market_ctx.get("crypto_price", 0.0),
            "context_text": market_ctx.get("context_text", ""),
        }

        # Laske todennäköisyys
        try:
            prob_engine = self._get_probability_engine()
            result = prob_engine.calculate_edge(question, outcome, token_price, context)
        except Exception as e:
            log.warning(f"Probability laskenta epäonnistui: {e}")
            fallback = self._approve("Probability engine epäonnistui — hyväksytään")
            self._analysis_cache[cache_key] = fallback
            return fallback

        result["data_quality"] = 0.5
        edge       = result.get("edge", 0.0)
        our_prob   = result.get("our_probability", token_price)
        confidence = result.get("confidence", "low")
        reasoning  = result.get("reasoning", "")

        # BUG #2+#3 KORJAUS: parannettu päätöslogiikka
        # Osta jos:
        #   - edge >= min_edge (0.08) JA confidence != "low"
        #   - TAI edge >= 0.15 JA confidence = "medium"
        #   - TAI edge >= 0.20 (selkeä virhe markkinassa — osta vaikka conf=low)
        should_bet = (
            (edge >= self.min_edge and confidence != "low") or
            (edge >= 0.15 and confidence == "medium") or
            (edge >= 0.20)
        )

        if should_bet:
            reason = (
                f"Edge löytyi! oma={our_prob:.2f} poly={token_price:.2f} "
                f"edge={edge:+.2f} conf={confidence} | {reasoning[:80]}"
            )
            log.info(f"✅ EdgeDetector: {question[:40]} → {reason}")
            final = {
                "approved":        True,
                "reason":          reason,
                "edge":            edge,
                "our_probability": our_prob,
                "confidence":      confidence,
            }
        else:
            reason = (
                f"Ei edgeä: oma={our_prob:.2f} poly={token_price:.2f} "
                f"edge={edge:+.2f} conf={confidence} | {reasoning[:80]}"
            )
            log.info(f"⏭️  EdgeDetector ohitti: {question[:40]} → {reason}")
            final = {
                "approved":        False,
                "reason":          reason,
                "edge":            edge,
                "our_probability": our_prob,
                "confidence":      confidence,
            }

        # Tallenna cacheen
        self._analysis_cache[cache_key] = final
        return final

    def _detect_sport(self, question: str) -> str:
        q = question.lower()
        if any(k in q for k in ["lol:", "cs2", "csgo", "valorant", "dota", "esports", "lck", "lec", "counter-strike"]):
            return "esports"
        if any(k in q for k in ["lakers", "celtics", "knicks", "nba", "thunder", "spurs", "76ers", "cavaliers", "pistons", "timberwolves"]):
            return "nba"
        if any(k in q for k in ["fc ", "arsenal", "chelsea", "liverpool", "madrid", "barcelona", "premier league", "la liga"]):
            return "football"
        if any(k in q for k in ["trump", "biden", "iran", "election", "fed", "btc", "eth", "tariff", "ceasefire"]):
            return "politics/macro"
        return "general"

    def _approve(self, reason: str, edge: float = 0.0, our_probability: float = 0.5, confidence: str = "medium") -> Dict:
        return {"approved": True, "reason": reason, "edge": edge, "our_probability": our_probability, "confidence": confidence}

    def _reject(self, reason: str) -> Dict:
        return {"approved": False, "reason": reason, "edge": 0.0, "our_probability": 0.0, "confidence": "low"}