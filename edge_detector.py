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
        self.max_token_price  = float(os.getenv("MAX_TOKEN_PRICE", 0.85))
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
        try:
            from fresh_context import clear_fresh_context_cache
            clear_fresh_context_cache()
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
        market_type = signal.get("market_type") or self._detect_sport(question)
        low, high = self._price_bounds(market_type, relaxed=True)
        if token_price < low:
            return self._reject(f"{market_type} hinta liian matala ({token_price:.3f} < {low:.2f})")
        if token_price > high:
            return self._reject(f"{market_type} hinta liian korkea ({token_price:.3f} > {high:.2f})")

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
                condition_id=signal.get("market_id", ""),
                token_id=signal.get("token_id", ""),
                token_price=token_price,
            )
        except Exception as e:
            log.debug(f"Market context haku epäonnistui: {e}")

        fresh_ctx = {}
        try:
            from fresh_context import get_fresh_context
            fresh_ctx = get_fresh_context(question, market_type)
        except Exception as e:
            log.debug(f"Fresh context haku epäonnistui: {e}")

        data_quality = max(
            float(market_ctx.get("data_quality", 0.0) or 0.0),
            float(fresh_ctx.get("data_quality", 0.0) or 0.0),
        )
        market_context_text = market_ctx.get("context_text", "")
        market_description = market_ctx.get("description", "")
        if self._market_context_mismatch(question, market_context_text, market_description):
            log.warning(f"Polymarket-konteksti hylätty ennen Claudea: {question[:40]}")
            market_context_text = ""
            market_description = ""
            data_quality = float(fresh_ctx.get("data_quality", 0.0) or 0.0)

        context = {
            "sport":        self._detect_sport(question),
            "home_team":    "",
            "away_team":    "",
            "injuries":     [],
            "recent_form":  [],
            "h2h":          [],
            "news":         [],
            "lineup_notes": [],
            "data_quality": data_quality,
            "fetched_at":   datetime.now(timezone.utc).isoformat(),
            "market_end_date": signal.get("end_date", ""),
            # Lisää Polymarket-konteksti
            "opponents":    market_ctx.get("opponents", ""),
            "tournament":   market_ctx.get("tournament", ""),
            "description":  market_description,
            "crypto_price": market_ctx.get("crypto_price", 0.0),
            "context_text": market_context_text,
            "fresh_context_text": fresh_ctx.get("context_text", ""),
            "fresh_data_quality": fresh_ctx.get("data_quality", 0.0),
            "fresh_sources": fresh_ctx.get("source", []),
        }

        # Laske todennäköisyys
        try:
            prob_engine = self._get_probability_engine()
            result = prob_engine.calculate_edge(question, outcome, token_price, context)
        except Exception as e:
            log.warning(f"Probability laskenta epäonnistui: {e}")
            if os.getenv("EDGE_DETECTOR_FAIL_OPEN", "false").lower() == "true":
                fallback = self._approve("Probability engine epäonnistui — fail-open")
            else:
                fallback = self._reject("Probability engine epäonnistui — fail-closed")
            self._analysis_cache[cache_key] = fallback
            return fallback

        result["data_quality"] = data_quality
        result["market_data_quality"] = float(market_ctx.get("data_quality", 0.0) or 0.0)
        result["fresh_data_quality"] = float(fresh_ctx.get("data_quality", 0.0) or 0.0)
        edge       = result.get("edge", 0.0)
        our_prob   = result.get("our_probability", token_price)
        confidence = result.get("confidence", "low")
        reasoning  = result.get("reasoning", "")

        if self._reasoning_flags_bad_context(reasoning):
            final = self._reject(f"Claude havaitsi virheellisen kontekstin: {reasoning[:100]}")
            self._analysis_cache[cache_key] = final
            log.warning(f"EdgeDetector hylkäsi virhekontekstin: {question[:40]} → {reasoning[:120]}")
            return final

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

        if confidence == "high" and edge >= 0.15:
            relaxed_low, relaxed_high = self._price_bounds(market_type, relaxed=True)
            if relaxed_low <= token_price <= relaxed_high:
                should_bet = True

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
            if any(k in q for k in ["game ", "map "]):
                return "esports_map"
            return "esports_match"
        if any(k in q for k in ["lakers", "celtics", "knicks", "nba", "thunder", "spurs", "76ers", "cavaliers", "pistons", "timberwolves"]):
            return "sports"
        if any(k in q for k in ["fc ", "arsenal", "chelsea", "liverpool", "madrid", "barcelona", "premier league", "la liga"]):
            return "sports"
        if any(k in q for k in ["trump", "biden", "iran", "election", "fed", "btc", "eth", "tariff", "ceasefire"]):
            return "macro"
        return "general"

    def _price_bounds(self, market_type: str, relaxed: bool = False) -> tuple:
        bounds = {
            "macro": (
                float(os.getenv("MACRO_MIN_TOKEN_PRICE", 0.20)),
                float(os.getenv("MACRO_MAX_TOKEN_PRICE", 0.85)),
            ),
            "sports": (
                float(os.getenv("SPORTS_MIN_TOKEN_PRICE", 0.25)),
                float(os.getenv("SPORTS_MAX_TOKEN_PRICE", 0.85)),
            ),
            "esports_match": (
                float(os.getenv("ESPORTS_MATCH_MIN_TOKEN_PRICE", 0.30)),
                float(os.getenv("ESPORTS_MATCH_MAX_TOKEN_PRICE", 0.78)),
            ),
            "esports_map": (
                float(os.getenv("ESPORTS_MAP_MIN_TOKEN_PRICE", 0.35)),
                float(os.getenv("ESPORTS_MAP_MAX_TOKEN_PRICE", 0.70)),
            ),
            "general": (
                self.min_token_price,
                self.max_token_price,
            ),
        }
        low, high = bounds.get(market_type, bounds["general"])
        if relaxed and market_type in ("macro", "sports", "esports_match"):
            return max(0.05, low - 0.05), min(0.90, high + 0.05)
        return low, high

    def _reasoning_flags_bad_context(self, reasoning: str) -> bool:
        text = (reasoning or "").lower()
        bad_context_phrases = [
            "konteksti virheellinen",
            "markkinakuvaus virheellinen",
            "kuvaus virheellinen",
            "ei vastaa markkinaa",
            "ei vastaa otsikkoa",
            "wrong context",
            "context mismatch",
            "description mismatch",
            "rihanna",
            "gta",
            "gta vi",
        ]
        return any(phrase in text for phrase in bad_context_phrases)

    def _market_context_mismatch(self, question: str, context_text: str, description: str) -> bool:
        q = (question or "").lower()
        ctx = (context_text or "").lower()
        desc = (description or "").lower()
        combined = f"{ctx} {desc}"
        if not combined:
            return False

        looks_like_single_game = any(k in q for k in [" vs. ", " vs ", " o/u ", "spread"])
        future_terms = [
            "nba finals", "finals", "championship", "win the nba", "win nba",
            "stanley cup", "world series", "super bowl", "conference finals",
        ]
        looks_like_future_market = any(k in combined for k in future_terms)

        if looks_like_single_game and looks_like_future_market:
            return True
        return False

    def _approve(self, reason: str, edge: float = 0.0, our_probability: float = 0.5, confidence: str = "medium") -> Dict:
        return {"approved": True, "reason": reason, "edge": edge, "our_probability": our_probability, "confidence": confidence}

    def _reject(self, reason: str) -> Dict:
        return {"approved": False, "reason": reason, "edge": 0.0, "our_probability": 0.0, "confidence": "low"}
