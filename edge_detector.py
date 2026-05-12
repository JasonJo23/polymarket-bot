"""
=============================================================================
edge_detector.py – EdgeDetector  (v1.0)
=============================================================================
STRATEGIA:
  Integraatiopiste joka yhdistää:
    1. SportDataFetcher   → kontekstidata
    2. ProbabilityEngine  → oma todennäköisyys
    3. Päätöslogiikka     → ostaako vai ei

  Kutsutaan tracker.py:n execute_order():sta ENNEN CLOB-ostoa.
  Jos edge löytyy → osta.
  Jos ei edgeä → ohita vaikka konsensus sanoisi osta.

PÄÄTÖSLOGIIKKA:
  Osta jos KAIKKI seuraavat toteutuvat:
    1. Oma todennäköisyys > Polymarket-hinta + MIN_EDGE (oletus 5%)
    2. Confidence != "low"
    3. Data quality >= MIN_DATA_QUALITY (oletus 0.3)
    4. Token-hinta välillä MIN_TOKEN_PRICE - MAX_TOKEN_PRICE

  Tämä on konservatiivinen — mieluummin ohitetaan hyvä kauppa
  kuin ostetaan huono.
=============================================================================
"""

import os
import logging
from typing import Dict, Any, Optional

log = logging.getLogger("Scout.EdgeDetector")


class EdgeDetector:

    def __init__(self):
        self.min_edge         = float(os.getenv("MIN_EDGE_THRESHOLD", 0.05))
        self.min_data_quality = float(os.getenv("MIN_DATA_QUALITY", 0.3))
        self.min_token_price  = float(os.getenv("MIN_TOKEN_PRICE", 0.25))
        self.max_token_price  = float(os.getenv("MAX_TOKEN_PRICE", 0.80))
        self.enabled          = os.getenv("EDGE_DETECTOR_ENABLED", "true").lower() == "true"

        # Lazy init — luodaan vasta tarvittaessa
        self._news_fetcher       = None
        self._probability_engine = None

    def _get_news_fetcher(self):
        if self._news_fetcher is None:
            from news_fetcher import SportDataFetcher
            self._news_fetcher = SportDataFetcher()
        return self._news_fetcher

    def _get_probability_engine(self):
        if self._probability_engine is None:
            from probability_engine import ProbabilityEngine
            self._probability_engine = ProbabilityEngine()
        return self._probability_engine

    # ===========================================================================
    # Päämetodi
    # ===========================================================================

    def should_buy(
        self,
        signal:      Dict[str, Any],
        token_price: float
    ) -> Dict[str, Any]:
        """
        Päättää ostaako signaalin perusteella.

        Args:
            signal:      Tracker.process():n palauttama signaali
            token_price: Nykyinen token-hinta Polymarketilla

        Returns:
            {
                "approved":        bool,
                "reason":          str,
                "edge":            float,
                "our_probability": float,
                "confidence":      str,
            }
        """
        question = signal.get("question", "")
        outcome  = signal.get("outcome", "")

        # Jos edge detector ei ole käytössä → hyväksy kaikki (fallback vanhaan logiikkaan)
        if not self.enabled:
            return self._approve(f"EdgeDetector ei käytössä — hyväksytään konsensuksen perusteella")

        # Hintatarkistus
        if token_price < self.min_token_price:
            return self._reject(f"Hinta liian matala ({token_price:.3f} < {self.min_token_price}) — longshot riski")

        if token_price > self.max_token_price:
            return self._reject(f"Hinta liian korkea ({token_price:.3f} > {self.max_token_price}) — ei edgeä")

        # Luo minimikonteksti — ulkoiset lähteet blokattu verkossa
        # Claude analysoi pelkän kysymyksen ja hinnan perusteella
        from datetime import datetime, timezone as _tz
        context = {
            "sport":        self._detect_sport(question),
            "home_team":    "",
            "away_team":    "",
            "injuries":     [],
            "recent_form":  [],
            "h2h":          [],
            "news":         [],
            "lineup_notes": [],
            "data_quality": 0.5,  # Neutraali — Claude toimii ilman ulkoista dataa
            "fetched_at":   datetime.now(_tz.utc).isoformat(),
        }
        data_quality = 0.5

        # Laske todennäköisyys
        try:
            prob_engine = self._get_probability_engine()
            result = prob_engine.calculate_edge(question, outcome, token_price, context)
        except Exception as e:
            log.warning(f"Probability laskenta epäonnistui: {e}")
            return self._approve(f"Probability engine epäonnistui — hyväksytään konservatiivisesti")

        result["data_quality"] = data_quality
        edge       = result.get("edge", 0.0)
        our_prob   = result.get("our_probability", token_price)
        confidence = result.get("confidence", "low")
        reasoning  = result.get("reasoning", "")
        should_bet = result.get("should_bet", False)

        if should_bet:
            reason = (
                f"Edge löytyi! oma={our_prob:.2f} poly={token_price:.2f} "
                f"edge={edge:+.2f} conf={confidence} | {reasoning[:80]}"
            )
            log.info(f"✅ EdgeDetector: {question[:40]} → {reason}")
            return {
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
            return {
                "approved":        False,
                "reason":          reason,
                "edge":            edge,
                "our_probability": our_prob,
                "confidence":      confidence,
            }

    # ===========================================================================
    # Apumetodit
    # ===========================================================================

    def _detect_sport(self, question: str) -> str:
        q = question.lower()
        if any(k in q for k in ["lol:", "cs2", "csgo", "valorant", "dota", "esports", "lck", "lec"]):
            return "esports"
        if any(k in q for k in ["lakers", "celtics", "knicks", "nba", "thunder", "spurs", "76ers"]):
            return "nba"
        if any(k in q for k in ["fc ", "arsenal", "chelsea", "liverpool", "madrid", "barcelona"]):
            return "football"
        if any(k in q for k in ["trump", "biden", "iran", "election", "fed", "btc", "eth"]):
            return "politics/macro"
        return "general"

    def _approve(
        self,
        reason:           str,
        edge:             float = 0.0,
        our_probability:  float = 0.5,
        confidence:       str   = "medium"
    ) -> Dict:
        return {
            "approved":        True,
            "reason":          reason,
            "edge":            edge,
            "our_probability": our_probability,
            "confidence":      confidence,
        }

    def _reject(self, reason: str) -> Dict:
        return {
            "approved":        False,
            "reason":          reason,
            "edge":            0.0,
            "our_probability": 0.0,
            "confidence":      "low",
        }