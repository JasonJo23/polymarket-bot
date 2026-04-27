"""
=============================================================================
intelligence.py – MarketIntelligence  (v1.1 – korjattu)
=============================================================================
Korjaukset:
  #5  Intelligence toimii ilman momentum-dataa — pelkkä quality riittää
  #9  Jalkapallo tunnistetaan urheiluksi oikein (lisätty avainsanat)
=============================================================================
"""

import logging
import requests
from typing import Dict, Any, Optional, Tuple

log = logging.getLogger("Scout.Intelligence")
CLOB_BASE = "https://clob.polymarket.com"

# FIX #9: Kattavampi lista — jalkapallo mukaan
SPORTS_KEYWORDS = [
    # Yleiset
    "vs.", "vs ", "game 1", "game 2", "game 3", "bo3", "bo5",
    "winner", "match", "series",
    # NBA/NHL/MLB/NFL
    "nba", "nfl", "nhl", "mlb", "wnba",
    "lakers", "celtics", "knicks", "hawks", "bulls", "heat",
    "thunder", "pistons", "magic", "rockets", "spurs", "raptors",
    "cavaliers", "76ers", "trail blazers", "nuggets", "timberwolves",
    "bruins", "sabres", "lightning", "oilers", "ducks", "avalanche",
    "kings", "canadiens", "flyers",
    "angels", "royals", "red sox", "orioles", "yankees",
    # Esport
    "lol:", "dota", "csgo", "valorant",
    # Jalkapallo FIX #9
    "fc ", "win on", "epl", "bundesliga", "serie a", "la liga",
    "premier league", "champions league", "barcelona", "madrid",
    "manchester", "arsenal", "liverpool", "chelsea", "tottenham",
    "juventus", "milan", "inter", "napoli", "marseille", "lille",
    "atletico", "Bayern", "borussia", "ajax",
    # MLB baseball
    "innings", "o/u", "over", "under", "spread",
]

HIGH_QUALITY_KEYWORDS = [
    "trump", "biden", "election", "fed", "bitcoin", "ethereum",
    "btc", "eth", "crypto", "gdp", "inflation", "iran", "ceasefire",
    "war", "congress", "senate", "president", "rate", "tariff",
    "treasury", "powell", "policy", "agreement", "deal", "treaty",
]


def _is_sports(question: str) -> bool:
    """Julkinen funktio tracker.py:tä varten."""
    q = question.lower()
    return any(kw in q for kw in SPORTS_KEYWORDS)


def _detect_category(question: str) -> Tuple[str, float]:
    q = question.lower()
    for kw in SPORTS_KEYWORDS:
        if kw in q:
            return "sports", 0.7
    for kw in HIGH_QUALITY_KEYWORDS:
        if kw in q:
            return "politics/macro", 1.0
    return "general", 0.9


def _get_order_book_quality(token_id: str) -> float:
    """Analysoi markkinan laadun order bookin perusteella."""
    try:
        r = requests.get(
            f"{CLOB_BASE}/book",
            params={"token_id": token_id},
            timeout=5
        )
        if r.status_code != 200:
            return 0.5

        book = r.json()
        bids = book.get("bids", [])
        asks = book.get("asks", [])

        if not bids or not asks:
            return 0.3

        best_bid = float(bids[0].get("price", 0)) if bids else 0
        best_ask = float(asks[0].get("price", 1)) if asks else 1
        spread = best_ask - best_bid
        spread_score = max(0, 1 - (spread / 0.05))

        bid_depth = sum(float(b.get("size", 0)) for b in bids[:5])
        ask_depth = sum(float(a.get("size", 0)) for a in asks[:5])
        total_depth = bid_depth + ask_depth
        depth_score = min(1.0, total_depth / 1000)

        quality = (0.6 * spread_score) + (0.4 * depth_score)
        log.debug(f"Order book: spread={spread:.3f} depth={total_depth:.0f} → quality={quality:.2f}")
        return round(quality, 3)
    except Exception as e:
        log.debug(f"Order book analyysi epäonnistui: {e}")
        return 0.5


def _get_price_momentum(token_id: str, current_price: float) -> float:
    """Analysoi hinnan momentum."""
    try:
        r = requests.get(
            f"{CLOB_BASE}/prices-history",
            params={"market": token_id, "interval": "1h", "fidelity": 10},
            timeout=5
        )
        if r.status_code != 200:
            return 0.5

        data = r.json()
        history = data.get("history", [])
        if len(history) < 2:
            return 0.5

        oldest_price = float(history[0].get("p", current_price))
        price_change = current_price - oldest_price
        normalized = price_change / 0.10
        score = max(0.0, min(1.0, 0.5 + normalized))
        log.debug(f"Momentum: {oldest_price:.3f}→{current_price:.3f} score={score:.2f}")
        return round(score, 3)
    except Exception:
        return 0.5


def analyze_signal(signal: Dict[str, Any], token_id: str, token_price: float) -> Dict[str, Any]:
    """
    FIX #5: Jos momentum-data ei ole saatavilla (0.5 = neutraali),
    päätös tehdään pelkän market_quality:n perusteella.
    Ei hylätä pelkästään siksi että momentum on 0.5.
    """
    import os
    min_confidence = float(os.getenv("MIN_CONFIDENCE", 30))
    min_liquidity  = float(os.getenv("MIN_LIQUIDITY", 0.1))

    question = signal.get("question", "")
    outcome  = signal.get("outcome", "")

    category, cat_multiplier = _detect_category(question)
    market_quality = _get_order_book_quality(token_id)
    momentum = _get_price_momentum(token_id, token_price)

    # FIX #5: Jos momentum on täsmälleen 0.5 (ei dataa), painota quality enemmän
    if momentum == 0.5:
        raw_confidence = market_quality * 100  # Vain quality
    else:
        raw_confidence = (0.5 * market_quality + 0.5 * momentum) * 100

    confidence = round(raw_confidence * cat_multiplier, 1)

    approved = True
    reason = ""

    if market_quality < min_liquidity:
        approved = False
        reason = f"Heikko likviditeetti ({market_quality:.2f} < {min_liquidity})"
    elif momentum < 0.20:  # Vain selvästi laskeva hinta hylätään
        approved = False
        reason = f"Hinta laskee vahvasti (momentum={momentum:.2f})"
    elif confidence < min_confidence:
        approved = False
        reason = f"Confidence liian matala ({confidence:.1f} < {min_confidence})"
    else:
        reason = f"OK — conf={confidence:.1f} quality={market_quality:.2f} momentum={momentum:.2f} [{category}]"

    result = {
        "approved":       approved,
        "confidence":     confidence,
        "category":       category,
        "market_quality": market_quality,
        "momentum":       momentum,
        "reason":         reason
    }

    if approved:
        log.info(f"✅ Intelligence: {question[:40]} → {reason}")
    else:
        log.warning(f"❌ Intelligence hylkäsi: {question[:40]} → {reason}")

    return result