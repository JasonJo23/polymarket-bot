"""
=============================================================================
intelligence.py – MarketIntelligence  (v2.0 – momentum poistettu)
=============================================================================
KORJAUKSET v1.1 → v2.0:

  BUG #1  CLOB /prices-history palauttaa tyhjää → momentum=0.5 aina
          → Confidence laskettu puolella datalla koko ajan
          → KORJAUS: momentum poistettu, confidence = quality × cat_mult × 100

  BUG #2  MIN_CONFIDENCE=30 + MIN_LIQUIDITY=0.3 liian tiukat
          → Sports max conf=70, hylkäsi kaiken jos quality < 0.43
          → KORJAUS: oletusarvot 20 ja 0.05

  BUG #3  Hinnan ääripää -tarkistus oli tracker.py:ssä sekavasti
          → KORJAUS: siirretty tänne, lokittaa selkeästi
=============================================================================
"""

import os
import logging
import requests
from typing import Dict, Any, Tuple

log = logging.getLogger("Scout.Intelligence")
CLOB_BASE = "https://clob.polymarket.com"

SPORTS_KEYWORDS = [
    "vs.", "vs ", "game 1", "game 2", "game 3", "bo3", "bo5",
    "winner", "match", "series",
    "nba", "nfl", "nhl", "mlb", "wnba",
    "lakers", "celtics", "knicks", "hawks", "bulls", "heat",
    "thunder", "pistons", "magic", "rockets", "spurs", "raptors",
    "cavaliers", "76ers", "trail blazers", "nuggets", "timberwolves",
    "pacers", "bucks", "clippers", "warriors", "suns", "jazz",
    "bruins", "sabres", "lightning", "oilers", "ducks", "avalanche",
    "kings", "canadiens", "flyers", "golden knights", "panthers",
    "angels", "royals", "red sox", "orioles", "yankees", "dodgers",
    "lol:", "dota", "csgo", "valorant", "counter-strike",
    "fc ", "win on", "epl", "bundesliga", "serie a", "la liga",
    "premier league", "champions league", "barcelona", "madrid",
    "manchester", "arsenal", "liverpool", "chelsea", "tottenham",
    "juventus", "milan", "inter", "napoli", "marseille", "lille",
    "atletico", "bayern", "borussia", "ajax", "porto", "benfica",
    "ufc", "mma", "fight night", "boxing",
    "innings", "o/u", "spread",
    "esports world cup", "lck", "lec", "lcs", "pgl", "esl",
]

HIGH_QUALITY_KEYWORDS = [
    "trump", "biden", "election", "fed", "bitcoin", "ethereum",
    "btc", "eth", "crypto", "gdp", "inflation", "iran", "ceasefire",
    "war", "congress", "senate", "president", "rate", "tariff",
    "treasury", "powell", "policy", "agreement", "deal", "treaty",
    "tariffs", "recession", "default", "sanctions", "nuclear",
    "elon", "musk", "apple", "nvidia", "tesla", "nasdaq",
]


def _is_sports(question: str) -> bool:
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
    try:
        r = requests.get(f"{CLOB_BASE}/book", params={"token_id": token_id}, timeout=5)
        if r.status_code != 200:
            return 0.5
        book = r.json()
        bids = book.get("bids", [])
        asks = book.get("asks", [])
        if not bids or not asks:
            return 0.2
        best_bid     = float(bids[0].get("price", 0))
        best_ask     = float(asks[0].get("price", 1))
        spread       = best_ask - best_bid
        spread_score = max(0.0, 1.0 - (spread / 0.05))
        bid_depth    = sum(float(b.get("size", 0)) for b in bids[:5])
        ask_depth    = sum(float(a.get("size", 0)) for a in asks[:5])
        depth_score  = min(1.0, (bid_depth + ask_depth) / 1000.0)
        quality      = round((0.6 * spread_score) + (0.4 * depth_score), 3)
        log.debug(f"Order book: spread={spread:.3f} depth={bid_depth+ask_depth:.0f} quality={quality:.2f}")
        return quality
    except Exception as e:
        log.debug(f"Order book analyysi epäonnistui: {e}")
        return 0.5


def analyze_signal(signal: Dict[str, Any], token_id: str, token_price: float) -> Dict[str, Any]:
    min_confidence = float(os.getenv("MIN_CONFIDENCE", 20))
    min_liquidity  = float(os.getenv("MIN_LIQUIDITY", 0.05))
    question = signal.get("question", "")

    if token_price < 0.05:
        reason = f"Hinta liian matala ({token_price:.3f} < 0.05)"
        log.warning(f"❌ Intelligence: {question[:40]} → {reason}")
        return _result(False, 0.0, "unknown", 0.0, reason)

    if token_price > 0.92:
        reason = f"Hinta liian korkea ({token_price:.3f} > 0.92) — konsenssus selvä"
        log.warning(f"❌ Intelligence: {question[:40]} → {reason}")
        return _result(False, 0.0, "unknown", 0.0, reason)

    category, cat_multiplier = _detect_category(question)
    market_quality = _get_order_book_quality(token_id)
    confidence     = round(market_quality * 100 * cat_multiplier, 1)

    if market_quality < min_liquidity:
        reason = f"Heikko likviditeetti (quality={market_quality:.2f} < {min_liquidity})"
        log.warning(f"❌ Intelligence: {question[:40]} → {reason}")
        return _result(False, confidence, category, market_quality, reason)

    if confidence < min_confidence:
        reason = f"Confidence liian matala ({confidence:.1f} < {min_confidence}) [{category}]"
        log.warning(f"❌ Intelligence: {question[:40]} → {reason}")
        return _result(False, confidence, category, market_quality, reason)

    reason = f"OK — conf={confidence:.1f} quality={market_quality:.2f} price={token_price:.3f} [{category}]"
    log.info(f"✅ Intelligence: {question[:40]} → {reason}")
    return _result(True, confidence, category, market_quality, reason)


def _result(approved, confidence, category, market_quality, reason):
    return {
        "approved":       approved,
        "confidence":     confidence,
        "category":       category,
        "market_quality": market_quality,
        "momentum":       0.5,
        "reason":         reason,
    }