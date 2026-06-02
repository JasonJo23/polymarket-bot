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
from market_types import classify_market, confidence_multiplier, is_sports, price_bounds

log = logging.getLogger("Scout.Intelligence")
CLOB_BASE = "https://clob.polymarket.com"

def _is_sports(question: str) -> bool:
    return is_sports(question)


def _detect_category(question: str) -> Tuple[str, float]:
    market_type = classify_market(question)
    return market_type, confidence_multiplier(market_type)


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
    category, cat_multiplier = _detect_category(question)
    low, high = price_bounds(category, relaxed=True)

    if token_price < low:
        reason = f"Hinta liian matala ({token_price:.3f} < {low:.2f}) [{category}]"
        log.warning(f"❌ Intelligence: {question[:40]} → {reason}")
        return _result(False, 0.0, category, 0.0, reason)

    if token_price > high:
        reason = f"Hinta liian korkea ({token_price:.3f} > {high:.2f}) — konsenssus selvä [{category}]"
        log.warning(f"❌ Intelligence: {question[:40]} → {reason}")
        return _result(False, 0.0, category, 0.0, reason)

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
