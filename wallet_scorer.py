"""
=============================================================================
wallet_scorer.py – WalletScorer  (v1.0)
=============================================================================
Laskee historiallisen win raten per lompakko suljettujen markkinoiden
perusteella ja tuottaa painokertoimen konsensukselle.

LOGIIKKA:
  1. Hae lompakon kauppahistoria (jo fetcherissä)
  2. Suodata suljetut markkinat (endDate menneisyydessä)
  3. Hae suljetun markkinan voittava outcome CLOB API:sta
  4. Laske win rate: oikeat / kaikki suljetut kaupat
  5. Muunna win rate painokertoimeksi (0.5–2.0)

PAINOKERROIN:
  win_rate < 0.40  → 0.5  (heikko, painaa vähemmän)
  win_rate 0.40-0.50 → 0.8
  win_rate 0.50-0.55 → 1.0  (normaali)
  win_rate 0.55-0.65 → 1.3
  win_rate > 0.65  → 2.0  (erinomainen, painaa tuplasti)
  alle 5 ratkaistua → 1.0  (ei tarpeeksi dataa)

HUOMIO: Alle 10 ratkaistua kauppaa = ei luotettavaa dataa.
Käytetään neutraalia painoa kunnes dataa on tarpeeksi.
=============================================================================
"""

import logging
import requests
from datetime import datetime, timezone
from typing import Dict, List, Optional
from functools import lru_cache

log = logging.getLogger("Scout.WalletScorer")

CLOB_BASE  = "https://clob.polymarket.com"
GAMMA_BASE = "https://gamma-api.polymarket.com"

# Cache markkinoiden tuloksille — ei haeta samaa useasti
_market_result_cache: Dict[str, Optional[str]] = {}


def _get_winning_outcome(condition_id: str) -> Optional[str]:
    """
    Hakee suljetun markkinan voittavan outcomen.
    Palauttaa outcome-nimen tai None jos markkina ei ole vielä ratkaistu.
    """
    if condition_id in _market_result_cache:
        return _market_result_cache[condition_id]

    try:
        r = requests.get(
            f"{CLOB_BASE}/markets/{condition_id}",
            timeout=5
        )
        if r.status_code != 200:
            _market_result_cache[condition_id] = None
            return None

        data = r.json()

        # Tarkista onko markkina ratkaistu
        if data.get("accepting_orders", True):
            _market_result_cache[condition_id] = None
            return None  # Vielä auki

        tokens = data.get("tokens", [])
        for token in tokens:
            price = float(token.get("price", 0))
            if price >= 0.99:  # Voittaja on lähellä 1.0
                winner = str(token.get("outcome", "")).upper()
                _market_result_cache[condition_id] = winner
                return winner

        _market_result_cache[condition_id] = None
        return None

    except Exception as e:
        log.debug(f"Voittavan outcomen haku epäonnistui: {e}")
        _market_result_cache[condition_id] = None
        return None


def calculate_wallet_score(
    wallet_address: str,
    trade_history: List[Dict],
    min_resolved: int = 5
) -> Dict:
    """
    Laskee lompakon historiallisen suorituskyvyn.

    Args:
        wallet_address: Lompakon osoite
        trade_history: Lista kaupoista (koko historia)
        min_resolved: Minimi ratkaistujen kauppojen määrä ennen painotusta

    Returns:
        {
            "address": str,
            "win_rate": float,        # 0.0–1.0
            "resolved_count": int,    # Ratkaistujen kauppojen määrä
            "correct_count": int,     # Oikeiden kauppojen määrä
            "weight": float,          # Painokerroin 0.5–2.0
            "reliable": bool          # Onko dataa tarpeeksi
        }
    """
    now = datetime.now(timezone.utc)

    # Suodata vain BUY-kaupat suljetuista markkinoista
    resolved_trades = []
    for trade in trade_history:
        side = str(trade.get("side", "")).upper()
        if side != "BUY":
            continue

        # Tarkista onko markkina suljettu
        condition_id = trade.get("conditionId", "")
        if not condition_id:
            continue

        # Hae markkinan sulkeutumisaika jos saatavilla
        # Käytetään CLOB API:a joka palauttaa accepting_orders=False suljetuille

        resolved_trades.append({
            "condition_id": condition_id,
            "outcome": str(trade.get("outcome", "")).upper(),
            "size": float(trade.get("usdcSize", 0) or trade.get("size", 0) or 0)
        })

    if not resolved_trades:
        return _default_score(wallet_address)

    # Laske win rate — hae voittajat (max 20 markkinaa per lompakko API-kutsujen rajoittamiseksi)
    correct = 0
    checked = 0
    seen_markets = set()

    for trade in resolved_trades[:30]:  # Rajoita API-kutsut
        cid = trade["condition_id"]
        if cid in seen_markets:
            continue
        seen_markets.add(cid)

        winner = _get_winning_outcome(cid)
        if winner is None:
            continue  # Markkina vielä auki tai ei dataa

        checked += 1
        if trade["outcome"] == winner:
            correct += 1

    if checked < min_resolved:
        return _default_score(wallet_address, checked, correct)

    win_rate = correct / checked if checked > 0 else 0.5
    weight = _win_rate_to_weight(win_rate)

    return {
        "address":       wallet_address,
        "win_rate":      round(win_rate, 3),
        "resolved_count": checked,
        "correct_count": correct,
        "weight":        weight,
        "reliable":      checked >= min_resolved
    }


def _win_rate_to_weight(win_rate: float) -> float:
    """Muuntaa win raten painokertoimeksi."""
    if win_rate >= 0.65:
        return 2.0
    elif win_rate >= 0.55:
        return 1.3
    elif win_rate >= 0.50:
        return 1.0
    elif win_rate >= 0.40:
        return 0.8
    else:
        return 0.5


def _default_score(address: str, checked: int = 0, correct: int = 0) -> Dict:
    """Palauttaa neutraalin painon kun dataa ei ole tarpeeksi."""
    return {
        "address":        address,
        "win_rate":       0.5,
        "resolved_count": checked,
        "correct_count":  correct,
        "weight":         1.0,
        "reliable":       False
    }


def score_wallets_batch(
    qualified_wallets: List[Dict],
    history_cache: Dict[str, List[Dict]]
) -> Dict[str, Dict]:
    """
    Laskee wallet scoren kaikille kvalifioituneille lompakoille.
    Käyttää jo haettua historiaa — ei tee uusia API-kutsuja historialle.

    Returns:
        Dict[address -> score_dict]
    """
    scores = {}
    high_weight = []
    low_weight  = []

    for wallet in qualified_wallets:
        addr    = wallet["address"]
        history = history_cache.get(addr.lower(), [])

        score = calculate_wallet_score(addr, history)
        scores[addr] = score

        if score["reliable"]:
            if score["weight"] >= 1.3:
                high_weight.append(f"{addr[:10]} w={score['weight']} wr={score['win_rate']:.0%}")
            elif score["weight"] <= 0.8:
                low_weight.append(f"{addr[:10]} w={score['weight']} wr={score['win_rate']:.0%}")

    if high_weight:
        log.info(f"🌟 Korkea paino ({len(high_weight)}): {', '.join(high_weight[:3])}")
    if low_weight:
        log.info(f"⬇️  Matala paino ({len(low_weight)}): {', '.join(low_weight[:3])}")

    return scores