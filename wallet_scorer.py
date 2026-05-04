"""
=============================================================================
wallet_scorer.py – WalletScorer  (v2.0 – ROI-pohjainen, korjattu)
=============================================================================
KORJAUKSET v1.0 → v2.0:

  BUG #1  seen_markets otti vain ENSIMMÄISEN kaupan per markkina
          → Sama lompakko voi ostaa YES ja sitten NO samalla markkinalla
          → Nyt ryhmitellään kaikki kaupat markkinoittain ja lasketaan
             nettopositio per outcome

  BUG #2  Win/loss -binääri ei huomioinut kauppakokoa
          → 10 USDC väärä + 5000 USDC oikea = "50% win rate" vanhassa
          → Nyt käytetään ROI-pohjaista laskentaa: voittava USDC / kaikki USDC

  BUG #3  continue ohitti avoimet markkinat laskematta niitä häviöiksi
          → Survivorship bias: vain helposti resolvattavat markkinat laskettiin
          → Nyt open-markkinat jätetään pois laskuista (ei häviötä eikä voittoa)
            mutta lokataan erikseen seurantaa varten

  BUG #4  analyzer.py ei käyttänyt scorer-painoja mitenkään
          → Nyt analyze()-metodi ottaa wallet_scores-parametrin ja
             järjestää lompakot painotetulla volyymilla

PAINOKERROIN (ROI-pohjainen):
  avg_roi >= +20%  → 2.0
  avg_roi >= +10%  → 1.5
  avg_roi >=   0%  → 1.0  (ei tappiolla, ei voitolla)
  avg_roi >= -10%  → 0.7
  avg_roi <  -10%  → 0.4
  alle 5 ratkaistua → 1.0  (neutraali, ei tarpeeksi dataa)
=============================================================================
"""

import logging
import requests
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from collections import defaultdict

log = logging.getLogger("Scout.WalletScorer")

CLOB_BASE  = "https://clob.polymarket.com"
GAMMA_BASE = "https://gamma-api.polymarket.com"

# Globaali cache markkinoiden tuloksille — ei haeta samaa useasti per sessio
_market_result_cache: Dict[str, Optional[str]] = {}


# ===========================================================================
# Markkinan tuloksen haku
# ===========================================================================

def _get_winning_outcome(condition_id: str) -> Optional[str]:
    """
    Hakee suljetun markkinan voittavan outcomen CLOB API:sta.

    Palauttaa:
        str   – voittava outcome (esim. "YES", "KNICKS") jos markkina ratkaistu
        None  – markkina vielä auki TAI tulosta ei saatu

    HUOM: None ei tarkoita häviötä — se tarkoittaa "ei tietoa vielä".
    Kutsuja ei lasketa win/loss -tilastoihin.
    """
    if condition_id in _market_result_cache:
        return _market_result_cache[condition_id]

    try:
        r = requests.get(
            f"{CLOB_BASE}/markets/{condition_id}",
            timeout=5
        )
        if r.status_code != 200:
            # Älä cacheta verkkovirhettä — yritetään uudelleen ensi kerralla
            return None

        data = r.json()

        # accepting_orders=True → markkina vielä auki
        if data.get("accepting_orders", True):
            _market_result_cache[condition_id] = None
            return None

        # Etsi token jonka hinta on lähellä 1.0 (voittaja)
        tokens = data.get("tokens", [])
        for token in tokens:
            price = float(token.get("price", 0))
            if price >= 0.99:
                winner = str(token.get("outcome", "")).upper().strip()
                _market_result_cache[condition_id] = winner
                log.debug(f"Voittaja löytyi: {condition_id[:16]} → {winner}")
                return winner

        # Markkina suljettu mutta voittajaa ei löydy (esim. N/A, cancelled)
        _market_result_cache[condition_id] = None
        return None

    except Exception as e:
        log.debug(f"_get_winning_outcome virhe ({condition_id[:16]}): {e}")
        return None


# ===========================================================================
# ROI-laskenta
# ===========================================================================

def _group_trades_by_market(trade_history: List[Dict]) -> Dict[str, Dict[str, float]]:
    """
    Ryhmittelee BUY-kaupat markkinoittain ja outcomeittain.

    Palauttaa:
        {
            condition_id: {
                "YES": 500.0,   # USDC ostettu YES-tokeneja
                "NO":  100.0,   # USDC ostettu NO-tokeneja
                ...
            }
        }

    BUG #1 KORJAUS: Ei enää oteta vain ensimmäistä kauppaa per markkina.
    Kaikki saman markkinan kaupat yhdistetään nettopositioksi.
    """
    market_positions: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))

    for trade in trade_history:
        # Vain ostot — myynneissä positio on jo tehty
        side = str(trade.get("side", "")).upper()
        if side != "BUY":
            continue

        condition_id = str(trade.get("conditionId", "")).strip()
        if not condition_id:
            continue

        outcome = str(trade.get("outcome", "")).upper().strip()
        if not outcome:
            continue

        # Koko: kokeile useita kenttänimiä
        size = 0.0
        for key in ("usdcSize", "size", "amount"):
            raw = trade.get(key)
            if raw is not None:
                try:
                    v = float(raw)
                    if v > 0:
                        size = v
                        break
                except (TypeError, ValueError):
                    pass

        if size > 0:
            market_positions[condition_id][outcome] += size

    return market_positions


def _calculate_market_roi(
    outcome_sizes: Dict[str, float],
    winning_outcome: str
) -> Tuple[float, float, float]:
    """
    Laskee yhden markkinan ROI lompakolle.

    BUG #2 KORJAUS: Huomioi kauppakoot — ei pelkkä win/loss binääri.

    Args:
        outcome_sizes:   {"YES": 500.0, "NO": 100.0}
        winning_outcome: "YES"

    Returns:
        (roi, winning_usdc, total_usdc)
        roi = (winning_usdc - total_usdc) / total_usdc
            = +1.0 jos kaikki rahat oikeassa outcomessa (100% ROI)
            = -1.0 jos kaikki rahat väärässä outcomessa (-100% ROI)
            = -0.67 jos 1/3 oikeassa, 2/3 väärässä
    """
    total_usdc   = sum(outcome_sizes.values())
    winning_usdc = outcome_sizes.get(winning_outcome, 0.0)

    if total_usdc <= 0:
        return 0.0, 0.0, 0.0

    roi = (winning_usdc - total_usdc) / total_usdc
    return round(roi, 4), winning_usdc, total_usdc


# ===========================================================================
# Pääfunktio: calculate_wallet_score
# ===========================================================================

def calculate_wallet_score(
    wallet_address: str,
    trade_history:  List[Dict],
    min_resolved:   int = 5,
    max_markets:    int = 50   # Max API-kutsut per lompakko
) -> Dict:
    """
    Laskee lompakon historiallisen suorituskyvyn ROI-pohjaisesti.

    KORJAUKSET:
      - Ryhmittelee kaikki saman markkinan kaupat (BUG #1)
      - Laskee painotetun ROI:n kauppakoon mukaan (BUG #2)
      - Ei laske avoimia markkinoita häviöiksi (BUG #3)

    Returns:
        {
            "address":         str,
            "win_rate":        float,   # Voittavien markkinoiden osuus (0–1)
            "avg_roi":         float,   # Keskimääräinen ROI per markkina
            "weighted_roi":    float,   # Volyymipainotettu ROI
            "resolved_count":  int,     # Ratkaistujen markkinoiden määrä
            "correct_count":   int,     # Voittavien markkinoiden määrä
            "total_usdc":      float,   # Kaikki USDC ratkaistuissa markkinoissa
            "weight":          float,   # Painokerroin 0.4–2.0
            "reliable":        bool     # Onko dataa tarpeeksi (>= min_resolved)
        }
    """
    # Vaihe 1: Ryhmittele kaupat markkinoittain
    market_positions = _group_trades_by_market(trade_history)

    if not market_positions:
        return _default_score(wallet_address)

    # Vaihe 2: Hae tulokset ja laske ROI
    correct       = 0
    checked       = 0
    total_roi_sum = 0.0
    weighted_roi_sum   = 0.0
    total_usdc_checked = 0.0

    markets_to_check = list(market_positions.items())[:max_markets]

    for condition_id, outcome_sizes in markets_to_check:
        winner = _get_winning_outcome(condition_id)

        # BUG #3 KORJAUS: None = ei tietoa, ei häviö — ohita kokonaan
        if winner is None:
            continue

        roi, winning_usdc, total_usdc = _calculate_market_roi(outcome_sizes, winner)
        checked += 1
        total_roi_sum      += roi
        weighted_roi_sum   += roi * total_usdc
        total_usdc_checked += total_usdc

        if roi > 0:
            correct += 1

        log.debug(
            f"  {condition_id[:16]} → winner={winner} "
            f"roi={roi:+.1%} winning={winning_usdc:.0f}/{total_usdc:.0f} USDC"
        )

    if checked < min_resolved:
        return _default_score(wallet_address, checked, correct)

    avg_roi      = total_roi_sum / checked
    weighted_roi = weighted_roi_sum / total_usdc_checked if total_usdc_checked > 0 else 0.0
    win_rate     = correct / checked
    weight       = _roi_to_weight(weighted_roi)  # Käytä volyymipainotettua ROI:ta

    result = {
        "address":        wallet_address,
        "win_rate":       round(win_rate, 3),
        "avg_roi":        round(avg_roi, 4),
        "weighted_roi":   round(weighted_roi, 4),
        "resolved_count": checked,
        "correct_count":  correct,
        "total_usdc":     round(total_usdc_checked, 2),
        "weight":         weight,
        "reliable":       True
    }

    log.debug(
        f"Score {wallet_address[:10]}: "
        f"wr={win_rate:.0%} avg_roi={avg_roi:+.1%} "
        f"w_roi={weighted_roi:+.1%} weight={weight} "
        f"({checked} markkinaa, {total_usdc_checked:.0f} USDC)"
    )
    return result


# ===========================================================================
# Apufunktiot
# ===========================================================================

def _roi_to_weight(weighted_roi: float) -> float:
    """
    Muuntaa volyymipainotetun ROI:n painokertoimeksi.

    BUG #2 KORJAUS: Win rate → ROI-pohjainen painotus.
    Huomioi sekä voittojen suuruuden että kauppakoot.
    """
    if weighted_roi >= 0.20:
        return 2.0
    elif weighted_roi >= 0.10:
        return 1.5
    elif weighted_roi >= 0.00:
        return 1.0
    elif weighted_roi >= -0.10:
        return 0.7
    else:
        return 0.4


def _default_score(address: str, checked: int = 0, correct: int = 0) -> Dict:
    """Neutraali paino kun dataa ei ole tarpeeksi."""
    return {
        "address":        address,
        "win_rate":       0.5,
        "avg_roi":        0.0,
        "weighted_roi":   0.0,
        "resolved_count": checked,
        "correct_count":  correct,
        "total_usdc":     0.0,
        "weight":         1.0,
        "reliable":       False
    }


# ===========================================================================
# Batch-prosessointi
# ===========================================================================

def score_wallets_batch(
    qualified_wallets: List[Dict],
    history_cache:     Dict[str, List[Dict]]
) -> Dict[str, Dict]:
    """
    Laskee wallet scoren kaikille kvalifioituneille lompakoille.
    Käyttää jo haettua historiaa — ei tee uusia API-kutsuja historialle.

    Returns:
        Dict[address -> score_dict]
    """
    scores     = {}
    high_scores = []
    low_scores  = []
    no_data     = 0

    for wallet in qualified_wallets:
        addr    = wallet["address"]
        history = history_cache.get(addr.lower(), [])

        score = calculate_wallet_score(addr, history)
        scores[addr] = score

        if not score["reliable"]:
            no_data += 1
        elif score["weight"] >= 1.5:
            high_scores.append(
                f"{addr[:10]} w={score['weight']} "
                f"roi={score['weighted_roi']:+.0%} "
                f"({score['resolved_count']} mkts)"
            )
        elif score["weight"] <= 0.7:
            low_scores.append(
                f"{addr[:10]} w={score['weight']} "
                f"roi={score['weighted_roi']:+.0%} "
                f"({score['resolved_count']} mkts)"
            )

    log.info(
        f"Wallet scoring valmis: "
        f"{len(high_scores)} korkea | "
        f"{len(low_scores)} matala | "
        f"{no_data} ei dataa"
    )
    if high_scores:
        log.info(f"🌟 TOP lompakot: {' | '.join(high_scores[:5])}")
    if low_scores:
        log.info(f"⬇️  HEIKOT lompakot: {' | '.join(low_scores[:3])}")

    return scores