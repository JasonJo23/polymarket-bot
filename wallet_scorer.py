"""
=============================================================================
wallet_scorer.py – WalletScorer  (v4.0 – pysyvä levy-cache)
=============================================================================
KORJAUKSET v3.0 → v4.0:

  PERF #1  Scoring 5-9 min per sykli koska CLOB /markets haetaan aina
           uudelleen vaikka suljettu tulos ei muutu koskaan
           → KORJAUS: market_cache.json levylle — haetaan kerran, pysyy

  BUG #1   accepting_orders oletus True → kaikki tulkittiin avoimiksi (v3:ssa korjattu)
  BUG #2   conditionId-kenttä eri nimillä → kokeillaan useita (v3:ssa korjattu)
  BUG #3   ROI-kynnys 20% epärealistinen → laskettu 8%:iin (v3:ssa korjattu)
  BUG #4   Market maker -suodatin puuttui (v3:ssa korjattu)
=============================================================================
"""

import os
import json
import logging
import requests
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
from collections import defaultdict

log = logging.getLogger("Scout.WalletScorer")

CLOB_BASE   = "https://clob.polymarket.com"
GAMMA_BASE  = "https://gamma-api.polymarket.com"
_CACHE_FILE = "market_cache.json"
_SCORE_CACHE_FILE = "wallet_score_cache.json"

_market_result_cache: Dict[str, Optional[str]] = {}
_cache_loaded = False
_wallet_score_cache: Dict[str, Dict] = {}
_score_cache_loaded = False


def _load_cache_from_disk():
    global _cache_loaded
    if _cache_loaded:
        return
    _cache_loaded = True
    try:
        with open(_CACHE_FILE, "r") as f:
            data = json.load(f)
        _market_result_cache.update(data)
        resolved = sum(1 for v in data.values() if v is not None)
        log.info(f"Market cache ladattu: {len(data)} merkintää ({resolved} ratkaistu)")
    except FileNotFoundError:
        log.debug("market_cache.json ei löydy — luodaan uusi")
    except Exception as e:
        log.warning(f"Cache lataus epäonnistui: {e}")


def _save_cache_to_disk():
    try:
        to_save = {k: v for k, v in _market_result_cache.items() if v is not None}
        with open(_CACHE_FILE, "w") as f:
            json.dump(to_save, f)
    except Exception as e:
        log.debug(f"Cache tallennus epäonnistui: {e}")


def _get_winning_outcome(condition_id: str) -> Optional[str]:
    _load_cache_from_disk()
    if condition_id in _market_result_cache:
        return _market_result_cache[condition_id]
    winner = _try_clob(condition_id)
    if winner is not None:
        _market_result_cache[condition_id] = winner
        return winner  # Tallennetaan levylle batch_save:ssa
    winner = _try_gamma(condition_id)
    if winner is not None:
        _market_result_cache[condition_id] = winner
    return winner


def _batch_save_cache():
    """Tallentaa cachen levylle kerran kaikkien lompakkojen jälkeen."""
    _save_cache_to_disk()


def _load_score_cache_from_disk():
    global _score_cache_loaded
    if _score_cache_loaded:
        return
    _score_cache_loaded = True
    try:
        with open(_SCORE_CACHE_FILE, "r") as f:
            data = json.load(f)
        if isinstance(data, dict):
            _wallet_score_cache.update(data)
        log.info(f"Wallet score cache ladattu: {len(_wallet_score_cache)} lompakkoa")
    except FileNotFoundError:
        pass
    except Exception as e:
        log.debug(f"Wallet score cache lataus epäonnistui: {e}")


def _save_score_cache_to_disk():
    try:
        with open(_SCORE_CACHE_FILE, "w") as f:
            json.dump(_wallet_score_cache, f)
    except Exception as e:
        log.debug(f"Wallet score cache tallennus epäonnistui: {e}")


def _history_fingerprint(trade_history: List[Dict]) -> Dict:
    latest = ""
    for trade in trade_history:
        ts = _parse_timestamp(trade)
        if ts is None:
            continue
        iso = ts.isoformat()
        if iso > latest:
            latest = iso
    return {
        "count": len(trade_history),
        "latest": latest,
    }


def _try_clob(condition_id: str) -> Optional[str]:
    try:
        r = requests.get(f"{CLOB_BASE}/markets/{condition_id}", timeout=5)
        if r.status_code != 200:
            return None
        data = r.json()
        if data.get("accepting_orders", False):
            return None
        for token in data.get("tokens", []):
            if float(token.get("price", 0)) >= 0.99:
                winner = str(token.get("outcome", "")).upper().strip()
                if winner:
                    return winner
        return None
    except Exception as e:
        log.debug(f"CLOB haku epäonnistui ({condition_id[:16]}): {e}")
        return None


def _try_gamma(condition_id: str) -> Optional[str]:
    try:
        r = requests.get(
            f"{GAMMA_BASE}/markets",
            params={"condition_id": condition_id, "closed": "true"},
            timeout=5
        )
        if r.status_code != 200:
            return None
        data    = r.json()
        markets = data if isinstance(data, list) else data.get("markets", [])
        if not markets:
            return None
        m = markets[0]
        outcome_prices = m.get("outcomePrices", m.get("outcome_prices", {}))
        if isinstance(outcome_prices, str):
            try:
                outcome_prices = json.loads(outcome_prices)
            except Exception:
                return None
        outcomes = m.get("outcomes", [])
        if isinstance(outcome_prices, list) and outcomes:
            for i, price in enumerate(outcome_prices):
                try:
                    if float(price) >= 0.99 and i < len(outcomes):
                        return str(outcomes[i]).upper().strip()
                except (ValueError, TypeError):
                    pass
        elif isinstance(outcome_prices, dict):
            for outcome, price in outcome_prices.items():
                try:
                    if float(price) >= 0.99:
                        return str(outcome).upper().strip()
                except (ValueError, TypeError):
                    pass
        return None
    except Exception as e:
        log.debug(f"Gamma haku epäonnistui ({condition_id[:16]}): {e}")
        return None


def _is_market_maker(outcome_sizes: Dict[str, float]) -> bool:
    if len(outcome_sizes) < 2:
        return False
    total = sum(outcome_sizes.values())
    if total <= 0:
        return False
    return min(outcome_sizes.values()) / total > 0.30


def _group_trades_by_market(trade_history: List[Dict]) -> Dict[str, Dict[str, float]]:
    market_positions: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for trade in trade_history:
        if str(trade.get("side", "")).upper() != "BUY":
            continue
        condition_id = (
            trade.get("conditionId") or
            trade.get("condition_id") or
            trade.get("market") or ""
        )
        condition_id = str(condition_id).strip()
        if not condition_id:
            continue
        outcome = str(trade.get("outcome", "")).upper().strip()
        if not outcome:
            continue
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


def _market_categories(trade_history: List[Dict]) -> Dict[str, str]:
    categories = {}
    for trade in trade_history:
        condition_id = (
            trade.get("conditionId") or
            trade.get("condition_id") or
            trade.get("market") or ""
        )
        condition_id = str(condition_id).strip()
        if not condition_id or condition_id in categories:
            continue
        categories[condition_id] = _classify_market_text(_trade_market_text(trade))
    return categories


def _calculate_market_roi(outcome_sizes: Dict[str, float], winning_outcome: str) -> Tuple[float, float, float]:
    total_usdc   = sum(outcome_sizes.values())
    winning_usdc = outcome_sizes.get(winning_outcome, 0.0)
    if total_usdc <= 0:
        return 0.0, 0.0, 0.0
    return round((winning_usdc - total_usdc) / total_usdc, 4), winning_usdc, total_usdc


def _roi_to_weight(weighted_roi: float) -> float:
    if weighted_roi >= 0.08:
        return 2.0
    elif weighted_roi >= 0.03:
        return 1.5
    elif weighted_roi >= 0.00:
        return 1.0
    elif weighted_roi >= -0.05:
        return 0.8
    else:
        return 0.4


def _classify_market_text(text: str) -> str:
    q = (text or "").lower()
    esports = any(k in q for k in [
        "lol:", "dota", "cs2", "csgo", "counter-strike", "valorant",
        "lck", "lec", "lpl", "vct", "iem", "pgl", "blast", "dreamleague",
    ])
    if esports:
        if any(k in q for k in ["game ", "map "]):
            return "esports_map"
        return "esports_match"
    if any(k in q for k in [
        "vs.", "vs ", "winner", "match", "series", "spread", "o/u",
        "nba", "nfl", "nhl", "mlb", "atp", "wta", "ufc", "fc ",
    ]):
        return "sports"
    if any(k in q for k in [
        "iran", "trump", "biden", "election", "fed", "btc", "eth",
        "bitcoin", "ethereum", "tariff", "ceasefire", "invade",
        "uranium", "peace deal",
    ]):
        return "macro"
    return "general"


def _trade_market_text(trade: Dict) -> str:
    for key in ("title", "question", "marketName", "market_name", "slug"):
        value = trade.get(key)
        if value:
            return str(value)
    return ""


def _parse_timestamp(trade: Dict) -> Optional[datetime]:
    raw = trade.get("timestamp")
    if raw is None:
        return None
    try:
        if isinstance(raw, (int, float)):
            ts = raw / 1000 if raw > 1e10 else raw
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        if isinstance(raw, str):
            return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except (ValueError, OSError):
        pass
    return None


def _activity_stats(trade_history: List[Dict]) -> Dict:
    now = datetime.now(timezone.utc)
    cutoff_7d = now - timedelta(days=7)
    cutoff_14d = now - timedelta(days=14)
    trades_7d = trades_14d = 0
    for trade in trade_history:
        ts = _parse_timestamp(trade)
        if ts is None:
            continue
        if ts >= cutoff_14d:
            trades_14d += 1
        if ts >= cutoff_7d:
            trades_7d += 1
    return {
        "trades_7d": trades_7d,
        "trades_14d": trades_14d,
        "active_recently": trades_14d >= int(os.getenv("MIN_TRADES_14D_FOR_ACTIVE", 5)),
    }


def _category_weights(category_stats: Dict[str, Dict]) -> Dict[str, Dict]:
    result = {}
    for category in ("macro", "sports", "esports_match", "esports_map", "general"):
        stats = category_stats.get(category, {})
        total_usdc = float(stats.get("total_usdc", 0.0))
        checked = int(stats.get("checked", 0))
        correct = int(stats.get("correct", 0))
        weighted_roi_sum = float(stats.get("weighted_roi_sum", 0.0))
        reliable = checked >= int(os.getenv("MIN_CATEGORY_RESOLVED", 3))
        weighted_roi = weighted_roi_sum / total_usdc if total_usdc > 0 else 0.0
        weight = _roi_to_weight(weighted_roi) if reliable else float(os.getenv("UNKNOWN_CATEGORY_WEIGHT", 0.7))
        result[category] = {
            "weight": round(weight, 3),
            "weighted_roi": round(weighted_roi, 4),
            "resolved_count": checked,
            "win_rate": round(correct / checked, 3) if checked else 0.5,
            "reliable": reliable,
        }
    return result


def _default_score(address: str, checked: int = 0, correct: int = 0) -> Dict:
    unknown_weight = float(os.getenv("UNKNOWN_WALLET_WEIGHT", 0.7))
    category_weights = {
        category: {
            "weight": unknown_weight,
            "weighted_roi": 0.0,
            "resolved_count": 0,
            "win_rate": 0.5,
            "reliable": False,
        }
        for category in ("macro", "sports", "esports_match", "esports_map", "general")
    }
    return {
        "address": address, "win_rate": 0.5, "avg_roi": 0.0,
        "weighted_roi": 0.0, "resolved_count": checked, "correct_count": correct,
        "total_usdc": 0.0, "weight": unknown_weight, "reliable": False, "mm_skipped": 0,
        "category_weights": category_weights,
        **_activity_stats([]),
    }


def _prefetch_outcomes(condition_ids: list):
    """
    Hakee puuttuvat markkinatulokset rinnakkain ThreadPoolExecutorilla.
    Täyttää cachen ennen scoring-looppia — eliminoi peräkkäiset API-kutsut.
    """
    import os as _os
    from concurrent.futures import ThreadPoolExecutor, as_completed

    missing = [cid for cid in condition_ids if cid not in _market_result_cache]
    if not missing:
        return

    max_workers = int(_os.getenv("SCORING_WORKERS", 10))
    log.debug(f"Prefetch {len(missing)} markkinatulosta ({max_workers} rinnakkain)...")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_try_clob, cid): cid for cid in missing}
        for future in as_completed(futures):
            cid = futures[future]
            try:
                winner = future.result()
                if winner is not None:
                    _market_result_cache[cid] = winner
                # Jos None → ei cacheta, yritetään Gammalla myöhemmin
            except Exception:
                pass


def calculate_wallet_score(
    wallet_address: str,
    trade_history:  List[Dict],
    min_resolved:   int = 5,
    max_markets:    int = 50
) -> Dict:
    _load_cache_from_disk()
    market_positions = _group_trades_by_market(trade_history)
    activity = _activity_stats(trade_history)
    if not market_positions:
        score = _default_score(wallet_address)
        score.update(activity)
        return score
    market_categories = _market_categories(trade_history)

    # Prefetch kaikki puuttuvat tulokset rinnakkain
    all_cids = [cid for cid, sizes in list(market_positions.items())[:max_markets]
                if not _is_market_maker(sizes)]
    _prefetch_outcomes(all_cids)

    correct = checked = mm_skipped = 0
    total_roi_sum = weighted_roi_sum = total_usdc_checked = 0.0
    category_stats: Dict[str, Dict] = defaultdict(lambda: {
        "checked": 0,
        "correct": 0,
        "total_usdc": 0.0,
        "weighted_roi_sum": 0.0,
    })

    for condition_id, outcome_sizes in list(market_positions.items())[:max_markets]:
        if _is_market_maker(outcome_sizes):
            mm_skipped += 1
            continue
        winner = _get_winning_outcome(condition_id)
        if winner is None:
            continue
        roi, winning_usdc, total_usdc = _calculate_market_roi(outcome_sizes, winner)
        checked            += 1
        total_roi_sum      += roi
        weighted_roi_sum   += roi * total_usdc
        total_usdc_checked += total_usdc
        if roi > 0:
            correct += 1
        category = market_categories.get(condition_id, "general")
        category_stats[category]["checked"] += 1
        category_stats[category]["total_usdc"] += total_usdc
        category_stats[category]["weighted_roi_sum"] += roi * total_usdc
        if roi > 0:
            category_stats[category]["correct"] += 1

    if checked < min_resolved:
        score = _default_score(wallet_address, checked, correct)
        score.update(activity)
        score["category_weights"] = _category_weights(category_stats)
        return score

    avg_roi      = total_roi_sum / checked
    weighted_roi = weighted_roi_sum / total_usdc_checked if total_usdc_checked > 0 else 0.0
    overall_weight = _roi_to_weight(weighted_roi)
    if not activity["active_recently"]:
        overall_weight = min(overall_weight, float(os.getenv("INACTIVE_WALLET_MAX_WEIGHT", 0.8)))

    return {
        "address":        wallet_address,
        "win_rate":       round(correct / checked, 3),
        "avg_roi":        round(avg_roi, 4),
        "weighted_roi":   round(weighted_roi, 4),
        "resolved_count": checked,
        "correct_count":  correct,
        "total_usdc":     round(total_usdc_checked, 2),
        "weight":         overall_weight,
        "reliable":       True,
        "mm_skipped":     mm_skipped,
        "category_weights": _category_weights(category_stats),
        **activity,
    }


def score_wallets_batch(
    qualified_wallets: List[Dict],
    history_cache:     Dict[str, List[Dict]]
) -> Dict[str, Dict]:
    _load_cache_from_disk()
    _load_score_cache_from_disk()
    scores = {}
    high_scores = []
    low_scores  = []
    mm_total = no_data = cache_hits = 0

    for wallet in qualified_wallets:
        addr    = wallet["address"]
        history = history_cache.get(addr.lower(), [])
        fingerprint = _history_fingerprint(history)
        cached = _wallet_score_cache.get(addr.lower())
        if cached and cached.get("fingerprint") == fingerprint:
            score = cached.get("score", {})
            cache_hits += 1
        else:
            score = calculate_wallet_score(addr, history)
            _wallet_score_cache[addr.lower()] = {
                "fingerprint": fingerprint,
                "score": score,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
        scores[addr] = score
        mm_total += score.get("mm_skipped", 0)
        if not score["reliable"]:
            no_data += 1
        elif score["weight"] >= 1.5:
            high_scores.append(f"{addr[:10]} w={score['weight']} roi={score['weighted_roi']:+.0%} ({score['resolved_count']} mkts)")
        elif score["weight"] <= 0.8:
            low_scores.append(f"{addr[:10]} w={score['weight']} roi={score['weighted_roi']:+.0%} ({score['resolved_count']} mkts)")

    # Tallenna cache kerran kaikkien lompakkojen jälkeen
    _batch_save_cache()
    _save_score_cache_to_disk()

    log.info(f"Wallet scoring valmis: {len(high_scores)} korkea | {len(low_scores)} matala | {no_data} ei dataa | {mm_total} MM-kauppaa suodatettu | cache hit {cache_hits}")
    if high_scores:
        log.info(f"🌟 TOP lompakot: {' | '.join(high_scores[:5])}")
    if low_scores:
        log.info(f"⬇️  HEIKOT: {' | '.join(low_scores[:3])}")
    return scores
