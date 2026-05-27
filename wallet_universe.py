"""
Persistent wallet universe for Polymarket Scout.

This keeps useful wallets across cycles so discovery is not limited to the
current market holders. The file is runtime state and intentionally ignored by
Git.
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Dict, List
from state_store import read_json, write_json

log = logging.getLogger("Scout.WalletUniverse")

UNIVERSE_FILE = "known_wallets.json"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load() -> Dict[str, Dict]:
    try:
        with open(UNIVERSE_FILE, "r") as f:
            data = json.load(f)
        wallets = data.get("wallets", {}) if isinstance(data, dict) else {}
        return wallets if isinstance(wallets, dict) else {}
    except FileNotFoundError:
        return {}
    except Exception as e:
        log.debug(f"Known wallet lataus epäonnistui: {e}")
        return {}


def _save(wallets: Dict[str, Dict]):
    try:
        max_wallets = int(os.getenv("KNOWN_WALLET_MAX_STORED", 1000))
        ranked = sorted(
            wallets.items(),
            key=lambda item: (
                float(item[1].get("weight", 0.0) or 0.0),
                int(item[1].get("trades_14d", 0) or 0),
                item[1].get("last_seen", ""),
            ),
            reverse=True,
        )[:max_wallets]
        with open(UNIVERSE_FILE, "w") as f:
            json.dump({"wallets": dict(ranked), "updated_at": _now()}, f, indent=2)
    except Exception as e:
        log.debug(f"Known wallet tallennus epäonnistui: {e}")


def _load() -> Dict[str, Dict]:
    data = read_json(UNIVERSE_FILE, {"wallets": {}})
    wallets = data.get("wallets", {}) if isinstance(data, dict) else {}
    return wallets if isinstance(wallets, dict) else {}


def _save(wallets: Dict[str, Dict]):
    try:
        max_wallets = int(os.getenv("KNOWN_WALLET_MAX_STORED", 1000))
        ranked = sorted(
            wallets.items(),
            key=lambda item: (
                float(item[1].get("weight", 0.0) or 0.0),
                int(item[1].get("trades_14d", 0) or 0),
                item[1].get("last_seen", ""),
            ),
            reverse=True,
        )[:max_wallets]
        write_json(UNIVERSE_FILE, {"wallets": dict(ranked), "updated_at": _now()}, indent=2)
    except Exception as e:
        log.debug(f"Known wallet tallennus epäonnistui: {e}")


def add_discovered_wallets(wallets: List[str], source: str):
    """Stores raw discovered wallets before score data is available."""
    if not wallets:
        return
    known = _load()
    now = _now()
    for wallet in wallets:
        addr = str(wallet).lower()
        if not (addr.startswith("0x") and len(addr) == 42):
            continue
        entry = known.setdefault(addr, {"address": addr, "first_seen": now})
        entry["last_seen"] = now
        sources = set(entry.get("sources", []))
        sources.add(source)
        entry["sources"] = sorted(sources)
    _save(known)


def update_from_scores(scores: Dict[str, Dict]):
    """Updates the universe after wallet scoring."""
    if not scores:
        return
    known = _load()
    now = _now()
    added_or_updated = 0
    for address, score in scores.items():
        addr = str(address).lower()
        if not (addr.startswith("0x") and len(addr) == 42):
            continue

        weight = float(score.get("weight", 0.7) or 0.7)
        reliable = bool(score.get("reliable", False))
        active_recently = bool(score.get("active_recently", False))
        trades_14d = int(score.get("trades_14d", 0) or 0)

        min_weight = float(os.getenv("KNOWN_WALLET_MIN_WEIGHT", 0.7))
        min_trades = int(os.getenv("KNOWN_WALLET_MIN_TRADES_14D", 5))
        if weight < min_weight and not active_recently:
            continue
        if trades_14d < min_trades and not reliable:
            continue

        entry = known.setdefault(addr, {"address": addr, "first_seen": now})
        entry.update({
            "last_seen": now,
            "weight": weight,
            "reliable": reliable,
            "weighted_roi": float(score.get("weighted_roi", 0.0) or 0.0),
            "resolved_count": int(score.get("resolved_count", 0) or 0),
            "trades_7d": int(score.get("trades_7d", 0) or 0),
            "trades_14d": trades_14d,
            "active_recently": active_recently,
            "category_weights": score.get("category_weights", {}),
        })
        sources = set(entry.get("sources", []))
        sources.add("scored")
        entry["sources"] = sorted(sources)
        added_or_updated += 1

    _save(known)
    log.info(f"Known wallet universe päivitetty: {added_or_updated} walletia")


def get_candidate_wallets(limit: int = None) -> List[str]:
    """Returns active known wallets worth rescanning."""
    known = _load()
    if not known:
        return []

    if limit is None:
        limit = int(os.getenv("KNOWN_WALLET_SCAN_LIMIT", 40))

    min_weight = float(os.getenv("KNOWN_WALLET_SCAN_MIN_WEIGHT", 0.7))
    min_trades = int(os.getenv("KNOWN_WALLET_SCAN_MIN_TRADES_14D", 5))

    candidates = []
    for addr, entry in known.items():
        if not (addr.startswith("0x") and len(addr) == 42):
            continue
        weight = float(entry.get("weight", 0.7) or 0.7)
        trades_14d = int(entry.get("trades_14d", 0) or 0)
        active = bool(entry.get("active_recently", False))
        reliable = bool(entry.get("reliable", False))
        if weight < min_weight:
            continue
        if trades_14d < min_trades and not active:
            continue
        candidates.append((addr, weight, reliable, trades_14d, entry.get("last_seen", "")))

    candidates.sort(key=lambda item: (item[1], item[2], item[3], item[4]), reverse=True)
    result = [addr for addr, *_ in candidates[:limit]]
    if result:
        log.info(f"Known wallet universe: {len(result)} lisäwalletia hakuun")
    return result
