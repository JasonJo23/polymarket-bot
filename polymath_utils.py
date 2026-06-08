"""
=============================================================================
polymath_utils.py – Shared helpers (HTTP session + parsing)
=============================================================================
Centralises two things that were previously copy-pasted (and had drifted)
across fetcher / analyzer / wallet_scorer / tracker / position_manager:

  1. A pooled, retrying requests.Session. Previously only fetcher.py pooled
     connections; every other module opened a fresh TCP+TLS connection per
     call. Reusing one session per process removes that handshake overhead.

  2. Robust parsers for timestamp / size / address / outcome. These existed
     in several modules with *different* key lists, so the same trade could be
     parsed differently depending on which module looked at it. One
     implementation here removes that silent drift.

Nothing here changes trading behaviour — it only makes parsing consistent and
networking faster.
=============================================================================
"""

from __future__ import annotations

import os
import re
import threading
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import requests
from requests.adapters import HTTPAdapter

try:  # urllib3 ships with requests; guard just in case
    from urllib3.util.retry import Retry
except Exception:  # pragma: no cover
    Retry = None

_session: Optional[requests.Session] = None
_session_lock = threading.Lock()


def get_session() -> requests.Session:
    """Return a process-wide pooled requests.Session with retry/backoff."""
    global _session
    if _session is not None:
        return _session
    with _session_lock:
        if _session is not None:
            return _session
        pool = int(os.getenv("FETCH_WORKERS", "16")) + 8
        session = requests.Session()
        retries = None
        if Retry is not None:
            retries = Retry(
                total=int(os.getenv("MAX_RETRIES", "3")),
                backoff_factor=0.5,
                status_forcelist=(429, 500, 502, 503, 504),
                allowed_methods=frozenset(["GET", "POST"]),
                raise_on_status=False,
            )
        adapter = HTTPAdapter(
            pool_connections=pool,
            pool_maxsize=pool,
            max_retries=retries,
        )
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        session.headers.update({
            "Accept": "application/json",
            "User-Agent": "PolymarketScout/7.0",
        })
        _session = session
        return _session


# ---------------------------------------------------------------------------
# Parsing helpers (superset of the per-module versions that existed before)
# ---------------------------------------------------------------------------

_TS_KEYS = ("timestamp", "createdAt", "created_at", "time")
_SIZE_KEYS = ("usdcSize", "size", "amount")
_ADDR_KEYS = ("proxyWallet", "proxy_wallet", "_wallet_address", "maker", "user")


def parse_timestamp(trade: Dict[str, Any]) -> Optional[datetime]:
    raw = None
    for key in _TS_KEYS:
        raw = trade.get(key)
        if raw is not None:
            break
    if raw is None:
        return None
    try:
        if isinstance(raw, (int, float)):
            v = raw / 1000 if raw > 1e10 else raw
            return datetime.fromtimestamp(v, tz=timezone.utc)
        if isinstance(raw, str):
            return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except (ValueError, OSError):
        pass
    return None


def parse_size_usdc(trade: Dict[str, Any]) -> float:
    for key in _SIZE_KEYS:
        raw = trade.get(key)
        if raw is not None:
            try:
                v = float(raw)
                if v > 0:
                    return v
            except (TypeError, ValueError):
                pass
    return 0.0


def extract_address(trade: Dict[str, Any]) -> Optional[str]:
    for key in _ADDR_KEYS:
        val = trade.get(key)
        if val and isinstance(val, str) and val.startswith("0x") and len(val) == 42:
            return val.lower()
    return None


def normalize_outcome(value: Any) -> str:
    return re.sub(r"[^A-Z0-9]+", " ", str(value or "").upper()).strip()
