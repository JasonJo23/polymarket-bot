#!/usr/bin/env python3
"""
=============================================================================
ledger.py - Persistent per-trade ledger
=============================================================================
Appends every BUY and SELL to trades.csv so the bot's true, cumulative
realized PnL is stored permanently (daily_metrics.json resets each day and
resolved_positions.json only captures market-resolution closes, so neither
shows the real bottom line).

One row per fill. Cumulative PnL is computed downstream (export_csv / Excel).
Writing is fully wrapped in try/except so a ledger problem can NEVER affect
trading.
=============================================================================
"""

import csv
import os
import threading
from datetime import datetime, timezone

_LEDGER_FILE = os.getenv("TRADES_LEDGER_FILE", "trades.csv")
_lock = threading.Lock()
_HEADERS = [
    "timestamp", "action", "market_type", "question", "outcome",
    "tokens", "price", "usdc", "cost", "pnl",
]


def record_trade(action, usdc=0.0, cost=0.0, pnl=0.0, tokens=0.0, price=0.0,
                 question="", outcome="", market_type=""):
    """Append one trade row. Never raises."""
    try:
        def _f(v):
            try:
                return round(float(v or 0.0), 6)
            except (TypeError, ValueError):
                return 0.0
        row = [
            datetime.now(timezone.utc).isoformat(),
            str(action or ""),
            str(market_type or ""),
            str(question or "")[:80],
            str(outcome or ""),
            _f(tokens), _f(price), _f(usdc), _f(cost), _f(pnl),
        ]
        with _lock:
            need_header = (not os.path.exists(_LEDGER_FILE)
                           or os.path.getsize(_LEDGER_FILE) == 0)
            with open(_LEDGER_FILE, "a", encoding="utf-8", newline="") as f:
                w = csv.writer(f)
                if need_header:
                    w.writerow(_HEADERS)
                w.writerow(row)
    except Exception:
        pass
