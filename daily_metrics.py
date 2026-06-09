"""Daily live risk and PnL accounting."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Dict, Any

from state_store import read_json, write_json

log = logging.getLogger("Scout.DailyMetrics")

_FILE = "daily_metrics.json"


def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _default() -> Dict[str, Any]:
    return {
        "date": _today(),
        "buy_spend_usdc": 0.0,
        "sell_proceeds_usdc": 0.0,
        "realized_pnl_usdc": 0.0,
        "buy_count": 0,
        "sell_count": 0,
    }


def load_metrics() -> Dict[str, Any]:
    data = read_json(_FILE, _default())
    if not isinstance(data, dict) or data.get("date") != _today():
        return _default()
    base = _default()
    base.update(data)
    return base


def save_metrics(metrics: Dict[str, Any]) -> None:
    metrics["date"] = _today()
    write_json(_FILE, metrics, indent=2)


def record_buy(spend_usdc: float, question: str = "", outcome: str = "",
               market_type: str = "", tokens: float = 0.0, price: float = 0.0) -> Dict[str, Any]:
    metrics = load_metrics()
    spend = round(max(0.0, float(spend_usdc or 0.0)), 2)
    if spend <= 0:
        return metrics
    metrics["buy_spend_usdc"] = round(float(metrics.get("buy_spend_usdc", 0.0)) + spend, 2)
    metrics["buy_count"] = int(metrics.get("buy_count", 0)) + 1
    save_metrics(metrics)
    try:
        from ledger import record_trade
        record_trade("BUY", usdc=spend, cost=spend, pnl=0.0, tokens=tokens, price=price,
                     question=question, outcome=outcome, market_type=market_type)
    except Exception:
        pass
    log.info(
        f"Daily metrics buy: spend={metrics['buy_spend_usdc']:.2f} USDC | "
        f"realized_pnl={float(metrics.get('realized_pnl_usdc', 0.0)):+.2f} USDC"
    )
    return metrics


def record_sell(proceeds_usdc: float, cost_usdc: float, question: str = "",
                outcome: str = "", market_type: str = "", tokens: float = 0.0,
                price: float = 0.0) -> Dict[str, Any]:
    metrics = load_metrics()
    proceeds = round(max(0.0, float(proceeds_usdc or 0.0)), 2)
    cost = round(max(0.0, float(cost_usdc or 0.0)), 2)
    pnl = round(proceeds - cost, 2)
    metrics["sell_proceeds_usdc"] = round(float(metrics.get("sell_proceeds_usdc", 0.0)) + proceeds, 2)
    metrics["realized_pnl_usdc"] = round(float(metrics.get("realized_pnl_usdc", 0.0)) + pnl, 2)
    metrics["sell_count"] = int(metrics.get("sell_count", 0)) + 1
    save_metrics(metrics)
    try:
        from ledger import record_trade
        record_trade("SELL", usdc=proceeds, cost=cost, pnl=pnl, tokens=tokens, price=price,
                     question=question, outcome=outcome, market_type=market_type)
    except Exception:
        pass
    log.info(
        f"Daily metrics sell: proceeds={proceeds:.2f} cost={cost:.2f} "
        f"pnl={pnl:+.2f} | day_pnl={metrics['realized_pnl_usdc']:+.2f} USDC"
    )
    return metrics


def reset_if_new_day() -> Dict[str, Any]:
    metrics = load_metrics()
    save_metrics(metrics)
    return metrics
