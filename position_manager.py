"""
=============================================================================
position_manager.py – PositionManager  (v2.0 – py_clob_client_v2)
=============================================================================
Korjaukset v2.0:
  - Myynti käyttää py_clob_client_v2 (ei vanhaa polymarket_apis)
  - 425 Too Early käsitellään siististi
  - Myynti GTC limit-orderina 2% alle nykyhinnan
=============================================================================
"""

import os
import json
import logging
import requests
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple
from state_store import read_json, write_json

log = logging.getLogger("Scout.PositionManager")

try:
    from notifier import notifier
except Exception:
    notifier = None

CLOB_BASE = "https://clob.polymarket.com"

SPORTS_KEYWORDS = [
    "vs.", "vs ", "game 1", "game 2", "game 3", "bo3", "bo5",
    "winner", "match", "series",
    "nba", "nfl", "nhl", "mlb", "wnba",
    "lakers", "celtics", "knicks", "hawks", "bulls", "heat",
    "thunder", "pistons", "magic", "rockets", "spurs", "raptors",
    "cavaliers", "76ers", "trail blazers", "nuggets", "timberwolves",
    "bruins", "sabres", "lightning", "oilers", "ducks", "avalanche",
    "kings", "canadiens", "flyers", "golden knights", "utah",
    "angels", "royals", "red sox", "orioles", "yankees",
    "lol:", "dota", "csgo", "valorant", "counter-strike",
    "fc ", "win on", "epl", "bundesliga", "serie a", "la liga",
    "premier league", "champions league", "barcelona", "madrid",
    "manchester", "arsenal", "liverpool", "chelsea", "tottenham",
    "juventus", "milan", "inter", "napoli", "marseille", "lille",
    "ufc", "mma", "fight night",
    "innings", "o/u", "over", "under", "spread",
]


def _is_sports(question: str) -> bool:
    q = question.lower()
    return any(kw in q for kw in SPORTS_KEYWORDS)


def _is_esports(question: str) -> bool:
    q = question.lower()
    return any(kw in q for kw in [
        "lol:", "dota", "csgo", "cs2", "valorant", "counter-strike",
        "lck", "lec", "lpl", "vct", "iem", "pgl", "blast", "dreamleague",
    ])


def _is_esports_map(question: str) -> bool:
    q = question.lower()
    return _is_esports(question) and any(kw in q for kw in ["game ", "map "])


def _get_current_price(token_id: str) -> Optional[float]:
    try:
        r = requests.get(
            f"{CLOB_BASE}/price",
            params={"token_id": token_id, "side": "SELL"},
            timeout=5
        )
        if r.status_code == 200:
            return float(r.json().get("price", 0))
    except Exception as e:
        log.debug(f"Hinnan haku epäonnistui: {e}")
    return None


def _get_hours_until_close(end_date_str: str) -> float:
    try:
        if not end_date_str or end_date_str == "?":
            return 24.0
        end_dt = datetime.fromisoformat(
            end_date_str.replace("Z", "+00:00").replace(" ", "T")
        )
        if not end_dt.tzinfo:
            end_dt = end_dt.replace(tzinfo=timezone.utc)
        delta = end_dt - datetime.now(timezone.utc)
        return max(0, delta.total_seconds() / 3600)
    except Exception:
        return 24.0


def _sell_position_v2(
    token_id: str,
    amount: float,
    current_price: float,
    reason: str
) -> Dict[str, Any]:
    """
    Myy positio py_clob_client_v2:lla.
    Käyttää GTC limit-orderia 2% alle nykyhinnan.
    """
    try:
        from py_clob_client_v2 import (
            ClobClient, ApiCreds, OrderArgs,
            OrderType, Side, PartialCreateOrderOptions
        )

        sell_price = round(max(0.01, current_price * 0.98), 3)

        creds = ApiCreds(
            api_key=os.getenv("CLOB_API_KEY"),
            api_secret=os.getenv("CLOB_API_SECRET"),
            api_passphrase=os.getenv("CLOB_PASSPHRASE")
        )
        client = ClobClient(
            host=CLOB_BASE,
            chain_id=137,
            key=os.getenv("PRIVATE_KEY"),
            creds=creds,
            signature_type=2,
            funder=os.getenv("PROXY_WALLET_ADDRESS")
        )

        actual_balance = _get_token_balance_v2(client, token_id)
        if actual_balance is not None:
            if actual_balance <= 0.0001:
                log.warning(f"Positio puuttuu walletista — poistetaan seurannasta: {token_id[:16]}")
                return {"closed": True, "status": "missing_balance", "resp": None}
            if actual_balance < amount:
                log.warning(
                    f"Paikallinen positio suurempi kuin wallet-saldo: "
                    f"{amount:.4f} → {actual_balance:.4f}"
                )
                amount = actual_balance

        # Hae tick size
        tick_size = "0.01"
        try:
            r = requests.get(
                f"{CLOB_BASE}/tick-size",
                params={"token_id": token_id},
                timeout=5
            )
            if r.status_code == 200:
                tick_size = str(r.json().get("minimum_tick_size", "0.01"))
        except Exception:
            pass

        options = PartialCreateOrderOptions(tick_size=tick_size)

        resp = client.create_and_post_order(
            order_args=OrderArgs(
                token_id=token_id,
                price=sell_price,
                size=amount,
                side=Side.SELL,
            ),
            options=options,
            order_type=OrderType.GTC,
        )

        if resp:
            log.info(f"✅ Myynti tehty @ {sell_price} | Syy: {reason} | {resp}")
            status = str(resp.get("status", "") if isinstance(resp, dict) else getattr(resp, "status", "")).lower()
            filled_usdc, filled_tokens = _extract_sell_fill(resp)
            if status in ("matched", "filled", "complete", "completed") and filled_tokens > 0:
                log.info(
                    f"Myynti fill vahvistettu: {filled_usdc:.2f} USDC / "
                    f"{filled_tokens:.4f} tokens | status={status}"
                )
                return {
                    "closed": True,
                    "status": status,
                    "resp": resp,
                    "filled_usdc": filled_usdc,
                    "filled_tokens": filled_tokens,
                }

            log.warning(f"Myyntiorderi jäi auki ({status or 'unknown'}) — positiota ei poisteta vielä")
            return {"closed": False, "status": status, "resp": resp}
        else:
            log.warning(f"⚠️ Myynti epäonnistui: {resp}")
            return {"closed": False, "status": "failed", "resp": resp}

    except Exception as e:
        err = str(e)
        if "425" in err or "Too Early" in err or "service not ready" in err:
            log.warning(f"Polymarket ei valmis myyntiin (425) — yritetään myöhemmin.")
        else:
            log.error(f"Myyntivirhe: {e}")
        return {"closed": False, "status": "error", "resp": None}


def _get_token_balance_v2(client, token_id: str) -> Optional[float]:
    """Hakee todellisen conditional-token saldon ennen myyntiä."""
    try:
        from py_clob_client_v2.clob_types import AssetType, BalanceAllowanceParams

        params = BalanceAllowanceParams(
            asset_type=AssetType.CONDITIONAL,
            token_id=token_id,
        )
        resp = client.get_balance_allowance(params)
        raw = float(resp.get("balance", 0) or 0)
        if raw <= 0:
            client.update_balance_allowance(params)
            resp = client.get_balance_allowance(params)
            raw = float(resp.get("balance", 0) or 0)
        return raw / 1e6
    except Exception as e:
        log.debug(f"Token-saldon haku epäonnistui: {e}")
        return None


def _extract_sell_fill(resp: Any) -> Tuple[float, float]:
    """Palauttaa myynnin USDC- ja token-fill maarat CLOB-responsesta."""
    if not isinstance(resp, dict):
        return 0.0, 0.0

    def _num(key: str) -> float:
        try:
            return float(resp.get(key, 0) or 0)
        except (TypeError, ValueError):
            return 0.0

    taking = _num("takingAmount")
    making = _num("makingAmount")
    if taking > 0 and making > 0:
        return taking, making

    return 0.0, 0.0


def _evaluate_sports_position(position: Dict) -> Tuple[bool, str]:
    """
    Urheilu: Hold to resolution — Polymarket maksaa 1.00 automaattisesti.
    Myydään vain kahdessa tilanteessa:
      1. Hinta alle 0.10 → peli selvästi hävitty, pelastetaan loput
      2. Hinta yli 0.92 → peli selvästi voitettu, lukitaan voitto
    Ei time exittejä, ei TP% laskentaa — pidetään loppuun.
    """
    current_price = float(position.get("current_price", 0.5))
    profit_lock = float(os.getenv("SPORTS_PROFIT_LOCK_PRICE", 0.92))
    stop_price = float(os.getenv("SPORTS_STOP_PRICE", 0.10))

    if current_price >= profit_lock:
        return True, f"Urheilu voitto lukkoon ({current_price:.2f} >= {profit_lock:.2f})"
    if current_price <= stop_price:
        return True, f"Urheilu stop ({current_price:.2f} <= {stop_price:.2f})"

    return False, ""


def _evaluate_esports_position(position: Dict) -> Tuple[bool, str]:
    buy_price = float(position.get("buy_price", 0.5))
    current_price = float(position.get("current_price", 0.5))
    hours_left = float(position.get("hours_left", 24))
    is_map = bool(position.get("is_esports_map", False))
    pnl_pct = (current_price - buy_price) / buy_price if buy_price > 0 else 0

    profit_lock = float(os.getenv("ESPORTS_PROFIT_LOCK_PRICE", 0.88 if is_map else 0.90))
    stop_loss = float(os.getenv("ESPORTS_MAP_STOP_LOSS", -0.30 if is_map else -0.35))
    take_profit = float(os.getenv("ESPORTS_MAP_TAKE_PROFIT", 0.30 if is_map else 0.40))

    if current_price >= profit_lock:
        return True, f"Esports voitto lukkoon ({current_price:.2f} >= {profit_lock:.2f})"
    if pnl_pct <= stop_loss:
        return True, f"Esports SL {stop_loss:.0%} ({pnl_pct:+.1%})"
    if pnl_pct >= take_profit and hours_left <= 6:
        return True, f"Esports TP {take_profit:.0%} ({pnl_pct:+.1%}, {hours_left:.1f}h)"
    if is_map and hours_left <= 1.0 and pnl_pct > 0:
        return True, f"Esports map time exit voitolla {pnl_pct:+.1%}"

    return False, ""


def _evaluate_macro_position(position: Dict) -> Tuple[bool, str]:
    buy_price     = float(position.get("buy_price", 0.5))
    current_price = float(position.get("current_price", 0.5))
    hours_left    = float(position.get("hours_left", 24))
    pnl_pct = (current_price - buy_price) / buy_price if buy_price > 0 else 0

    if hours_left >= 24:
        tp_threshold = 0.30
    elif hours_left >= 6:
        tp_threshold = 0.20
    elif hours_left >= 2:
        tp_threshold = 0.10
    else:
        tp_threshold = 0.05

    if pnl_pct >= tp_threshold:
        return True, f"Makro TP +{tp_threshold:.0%} ({pnl_pct:+.1%}, {hours_left:.1f}h)"
    if pnl_pct <= -0.40:
        return True, f"Makro SL -40% ({pnl_pct:+.1%})"
    if hours_left <= 2.0 and pnl_pct > 0:
        return True, f"Makro time exit <2h, voitolla {pnl_pct:+.1%}"
    if hours_left <= 0.5:
        return True, f"Makro hätäexit <30min"

    return False, ""


def load_positions() -> List[Dict]:
    data = read_json("open_positions.json", {"positions": []})
    positions = data.get("positions", []) if isinstance(data, dict) else []
    return positions if isinstance(positions, list) else []


def save_positions(positions: List[Dict]):
    try:
        write_json("open_positions.json", {"positions": positions}, indent=2)
    except Exception as e:
        log.warning(f"Positioiden tallennus epäonnistui: {e}")


def add_position(signal: Dict, token_id: str, buy_price: float, amount: float, end_date: str):
    positions = load_positions()
    for p in positions:
        if p.get("token_id") == token_id:
            log.debug(f"Positio jo olemassa: {token_id[:16]}")
            return

    edge = signal.get("edge") or {}
    intelligence = signal.get("intelligence") or {}

    position = {
        "market_id":  signal.get("market_id", ""),
        "question":   signal.get("question", "")[:60],
        "outcome":    signal.get("outcome", ""),
        "token_id":   token_id,
        "buy_price":  buy_price,
        "amount":     amount,
        "end_date":   end_date,
        "is_sports":  _is_sports(signal.get("question", "")),
        "is_esports": _is_esports(signal.get("question", "")),
        "is_esports_map": _is_esports_map(signal.get("question", "")),
        "market_type": signal.get("market_type", ""),
        "bought_at":  datetime.now(timezone.utc).isoformat(),
        "support_count":    signal.get("support_count", 0),
        "weighted_support": signal.get("weighted_support", 0),
        "positive_roi_support": signal.get("positive_roi_support", 0),
        "category_positive_support": signal.get("category_positive_support", 0),
        "active_support": signal.get("active_support", 0),
        "reliable_support": signal.get("reliable_support", 0),
        "unknown_support": signal.get("unknown_support", 0),
        "total_size_usdc":  signal.get("total_size_usdc", 0),
        "edge":             edge.get("edge", 0.0),
        "our_probability":  edge.get("our_probability", buy_price),
        "edge_confidence":  edge.get("confidence", ""),
        "edge_reason":      edge.get("reason", ""),
        "market_quality":   intelligence.get("market_quality", 0.0),
        "intel_confidence": intelligence.get("confidence", 0.0),
    }
    positions.append(position)
    save_positions(positions)
    log.info(f"📌 Positio lisätty: {position['question'][:40]} | {position['outcome']} @ {buy_price}")


def check_and_exit_positions():
    positions = load_positions()
    if not positions:
        return

    remaining = []
    sold_count = 0

    for pos in positions:
        token_id   = pos.get("token_id", "")
        question   = pos.get("question", "")
        end_date   = pos.get("end_date", "")
        buy_price  = float(pos.get("buy_price", 0.5))
        amount     = float(pos.get("amount", 0))
        is_sports  = pos.get("is_sports", False)
        is_esports = pos.get("is_esports", False) or _is_esports(question)

        current_price = _get_current_price(token_id)
        if current_price is None:
            remaining.append(pos)
            continue

        hours_left = _get_hours_until_close(end_date)
        pnl_pct    = (current_price - buy_price) / buy_price if buy_price > 0 else 0

        pos["current_price"] = current_price
        pos["hours_left"]    = hours_left
        pos["is_esports_map"] = pos.get("is_esports_map", False) or _is_esports_map(question)

        log.info(f"📊 {question[:35]} | {pnl_pct:+.1%} | {hours_left:.1f}h jäljellä")

        if is_esports:
            should_sell, reason = _evaluate_esports_position(pos)
        elif is_sports:
            should_sell, reason = _evaluate_sports_position(pos)
        else:
            should_sell, reason = _evaluate_macro_position(pos)

        if should_sell:
            sell_result = _sell_position_v2(token_id, amount, current_price, reason)
            if sell_result.get("closed"):
                sold_count += 1
                log.info(f"💰 Myyty: {question[:35]} | P&L: {pnl_pct:+.1%} | {reason}")
                if notifier:
                    notifier.notify_sell(question, pnl_pct, reason)
                continue
            log.info(
                f"Positio pidetaan seurannassa: {question[:35]} | "
                f"sell_status={sell_result.get('status', 'unknown')}"
            )

        remaining.append(pos)

    if sold_count > 0:
        log.info(f"Position manager myi {sold_count} positiota.")

    save_positions(remaining)
