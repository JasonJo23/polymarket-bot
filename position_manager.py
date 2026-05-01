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

log = logging.getLogger("Scout.PositionManager")

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
) -> bool:
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
            return True
        else:
            log.warning(f"⚠️ Myynti epäonnistui: {resp}")
            return False

    except Exception as e:
        err = str(e)
        if "425" in err or "Too Early" in err or "service not ready" in err:
            log.warning(f"Polymarket ei valmis myyntiin (425) — yritetään myöhemmin.")
        else:
            log.error(f"Myyntivirhe: {e}")
        return False


def _evaluate_sports_position(position: Dict) -> Tuple[bool, str]:
    buy_price     = float(position.get("buy_price", 0.5))
    current_price = float(position.get("current_price", 0.5))
    hours_left    = float(position.get("hours_left", 24))
    pnl_pct = (current_price - buy_price) / buy_price if buy_price > 0 else 0

    if pnl_pct >= 0.30:
        return True, f"Urheilu TP +30% ({pnl_pct:+.1%})"
    if current_price >= 0.85:
        return True, f"Urheilu peli voitettu ({current_price:.2f} ≥ 0.85)"
    if pnl_pct <= -0.35:
        return True, f"Urheilu SL -35% ({pnl_pct:+.1%})"
    if current_price <= 0.15:
        return True, f"Urheilu peli hävitty ({current_price:.2f} ≤ 0.15)"
    if hours_left <= 0.5 and pnl_pct > 0:
        return True, f"Urheilu time exit <30min, voitolla {pnl_pct:+.1%}"

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
    try:
        with open("open_positions.json", "r") as f:
            return json.load(f).get("positions", [])
    except (FileNotFoundError, Exception):
        return []


def save_positions(positions: List[Dict]):
    try:
        with open("open_positions.json", "w") as f:
            json.dump({"positions": positions}, f, indent=2)
    except Exception as e:
        log.warning(f"Positioiden tallennus epäonnistui: {e}")


def add_position(signal: Dict, token_id: str, buy_price: float, amount: float, end_date: str):
    positions = load_positions()
    for p in positions:
        if p.get("token_id") == token_id:
            log.debug(f"Positio jo olemassa: {token_id[:16]}")
            return

    position = {
        "market_id":  signal.get("market_id", ""),
        "question":   signal.get("question", "")[:60],
        "outcome":    signal.get("outcome", ""),
        "token_id":   token_id,
        "buy_price":  buy_price,
        "amount":     amount,
        "end_date":   end_date,
        "is_sports":  _is_sports(signal.get("question", "")),
        "bought_at":  datetime.now(timezone.utc).isoformat(),
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

        current_price = _get_current_price(token_id)
        if current_price is None:
            remaining.append(pos)
            continue

        hours_left = _get_hours_until_close(end_date)
        pnl_pct    = (current_price - buy_price) / buy_price if buy_price > 0 else 0

        pos["current_price"] = current_price
        pos["hours_left"]    = hours_left

        log.info(f"📊 {question[:35]} | {pnl_pct:+.1%} | {hours_left:.1f}h jäljellä")

        if is_sports:
            should_sell, reason = _evaluate_sports_position(pos)
        else:
            should_sell, reason = _evaluate_macro_position(pos)

        if should_sell:
            success = _sell_position_v2(token_id, amount, current_price, reason)
            if success:
                sold_count += 1
                log.info(f"💰 Myyty: {question[:35]} | P&L: {pnl_pct:+.1%} | {reason}")
                continue

        remaining.append(pos)

    if sold_count > 0:
        log.info(f"Position manager myi {sold_count} positiota.")

    save_positions(remaining)