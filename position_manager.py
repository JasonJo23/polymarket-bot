"""
=============================================================================
position_manager.py – PositionManager  (v1.0)
=============================================================================
Hallinnoi avoimia positioita ja tekee myyntipäätökset automaattisesti.

KAKSI STRATEGIAA:

1. URHEILU (sports)
   - Take profit: +30% ennen peliä, +85% pelin aikana
   - Stop loss: -35% ennen peliä, hinta < 0.15 pelin aikana
   - Time exit: myy kaikki 30min ennen pelin alkua jos ei selkeää suuntaa

2. MUU (politiikka, crypto, makro)
   - Dynaaminen take profit: riippuu ajasta jäljellä
     24h+ → TP 30% | 6-24h → TP 20% | <6h → TP 10%
   - Stop loss: -40% aina
   - Time exit: myy voitolliset 2h ennen sulkeutumista

MISSÄ VOI EPÄONNISTUA:
  1. Likviditeetti voi olla heikko → myynti ei onnistu halutussa hinnassa
  2. Hintatiedot voivat olla viiveellisiä CLOB API:ssa
  3. Urheilu time exit 30min voi olla liian aikainen jos peli viivästyy
=============================================================================
"""

import os
import json
import logging
import requests
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional, Tuple

log = logging.getLogger("Scout.PositionManager")

CLOB_BASE  = "https://clob.polymarket.com"
GAMMA_BASE = "https://gamma-api.polymarket.com"

# Urheilu-avainsanat (sama kuin intelligence.py)
SPORTS_KEYWORDS = [
    "vs.", "vs ", "game 1", "game 2", "game 3", "bo3", "bo5",
    "nba", "nfl", "nhl", "mlb", "wnba", "epl", "lol:", "dota",
    "csgo", "valorant", "winner", "match", "series",
    "lakers", "celtics", "knicks", "hawks", "bulls", "heat",
    "thunder", "pistons", "magic", "rockets", "spurs", "raptors",
    "cavaliers", "76ers", "trail blazers", "inter", "fc ", "win on"
]


def _is_sports(question: str) -> bool:
    q = question.lower()
    return any(kw in q for kw in SPORTS_KEYWORDS)


def _get_current_price(token_id: str) -> Optional[float]:
    """Hakee tokenin nykyisen hinnan CLOB API:sta."""
    try:
        r = requests.get(
            f"{CLOB_BASE}/price",
            params={"token_id": token_id, "side": "BUY"},
            timeout=5
        )
        if r.status_code == 200:
            return float(r.json().get("price", 0))
    except Exception as e:
        log.debug(f"Hinnan haku epäonnistui: {e}")
    return None


def _get_hours_until_close(end_date_str: str) -> float:
    """Laskee tunteja jäljellä markkinan sulkeutumiseen."""
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


def _sell_position(
    token_id: str,
    amount: float,
    current_price: float,
    private_key: str,
    proxy_address: str,
    reason: str
) -> bool:
    """
    Myy positio CLOB API:n kautta.
    Asettaa myyntihinnan hieman alle nykyhinnan jotta myynti täyttyy.
    """
    try:
        from polymarket_apis import PolymarketClobClient, MarketOrderArgs, OrderType

        # Myyntihinta: 2% alle nykyhinnan jotta löytyy ostaja
        sell_price = round(max(0.01, current_price * 0.98), 3)

        client = PolymarketClobClient(
            private_key=private_key,
            address=proxy_address
        )

        order_args = MarketOrderArgs(
            token_id=token_id,
            amount=amount,
            side="SELL",
            price=sell_price,
            order_type=OrderType.GTC,
        )
        resp = client.create_and_post_market_order(order_args)

        if resp and getattr(resp, "success", False):
            log.info(f"✅ Myynti tehty @ {sell_price} | Syy: {reason} | {resp}")
            return True
        else:
            log.warning(f"⚠️ Myynti epäonnistui: {resp}")
            return False

    except Exception as e:
        log.error(f"Myyntivirhe: {e}")
        return False


def _evaluate_sports_position(position: Dict) -> Tuple[bool, str]:
    """
    Arvioi urheiluposition myyntitarpeen.
    
    Palauttaa: (should_sell, reason)
    """
    buy_price     = float(position.get("buy_price", 0.5))
    current_price = float(position.get("current_price", 0.5))
    hours_left    = float(position.get("hours_left", 24))

    pnl_pct = (current_price - buy_price) / buy_price

    # Take profit: +30% ennen peliä
    if pnl_pct >= 0.30:
        return True, f"Urheilu TP +30% ({pnl_pct:+.1%})"

    # In-game: hinta yli 0.85 → peli käytännössä voitettu
    if current_price >= 0.85:
        return True, f"Urheilu peli voitettu ({current_price:.2f} ≥ 0.85)"

    # Stop loss: -35%
    if pnl_pct <= -0.35:
        return True, f"Urheilu SL -35% ({pnl_pct:+.1%})"

    # In-game: hinta alle 0.15 → peli käytännössä hävitty
    if current_price <= 0.15:
        return True, f"Urheilu peli hävitty ({current_price:.2f} ≤ 0.15)"

    # Time exit: alle 30min jäljellä → myy jos voitolla
    if hours_left <= 0.5 and pnl_pct > 0:
        return True, f"Urheilu time exit <30min, voitolla {pnl_pct:+.1%}"

    return False, ""


def _evaluate_macro_position(position: Dict) -> Tuple[bool, str]:
    """
    Arvioi politiikka/makro/crypto-position myyntitarpeen.
    Dynaaminen take profit perustuu aikaan jäljellä.
    
    Palauttaa: (should_sell, reason)
    """
    buy_price     = float(position.get("buy_price", 0.5))
    current_price = float(position.get("current_price", 0.5))
    hours_left    = float(position.get("hours_left", 24))

    pnl_pct = (current_price - buy_price) / buy_price

    # Dynaaminen take profit
    if hours_left >= 24:
        tp_threshold = 0.30   # 24h+ → anna juosta, TP 30%
    elif hours_left >= 6:
        tp_threshold = 0.20   # 6-24h → TP 20%
    elif hours_left >= 2:
        tp_threshold = 0.10   # 2-6h → TP 10%
    else:
        tp_threshold = 0.05   # <2h → TP 5%, lukitse voitto

    if pnl_pct >= tp_threshold:
        return True, f"Makro dynaaminen TP +{tp_threshold:.0%} ({pnl_pct:+.1%}, {hours_left:.1f}h jäljellä)"

    # Stop loss: -40% aina
    if pnl_pct <= -0.40:
        return True, f"Makro SL -40% ({pnl_pct:+.1%})"

    # Time exit: 2h ennen sulkeutumista → myy voitolliset
    if hours_left <= 2.0 and pnl_pct > 0:
        return True, f"Makro time exit <2h, voitolla {pnl_pct:+.1%}"

    # Hätäexit: alle 30min jäljellä → myy kaikki
    if hours_left <= 0.5:
        return True, f"Makro hätäexit <30min jäljellä"

    return False, ""


def load_positions() -> List[Dict]:
    """
    Lataa avoimet positiot tiedostosta.
    Tiedosto päivitetään aina kun osto tehdään.
    """
    try:
        with open("open_positions.json", "r") as f:
            data = json.load(f)
            return data.get("positions", [])
    except (FileNotFoundError, Exception):
        return []


def save_positions(positions: List[Dict]):
    """Tallentaa avoimet positiot tiedostoon."""
    try:
        with open("open_positions.json", "w") as f:
            json.dump({"positions": positions}, f, indent=2)
    except Exception as e:
        log.warning(f"Positioiden tallennus epäonnistui: {e}")


def add_position(signal: Dict, token_id: str, buy_price: float, amount: float, end_date: str):
    """
    Lisää uuden position seurantaan oston jälkeen.
    Kutsutaan tracker.py:stä onnistuneen oston jälkeen.
    """
    positions = load_positions()

    # Tarkista onko jo olemassa
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
    """
    Pääfunktio — tarkistaa kaikki avoimet positiot ja myy tarvittaessa.
    Kutsutaan main.py:stä jokaisen syklin alussa.
    """
    positions = load_positions()
    if not positions:
        return

    private_key    = os.getenv("PRIVATE_KEY", "")
    proxy_address  = os.getenv("PROXY_WALLET_ADDRESS", "")

    if not private_key or not proxy_address:
        log.error("PRIVATE_KEY tai PROXY_WALLET_ADDRESS puuttuu")
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

        # Hae nykyinen hinta
        current_price = _get_current_price(token_id)
        if current_price is None:
            log.debug(f"Hinnan haku epäonnistui: {token_id[:16]} — pidetään positio")
            remaining.append(pos)
            continue

        hours_left = _get_hours_until_close(end_date)
        pnl_pct    = (current_price - buy_price) / buy_price if buy_price > 0 else 0

        pos["current_price"] = current_price
        pos["hours_left"]    = hours_left

        log.debug(f"📊 {question[:35]} | {pnl_pct:+.1%} | {hours_left:.1f}h jäljellä")

        # Arvioi myyntitarve
        if is_sports:
            should_sell, reason = _evaluate_sports_position(pos)
        else:
            should_sell, reason = _evaluate_macro_position(pos)

        if should_sell:
            success = _sell_position(
                token_id, amount, current_price,
                private_key, proxy_address, reason
            )
            if success:
                sold_count += 1
                log.info(f"💰 Myyty: {question[:35]} | P&L: {pnl_pct:+.1%} | {reason}")
                continue  # Älä lisää remaining-listalle

        remaining.append(pos)

    if sold_count > 0:
        log.info(f"Position manager myi {sold_count} positiota.")

    save_positions(remaining)