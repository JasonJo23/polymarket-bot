"""
=============================================================================
tracker.py – SignalTracker  (v6.0 – py_clob_client_v2 migraatio)
=============================================================================
Muutos v6.0:
  - Siirtyy polymarket_apis → py_clob_client_v2 (CLOB V2 API)
  - Uusi client-rakenne: ClobClient + ApiCreds
  - Tilaukset: create_and_post_order (GTC) ja create_and_post_market_order (FOK)
  - Saldon haku: get_usdc_balance() erillisellä REST-kutsulla
=============================================================================
"""

from typing import Optional, Dict, List, Any, Set
import os
import logging
import requests
from datetime import datetime, timezone, date
from collections import defaultdict

log = logging.getLogger("Scout.Tracker")

CLOB_BASE  = "https://clob.polymarket.com"
GAMMA_BASE = "https://gamma-api.polymarket.com"


def get_usdc_balance_v2() -> float:
    """Hakee USDC-saldon suoraan REST-kutsulla (v2 API)."""
    try:
        from py_clob_client_v2 import ClobClient, ApiCreds
        creds = ApiCreds(
            api_key=os.getenv("CLOB_API_KEY", ""),
            api_secret=os.getenv("CLOB_API_SECRET", ""),
            api_passphrase=os.getenv("CLOB_PASSPHRASE", "")
        )
        client = ClobClient(
            host=CLOB_BASE,
            chain_id=137,
            key=os.getenv("PRIVATE_KEY"),
            creds=creds,
            signature_type=2,
            funder=os.getenv("PROXY_WALLET_ADDRESS")
        )
        # V2: hae saldo suoraan CLOB API:sta
        headers = client._create_l2_headers("GET", "/balance-allowance", None)
        r = requests.get(
            f"{CLOB_BASE}/balance-allowance",
            headers=headers,
            params={"asset_type": "COLLATERAL", "signature_type": 2},
            timeout=8
        )
        if r.status_code == 200:
            return float(r.json().get("balance", 0)) / 1e6  # pUSD on 6 desimaalia
    except Exception as e:
        log.debug(f"Saldon haku v2 epäonnistui: {e}")
    return float(os.getenv("CURRENT_BANKROLL_USDC", 100.0))


class SignalTracker:

    def __init__(self, smart_threshold: int = 2, dry_run: bool = True):
        self.smart_threshold = smart_threshold
        self.dry_run         = dry_run
        self.min_signal_size = float(os.getenv("MIN_SIGNAL_SIZE_USDC", 50000))
        self.max_order_usdc  = float(os.getenv("MAX_ORDER_SIZE_USDC", 5))

        self._executed_file = "executed_today.json"
        self._executed_today: Set[str] = set()
        self._executed_date: str = date.today().isoformat()
        self._load_executed()
        self._market_cache: Dict[str, Dict] = {}

        if dry_run:
            log.warning("DRY RUN -tila – ostoja ei tehdä.")
        else:
            log.warning("LIVE-tila – OIKEAT ostot käytössä!")

    def _load_executed(self):
        import json as _json
        try:
            with open(self._executed_file, "r") as f:
                data = _json.load(f)
                if data.get("date", "") == date.today().isoformat():
                    raw = data.get("signals", [])
                    # Hyväksy vain puhtaat market_id:t
                    self._executed_today = set(
                        s for s in raw
                        if s.startswith("0x") and "_" not in s
                    )
                    log.info(f"Ladattu {len(self._executed_today)} aiemmin ostettua signaalia tänään.")
                else:
                    log.info("Uusi päivä — aiemmat signaalit nollattu.")
        except (FileNotFoundError, Exception):
            pass

    def _save_executed(self):
        import json as _json
        try:
            with open(self._executed_file, "w") as f:
                _json.dump({
                    "date": self._executed_date,
                    "signals": list(self._executed_today)
                }, f)
        except Exception as e:
            log.warning(f"Signaalien tallennus epäonnistui: {e}")

    def _reset_daily_if_needed(self):
        today = date.today().isoformat()
        if today != self._executed_date:
            self._executed_today.clear()
            self._executed_date = today
            self._save_executed()
            log.info("Uusi päivä — päivittäinen ostosuoja nollattu.")

    def process(self, qualified_wallets, raw_trades):
        self._reset_daily_if_needed()
        if not qualified_wallets:
            return []

        market_support: Dict[str, Dict[str, List]] = defaultdict(lambda: defaultdict(list))
        for wallet in qualified_wallets:
            for trade in wallet.get("recent_trades", []):
                market_id = trade.get("conditionId")
                outcome   = str(trade.get("outcome", "")).upper()
                side      = str(trade.get("side", "")).upper()
                if not market_id or not outcome or side != "BUY":
                    continue
                size = self._extract_size(trade)
                market_support[market_id][outcome].append({
                    "wallet":    wallet["address"],
                    "size_usdc": size,
                })

        from concurrent.futures import ThreadPoolExecutor, as_completed
        market_ids = list(market_support.keys())

        def fetch_market(mid):
            return mid, self._get_market_info_clob(mid)

        market_infos: Dict[str, Dict] = {}
        max_workers = int(os.getenv("FETCH_WORKERS", 4))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(fetch_market, mid): mid for mid in market_ids}
            for future in as_completed(futures):
                try:
                    mid, info = future.result()
                    market_infos[mid] = info or {}
                except Exception:
                    pass

        signals = []
        for market_id, outcomes in market_support.items():
            market_info = market_infos.get(market_id, {})
            if not market_info or not market_info.get("accepting_orders", False):
                continue
            tokens = market_info.get("tokens", [])
            prices = [float(t.get("price", 0.5)) for t in tokens if t.get("price")]
            if prices and max(prices) > 0.90:
                continue
            question = market_info.get("question", market_id[:20])
            end_date = market_info.get("end_date_iso", "")

            for outcome, supporters in outcomes.items():
                unique_wallets = {s["wallet"] for s in supporters}
                total_size     = sum(s["size_usdc"] for s in supporters)
                if (len(unique_wallets) >= self.smart_threshold and
                        total_size >= self.min_signal_size):
                    signals.append({
                        "market_id":       market_id,
                        "question":        question,
                        "end_date":        end_date[:10] if end_date else "?",
                        "outcome":         outcome,
                        "support_count":   len(unique_wallets),
                        "supporters":      list(unique_wallets),
                        "total_size_usdc": total_size,
                        "timestamp":       datetime.now(timezone.utc).isoformat(),
                    })

        best_per_market: Dict[str, Dict] = {}
        for sig in signals:
            mid = sig["market_id"]
            if mid not in best_per_market:
                best_per_market[mid] = sig
            else:
                current = best_per_market[mid]
                if (sig["support_count"], sig["total_size_usdc"]) > \
                   (current["support_count"], current["total_size_usdc"]):
                    best_per_market[mid] = sig

        signals = list(best_per_market.values())
        signals.sort(key=lambda s: (s["support_count"], s["total_size_usdc"]), reverse=True)
        return signals

    def execute_order(self, signal: Dict[str, Any]) -> bool:
        self._reset_daily_if_needed()

        sig_key = signal["market_id"]
        if sig_key in self._executed_today:
            log.debug(f"Duplikaatti tänään: {sig_key[:20]} — ohitetaan.")
            return False

        order_size = min(self.max_order_usdc, signal["total_size_usdc"] * 0.01)
        order_size = max(round(order_size, 2), 1.0)

        if self.dry_run:
            log.info(
                f"[DRY RUN] {signal.get('question','')[:35]} | "
                f"{signal['outcome']} | {order_size} USDC"
            )
            return True

        if not os.getenv("CLOB_API_KEY"):
            log.error("CLOB_API_KEY puuttuu.")
            return False

        try:
            from py_clob_client_v2 import (
                ClobClient, ApiCreds, MarketOrderArgs, OrderArgs,
                OrderType, Side, PartialCreateOrderOptions
            )
            from intelligence import _is_sports as _check_sports

            condition_id = signal["market_id"]
            outcome_name = signal["outcome"].strip('"').upper()

            # Hae token tiedot CLOB API:sta
            r = requests.get(f"{CLOB_BASE}/markets/{condition_id}", timeout=8)
            if r.status_code != 200:
                log.error(f"CLOB markets haku epäonnistui: {r.status_code}")
                return False

            market_data = r.json()
            if not market_data.get("accepting_orders", False):
                log.warning("Markkina ei hyväksy tilauksia — ohitetaan.")
                return False

            tokens_list = market_data.get("tokens", [])
            token_id    = None
            token_price = 0.5
            tick_size   = "0.01"

            for token in tokens_list:
                t_outcome = str(token.get("outcome", "")).upper()
                # Fuzzy match: vertaa normalisoituja nimiä
                if t_outcome == outcome_name or \
                   t_outcome.replace("'", "").replace(" ", "") == outcome_name.replace("'", "").replace(" ", ""):
                    token_id    = token.get("token_id")
                    token_price = round(float(token.get("price", 0.5)), 3)
                    break

            if not token_id:
                log.error(f"Outcome '{outcome_name}' ei löydy: {[t.get('outcome') for t in tokens_list]}")
                return False

            if token_price < 0.05 or token_price > 0.90:
                log.warning(f"Hinta {token_price} äärimmäinen — ohitetaan.")
                return False

            # Intelligence-tarkistus
            try:
                from intelligence import analyze_signal
                intel = analyze_signal(signal, token_id, token_price)
                if not intel["approved"]:
                    log.warning(f"Intelligence hylkäsi: {intel['reason']}")
                    return False
            except Exception as e:
                log.debug(f"Intelligence epäonnistui: {e}")

            # Hae tick size
            try:
                r_tick = requests.get(
                    f"{CLOB_BASE}/tick-size",
                    params={"token_id": token_id},
                    timeout=5
                )
                if r_tick.status_code == 200:
                    tick_size = str(r_tick.json().get("minimum_tick_size", "0.01"))
            except Exception:
                pass

            # Rakenna v2 client
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

            is_sports = _check_sports(signal.get("question", ""))
            options = PartialCreateOrderOptions(tick_size=tick_size)

            if is_sports:
                # FOK urheilu — täyty heti tai peruutu
                slippage   = float(os.getenv("SLIPPAGE_PCT", 0.02))
                exec_price = round(min(token_price * (1 + slippage), 0.90), 3)
                log.info(f"FOK urheilu: {token_price} → {exec_price} | {order_size} USDC")
                resp = client.create_and_post_market_order(
                    order_args=MarketOrderArgs(
                        token_id=token_id,
                        amount=order_size,
                        side=Side.BUY,
                        order_type=OrderType.FOK,
                    ),
                    options=options,
                    order_type=OrderType.FOK,
                )
            else:
                # GTC makro — jää odottamaan
                exec_price = round(token_price, 3)
                log.info(f"GTC makro: {exec_price} | {order_size} USDC")
                resp = client.create_and_post_order(
                    order_args=OrderArgs(
                        token_id=token_id,
                        price=exec_price,
                        size=order_size,
                        side=Side.BUY,
                    ),
                    options=options,
                    order_type=OrderType.GTC,
                )

            if resp is None:
                log.warning("⚠️ Osto epäonnistui: resp on None")
                return False

            log.info(f"✅ Osto tehty: {resp}")
            status = resp.get("status", "") if isinstance(resp, dict) else getattr(resp, "status", "")

            if status == "matched":
                token_amount = round(order_size / exec_price, 4)
                try:
                    from position_manager import add_position
                    add_position(
                        signal=signal,
                        token_id=token_id,
                        buy_price=exec_price,
                        amount=token_amount,
                        end_date=signal.get("end_date", "")
                    )
                except Exception as e:
                    log.debug(f"Position lisäys epäonnistui: {e}")
            else:
                log.info(f"Status: {status} — positiota ei lisätty")

            self._executed_today.add(sig_key)
            self._save_executed()
            if status == "matched":
                signal["_actual_order_size"] = order_size
            else:
                signal["_actual_order_size"] = 0
            return True

        except Exception as e:
            err = str(e)
            if "fully filled or killed" in err or "FOK" in err:
                log.info("FOK ei täyttynyt — normaali tilanne, ohitetaan.")
                return False
            log.error(f"CLOB-osto epäonnistui: {e}")
            return False

    def _get_market_info_clob(self, condition_id: str) -> Optional[Dict]:
        if condition_id in self._market_cache:
            return self._market_cache[condition_id]
        try:
            r = requests.get(f"{CLOB_BASE}/markets/{condition_id}", timeout=8)
            if r.status_code == 200:
                data = r.json()
                result = {
                    "question":         data.get("question", condition_id[:20]),
                    "end_date_iso":     data.get("end_date_iso", ""),
                    "accepting_orders": data.get("accepting_orders", False),
                    "tokens":           data.get("tokens", []),
                }
                if not data.get("question"):
                    slug = data.get("market_slug", "")
                    if slug:
                        try:
                            r2 = requests.get(
                                f"{GAMMA_BASE}/markets",
                                params={"slug": slug},
                                timeout=5
                            )
                            if r2.status_code == 200:
                                d2 = r2.json()
                                if d2:
                                    m = d2[0] if isinstance(d2, list) else d2
                                    result["question"] = m.get("question", condition_id[:20])
                        except Exception:
                            pass
                self._market_cache[condition_id] = result
                return result
        except Exception as e:
            log.debug(f"CLOB market info epäonnistui: {e}")
        self._market_cache[condition_id] = {}
        return {}

    def _extract_size(self, trade: Dict) -> float:
        for key in ("usdcSize", "size", "amount"):
            raw = trade.get(key)
            if raw is not None:
                try:
                    v = float(raw)
                    if v > 0:
                        return v
                except (TypeError, ValueError):
                    pass
        return 0.0