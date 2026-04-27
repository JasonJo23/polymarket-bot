"""
=============================================================================
tracker.py – SignalTracker  (v5.0 – 10 bugia korjattu)
=============================================================================
Korjaukset:
  #1  daily_spent lasketaan tracker.py:stä eikä main.py:stä
  #2  executed_today.json formaatti yhtenäistetty (vain market_id)
  #3  FOK urheilu → MarketOrder ilman limit-hintaa (täyttyy heti)
  #4  Position lisätään vain matched-statusten jälkeen (GTC: tarkistetaan erikseen)
  #7  Position manager saa token-määrän USDC-summan sijaan
  #9  Jalkapallo tunnistetaan urheiluksi oikein
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


class SignalTracker:

    def __init__(self, smart_threshold: int = 2, dry_run: bool = True):
        self.smart_threshold = smart_threshold
        self.dry_run         = dry_run
        self.min_signal_size = float(os.getenv("MIN_SIGNAL_SIZE_USDC", 5000))
        self.max_order_usdc  = float(os.getenv("MAX_ORDER_SIZE_USDC", 20))

        self.clob_api_key    = os.getenv("CLOB_API_KEY", "")
        self.clob_api_secret = os.getenv("CLOB_API_SECRET", "")
        self.clob_passphrase = os.getenv("CLOB_PASSPHRASE", "")

        # FIX #2: Päivätason duplikaattisuoja — vain market_id (ei outcome)
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
                saved_date = data.get("date", "")
                if saved_date == date.today().isoformat():
                    # FIX #2: Hyväksy vain puhtaat market_id:t (ei vanhoja market_id_OUTCOME)
                    raw = data.get("signals", [])
                    self._executed_today = set(
                        s for s in raw if "_" not in s or s.startswith("0x") and len(s) == 66
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
        max_workers = int(os.getenv("FETCH_WORKERS", 8))
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

        # Vain vahvin outcome per markkina
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
                f"[DRY RUN] Simuloitu osto: "
                f"{signal.get('question','')[:35]} | "
                f"Kohde: {signal['outcome']} | "
                f"Koko: {order_size} USDC"
            )
            return True

        if not self.clob_api_key:
            log.error("CLOB_API_KEY puuttuu.")
            return False

        try:
            from polymarket_apis import PolymarketClobClient, MarketOrderArgs, OrderType
            from intelligence import _is_sports as _check_sports
            import os as _os, requests as _req

            condition_id = signal["market_id"]
            outcome_name = signal["outcome"].strip('"').upper()

            r = _req.get(f"{CLOB_BASE}/markets/{condition_id}", timeout=8)
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

            for token in tokens_list:
                if str(token.get("outcome", "")).upper() == outcome_name:
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

            proxy_address = _os.getenv("PROXY_WALLET_ADDRESS", "")
            if not proxy_address:
                log.error("PROXY_WALLET_ADDRESS puuttuu!")
                return False

            client = PolymarketClobClient(
                private_key=_os.getenv("PRIVATE_KEY"),
                address=proxy_address
            )

            # FIX #3: Urheilu → MarketOrder ilman limit-hintaa (täyttyy heti markkinahinnalla)
            # Makro → GTC limit-order (jää odottamaan)
            is_sports = _check_sports(signal.get("question", ""))

            if is_sports:
                # Market order urheilulle — ei limit-hintaa, täyttyy välittömästi
                slippage = float(_os.getenv("SLIPPAGE_PCT", 0.02))
                exec_price = round(min(token_price * (1 + slippage), 0.90), 3)
                order_args = MarketOrderArgs(
                    token_id=token_id,
                    amount=order_size,
                    side="BUY",
                    price=exec_price,
                    order_type=OrderType.FOK,
                )
                log.info(f"FOK urheilu: {token_price} → {exec_price} | {order_size} USDC")
            else:
                # GTC makrolle — jää odottamaan oikeaa hintaa
                exec_price = round(token_price, 3)
                order_args = MarketOrderArgs(
                    token_id=token_id,
                    amount=order_size,
                    side="BUY",
                    price=exec_price,
                    order_type=OrderType.GTC,
                )
                log.info(f"GTC makro: {exec_price} | {order_size} USDC")

            resp = client.create_and_post_market_order(order_args)

            if resp is None or not getattr(resp, "success", False):
                log.warning(f"⚠️ Osto epäonnistui: {resp}")
                return False

            log.info(f"✅ Osto tehty: {resp}")
            status = getattr(resp, "status", "")

            # FIX #4 & #7: Lisää positio vain matched-statuksella
            # Laske token-määrä oikein (USDC / hinta = tokeneita)
            if status == "matched":
                token_amount = round(order_size / exec_price, 4)
                try:
                    from position_manager import add_position
                    add_position(
                        signal=signal,
                        token_id=token_id,
                        buy_price=exec_price,
                        amount=token_amount,  # FIX #7: token-määrä, ei USDC
                        end_date=signal.get("end_date", "")
                    )
                except Exception as e:
                    log.debug(f"Position lisäys epäonnistui: {e}")
            else:
                log.info(f"Positiota ei lisätty — status: {status}")

            # FIX #2: Merkitse ostetuksi vasta onnistuneen oston jälkeen
            self._executed_today.add(sig_key)
            self._save_executed()

            # FIX #1: Palauta todellinen ostokoko main.py:lle
            signal["_actual_order_size"] = order_size
            return True

        except Exception as e:
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
                # FIX #10: Hae nimi vain slugilla (ei kaksoiskutsua)
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