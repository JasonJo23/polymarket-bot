"""
=============================================================================
tracker.py – SignalTracker  (v4.0 – kaikki ongelmat korjattu)
=============================================================================
Korjaukset v4.0:
  1. _get_market_info käyttää CLOB API:ta (ei Gamma API:ta) → oikeat nimet
  2. Duplikaattiostosuoja päivätasolla (ei nollaudu per sykli)
  3. Hintasuodatus 0.05-0.95
  4. accepting_orders -tarkistus
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

        # Päivätason duplikaattisuoja — säilyy uudelleenkäynnistyksen yli
        self._executed_file = "executed_today.json"
        self._executed_today: Set[str] = set()
        self._executed_date: str = date.today().isoformat()
        self._load_executed()

        # Välimuisti markkinatiedoille
        self._market_cache: Dict[str, Dict] = {}

        if dry_run:
            log.warning("DRY RUN -tila – ostoja ei tehdä.")
        else:
            log.warning("LIVE-tila – OIKEAT ostot käytössä!")

    def _load_executed(self):
        """Lataa ostetut signaalit levyltä käynnistyksessä."""
        import json as _json
        try:
            with open(self._executed_file, "r") as f:
                data = _json.load(f)
                saved_date = data.get("date", "")
                if saved_date == date.today().isoformat():
                    self._executed_today = set(data.get("signals", []))
                    log.info(f"Ladattu {len(self._executed_today)} aiemmin ostettua signaalia tänään.")
                else:
                    log.info("Uusi päivä — aiemmat signaalit nollattu.")
        except (FileNotFoundError, Exception):
            pass

    def _save_executed(self):
        """Tallentaa ostetut signaalit levylle."""
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
        """Nollaa päivittäinen ostosuoja uuden päivän alkaessa."""
        today = date.today().isoformat()
        if today != self._executed_date:
            self._executed_today.clear()
            self._executed_date = today
            self._save_executed()
            log.info("Uusi päivä — päivittäinen ostosuoja nollattu.")

    def process(
        self,
        qualified_wallets: List[Dict[str, Any]],
        raw_trades: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Analysoi kvalifioitujen lompakoiden viimeisimmät kaupat.
        Palauttaa konsensussignaalit järjestettynä vahvuuden mukaan.
        """
        self._reset_daily_if_needed()

        if not qualified_wallets:
            return []

        # Laske per-markkina konsensus
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

        # Hae markkinatiedot rinnakkain
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

            # Suodata suljetut markkinat
            if not market_info or not market_info.get("accepting_orders", False):
                continue

            # Suodata käytännössä ratkaistut markkinat
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

        # Ota vain vahvin outcome per markkina — ei osteta molempia puolia
        best_per_market: Dict[str, Dict] = {}
        for sig in signals:
            mid = sig["market_id"]
            if mid not in best_per_market:
                best_per_market[mid] = sig
            else:
                current = best_per_market[mid]
                if (sig["support_count"], sig["total_size_usdc"]) >                    (current["support_count"], current["total_size_usdc"]):
                    best_per_market[mid] = sig

        signals = list(best_per_market.values())
        signals.sort(
            key=lambda s: (s["support_count"], s["total_size_usdc"]),
            reverse=True
        )
        return signals

    def execute_order(self, signal: Dict[str, Any]) -> bool:
        """DRY RUN tai oikea CLOB-osto."""
        self._reset_daily_if_needed()

        # KORJAUS 2: Päivätason duplikaattisuoja
        # Blokaa koko markkina yhden oston jälkeen — ei osteta molempia puolia
        sig_key = signal["market_id"]
        if sig_key in self._executed_today:
            log.debug(f"Duplikaatti tänään: {sig_key[:30]} — ohitetaan.")
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

        # LIVE-osto
        if not self.clob_api_key:
            log.error("CLOB_API_KEY puuttuu – ei voi tehdä live-ostoa.")
            return False

        try:
            from polymarket_apis import PolymarketClobClient, MarketOrderArgs
            import os as _os, requests as _req

            condition_id = signal["market_id"]
            outcome_name = signal["outcome"].strip('"').upper()

            # Hae token ID CLOB API:sta
            r = _req.get(f"{CLOB_BASE}/markets/{condition_id}", timeout=8)
            if r.status_code != 200:
                log.error(f"CLOB markets haku epäonnistui: {r.status_code}")
                return False

            market_data = r.json()

            # KORJAUS 3+4: accepting_orders ja hintasuodatus
            if not market_data.get("accepting_orders", False):
                log.warning(f"Markkina ei hyväksy tilauksia — ohitetaan.")
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
                log.error(f"Outcome '{outcome_name}' ei löydy markkinalta {condition_id[:16]}")
                log.error(f"  Saatavilla: {[t.get('outcome') for t in tokens_list]}")
                return False

            # KORJAUS 3: Hintasuodatus
            if token_price < 0.05 or token_price > 0.90:
                log.warning(f"Hinta {token_price} liian äärimmäinen — markkina ratkaistu. Ohitetaan.")
                return False

            # INTELLIGENCE: Analysoi markkinan laatu ja momentum
            try:
                from intelligence import analyze_signal
                intel = analyze_signal(signal, token_id, token_price)
                if not intel["approved"]:
                    log.warning(f"Intelligence hylkäsi oston: {intel['reason']}")
                    return False
            except Exception as e:
                log.debug(f"Intelligence analyysi epäonnistui: {e} — jatketaan ilman")

            # Lisää 2% slippage jotta tilaus täyttyy nopealiikkeisillä markkinoilla
            slippage = float(os.getenv("SLIPPAGE_PCT", 0.02))
            exec_price = round(min(token_price * (1 + slippage), 0.90), 3)
            log.info(f"Token ID: {token_id[:16]}... | hinta: {token_price} → {exec_price} (+{slippage:.0%} slippage) | koko: {order_size} USDC")
            token_price = exec_price

            proxy_address = _os.getenv("PROXY_WALLET_ADDRESS", "")
            if not proxy_address:
                log.error("PROXY_WALLET_ADDRESS puuttuu .env-tiedostosta!")
                return False

            client = PolymarketClobClient(
                private_key=_os.getenv("PRIVATE_KEY"),
                address=proxy_address
            )

            from polymarket_apis import OrderType
            order_args = MarketOrderArgs(
                token_id=token_id,
                amount=order_size,
                side="BUY",
                price=token_price,
                order_type=OrderType.GTC,  # Good Till Cancelled — jää open orderiksi
            )
            resp = client.create_and_post_market_order(order_args)

            if resp is None or not getattr(resp, "success", False):
                log.warning(f"⚠️ Osto epäonnistui tai ei täyttynyt: {resp}")
                return False

            log.info(f"✅ Osto tehty: {resp}")

            # Lisää positio seurantaan
            try:
                from position_manager import add_position
                add_position(
                    signal=signal,
                    token_id=token_id,
                    buy_price=token_price,
                    amount=order_size,
                    end_date=signal.get("end_date", "")
                )
            except Exception as e:
                log.debug(f"Position lisäys epäonnistui: {e}")

            # Merkitse ostetuksi VASTA onnistuneen oston jälkeen
            self._executed_today.add(sig_key)
            self._save_executed()
            return True

        except Exception as e:
            log.error(f"CLOB-osto epäonnistui: {e}")
            return False

    # ------------------------------------------------------------------
    # KORJAUS 1: CLOB API markkinatiedoille
    # ------------------------------------------------------------------

    def _get_market_info_clob(self, condition_id: str) -> Optional[Dict]:
        """
        Hakee markkinatiedot CLOB API:sta — toimii condition ID:llä oikein.
        Gamma API ignoroi conditionId-parametrin, CLOB ei.
        """
        if condition_id in self._market_cache:
            return self._market_cache[condition_id]
        try:
            r = requests.get(
                f"{CLOB_BASE}/markets/{condition_id}",
                timeout=8
            )
            if r.status_code == 200:
                data = r.json()
                # CLOB API ei palauta question-kenttää — haetaan Gamma API:sta slugilla
                # mutta käytetään condition_id:tä fallbackina
                result = {
                    "question":       data.get("question", condition_id[:20]),
                    "end_date_iso":   data.get("end_date_iso", ""),
                    "accepting_orders": data.get("accepting_orders", False),
                    "tokens":         data.get("tokens", []),
                }
                # Jos question puuttuu CLOB:sta, hae Gamma API:sta condition_id -> slug
                if not data.get("question"):
                    gamma_q = self._get_question_from_gamma(condition_id)
                    if gamma_q:
                        result["question"] = gamma_q
                self._market_cache[condition_id] = result
                return result
        except Exception as e:
            log.debug(f"CLOB market info haku epäonnistui: {e}")
        self._market_cache[condition_id] = {}
        return {}

    def _get_question_from_gamma(self, condition_id: str) -> str:
        """
        Yrittää hakea markkinan nimen Gamma API:sta.
        Koska conditionId-parametri ei toimi, haetaan slug CLOB:sta ensin.
        """
        try:
            # Hae slug CLOB API:sta
            r = requests.get(f"{CLOB_BASE}/markets/{condition_id}", timeout=5)
            if r.status_code == 200:
                market_slug = r.json().get("market_slug", "")
                if market_slug:
                    r2 = requests.get(
                        f"{GAMMA_BASE}/markets",
                        params={"slug": market_slug},
                        timeout=5
                    )
                    if r2.status_code == 200:
                        data = r2.json()
                        if data:
                            m = data[0] if isinstance(data, list) else data
                            return m.get("question", "")
        except Exception:
            pass
        return ""

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