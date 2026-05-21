"""
=============================================================================
tracker.py – SignalTracker  (v7.0 – 3 bugia korjattu)
=============================================================================
KORJAUKSET v6.0 → v7.0:

  BUG #1  _load_executed hylkäsi kaikki vanhat signaalit koska ne
          sisältävät '_'-merkin (esim. "0xabc_AURORA")
          → Botti unohti ostot ja saattoi ostaa saman markkinan uudelleen
          → Korjaus: tallennetaan market_id ilman outcome-suffiksia,
            ja vanha formaatti luetaan oikein

  BUG #2  score_wallets_batch() kutsuttiin sekä main.py:ssä että
          tracker.py:ssä → tuplattu API-kutsujen määrä
          → Korjaus: process() ottaa wallet_scores-parametrin
            jos main.py on jo laskenut sen, ei lasketa uudelleen

  BUG #3  Signaalit järjestettiin support_count:lla vaikka
          weighted_support on parempi mittari (huomioi track recordin)
          → Korjaus: järjestys (weighted_support, total_size_usdc)
=============================================================================
"""

from typing import Optional, Dict, List, Any, Set
import os
import logging
import requests
from datetime import datetime, timezone, timedelta, date
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
        headers = client._create_l2_headers("GET", "/balance-allowance", None)
        r = requests.get(
            f"{CLOB_BASE}/balance-allowance",
            headers=headers,
            params={"asset_type": "COLLATERAL", "signature_type": 2},
            timeout=8
        )
        if r.status_code == 200:
            return float(r.json().get("balance", 0)) / 1e6
    except Exception as e:
        log.debug(f"Saldon haku v2 epäonnistui: {e}")
    return float(os.getenv("CURRENT_BANKROLL_USDC", 100.0))


class SignalTracker:

    def __init__(self, smart_threshold: int = 2, dry_run: bool = True):
        self.smart_threshold = smart_threshold
        self.dry_run         = dry_run
        self.min_signal_size = float(os.getenv("MIN_SIGNAL_SIZE_USDC", 50000))
        self.max_order_usdc  = float(os.getenv("MAX_ORDER_SIZE_USDC", 5))

        self._executed_file   = "executed_today.json"
        self._executed_today: Set[str] = set()  # Sisältää vain market_id:t (ei outcomea)
        self._load_executed()
        self._market_cache: Dict[str, Dict] = {}

        if dry_run:
            log.warning("DRY RUN -tila – ostoja ei tehdä.")
        else:
            log.warning("LIVE-tila – OIKEAT ostot käytössä!")

    # ------------------------------------------------------------------
    # BUG #1 KORJAUS: 48h muisti toimii oikein
    # ------------------------------------------------------------------

    def _load_executed(self):
        """
        Lataa ostetut markkinat tiedostosta.
        Muisti: 48h — estää saman markkinan ostamisen uudelleen.

        BUG #1 KORJAUS: Vanha formaatti sisälsi outcome-suffiksin
        (esim. "0xabc_AURORA") joka filtteröitiin pois '_' tarkistuksella.
        Nyt tallennetaan ja luetaan pelkkä market_id ilman suffiksia.
        """
        import json as _json
        try:
            with open(self._executed_file, "r") as f:
                data = _json.load(f)

            signals = data.get("signals", [])
            if not signals:
                return

            cutoff = (datetime.now() - timedelta(hours=48)).isoformat()

            # Uusi formaatti: lista dictionaryja
            if isinstance(signals[0], dict):
                for s in signals:
                    bought_at = s.get("bought_at", "")
                    if bought_at > cutoff:
                        market_id = s.get("market_id", "")
                        if market_id:
                            self._executed_today.add(market_id)

            # BUG #1 KORJAUS: Vanha formaatti — stringejä kuten "0xabc_AURORA"
            # Otetaan market_id (osa ennen '_') eikä hylätä koko stringiä
            elif isinstance(signals[0], str):
                for s in signals:
                    if s.startswith("0x"):
                        # Erottele market_id outcome-suffiksista
                        market_id = s.split("_")[0] if "_" in s else s
                        self._executed_today.add(market_id)

            log.info(f"Ladattu {len(self._executed_today)} ostettua markkinaa (48h muisti).")

        except (FileNotFoundError, Exception):
            pass

    def _save_executed(self):
        """Tallentaa ostetut markkinat uudella formaatilla (dict + timestamp)."""
        import json as _json
        try:
            # Lataa vanhat merkinnät
            try:
                with open(self._executed_file, "r") as f:
                    data = _json.load(f)
                existing = data.get("signals", [])
                # Konvertoi vanha formaatti uuteen
                if existing and isinstance(existing[0], str):
                    existing = []
            except Exception:
                existing = []

            existing_ids = {s.get("market_id") for s in existing if isinstance(s, dict)}

            for market_id in self._executed_today:
                if market_id not in existing_ids:
                    existing.append({
                        "market_id": market_id,
                        "bought_at": datetime.now().isoformat()
                    })

            with open(self._executed_file, "w") as f:
                _json.dump({"signals": existing}, f)
        except Exception as e:
            log.warning(f"Signaalien tallennus epäonnistui: {e}")

    # ------------------------------------------------------------------
    # BUG #2 KORJAUS: process() ottaa valmiit wallet_scores parametrina
    # ------------------------------------------------------------------

    def process(
        self,
        qualified_wallets: List[Dict],
        raw_trades:        List[Dict],
        wallet_scores:     Dict = None   # BUG #2 KORJAUS: ei lasketa uudelleen
    ) -> List[Dict]:
        """
        Prosessoi kvalifioituneet lompakot signaaleiksi.

        Args:
            qualified_wallets: Analyzer.analyze():n tulos
            raw_trades:        Raaka kauppalista fetcheriltä
            wallet_scores:     Valmiit scorer-tulokset main.py:ltä.
                               Jos None, lasketaan tässä (fallback).
        """
        if not qualified_wallets:
            return []

        # Rakenna market_support: {market_id: {outcome: [supporters]}}
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
                    "weight":    wallet.get("wallet_weight", 1.0),  # Suoraan analyzer-tuloksesta
                })

        # Hae markkinatiedot rinnakkain
        from concurrent.futures import ThreadPoolExecutor, as_completed
        market_ids   = list(market_support.keys())
        market_infos: Dict[str, Dict] = {}
        max_workers  = int(os.getenv("FETCH_WORKERS", 4))

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(self._get_market_info_clob, mid): mid for mid in market_ids}
            for future in as_completed(futures):
                try:
                    mid  = futures[future]
                    info = future.result()
                    market_infos[mid] = info or {}
                except Exception:
                    pass

        # BUG #2 KORJAUS: Käytä valmiita scorer-tuloksia jos saatavilla
        if wallet_scores is None:
            log.debug("wallet_scores puuttuu — lasketaan tracker.py:ssä (fallback)")
            try:
                from wallet_scorer import score_wallets_batch
                history_cache = {
                    w["address"].lower(): w.get("all_trades", [])
                    for w in qualified_wallets
                }
                wallet_scores = score_wallets_batch(qualified_wallets, history_cache)
            except Exception as e:
                log.debug(f"Wallet scoring epäonnistui: {e}")
                wallet_scores = {}
        else:
            log.debug(f"Käytetään valmiita wallet_scores ({len(wallet_scores)} lompakon)")

        # Rakenna signaalit
        signals = []
        for market_id, outcomes in market_support.items():
            market_info = market_infos.get(market_id, {})
            if not market_info or not market_info.get("accepting_orders", False):
                continue

            # Hylkää markkinat joissa hinta jo yli 0.90 (konsenssus selvä)
            tokens = market_info.get("tokens", [])
            prices = [float(t.get("price", 0.5)) for t in tokens if t.get("price")]
            if prices and max(prices) > 0.90:
                continue

            question = market_info.get("question", market_id[:20])
            end_date = market_info.get("end_date_iso", "")

            for outcome, supporters in outcomes.items():
                unique_wallets = {s["wallet"] for s in supporters}
                total_size     = sum(s["size_usdc"] for s in supporters)

                # BUG #3 KORJAUS: Käytä wallet.weight suoraan supporter-listasta
                # (analyzer on jo liittänyt painon, ei tarvitse hakea scorer-dictistä uudelleen)
                weighted_support = sum(
                    s.get("weight", wallet_scores.get(s["wallet"], {}).get("weight", 1.0))
                    for s in supporters
                    if s["wallet"] in unique_wallets
                )

                if (len(unique_wallets) >= self.smart_threshold and
                        total_size >= self.min_signal_size):
                    signals.append({
                        "market_id":        market_id,
                        "question":         question,
                        "end_date":         end_date[:10] if end_date else "?",
                        "outcome":          outcome,
                        "support_count":    len(unique_wallets),
                        "weighted_support": round(weighted_support, 2),
                        "supporters":       list(unique_wallets),
                        "total_size_usdc":  total_size,
                        "timestamp":        datetime.now(timezone.utc).isoformat(),
                    })

        # Yksi paras per markkina
        best_per_market: Dict[str, Dict] = {}
        for sig in signals:
            mid = sig["market_id"]
            if mid not in best_per_market:
                best_per_market[mid] = sig
            else:
                current = best_per_market[mid]
                # BUG #3 KORJAUS: Järjestä weighted_support:lla, ei support_count:lla
                if (sig["weighted_support"], sig["total_size_usdc"]) > \
                   (current["weighted_support"], current["total_size_usdc"]):
                    best_per_market[mid] = sig

        signals = list(best_per_market.values())
        # BUG #3 KORJAUS: Lopullinen järjestys weighted_support:lla
        signals.sort(
            key=lambda s: (s["weighted_support"], s["total_size_usdc"]),
            reverse=True
        )
        return signals

    # ------------------------------------------------------------------
    # Tilauksen toteutus (ennallaan v6.0:sta, ei bugia)
    # ------------------------------------------------------------------

    def execute_order(self, signal: Dict[str, Any]) -> bool:
        sig_key = signal["market_id"]

        if sig_key in self._executed_today:
            log.debug(f"Duplikaatti (48h muisti): {sig_key[:20]} — ohitetaan.")
            return False

        # Tarkista vastakkainen positio
        try:
            import json as _j
            with open("open_positions.json", "r") as f:
                open_pos = _j.load(f).get("positions", [])
            for pos in open_pos:
                if pos.get("market_id") == sig_key:
                    existing_outcome = pos.get("outcome", "")
                    new_outcome      = signal.get("outcome", "")
                    if existing_outcome != new_outcome:
                        log.warning(f"Vastakkainen positio auki: {existing_outcome} vs {new_outcome} — ohitetaan.")
                    else:
                        log.debug(f"Sama positio jo auki: {existing_outcome} — ohitetaan.")
                    return False
        except Exception:
            pass

        order_size = min(self.max_order_usdc, signal["total_size_usdc"] * 0.01)
        order_size = max(round(order_size, 2), 1.0)

        if self.dry_run:
            log.info(
                f"[DRY RUN] {signal.get('question','')[:35]} | "
                f"{signal['outcome']} | {order_size} USDC | "
                f"w_support={signal.get('weighted_support', '?')}"
            )
            self._executed_today.add(sig_key)
            self._save_executed()
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

            import time; time.sleep(1.0)
            r = requests.get(f"{CLOB_BASE}/markets/{condition_id}", timeout=8)
            if r.status_code == 429:
                import time; time.sleep(3.0)
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

            def _normalize(s):
                import re
                s = s.upper()
                s = s.replace("'", "").replace("'", "").replace("`", "")
                s = re.sub(r'[^A-Z0-9 ]', ' ', s)
                s = re.sub(r'\s+', ' ', s).strip()
                return s

            outcome_norm = _normalize(outcome_name)

            for token in tokens_list:
                t_norm = _normalize(str(token.get("outcome", "")))
                if t_norm == outcome_norm or t_norm.replace(" ", "") == outcome_norm.replace(" ", ""):
                    token_id    = token.get("token_id")
                    token_price = round(float(token.get("price", 0.5)), 3)
                    break

            if not token_id:
                for token in tokens_list:
                    t_norm = _normalize(str(token.get("outcome", "")))
                    if outcome_norm in t_norm or t_norm in outcome_norm:
                        token_id    = token.get("token_id")
                        token_price = round(float(token.get("price", 0.5)), 3)
                        log.info(f"Fuzzy match: '{outcome_name}' → '{token.get('outcome')}'")
                        break

            if not token_id:
                log.error(f"Outcome '{outcome_name}' ei löydy: {[t.get('outcome') for t in tokens_list]}")
                return False

            if token_price < 0.05 or token_price > 0.90:
                log.warning(f"Hinta {token_price} äärimmäinen — ohitetaan.")
                return False

            # Tarkista CLOB:sta onko markkina vielä aktiivinen
            try:
                import time as _time
                _time.sleep(0.5)
                r_check = requests.get(
                    f"{CLOB_BASE}/markets-by-token/{token_id}",
                    timeout=5
                )
                if r_check.status_code == 200:
                    mdata = r_check.json()
                    if isinstance(mdata, dict):
                        if not mdata.get("accepting_orders", True):
                            log.warning(f"Markkina ei enää hyväksy tilauksia — ohitetaan.")
                            return False
            except Exception:
                pass

            # Intelligence-tarkistus
            try:
                from intelligence import analyze_signal
                intel = analyze_signal(signal, token_id, token_price)
                if not intel["approved"]:
                    log.warning(f"Intelligence hylkäsi: {intel['reason']}")
                    return False
            except Exception as e:
                log.debug(f"Intelligence epäonnistui: {e}")
# EdgeDetector — Claude API analysoi edgen
            try:
                from edge_detector import EdgeDetector as _ED
                _ed = _ED()
                edge_result = _ed.should_buy(signal, token_price)
                if not edge_result["approved"]:
                    log.info(f"⏭️  EdgeDetector ohitti: {edge_result['reason'][:80]}")
                    return False
                signal["_edge"]       = edge_result.get("edge", 0.0)
                signal["_confidence"] = edge_result.get("confidence", "medium")
                log.info(f"✅ EdgeDetector hyväksyi: edge={edge_result.get('edge',0):+.2f} conf={edge_result.get('confidence','?')}")
            except Exception as e:
                log.debug(f"EdgeDetector epäonnistui — jatketaan ilman: {e}")
            # Tick size
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
            options   = PartialCreateOrderOptions(tick_size=tick_size)

            if is_sports:
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

            if status in ("matched", "delayed"):
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
                try:
                    from notifier import notifier
                    if notifier:
                        notifier.notify_buy(signal, exec_price, order_size, status)
                except Exception:
                    pass
            else:
                log.info(f"Status: {status} — positiota ei lisätty")

            self._executed_today.add(sig_key)
            self._save_executed()
            signal["_actual_order_size"] = order_size if status in ("matched", "delayed") else 0
            return True

        except Exception as e:
            err = str(e)
            if "fully filled or killed" in err or "FOK" in err:
                log.info("FOK ei täyttynyt — normaali tilanne, ohitetaan.")
                return False
            log.error(f"CLOB-osto epäonnistui: {e}")
            return False

    # ------------------------------------------------------------------
    # Apumetodit
    # ------------------------------------------------------------------

    def _get_market_info_clob(self, condition_id: str) -> Optional[Dict]:
        if condition_id in self._market_cache:
            return self._market_cache[condition_id]
        try:
            r = requests.get(f"{CLOB_BASE}/markets/{condition_id}", timeout=8)
            if r.status_code == 200:
                data   = r.json()
                result = {
                    "question":         data.get("question", condition_id[:20]),
                    "end_date_iso":     data.get("end_date_iso", ""),
                    "accepting_orders": data.get("accepting_orders", False),
                    "tokens":           data.get("tokens", []),
                }
                # Fallback: hae kysymys Gamma API:sta jos CLOB ei palauta sitä
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
