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

_edge_detector_instance = None


def _get_edge_detector():
    global _edge_detector_instance
    if _edge_detector_instance is None:
        from edge_detector import EdgeDetector
        _edge_detector_instance = EdgeDetector()
    return _edge_detector_instance


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
        self.min_order_usdc  = float(os.getenv("MIN_ORDER_SIZE_USDC", 10))
        self.max_order_usdc  = float(os.getenv("MAX_ORDER_SIZE_USDC", 40))
        self.min_max_profit_usdc = float(os.getenv("MIN_MAX_PROFIT_USDC", 5))
        self.min_positive_roi_support = int(os.getenv("MIN_POSITIVE_ROI_SUPPORT", 1))
        self.unknown_support_override = int(os.getenv("UNKNOWN_SUPPORT_OVERRIDE", 8))

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
                    "weight":    wallet.get("wallet_weight", 1.0),
                    "roi":       wallet.get("wallet_roi", 0.0),
                    "reliable":  wallet.get("wallet_reliable", False),
                    "category_weights": wallet.get("category_weights", {}),
                    "active_recently": wallet.get("active_recently", False),
                    "trades_14d": wallet.get("trades_14d", 0),
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
            market_type = self._classify_market(question)

            for outcome, supporters in outcomes.items():
                unique_wallets = {s["wallet"] for s in supporters}
                total_size     = sum(s["size_usdc"] for s in supporters)

                by_wallet = {}
                for s in supporters:
                    addr = s["wallet"]
                    if addr not in by_wallet or s["size_usdc"] > by_wallet[addr]["size_usdc"]:
                        by_wallet[addr] = s

                weighted_support = sum(
                    self._wallet_market_weight(s, market_type)
                    for s in by_wallet.values()
                )
                positive_roi_support = sum(
                    1 for s in by_wallet.values()
                    if s.get("reliable", False) and s.get("roi", 0.0) > 0
                )
                category_positive_support = sum(
                    1 for s in by_wallet.values()
                    if self._wallet_category_positive(s, market_type)
                )
                active_support = sum(1 for s in by_wallet.values() if s.get("active_recently", False))
                reliable_support = sum(1 for s in by_wallet.values() if s.get("reliable", False))
                unknown_support = len(unique_wallets) - reliable_support

                if (len(unique_wallets) >= self.smart_threshold and
                        total_size >= self.min_signal_size and
                        self._passes_wallet_quality(
                            market_type=market_type,
                            support_count=len(unique_wallets),
                            weighted_support=weighted_support,
                            positive_roi_support=positive_roi_support,
                            category_positive_support=category_positive_support,
                            active_support=active_support,
                        )):
                    signals.append({
                        "market_id":        market_id,
                        "question":         question,
                        "end_date":         end_date if end_date else "?",
                        "outcome":          outcome,
                        "market_type":      market_type,
                        "support_count":    len(unique_wallets),
                        "weighted_support": round(weighted_support, 2),
                        "positive_roi_support": positive_roi_support,
                        "category_positive_support": category_positive_support,
                        "active_support": active_support,
                        "reliable_support": reliable_support,
                        "unknown_support": unknown_support,
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

        try:
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

            signal["token_id"] = token_id
            signal["token_price"] = token_price
            signal["market_type"] = signal.get("market_type") or self._classify_market(signal.get("question", ""))

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
                signal["intelligence"] = intel
            except Exception as e:
                log.debug(f"Intelligence epäonnistui: {e}")
            try:
                edge_result = _get_edge_detector().should_buy(signal, token_price)
                signal["edge"] = edge_result
                if not edge_result.get("approved", False):
                    log.warning(f"EdgeDetector hylkäsi: {edge_result.get('reason', '')}")
                    return False
            except Exception as e:
                if os.getenv("EDGE_DETECTOR_FAIL_OPEN", "false").lower() == "true":
                    log.warning(f"EdgeDetector epäonnistui, fail-open: {e}")
                    edge_result = {
                        "approved": True,
                        "reason": "EdgeDetector fail-open",
                        "edge": 0.0,
                        "our_probability": token_price,
                        "confidence": "low",
                    }
                    signal["edge"] = edge_result
                else:
                    log.warning(f"EdgeDetector epäonnistui — ohitetaan signaali: {e}")
                    return False

            price_check = self._passes_price_rules(signal["market_type"], token_price, signal.get("edge", {}))
            if not price_check["approved"]:
                log.warning(f"Riskisäännöt hylkäsivät edgen jälkeen: {price_check['reason']}")
                return False

            order_size = self._calculate_order_size(signal)
            profit_check = self._passes_profit_floor(signal["market_type"], token_price, order_size)
            if not profit_check["approved"]:
                log.warning(f"Riskisäännöt hylkäsivät tuotto-riskin: {profit_check['reason']}")
                return False

            if self.dry_run:
                edge = signal.get("edge", {})
                log.info(
                    f"[DRY RUN] {signal.get('question','')[:35]} | "
                    f"{signal['outcome']} | {order_size} USDC | "
                    f"w_support={signal.get('weighted_support', '?')} | "
                    f"edge={edge.get('edge', 0):+.3f} conf={edge.get('confidence', '?')}"
                )
                self._executed_today.add(sig_key)
                self._save_executed()
                signal["_actual_order_size"] = 0
                return True

            if not os.getenv("CLOB_API_KEY"):
                log.error("CLOB_API_KEY puuttuu.")
                return False

            from py_clob_client_v2 import (
                ClobClient, ApiCreds, MarketOrderArgs, OrderArgs,
                OrderType, Side, PartialCreateOrderOptions
            )

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
                token_size = round(order_size / exec_price, 6)
                log.info(f"GTC makro: {exec_price} | {order_size} USDC | {token_size} tokenia")
                resp = client.create_and_post_order(
                    order_args=OrderArgs(
                        token_id=token_id,
                        price=exec_price,
                        size=token_size,
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
            filled_usdc, filled_tokens = self._extract_fill_amounts(resp, order_size, exec_price)

            if status in ("matched", "delayed"):
                token_amount = round(filled_tokens, 4)
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
            signal["_actual_order_size"] = filled_usdc if status in ("matched", "delayed") else 0
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

    def _calculate_order_size(self, signal: Dict[str, Any]) -> float:
        """Skaalaa panos markkinatyypin, edgen ja confidence-tason mukaan."""
        edge_info = signal.get("edge") or {}
        edge = max(0.0, float(edge_info.get("edge", 0.0) or 0.0))
        confidence = str(edge_info.get("confidence", "medium")).lower()
        market_type = signal.get("market_type") or self._classify_market(signal.get("question", ""))

        confidence_base = {
            "high": float(os.getenv("ORDER_SIZE_HIGH_CONFIDENCE", 40)),
            "medium": float(os.getenv("ORDER_SIZE_MEDIUM_CONFIDENCE", 20)),
            "low": float(os.getenv("ORDER_SIZE_LOW_CONFIDENCE", 10)),
        }.get(confidence, float(os.getenv("ORDER_SIZE_MEDIUM_CONFIDENCE", 20)))

        edge_multiplier = 1.0
        if edge >= 0.20:
            edge_multiplier = 1.5
        elif edge >= 0.15:
            edge_multiplier = 1.3
        elif edge >= 0.10:
            edge_multiplier = 1.15

        market_multiplier = {
            "macro": float(os.getenv("MACRO_ORDER_MULTIPLIER", 1.0)),
            "sports": float(os.getenv("SPORTS_ORDER_MULTIPLIER", 1.0)),
            "esports_match": float(os.getenv("ESPORTS_MATCH_ORDER_MULTIPLIER", 0.75)),
            "esports_map": float(os.getenv("ESPORTS_MAP_ORDER_MULTIPLIER", 0.5)),
            "general": float(os.getenv("GENERAL_ORDER_MULTIPLIER", 0.8)),
        }.get(market_type, 0.8)

        weighted_support = float(signal.get("weighted_support", 1.0) or 1.0)
        support_multiplier = min(1.3, max(0.8, weighted_support / max(self.smart_threshold, 1)))

        size = confidence_base * edge_multiplier * market_multiplier * support_multiplier
        market_cap = self._market_order_cap(market_type)
        return max(round(min(size, self.max_order_usdc, market_cap), 2), self.min_order_usdc)

    def _passes_profit_floor(self, market_type: str, token_price: float, order_size: float) -> Dict[str, Any]:
        """Vältä vetoja, joissa voitto on liian pieni suhteessa panokseen."""
        if self.min_max_profit_usdc <= 0:
            return {"approved": True, "reason": "OK"}

        slippage = float(os.getenv("SLIPPAGE_PCT", 0.02))
        estimated_price = token_price
        if market_type in ("sports", "esports_match", "esports_map"):
            estimated_price = min(token_price * (1 + slippage), 0.90)

        if estimated_price <= 0:
            return {"approved": False, "reason": "virheellinen hinta"}

        max_profit = order_size * ((1 / estimated_price) - 1)
        if max_profit < self.min_max_profit_usdc:
            return {
                "approved": False,
                "reason": (
                    f"maksimivoitto {max_profit:.2f} USDC < "
                    f"{self.min_max_profit_usdc:.2f} USDC "
                    f"(panos {order_size:.2f}, hinta {estimated_price:.3f})"
                ),
            }
        return {"approved": True, "reason": f"maksimivoitto {max_profit:.2f} USDC"}

    def _extract_fill_amounts(self, resp: Any, fallback_usdc: float, price: float) -> tuple:
        """Palauttaa toteutuneen USDC- ja token-määrän CLOB-responsesta."""
        if not isinstance(resp, dict):
            tokens = fallback_usdc / price if price > 0 else 0.0
            return fallback_usdc, tokens

        def _num(key: str) -> float:
            try:
                return float(resp.get(key, 0) or 0)
            except (TypeError, ValueError):
                return 0.0

        making = _num("makingAmount")
        taking = _num("takingAmount")
        if making > 0 and taking > 0:
            return making, taking

        tokens = fallback_usdc / price if price > 0 else 0.0
        return fallback_usdc, tokens

    def _classify_market(self, question: str) -> str:
        q = question.lower()
        esports = any(k in q for k in [
            "lol:", "dota", "cs2", "csgo", "counter-strike", "valorant",
            "lck", "lec", "lpl", "vct", "iem", "pgl", "blast", "dreamleague",
        ])
        if esports:
            if any(k in q for k in ["game ", "map "]):
                return "esports_map"
            return "esports_match"
        if any(k in q for k in [
            "vs.", "vs ", "winner", "match", "series", "spread", "o/u",
            "nba", "nfl", "nhl", "mlb", "atp", "wta", "ufc", "fc ",
        ]):
            return "sports"
        if any(k in q for k in [
            "iran", "trump", "biden", "election", "fed", "btc", "eth",
            "bitcoin", "ethereum", "tariff", "ceasefire", "invade",
            "uranium", "peace deal",
        ]):
            return "macro"
        return "general"

    def _price_bounds(self, market_type: str) -> tuple:
        bounds = {
            "macro": (
                float(os.getenv("MACRO_MIN_TOKEN_PRICE", 0.20)),
                float(os.getenv("MACRO_MAX_TOKEN_PRICE", 0.85)),
            ),
            "sports": (
                float(os.getenv("SPORTS_MIN_TOKEN_PRICE", 0.25)),
                float(os.getenv("SPORTS_MAX_TOKEN_PRICE", 0.85)),
            ),
            "esports_match": (
                float(os.getenv("ESPORTS_MATCH_MIN_TOKEN_PRICE", 0.30)),
                float(os.getenv("ESPORTS_MATCH_MAX_TOKEN_PRICE", 0.78)),
            ),
            "esports_map": (
                float(os.getenv("ESPORTS_MAP_MIN_TOKEN_PRICE", 0.35)),
                float(os.getenv("ESPORTS_MAP_MAX_TOKEN_PRICE", 0.70)),
            ),
            "general": (
                float(os.getenv("GENERAL_MIN_TOKEN_PRICE", 0.25)),
                float(os.getenv("GENERAL_MAX_TOKEN_PRICE", 0.80)),
            ),
        }
        return bounds.get(market_type, bounds["general"])

    def _market_order_cap(self, market_type: str) -> float:
        caps = {
            "macro": float(os.getenv("MACRO_MAX_ORDER_SIZE_USDC", self.max_order_usdc)),
            "sports": float(os.getenv("SPORTS_MAX_ORDER_SIZE_USDC", self.max_order_usdc)),
            "esports_match": float(os.getenv("ESPORTS_MATCH_MAX_ORDER_SIZE_USDC", 25)),
            "esports_map": float(os.getenv("ESPORTS_MAP_MAX_ORDER_SIZE_USDC", 15)),
            "general": float(os.getenv("GENERAL_MAX_ORDER_SIZE_USDC", 20)),
        }
        return caps.get(market_type, caps["general"])

    def _wallet_market_weight(self, supporter: Dict[str, Any], market_type: str) -> float:
        category_weights = supporter.get("category_weights") or {}
        category = category_weights.get(market_type) or {}
        weight = float(category.get("weight", supporter.get("weight", 0.7)) or 0.7)
        if not supporter.get("active_recently", False):
            weight = min(weight, float(os.getenv("INACTIVE_SIGNAL_MAX_WEIGHT", 0.8)))
        return weight

    def _wallet_category_positive(self, supporter: Dict[str, Any], market_type: str) -> bool:
        category_weights = supporter.get("category_weights") or {}
        category = category_weights.get(market_type) or {}
        return (
            bool(category.get("reliable", False)) and
            float(category.get("weighted_roi", 0.0) or 0.0) > 0
        )

    def _passes_price_rules(self, market_type: str, token_price: float, edge_info: Dict[str, Any]) -> Dict[str, Any]:
        low, high = self._price_bounds(market_type)
        edge = float(edge_info.get("edge", 0.0) or 0.0)
        confidence = str(edge_info.get("confidence", "low")).lower()

        if confidence == "high" and edge >= 0.15 and market_type in ("macro", "sports", "esports_match"):
            low = max(0.05, low - 0.05)
            high = min(0.90, high + 0.05)

        if token_price < low:
            return {"approved": False, "reason": f"{market_type} hinta liian matala {token_price:.3f} < {low:.2f}"}
        if token_price > high:
            return {"approved": False, "reason": f"{market_type} hinta liian korkea {token_price:.3f} > {high:.2f}"}
        return {"approved": True, "reason": "OK"}

    def _passes_wallet_quality(
        self,
        market_type: str,
        support_count: int,
        weighted_support: float,
        positive_roi_support: int,
        category_positive_support: int,
        active_support: int,
    ) -> bool:
        min_active = int(os.getenv("MIN_ACTIVE_SUPPORT", 1))
        if active_support < min_active:
            return False

        if category_positive_support >= self.min_positive_roi_support:
            pass
        elif positive_roi_support >= self.min_positive_roi_support and market_type in ("macro", "sports", "general"):
            pass
        elif support_count < self.unknown_support_override:
            return False

        if market_type == "esports_map":
            min_support = int(os.getenv("ESPORTS_MAP_MIN_SUPPORT", max(self.smart_threshold + 2, 6)))
            return support_count >= min_support and weighted_support + 1e-9 >= min_support * 0.8
        if market_type == "esports_match":
            min_support = int(os.getenv("ESPORTS_MATCH_MIN_SUPPORT", max(self.smart_threshold + 1, 5)))
            return support_count >= min_support and weighted_support + 1e-9 >= min_support * 0.75
        return True

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
