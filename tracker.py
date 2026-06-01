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
import json
import logging
import requests
from datetime import datetime, timezone, timedelta, date
from collections import defaultdict
from state_store import read_json, write_json

log = logging.getLogger("Scout.Tracker")

CLOB_BASE  = "https://clob.polymarket.com"
GAMMA_BASE = "https://gamma-api.polymarket.com"
_SIGNAL_STATE_FILE = "signal_state.json"
_SIGNAL_SNAPSHOTS_FILE = "signal_snapshots.jsonl"
_PENDING_ORDERS_FILE = "pending_orders.json"

_edge_detector_instance = None


def _get_edge_detector():
    global _edge_detector_instance
    if _edge_detector_instance is None:
        from edge_detector import EdgeDetector
        _edge_detector_instance = EdgeDetector()
    return _edge_detector_instance


def get_usdc_balance_v2(allow_fallback: bool = False) -> float:
    """Hakee vapaan USDC-saldon CLOBista."""
    fallback = float(os.getenv("CURRENT_BANKROLL_USDC", 100.0))
    try:
        from py_clob_client_v2 import ClobClient, ApiCreds
        from py_clob_client_v2.clob_types import AssetType, BalanceAllowanceParams
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
        params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
        resp = client.get_balance_allowance(params)
        balance = float(resp.get("balance", 0) or 0) / 1e6

        if balance <= 0:
            client.update_balance_allowance(params)
            resp = client.get_balance_allowance(params)
            balance = float(resp.get("balance", 0) or 0) / 1e6

        return balance
    except Exception as e:
        log.warning(f"USDC-saldon haku epäonnistui: {e}")
    if allow_fallback:
        log.warning(f"Käytetään fallback-kassaa CURRENT_BANKROLL_USDC={fallback:.2f}")
        return fallback

    log.error("Live-USDC saldoa ei saatu haettua - palautetaan 0.00, jotta ostot pysähtyvät")
    return 0.0


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
        self.max_signal_price_move = float(os.getenv("MAX_SIGNAL_PRICE_MOVE", 0.10))
        self.edge_reject_cooldown_cycles = int(os.getenv("EDGE_REJECT_COOLDOWN_CYCLES", 10))
        self.edge_cooldown_price_move = float(os.getenv("EDGE_COOLDOWN_PRICE_MOVE", 0.03))
        self.edge_cooldown_weight_move = float(os.getenv("EDGE_COOLDOWN_WEIGHT_MOVE", 2.0))

        self._executed_file   = "executed_today.json"
        self._executed_today: Set[str] = set()  # Sisältää vain market_id:t (ei outcomea)
        self._load_executed()
        self._market_cache: Dict[str, Dict] = {}
        self._cycle_index = 0
        self._edge_reject_cooldowns: Dict[str, Dict[str, Any]] = {}
        self._pending_orders = self._load_pending_orders()
        self._signal_state: Dict[str, Dict[str, Any]] = read_json(_SIGNAL_STATE_FILE, {"signals": {}})
        if not isinstance(self._signal_state, dict):
            self._signal_state = {"signals": {}}
        self.last_funnel_stats: Dict[str, int] = {}

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
        try:
            data = read_json(self._executed_file, {"signals": []})

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

        except Exception as e:
            log.debug(f"Ostomuistin lataus epäonnistui: {e}")

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

    def _save_executed(self):
        """Tallentaa ostetut markkinat atomisesti."""
        try:
            data = read_json(self._executed_file, {"signals": []})
            existing = data.get("signals", [])
            if existing and isinstance(existing[0], str):
                existing = []

            existing_ids = {s.get("market_id") for s in existing if isinstance(s, dict)}
            for market_id in self._executed_today:
                if market_id not in existing_ids:
                    existing.append({
                        "market_id": market_id,
                        "bought_at": datetime.now().isoformat(),
                    })

            write_json(self._executed_file, {"signals": existing})
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
        self._cycle_index += 1
        self._prune_edge_cooldowns()
        self.reconcile_pending_orders()
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
        funnel = defaultdict(int)
        funnel["markets_seen"] = len(market_support)
        now_dt = datetime.now(timezone.utc)
        for market_id, outcomes in market_support.items():
            market_info = market_infos.get(market_id, {})
            if not market_info or not market_info.get("accepting_orders", False):
                funnel["market_closed_or_missing"] += 1
                continue

            tokens = market_info.get("tokens", [])
            question = market_info.get("question", market_id[:20])
            end_date = market_info.get("end_date_iso", "")
            market_type = self._classify_market(question)

            for outcome, supporters in outcomes.items():
                funnel["outcome_candidates"] += 1
                token_price = self._token_price_for_outcome(tokens, outcome)
                if token_price is None:
                    funnel["missing_price"] += 1
                    continue

                price_check = self._passes_candidate_price_rules(market_type, token_price)
                if not price_check["approved"]:
                    funnel["price_extreme"] += 1
                    continue

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
                high_weight_support = sum(
                    1 for s in by_wallet.values()
                    if self._wallet_market_weight(s, market_type) >= 1.5
                )
                timing = self._update_signal_timing(
                    market_id=market_id,
                    outcome=outcome,
                    token_price=token_price,
                    now_dt=now_dt,
                )
                if abs(timing["price_move_since_first_seen"]) > self.max_signal_price_move:
                    funnel["late_or_volatile"] += 1
                    self._append_signal_snapshot({
                        "market_id": market_id,
                        "question": question,
                        "outcome": outcome,
                        "market_type": market_type,
                        "token_price": token_price,
                        **timing,
                    }, status="rejected_late_or_volatile")
                    continue

                if (len(unique_wallets) >= self.smart_threshold and
                        total_size >= self.min_signal_size and
                        self._passes_wallet_quality(
                            market_type=market_type,
                            support_count=len(unique_wallets),
                            weighted_support=weighted_support,
                            positive_roi_support=positive_roi_support,
                            category_positive_support=category_positive_support,
                            active_support=active_support,
                            high_weight_support=high_weight_support,
                        )):
                    signals.append({
                        "market_id":        market_id,
                        "question":         question,
                        "end_date":         end_date if end_date else "?",
                        "outcome":          outcome,
                        "market_type":      market_type,
                        "token_price":       token_price,
                        "support_count":    len(unique_wallets),
                        "weighted_support": round(weighted_support, 2),
                        "high_weight_support": high_weight_support,
                        "positive_roi_support": positive_roi_support,
                        "category_positive_support": category_positive_support,
                        "active_support": active_support,
                        "reliable_support": reliable_support,
                        "unknown_support": unknown_support,
                        "supporters":       list(unique_wallets),
                        "total_size_usdc":  total_size,
                        "timestamp":        datetime.now(timezone.utc).isoformat(),
                        **timing,
                    })
                    funnel["accepted_candidates"] += 1
                    self._append_signal_snapshot(signals[-1], status="accepted_candidate")
                else:
                    funnel["wallet_quality_or_size"] += 1

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
        self._save_signal_state()
        self.last_funnel_stats = dict(funnel)
        log.info(
            "Signal funnel: "
            f"markets={funnel['markets_seen']} | "
            f"outcomes={funnel['outcome_candidates']} | "
            f"accepted={funnel['accepted_candidates']} | "
            f"closed/missing={funnel['market_closed_or_missing']} | "
            f"price_extreme={funnel['price_extreme']} | "
            f"late/volatile={funnel['late_or_volatile']} | "
            f"wallet_quality/size={funnel['wallet_quality_or_size']} | "
            f"missing_price={funnel['missing_price']}"
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
            open_pos = read_json("open_positions.json", {"positions": []}).get("positions", [])
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

            cooldown = self._edge_cooldown_reason(signal, token_price)
            if cooldown:
                log.info(cooldown)
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
                signal["intelligence"] = intel
            except Exception as e:
                log.debug(f"Intelligence epäonnistui: {e}")
            try:
                edge_result = _get_edge_detector().should_buy(signal, token_price)
                signal["edge"] = edge_result
                if not edge_result.get("approved", False):
                    self._remember_edge_reject(signal, token_price, edge_result.get("reason", ""))
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
            has_fill_amounts = filled_usdc > 0 and filled_tokens > 0
            actual_fill_price = (
                round(filled_usdc / filled_tokens, 4)
                if has_fill_amounts and filled_tokens > 0
                else exec_price
            )

            if status == "matched" and has_fill_amounts:
                token_amount = round(filled_tokens, 4)
                bad_fill = self._log_fill_quality(
                    signal=signal,
                    requested_price=exec_price,
                    actual_price=actual_fill_price,
                    filled_usdc=filled_usdc,
                    filled_tokens=filled_tokens,
                )
                try:
                    from position_manager import add_position
                    position_signal = self._position_signal_with_fill_guard(
                        signal=signal,
                        bad_fill=bad_fill,
                        requested_price=exec_price,
                        actual_price=actual_fill_price,
                    )
                    add_position(
                        signal=position_signal,
                        token_id=token_id,
                        buy_price=actual_fill_price,
                        amount=token_amount,
                        end_date=signal.get("end_date", "")
                    )
                except Exception as e:
                    log.debug(f"Position lisäys epäonnistui: {e}")
                try:
                    from notifier import notifier
                    if notifier:
                        notifier.notify_buy(signal, actual_fill_price, filled_usdc, status)
                except Exception:
                    pass
            elif status in ("delayed", "live"):
                self._remember_pending_order(
                    resp=resp,
                    signal=signal,
                    token_id=token_id,
                    requested_price=exec_price,
                    order_size=order_size,
                )
                log.warning("Order delayed ilman varmaa fill-määrää — positiota ei lisätä vielä")
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
            signal["_actual_order_size"] = filled_usdc if status == "matched" and has_fill_amounts else 0
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

    def _create_clob_client(self):
        from py_clob_client_v2 import ClobClient, ApiCreds

        creds = ApiCreds(
            api_key=os.getenv("CLOB_API_KEY"),
            api_secret=os.getenv("CLOB_API_SECRET"),
            api_passphrase=os.getenv("CLOB_PASSPHRASE"),
        )
        return ClobClient(
            host=CLOB_BASE,
            chain_id=137,
            key=os.getenv("PRIVATE_KEY"),
            creds=creds,
            signature_type=2,
            funder=os.getenv("PROXY_WALLET_ADDRESS"),
        )

    def _load_pending_orders(self) -> List[Dict[str, Any]]:
        data = read_json(_PENDING_ORDERS_FILE, {"orders": []})
        orders = data.get("orders", []) if isinstance(data, dict) else []
        return orders if isinstance(orders, list) else []

    def _save_pending_orders(self) -> None:
        try:
            write_json(_PENDING_ORDERS_FILE, {"orders": self._pending_orders}, indent=2)
        except Exception as e:
            log.warning(f"Pending-orderien tallennus epaonnistui: {e}")

    def _pending_signal_snapshot(self, signal: Dict[str, Any]) -> Dict[str, Any]:
        keys = [
            "market_id", "question", "outcome", "end_date", "market_type",
            "support_count", "weighted_support", "positive_roi_support",
            "category_positive_support", "active_support", "reliable_support",
            "unknown_support", "total_size_usdc", "token_price", "edge",
            "intelligence",
        ]
        return {key: signal.get(key) for key in keys if key in signal}

    def _remember_pending_order(
        self,
        resp: Any,
        signal: Dict[str, Any],
        token_id: str,
        requested_price: float,
        order_size: float,
    ) -> None:
        if not isinstance(resp, dict):
            return

        order_id = resp.get("orderID") or resp.get("order_id") or resp.get("id")
        if not order_id:
            log.warning("Delayed order ilman orderID:tä - ei voida seurata")
            return

        if any(o.get("order_id") == order_id for o in self._pending_orders):
            return

        self._pending_orders.append({
            "order_id": order_id,
            "token_id": token_id,
            "signal": self._pending_signal_snapshot(signal),
            "requested_price": requested_price,
            "order_size": order_size,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "attempts": 0,
            "last_status": "delayed",
        })
        self._save_pending_orders()
        log.warning(f"Pending order tallennettu seurantaan: {order_id[:12]} | {signal.get('question', '')[:45]}")

    def reconcile_pending_orders(self) -> None:
        if self.dry_run or not self._pending_orders or not os.getenv("CLOB_API_KEY"):
            return

        try:
            client = self._create_clob_client()
        except Exception as e:
            log.debug(f"Pending-order clientin luonti epaonnistui: {e}")
            return

        keep: List[Dict[str, Any]] = []
        changed = False
        max_attempts = int(os.getenv("PENDING_ORDER_MAX_ATTEMPTS", 24))

        for pending in self._pending_orders:
            pending["attempts"] = int(pending.get("attempts", 0)) + 1
            order_id = pending.get("order_id", "")
            try:
                order = self._fetch_order_status(client, order_id)
            except Exception as e:
                log.debug(f"Pending order {order_id[:12]} statushaku epaonnistui: {e}")
                order = None

            if not order:
                if pending["attempts"] <= max_attempts:
                    keep.append(pending)
                else:
                    log.warning(f"Pending order vanheni ilman statusta: {order_id[:12]}")
                    changed = True
                continue

            status = str(order.get("status", "") or order.get("state", "")).lower()
            pending["last_status"] = status or pending.get("last_status", "")
            filled_usdc, filled_tokens = self._extract_fill_amounts(
                order,
                float(pending.get("order_size", 0) or 0),
                float(pending.get("requested_price", 0) or 0),
            )

            if (
                filled_usdc > 0
                and filled_tokens > 0
                and status not in ("delayed", "open", "live", "active", "pending")
            ):
                self._add_pending_position(pending, order, filled_usdc, filled_tokens)
                changed = True
                continue

            if status in ("cancelled", "canceled", "expired", "failed"):
                log.warning(f"Pending order ei tayttynyt: {order_id[:12]} status={status}")
                changed = True
                continue

            keep.append(pending)

        if changed or len(keep) != len(self._pending_orders):
            self._pending_orders = keep
            self._save_pending_orders()
        else:
            self._pending_orders = keep

    def _fetch_order_status(self, client: Any, order_id: str) -> Optional[Dict[str, Any]]:
        if not order_id:
            return None

        if hasattr(client, "get_order"):
            try:
                order = client.get_order(order_id)
                if isinstance(order, dict):
                    return order
            except Exception:
                pass

        if hasattr(client, "get_open_orders"):
            try:
                from py_clob_client_v2.clob_types import OpenOrderParams
                orders = client.get_open_orders(OpenOrderParams(id=order_id), only_first_page=True)
                if orders:
                    first = orders[0]
                    return first if isinstance(first, dict) else getattr(first, "__dict__", None)
            except Exception:
                pass

        return None

    def _add_pending_position(
        self,
        pending: Dict[str, Any],
        order: Dict[str, Any],
        filled_usdc: float,
        filled_tokens: float,
    ) -> None:
        signal = pending.get("signal", {}) if isinstance(pending.get("signal"), dict) else {}
        token_id = pending.get("token_id", "")
        actual_price = round(filled_usdc / filled_tokens, 4) if filled_tokens > 0 else pending.get("requested_price", 0)
        token_amount = round(filled_tokens, 4)

        bad_fill = self._log_fill_quality(
            signal=signal,
            requested_price=float(pending.get("requested_price", 0) or 0),
            actual_price=actual_price,
            filled_usdc=filled_usdc,
            filled_tokens=filled_tokens,
        )

        try:
            from position_manager import add_position
            position_signal = self._position_signal_with_fill_guard(
                signal=signal,
                bad_fill=bad_fill,
                requested_price=float(pending.get("requested_price", 0) or 0),
                actual_price=actual_price,
            )
            add_position(
                signal=position_signal,
                token_id=token_id,
                buy_price=actual_price,
                amount=token_amount,
                end_date=signal.get("end_date", ""),
            )
            log.info(
                f"Pending order matched -> positio lisatty: {signal.get('question', '')[:45]} "
                f"| {signal.get('outcome', '')} @ {actual_price}"
            )
        except Exception as e:
            log.warning(f"Pending-position lisays epaonnistui: {e}")
            return

        try:
            from notifier import notifier
            if notifier:
                notifier.notify_buy(signal, actual_price, filled_usdc, str(order.get("status", "matched")))
        except Exception:
            pass

    def _cooldown_key(self, signal: Dict[str, Any]) -> str:
        return f"{signal.get('market_id', '')}|{signal.get('outcome', '')}".lower()

    def _edge_cooldown_reason(self, signal: Dict[str, Any], token_price: float) -> str:
        if self.edge_reject_cooldown_cycles <= 0:
            return ""

        key = self._cooldown_key(signal)
        entry = self._edge_reject_cooldowns.get(key)
        if not entry:
            return ""

        age = self._cycle_index - int(entry.get("cycle", 0))
        if age >= self.edge_reject_cooldown_cycles:
            self._edge_reject_cooldowns.pop(key, None)
            return ""

        old_price = float(entry.get("price", token_price) or token_price)
        old_weight = float(entry.get("weighted_support", 0.0) or 0.0)
        new_weight = float(signal.get("weighted_support", 0.0) or 0.0)
        price_move = abs(token_price - old_price)
        weight_move = abs(new_weight - old_weight)

        if price_move >= self.edge_cooldown_price_move or weight_move >= self.edge_cooldown_weight_move:
            self._edge_reject_cooldowns.pop(key, None)
            log.info(
                f"Edge cooldown ohitettu: {signal.get('question', '')[:40]} | "
                f"hinta {old_price:.3f}->{token_price:.3f}, w {old_weight:.1f}->{new_weight:.1f}"
            )
            return ""

        remaining = self.edge_reject_cooldown_cycles - age
        return (
            f"Edge cooldown: {signal.get('question', '')[:40]} | {signal.get('outcome', '')} "
            f"ohitetaan {remaining} sykliä (ei hinta/tuki-muutosta)"
        )

    def _remember_edge_reject(self, signal: Dict[str, Any], token_price: float, reason: str) -> None:
        if self.edge_reject_cooldown_cycles <= 0:
            return
        self._edge_reject_cooldowns[self._cooldown_key(signal)] = {
            "cycle": self._cycle_index,
            "price": float(token_price or 0.0),
            "weighted_support": float(signal.get("weighted_support", 0.0) or 0.0),
            "support_count": int(signal.get("support_count", 0) or 0),
            "reason": reason,
        }

    def _prune_edge_cooldowns(self) -> None:
        if not self._edge_reject_cooldowns:
            return
        expired = [
            key for key, entry in self._edge_reject_cooldowns.items()
            if self._cycle_index - int(entry.get("cycle", 0)) >= self.edge_reject_cooldown_cycles
        ]
        for key in expired:
            self._edge_reject_cooldowns.pop(key, None)

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

        matched_tokens = 0.0
        for key in ("size_matched", "sizeMatched", "matched_size", "filledSize", "filled_size"):
            matched_tokens = _num(key)
            if matched_tokens > 0:
                break

        order_price = price
        for key in ("avgPrice", "avg_price", "price", "matched_price"):
            val = _num(key)
            if val > 0:
                order_price = val
                break

        if matched_tokens > 0 and order_price > 0:
            return matched_tokens * order_price, matched_tokens

        if str(resp.get("status", "")).lower() == "delayed":
            return 0.0, 0.0

        tokens = fallback_usdc / price if price > 0 else 0.0
        return fallback_usdc, tokens

    def _log_fill_quality(
        self,
        signal: Dict[str, Any],
        requested_price: float,
        actual_price: float,
        filled_usdc: float,
        filled_tokens: float,
    ) -> bool:
        if requested_price <= 0 or actual_price <= 0:
            return False

        diff = actual_price - requested_price
        slippage_pct = diff / requested_price
        question = signal.get("question", "")[:45]
        outcome = signal.get("outcome", "")

        log.info(
            f"Actual fill: requested={requested_price:.4f} actual={actual_price:.4f} "
            f"slippage={slippage_pct:+.1%} | {filled_usdc:.2f} USDC / {filled_tokens:.4f} tokens | "
            f"{question} | {outcome}"
        )

        bad_fill_pct = float(os.getenv("BAD_FILL_SLIPPAGE_PCT", 0.05))
        bad_fill_abs = float(os.getenv("BAD_FILL_SLIPPAGE_ABS", 0.03))
        if diff > 0 and (slippage_pct >= bad_fill_pct or diff >= bad_fill_abs):
            log.warning(
                f"BAD FILL: requested={requested_price:.4f} actual={actual_price:.4f} "
                f"slippage={slippage_pct:+.1%} diff={diff:+.4f} | "
                f"{question} | {outcome}"
            )
            try:
                from notifier import notifier
                if notifier:
                    notifier.notify_bad_fill(
                        signal=signal,
                        requested_price=requested_price,
                        actual_price=actual_price,
                        slippage_pct=slippage_pct,
                        size=filled_usdc,
                    )
            except Exception:
                pass
            return True
        return False

    def _position_signal_with_fill_guard(
        self,
        signal: Dict[str, Any],
        bad_fill: bool,
        requested_price: float,
        actual_price: float,
    ) -> Dict[str, Any]:
        """Mark clearly bad fills so PositionManager can try to unwind them."""
        if not bad_fill or os.getenv("BAD_FILL_FORCE_EXIT", "true").lower() != "true":
            return signal

        guarded = dict(signal)
        guarded["bad_fill"] = True
        guarded["force_exit"] = True
        guarded["bad_fill_reason"] = (
            f"Bad fill force exit: requested {requested_price:.4f}, "
            f"actual {actual_price:.4f}"
        )
        return guarded

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

    def _passes_candidate_price_rules(self, market_type: str, token_price: float) -> Dict[str, Any]:
        low, high = self._price_bounds(market_type)
        buffer = float(os.getenv("CANDIDATE_PRICE_BUFFER", 0.02))
        low = max(0.01, low - buffer)
        high = min(0.99, high + buffer)
        if token_price < low:
            return {"approved": False, "reason": f"{market_type} candidate price too low {token_price:.3f} < {low:.2f}"}
        if token_price > high:
            return {"approved": False, "reason": f"{market_type} candidate price too high {token_price:.3f} > {high:.2f}"}
        return {"approved": True, "reason": "OK"}

    def _token_price_for_outcome(self, tokens: List[Dict[str, Any]], outcome_name: str) -> Optional[float]:
        def _normalize(s: str) -> str:
            import re
            s = str(s or "").upper()
            s = s.replace("'", "").replace("`", "")
            s = re.sub(r"[^A-Z0-9 ]", " ", s)
            return re.sub(r"\s+", " ", s).strip()

        outcome_norm = _normalize(outcome_name)
        fallback = None
        for token in tokens or []:
            t_norm = _normalize(token.get("outcome", ""))
            if not t_norm:
                continue
            try:
                price = round(float(token.get("price", 0.0)), 3)
            except (TypeError, ValueError):
                continue
            if t_norm == outcome_norm or t_norm.replace(" ", "") == outcome_norm.replace(" ", ""):
                return price
            if outcome_norm in t_norm or t_norm in outcome_norm:
                fallback = price
        return fallback

    def _update_signal_timing(
        self,
        market_id: str,
        outcome: str,
        token_price: float,
        now_dt: datetime,
    ) -> Dict[str, Any]:
        signals = self._signal_state.setdefault("signals", {})
        key = f"{market_id}|{outcome}"
        entry = signals.get(key)
        now_iso = now_dt.isoformat()
        if not entry:
            entry = {
                "first_seen_at": now_iso,
                "price_at_first_seen": token_price,
            }
            signals[key] = entry

        first_seen_at = entry.get("first_seen_at", now_iso)
        first_price = float(entry.get("price_at_first_seen", token_price) or token_price)
        try:
            first_dt = datetime.fromisoformat(first_seen_at)
            age_minutes = max(0.0, (now_dt - first_dt).total_seconds() / 60)
        except Exception:
            age_minutes = 0.0

        entry["last_seen_at"] = now_iso
        entry["last_price"] = token_price
        return {
            "first_seen_at": first_seen_at,
            "price_at_first_seen": round(first_price, 3),
            "signal_age_minutes": round(age_minutes, 1),
            "price_move_since_first_seen": round(token_price - first_price, 3),
        }

    def _save_signal_state(self) -> None:
        signals = self._signal_state.get("signals", {})
        cutoff = datetime.now(timezone.utc) - timedelta(days=int(os.getenv("SIGNAL_STATE_RETENTION_DAYS", 7)))
        kept = {}
        for key, entry in signals.items():
            try:
                last_seen = datetime.fromisoformat(entry.get("last_seen_at") or entry.get("first_seen_at"))
            except Exception:
                continue
            if last_seen >= cutoff:
                kept[key] = entry
        self._signal_state["signals"] = kept
        try:
            write_json(_SIGNAL_STATE_FILE, self._signal_state)
        except Exception as e:
            log.debug(f"Signal state tallennus epÃ¤onnistui: {e}")

    def _append_signal_snapshot(self, signal: Dict[str, Any], status: str) -> None:
        if os.getenv("SIGNAL_SNAPSHOT_ENABLED", "true").lower() != "true":
            return
        snapshot = {
            "recorded_at": datetime.now(timezone.utc).isoformat(),
            "status": status,
            "market_id": signal.get("market_id"),
            "question": signal.get("question"),
            "outcome": signal.get("outcome"),
            "market_type": signal.get("market_type"),
            "token_price": signal.get("token_price"),
            "price_at_first_seen": signal.get("price_at_first_seen"),
            "price_move_since_first_seen": signal.get("price_move_since_first_seen"),
            "signal_age_minutes": signal.get("signal_age_minutes"),
            "support_count": signal.get("support_count"),
            "weighted_support": signal.get("weighted_support"),
            "high_weight_support": signal.get("high_weight_support"),
            "positive_roi_support": signal.get("positive_roi_support"),
            "category_positive_support": signal.get("category_positive_support"),
            "active_support": signal.get("active_support"),
            "reliable_support": signal.get("reliable_support"),
            "unknown_support": signal.get("unknown_support"),
            "total_size_usdc": signal.get("total_size_usdc"),
        }
        try:
            with open(_SIGNAL_SNAPSHOTS_FILE, "a", encoding="utf-8") as f:
                f.write(json.dumps(snapshot, ensure_ascii=True) + "\n")
        except Exception as e:
            log.debug(f"Signal snapshot tallennus epÃ¤onnistui: {e}")

    def _passes_wallet_quality(
        self,
        market_type: str,
        support_count: int,
        weighted_support: float,
        positive_roi_support: int,
        category_positive_support: int,
        active_support: int,
        high_weight_support: int = 0,
    ) -> bool:
        min_active = int(os.getenv("MIN_ACTIVE_SUPPORT", 1))
        if active_support < min_active:
            return False

        min_weighted_ratio = float(os.getenv("MIN_WEIGHTED_SUPPORT_RATIO", 0.75))
        if weighted_support + 1e-9 < support_count * min_weighted_ratio:
            return False

        min_high_weight = int(os.getenv("MIN_HIGH_WEIGHT_SUPPORT", 0))
        if high_weight_support < min_high_weight:
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
