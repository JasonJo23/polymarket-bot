"""
=============================================================================
fetcher.py – PolymarketFetcher  (v6.0 – HISTORY_LIMIT_PER_WALLET)
=============================================================================
KORJAUKSET v5.0 → v6.0:

  PERF #1  limit=500 kovakoodattu per lompakko
           → 250 lompakkoa × 500 = 125 000 kauppaa haettavana
           → Käytännössä 48h dataan riittää 100 kauppaa
           → KORJAUS: luetaan HISTORY_LIMIT_PER_WALLET .env:stä
             oletus 100 (aiempi 500)

  PERF #2  Historia-haku hakee kaiken vaikka tarvitaan vain 48h
           → recent-suodatus tehdään JÄLKEEN haun
           → Ei muutosta logiikkaan mutta pienempi limit auttaa
=============================================================================
"""

import os
import time
import logging
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional

log = logging.getLogger("Scout.Fetcher")

GAMMA_BASE = "https://gamma-api.polymarket.com"
DATA_BASE  = "https://data-api.polymarket.com"


class PolymarketFetcher:

    def __init__(self):
        max_workers = int(os.getenv("FETCH_WORKERS", 16))
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=max_workers,
            pool_maxsize=max_workers + 4
        )
        self.session = requests.Session()
        self.session.mount("https://", adapter)
        self.session.mount("http://",  adapter)
        self.session.headers.update({
            "Accept":     "application/json",
            "User-Agent": "PolymarketScout/6.0"
        })
        self.request_delay        = float(os.getenv("REQUEST_DELAY_SECONDS", 0.3))
        self.max_retries          = int(os.getenv("MAX_RETRIES", 3))
        self.top_markets          = int(os.getenv("TOP_MARKETS", 10))
        self.top_holders          = int(os.getenv("TOP_HOLDERS", 15))
        self.closing_days         = int(os.getenv("CLOSING_DAYS", 3))
        self.min_volume_24h       = float(os.getenv("MIN_VOLUME_24H", 50000))
        self.market_scan_limit    = int(os.getenv("MARKET_SCAN_LIMIT", 500))
        self.market_page_size     = int(os.getenv("MARKET_PAGE_SIZE", 100))
        self.history_limit        = int(os.getenv("HISTORY_LIMIT_PER_WALLET", 100))  # PERF #1
        self._history_cache: Dict[str, List[Dict]] = {}

    # ------------------------------------------------------------------
    # Päämetodi
    # ------------------------------------------------------------------

    def fetch_recent_trades(self, hours_back: int = 48) -> List[Dict[str, Any]]:
        """
        Closing Soon -strategia:
          1. Hae korkeavolyymiset markkinat jotka sulkeutuvat pian
          2. Kerää top-holderit näiltä markkinoilta
          3. Hae jokaisen holderin historia (HISTORY_LIMIT_PER_WALLET kauppaa)
          4. Palauta viimeiset 48h kaupat analyysiin
        """
        self._history_cache.clear()

        # Vaihe 1: Pian sulkeutuvat markkinat
        markets = self._fetch_closing_soon_markets()
        if not markets:
            if os.getenv("ALLOW_TOP_MARKETS_FALLBACK", "false").lower() == "true":
                log.warning("Ei sopivia markkinoita – kokeillaan top-volyymi ilman aikarajoitusta.")
                markets = self._fetch_top_markets_fallback()
            else:
                log.warning("Ei sopivia closing-soon markkinoita – sykli ohitetaan.")
                return []

        if not markets:
            log.error("Ei markkinoita saatu.")
            return []

        log.info(f"Scouttaus {len(markets)} markkinalla.")
        for m in markets[:3]:
            log.info(f"  📊 {m.get('question','')[:55]} | endDate: {m.get('endDate','')[:10]}")

        # Vaihe 2: Holderit + pysyvä known-wallet universumi
        wallets = self._collect_wallets_from_holders(markets)
        wallets = self._merge_known_wallets(wallets)
        log.info(f"Uniikit lompakot: {len(wallets)}")
        if not wallets:
            return []

        # Vaihe 3: Historia rinnakkain
        # Kaksi eri limittia:
        #   scoring_limit  = SCORING_HISTORY_LIMIT (oletus 300) — pitää sisältää
        #                    suljettuja markkinoita joista ROI lasketaan
        #   recent_limit   = HISTORY_LIMIT_PER_WALLET (oletus 100) — 48h kaupat
        # Haetaan scoring_limit mutta tallennetaan koko historia cacheen,
        # recent-suodatus tehdään kuten ennenkin cutoff:lla.
        scoring_limit = int(os.getenv("SCORING_HISTORY_LIMIT", 300))
        fetch_limit   = max(self.history_limit, scoring_limit)

        cutoff      = datetime.now(timezone.utc) - timedelta(hours=hours_back)
        all_recent: List[Dict] = []

        def fetch_one(wallet: str):
            history = self._fetch_wallet_activity(wallet, limit=fetch_limit)
            recent  = [
                t for t in history
                if self._ts(t) is not None and self._ts(t) >= cutoff
            ]
            for t in recent:
                t.setdefault("proxyWallet", wallet)
            return wallet, history, recent

        max_workers = int(os.getenv("FETCH_WORKERS", 16))
        log.info(
            f"Haetaan {len(wallets)} lompakon historia "
            f"({max_workers} rinnakkain, "
            f"recent={self.history_limit} scoring={scoring_limit}/lompakko)..."
        )

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(fetch_one, w): w for w in wallets}
            done = 0
            for future in as_completed(futures):
                try:
                    wallet, history, recent = future.result()
                    self._history_cache[wallet.lower()] = history
                    all_recent.extend(recent)
                    done += 1
                    if done % 20 == 0:
                        log.info(f"  Historia: {done}/{len(wallets)} valmis...")
                except Exception as e:
                    log.debug(f"Historia haku epäonnistui: {e}")

        log.info(f"Yhteensä {len(all_recent)} tuoretta kauppaa {len(wallets)} lompakolta.")
        return all_recent

    def get_wallet_history_cache(self) -> Dict[str, List[Dict]]:
        return self._history_cache

    # ------------------------------------------------------------------
    # Markkinoiden haku
    # ------------------------------------------------------------------

    def _fetch_closing_soon_markets(self) -> List[Dict]:
        """
        Hakee markkinat jotka:
        - Ovat aktiivisia ja avoimia
        - Sulkeutuvat seuraavan CLOSING_DAYS päivän sisällä
        - Volyymi yli MIN_VOLUME_24H
        """
        now   = datetime.now(timezone.utc)
        limit = now + timedelta(days=self.closing_days)

        page_size = max(1, min(self.market_page_size, 500))
        scan_limit = max(page_size, self.market_scan_limit)
        markets: List[Dict[str, Any]] = []

        for offset in range(0, scan_limit, page_size):
            data = self._get(f"{GAMMA_BASE}/markets", {
                "limit":     page_size,
                "offset":    offset,
                "active":    "true",
                "closed":    "false",
                "order":     "volume24hr",
                "ascending": "false",
            })
            if not isinstance(data, list) or not data:
                break
            markets.extend(data)
            if len(data) < page_size:
                break

        closing_soon = []
        for m in markets:
            end_raw = m.get("endDate") or m.get("end_date", "")
            if not end_raw:
                continue
            try:
                end_dt = datetime.fromisoformat(
                    end_raw.replace("Z", "+00:00").replace(" ", "T")
                )
                if not end_dt.tzinfo:
                    end_dt = end_dt.replace(tzinfo=timezone.utc)
            except ValueError:
                continue

            vol        = float(m.get("volume24hr") or m.get("volume") or 0)
            hours_left = (end_dt - now).total_seconds() / 3600

            accepting_orders = m.get("acceptingOrders")
            if accepting_orders is None:
                accepting_orders = m.get("accepting_orders", True)

            if (
                now <= end_dt <= limit
                and vol >= self.min_volume_24h
                and hours_left >= 1.0
                and bool(accepting_orders)
            ):
                closing_soon.append(m)

        closing_soon.sort(key=lambda x: float(x.get("volume24hr") or 0), reverse=True)
        result = closing_soon[:self.top_markets]

        log.info(
            f"Pian sulkeutuvia markkinoita "
            f"({self.closing_days}pv, >{self.min_volume_24h:.0f} USDC): {len(result)} "
            f"(skannattu {len(markets)} markkinaa)"
        )
        return result

    def _fetch_top_markets_fallback(self) -> List[Dict]:
        """Fallback: top-volyymi ilman aikarajoitusta."""
        data = self._get(f"{GAMMA_BASE}/markets", {
            "limit":     self.top_markets,
            "active":    "true",
            "closed":    "false",
            "order":     "volume24hr",
            "ascending": "false",
        })
        if not isinstance(data, list):
            return []
        log.info(f"Fallback: {len(data)} top-volyymi markkinaa.")
        return data

    def _collect_wallets_from_holders(self, markets: List[Dict]) -> List[str]:
        """
        Kerää uniikit lompakot kahdella strategialla:

        STRATEGIA 1 — Volyymi-piikki (päästrategia):
          Hakee lompakot jotka ovat ostaneet viimeisen VOLUME_SPIKE_HOURS
          tunnin aikana. Nämä ovat todennäköisemmin informoituja treidaajia
          jotka reagoivat tuoreeseen tietoon (kokoonpano, uutinen, insider).

        STRATEGIA 2 — Top holders (fallback):
          Jos volyymi-piikki ei löydä tarpeeksi lompakoita, täydennetään
          top-holdereilla. Tämä pitää signaalin ehjänä hiljaisinakin hetkinä.

        Miksi volyymi-piikki on parempi:
          - Top holder on ollut sisällä viikkoja → markkinat tietävät jo
          - Tuore osto 2h sisällä → reaktio uuteen informaatioon
          - Edge syntyy nopeudesta, ei positiokoosta
        """
        spike_hours  = int(os.getenv("VOLUME_SPIKE_HOURS", 2))
        min_spike_wallets = int(os.getenv("MIN_SPIKE_WALLETS", 10))
        cutoff_ts    = int((datetime.now(timezone.utc) - timedelta(hours=spike_hours)).timestamp())

        wallets_spike: set = set()
        wallets_holders: set = set()

        for market in markets:
            cid = market.get("conditionId") or market.get("condition_id")
            if not cid:
                continue

            recent_trades = self._fetch_market_recent_trades(cid, cutoff_ts)
            for trade in recent_trades:
                side = str(trade.get("side", "")).upper()
                addr = self._extract_address(trade)
                if side == "BUY" and addr:
                    wallets_spike.add(addr)

            # Hae top-holderit fallbackiksi ja scoringin historian lähteeksi.
            data = self._get(f"{DATA_BASE}/holders", {
                "market": cid,
                "limit":  self.top_holders,
            })
            if isinstance(data, list):
                for token_obj in data:
                    for h in token_obj.get("holders", []):
                        addr = h.get("proxyWallet", "")
                        if addr and addr.startswith("0x") and len(addr) == 42:
                            wallets_holders.add(addr.lower())

            time.sleep(self.request_delay)

        # Yhdistä: volyymi-piikki ensin, täydennä holdereilla jos liian vähän
        combined = list(wallets_spike)
        spike_count = len(wallets_spike)

        if spike_count < min_spike_wallets:
            # Lisää top-holderit jotka eivät ole jo listalla
            for w in wallets_holders:
                if w not in wallets_spike:
                    combined.append(w)

        log.info(
            f"Lompakkohaku: {spike_count} volyymi-piikki ({spike_hours}h) + "
            f"{len(combined) - spike_count} top-holder täydennystä = {len(combined)} yhteensä"
        )
        try:
            from wallet_universe import add_discovered_wallets
            add_discovered_wallets(combined, "market_discovery")
        except Exception as e:
            log.debug(f"Known wallet päivitys epäonnistui: {e}")
        return combined

    def _merge_known_wallets(self, wallets: List[str]) -> List[str]:
        """Lisää mukaan parhaat aiemmin löydetyt walletit konservatiivisella limitillä."""
        if os.getenv("KNOWN_WALLETS_ENABLED", "true").lower() != "true":
            return wallets
        try:
            from wallet_universe import get_candidate_wallets
            known_wallets = get_candidate_wallets()
        except Exception as e:
            log.debug(f"Known wallet haku epäonnistui: {e}")
            return wallets

        seen = set()
        combined = []
        for wallet in list(wallets) + known_wallets:
            addr = str(wallet).lower()
            if addr in seen:
                continue
            if addr.startswith("0x") and len(addr) == 42:
                seen.add(addr)
                combined.append(addr)

        extra = len(combined) - len(wallets)
        if extra > 0:
            log.info(f"Known wallet täydennys: +{extra} aiemmin löydettyä walletia")
        return combined

    def _fetch_market_recent_trades(self, condition_id: str, cutoff_ts: int) -> List[Dict]:
        """Hakee markkinan tuoreet treidit ja palauttaa vain cutoffin jälkeiset."""
        try:
            resp = self.session.get(
                f"{DATA_BASE}/activity",
                params={
                    "market":        condition_id,
                    "type":          "TRADE",
                    "side":          "BUY",
                    "start":         cutoff_ts,
                    "sortBy":        "TIMESTAMP",
                    "sortDirection": "DESC",
                    "limit":         int(os.getenv("MARKET_ACTIVITY_LIMIT", 100)),
                },
                timeout=8,
            )
            if resp.status_code != 200:
                log.debug(f"Market activity ei saatavilla ({resp.status_code}): {resp.text[:120]}")
                return []
            data = resp.json()
        except requests.exceptions.RequestException as e:
            log.debug(f"Market activity haku epäonnistui: {e}")
            return []

        trades = data if isinstance(data, list) else data.get("data", [])
        recent = []
        for trade in trades:
            trade_cid = self._extract_condition_id(trade)
            if trade_cid and trade_cid != condition_id:
                continue
            ts = self._ts(trade)
            if ts is None or int(ts.timestamp()) < cutoff_ts:
                continue
            recent.append(trade)
        return recent

    def _fetch_wallet_activity(self, wallet: str, limit: int = 100) -> List[Dict]:
        """Hakee lompakon kauppahistorian."""
        data = self._get(f"{DATA_BASE}/activity", {
            "user":          wallet,
            "type":          "TRADE",
            "sortBy":        "TIMESTAMP",
            "sortDirection": "DESC",
            "limit":         limit,
        })
        if not data:
            return []
        return data if isinstance(data, list) else data.get("data", [])

    def _extract_address(self, trade: Dict) -> Optional[str]:
        for key in ("proxyWallet", "proxy_wallet", "maker", "user"):
            val = trade.get(key)
            if val and isinstance(val, str) and val.startswith("0x") and len(val) == 42:
                return val.lower()
        return None

    def _extract_condition_id(self, trade: Dict) -> Optional[str]:
        for key in ("conditionId", "condition_id", "market", "marketId"):
            val = trade.get(key)
            if val and isinstance(val, str):
                return val
        return None

    # ------------------------------------------------------------------
    # Apumetodit
    # ------------------------------------------------------------------

    def _ts(self, trade: Dict) -> Optional[datetime]:
        raw = None
        for key in ("timestamp", "createdAt", "created_at", "time"):
            raw = trade.get(key)
            if raw is not None:
                break
        if raw is None:
            return None
        try:
            if isinstance(raw, (int, float)):
                v = raw / 1000 if raw > 1e10 else raw
                return datetime.fromtimestamp(v, tz=timezone.utc)
            if isinstance(raw, str):
                return datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except (ValueError, OSError):
            pass
        return None

    def _get(self, url: str, params: dict) -> Optional[Any]:
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self.session.get(url, params=params, timeout=15)
                resp.raise_for_status()
                return resp.json()
            except requests.exceptions.Timeout:
                log.warning(f"Timeout (yritys {attempt}): {url}")
            except requests.exceptions.HTTPError as e:
                status = e.response.status_code if e.response is not None else None
                body   = e.response.text[:150] if e.response is not None else ""
                log.warning(f"HTTP {status} (yritys {attempt}): {body}")
                if status == 429:
                    time.sleep(2 ** attempt * 5)
                    continue
                if status and 400 <= status < 500:
                    return None
            except requests.exceptions.RequestException as e:
                log.warning(f"Verkkovirhe (yritys {attempt}): {e}")
            if attempt < self.max_retries:
                time.sleep(2 ** attempt)
        log.error(f"Kaikki yritykset epäonnistuivat: {url}")
        return None


GammaFetcher = PolymarketFetcher
