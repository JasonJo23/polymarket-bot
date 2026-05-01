"""
=============================================================================
fetcher.py – PolymarketFetcher  (v5.0 – Closing Soon -strategia)
=============================================================================
STRATEGIA:
  Vanha ongelma: top-holderit operoivat avoimilla markkinoilla
  → win rate -laskenta mahdotonta (ei suljettua dataa)

  Uusi ratkaisu:
  1. Hae markkinat jotka sulkeutuvat seuraavan N päivän sisällä
     JA joilla on korkea volyymi (smart money on jo sisällä)
  2. Hae näiden markkinoiden top-holderit
  3. Hae jokaisen holderin KOKO historia (myös vanhat markkinat)
  4. Analyzer laskee win raten suljetuista historiallisista markkinoista
  5. Scout seuraa holdereita joilla on todistettu track record

TESTATUT ENDPOINTIT:
  ✅ GET gamma-api.polymarket.com/markets
       params: active, closed, order, ascending, limit
  ✅ GET data-api.polymarket.com/holders?market=conditionId
  ✅ GET data-api.polymarket.com/activity?user=0x...
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
        self.session = requests.Session()
        self.session.headers.update({
            "Accept":     "application/json",
            "User-Agent": "PolymarketScout/5.0"
        })
        self.request_delay    = float(os.getenv("REQUEST_DELAY_SECONDS", 1.5))
        self.max_retries      = int(os.getenv("MAX_RETRIES", 3))
        self.top_markets      = int(os.getenv("TOP_MARKETS", 15))
        self.top_holders      = int(os.getenv("TOP_HOLDERS", 20))
        self.closing_days     = int(os.getenv("CLOSING_DAYS", 7))      # markkinat jotka sulkeutuvat N päivän sisällä
        self.min_volume_24h   = float(os.getenv("MIN_VOLUME_24H", 50000))  # min volyymi USDC
        self._history_cache: Dict[str, List[Dict]] = {}

    # ------------------------------------------------------------------
    # Päämetodi
    # ------------------------------------------------------------------

    def fetch_recent_trades(self, hours_back: int = 48) -> List[Dict[str, Any]]:
        """
        Closing Soon -strategia:
          1. Hae korkeavolyymiset markkinat jotka sulkeutuvat pian
          2. Kerää top-holderit näiltä markkinoilta
          3. Hae jokaisen holderin koko historia win rate -laskentaa varten
          4. Palauta viimeiset 48h kaupat analyysiin
        """
        self._history_cache.clear()

        # Vaihe 1: Pian sulkeutuvat korkeavolyymiset markkinat
        markets = self._fetch_closing_soon_markets()
        if not markets:
            log.warning("Ei sopivia markkinoita – kokeillaan top-volyymi ilman aikarajoitusta.")
            markets = self._fetch_top_markets_fallback()

        if not markets:
            log.error("Ei markkinoita saatu.")
            return []

        log.info(f"Scouttaus {len(markets)} markkinalla.")
        for m in markets[:3]:
            log.info(f"  📊 {m.get('question','')[:55]} | endDate: {m.get('endDate','')[:10]}")

        # Vaihe 2: Holderit
        wallets = self._collect_wallets_from_holders(markets)
        log.info(f"Uniikit lompakot: {len(wallets)}")
        if not wallets:
            return []

        # Vaihe 3: Historia rinnakkain (ThreadPoolExecutor)
        cutoff     = datetime.now(timezone.utc) - timedelta(hours=hours_back)
        all_recent: List[Dict] = []

        def fetch_one(wallet: str):
            history = self._fetch_wallet_activity(wallet, limit=500)
            recent  = [
                t for t in history
                if self._ts(t) is not None and self._ts(t) >= cutoff
            ]
            for t in recent:
                t.setdefault("proxyWallet", wallet)
            return wallet, history, recent

        max_workers = int(os.getenv("FETCH_WORKERS", 8))
        log.info(f"Haetaan {len(wallets)} lompakon historia ({max_workers} rinnakkain)...")

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

        Järjestetään volyymilla — smart money on jo sisällä.
        """
        now   = datetime.now(timezone.utc)
        limit = now + timedelta(days=self.closing_days)

        # Hae suurivolyymiset markkinat ja suodata päätymispäivän mukaan
        data = self._get(f"{GAMMA_BASE}/markets", {
            "limit":     50,
            "active":    "true",
            "closed":    "false",
            "order":     "volume24hr",
            "ascending": "false",
        })

        if not isinstance(data, list):
            return []

        closing_soon = []
        for m in data:
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

            vol = float(m.get("volume24hr") or m.get("volume") or 0)

            # Suodata pois markkinat jotka sulkeutuvat alle 1 tunnin päästä
            hours_left = (end_dt - now).total_seconds() / 3600
            if now <= end_dt <= limit and vol >= self.min_volume_24h and hours_left >= 1.0:
                closing_soon.append(m)

        # Järjestä volyymilla
        closing_soon.sort(key=lambda x: float(x.get("volume24hr") or 0), reverse=True)
        result = closing_soon[:self.top_markets]

        log.info(f"Pian sulkeutuvia markkinoita ({self.closing_days}pv, >{self.min_volume_24h:.0f} USDC): {len(result)}")
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
        """Kerää uniikit lompakot /holders-endpointista."""
        wallets: set = set()

        for market in markets:
            cid = market.get("conditionId") or market.get("condition_id")
            if not cid:
                continue

            data = self._get(f"{DATA_BASE}/holders", {
                "market": cid,
                "limit":  self.top_holders,
            })

            if isinstance(data, list):
                for token_obj in data:
                    for h in token_obj.get("holders", []):
                        addr = h.get("proxyWallet", "")
                        if addr and addr.startswith("0x") and len(addr) == 42:
                            wallets.add(addr.lower())
            time.sleep(self.request_delay)

        return list(wallets)

    def _fetch_wallet_activity(self, wallet: str, limit: int = 500) -> List[Dict]:
        """Hakee lompakon täyden kauppahistorian."""
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

    # ------------------------------------------------------------------
    # Apumetodit
    # ------------------------------------------------------------------

    def _ts(self, trade: Dict) -> Optional[datetime]:
        raw = trade.get("timestamp")
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
                status = e.response.status_code if e.response else None
                body   = e.response.text[:150] if e.response else ""
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