"""
=============================================================================
analyzer.py – WalletAnalyzer  (v4.0 – Konsensuspohjainen, ei win ratea)
=============================================================================
Strategia: Win rate -laskenta on mahdoton ilman settlement-dataa.
Korvataan se konsensuspohjaisella analyysillä:

KRITEERIT lompakkojen valintaan:
  1. Aktiivisuus: ≥3 kauppaa viimeisen 48h aikana
  2. Kauppakoko: keskiarvo 200-5000 USDC (ei pieniä botteja)
  3. Sitoutuminen: ostaa pian sulkeutuvaa markkinaa (≤7pv)

SMART FOLLOW -signaali:
  - ≥3 kvalifioitua lompakon ostaa samaa outcomea samalla markkinalla
  - Yhdistetty positiokoko ≥10 000 USDC
  - Markkina sulkeutuu ≤7 päivän sisällä

Tämä mittaa markkinakonsensusta — parempi signaali kuin yksittäinen win rate.
=============================================================================
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
from collections import defaultdict

log = logging.getLogger("Scout.Analyzer")


class WalletAnalyzer:

    def __init__(
        self,
        min_win_rate:   float = 0.60,   # Ei käytetä — pidetään yhteensopivuuden vuoksi
        min_trades_48h: int   = 3,
        min_avg_size:   float = 200.0,
        max_avg_size:   float = 5000.0
    ):
        self.min_trades_48h = min_trades_48h
        self.min_avg_size   = min_avg_size
        self.max_avg_size   = max_avg_size

    def analyze(
        self,
        raw_trades: List[Dict[str, Any]],
        history_cache: Dict = None
    ) -> List[Dict[str, Any]]:
        """
        Ryhmittelee kaupat lompakoittain ja suodattaa aktiivisuuden
        ja kauppakoon perusteella. Win ratea ei lasketa.
        """
        history_cache = history_cache or {}

        wallet_trades: Dict[str, List[Dict]] = defaultdict(list)
        for trade in raw_trades:
            addr = self._extract_address(trade)
            if addr:
                wallet_trades[addr].append(trade)

        log.info(f"Uniikit lompakot: {len(wallet_trades)}")

        cutoff_48h = datetime.now(timezone.utc) - timedelta(hours=48)
        qualified  = []

        for address, recent_trades in wallet_trades.items():
            metrics = self._calculate_metrics(address, recent_trades, cutoff_48h)
            if metrics and self._passes_filter(metrics):
                qualified.append(metrics)

        # Järjestä kauppakoon mukaan — suurimmat positiot ensin
        qualified.sort(key=lambda x: x["total_volume_usdc"], reverse=True)
        log.info(f"Kvalifioituja lompakoita: {len(qualified)}")
        return qualified

    # ------------------------------------------------------------------

    def _calculate_metrics(
        self,
        address:    str,
        trades:     List[Dict],
        cutoff_48h: datetime
    ) -> Optional[Dict[str, Any]]:

        recent = [
            t for t in trades
            if self._parse_timestamp(t) is not None
            and self._parse_timestamp(t) >= cutoff_48h
        ]
        trades_48h = len(recent)

        sizes = [s for t in trades if (s := self._parse_size_usdc(t)) > 0]
        if not sizes:
            return None

        avg_size     = sum(sizes) / len(sizes)
        total_volume = sum(sizes)

        return {
            "address":           address,
            "win_rate":          0.0,     # Ei lasketa — pidetään kentän yhteensopivuuden vuoksi
            "trades_48h":        trades_48h,
            "avg_size_usdc":     avg_size,
            "total_volume_usdc": total_volume,
            "resolved_count":    0,
            "recent_trades":     recent,
            "all_trades":        trades
        }

    def _passes_filter(self, m: Dict) -> bool:
        return (
            m["trades_48h"]    >= self.min_trades_48h and
            m["avg_size_usdc"] >= self.min_avg_size   and
            m["avg_size_usdc"] <= self.max_avg_size
        )

    def _extract_address(self, trade: Dict) -> Optional[str]:
        for key in ("proxyWallet", "proxy_wallet", "_wallet_address", "maker"):
            val = trade.get(key)
            if val and isinstance(val, str) and val.startswith("0x") and len(val) == 42:
                return val.lower()
        return None

    def _parse_timestamp(self, trade: Dict) -> Optional[datetime]:
        raw = trade.get("timestamp")
        if raw is None:
            return None
        try:
            if isinstance(raw, (int, float)):
                ts = raw / 1000 if raw > 1e10 else raw
                return datetime.fromtimestamp(ts, tz=timezone.utc)
            if isinstance(raw, str):
                return datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except (ValueError, OSError):
            pass
        return None

    def _parse_size_usdc(self, trade: Dict) -> float:
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