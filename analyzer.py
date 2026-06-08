"""
=============================================================================
analyzer.py – WalletAnalyzer  (v5.1 – MIN_WALLET_WEIGHT korjattu)
=============================================================================
KORJAUKSET v5.0 → v5.1:

  BUG #1  MIN_WALLET_WEIGHT=0.7 hylkää kaikki luotettavat lompakot
          koska kaikki 100 luotettavaa ovat tappiolla (weight <= 0.8)
          → Analyzer suodatti pois 75-80% kaikista lompakkoista
          → Jäljelle jäi vain "ei dataa" lompakot weight=1.0
          → KORJAUS: MIN_WALLET_WEIGHT=0.4 — hylätään vain
            selvästi häviävät (alle -5% ROI), muut pääsevät läpi

  BUG #2  avg_size lasketaan KAIKISTA kaupoista (myös vanhemmista)
          eikä pelkästään 48h kaupoista
          → Lompakko joka kävi aktiivinen 6kk sitten mutta on nyt
            hiljainen näyttää hyvältä
          → KORJAUS: avg_size ja total_volume lasketaan vain
            viimeisen 48h kaupoista jos niitä on tarpeeksi,
            muutoin koko historiasta (fallback)
=============================================================================
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
from collections import defaultdict

from polymath_utils import parse_timestamp, parse_size_usdc, extract_address

log = logging.getLogger("Scout.Analyzer")


class WalletAnalyzer:

    def __init__(
        self,
        min_win_rate:   float = 0.60,
        min_trades_48h: int   = 3,
        min_avg_size:   float = 200.0,
        max_avg_size:   float = 5000.0,
        min_weight:     float = 0.4,   # BUG #1 KORJAUS: oli 0.7, liian tiukka
    ):
        self.min_trades_48h = min_trades_48h
        self.min_avg_size   = min_avg_size
        self.max_avg_size   = max_avg_size
        self.min_weight     = min_weight

    def analyze(
        self,
        raw_trades:    List[Dict[str, Any]],
        history_cache: Dict = None,
        wallet_scores: Dict = None,
    ) -> List[Dict[str, Any]]:
        history_cache = history_cache or {}
        wallet_scores = wallet_scores or {}

        wallet_trades: Dict[str, List[Dict]] = defaultdict(list)
        for trade in raw_trades:
            addr = self._extract_address(trade)
            if addr:
                wallet_trades[addr].append(trade)

        log.info(f"Uniikit lompakot raakakauppaistossa: {len(wallet_trades)}")

        cutoff_48h = datetime.now(timezone.utc) - timedelta(hours=48)
        qualified  = []
        filtered_low_weight = 0

        for address, trades in wallet_trades.items():
            metrics = self._calculate_metrics(address, trades, cutoff_48h)
            if not metrics:
                continue

            score = wallet_scores.get(address) or wallet_scores.get(address.lower()) or {}
            metrics["wallet_weight"]   = score.get("weight",       1.0)
            metrics["wallet_roi"]      = score.get("weighted_roi", 0.0)
            metrics["wallet_win_rate"] = score.get("win_rate",     0.5)
            metrics["wallet_reliable"] = score.get("reliable",     False)
            metrics["resolved_count"]  = score.get("resolved_count", 0)
            metrics["category_weights"] = score.get("category_weights", {})
            metrics["trades_7d"] = score.get("trades_7d", 0)
            metrics["trades_14d"] = score.get("trades_14d", 0)
            metrics["active_recently"] = score.get("active_recently", False)

            if not self._passes_base_filter(metrics):
                continue

            # Hylkää vain selvästi tappiolla olevat lompakot (weight < min_weight)
            # Epäluotettavat (ei dataa) saavat neutraalin painon 1.0 → läpäisevät
            if metrics["wallet_reliable"] and metrics["wallet_weight"] < self.min_weight:
                filtered_low_weight += 1
                continue

            qualified.append(metrics)

        qualified.sort(
            key=lambda x: x["total_volume_usdc"] * x["wallet_weight"],
            reverse=True
        )

        log.info(
            f"Kvalifioituja lompakoita: {len(qualified)} "
            f"(hylätty matalan painon takia: {filtered_low_weight})"
        )

        for w in qualified[:5]:
            reliable_str = f"roi={w['wallet_roi']:+.1%}" if w["wallet_reliable"] else "ei dataa"
            log.info(
                f"  ✅ {w['address'][:10]}... | "
                f"48h={w['trades_48h']} kauppaa | "
                f"avg={w['avg_size_usdc']:.0f} USDC | "
                f"weight={w['wallet_weight']} | {reliable_str}"
            )

        return qualified

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

        # BUG #2 KORJAUS: laske koot 48h kaupoista jos niitä on tarpeeksi
        size_source = recent if len(recent) >= 3 else trades
        sizes = [s for t in size_source if (s := self._parse_size_usdc(t)) > 0]
        if not sizes:
            return None

        avg_size     = sum(sizes) / len(sizes)
        total_volume = sum(sizes)

        return {
            "address":           address,
            "wallet_source":     self._wallet_source(trades),
            "win_rate":          0.0,
            "trades_48h":        trades_48h,
            "avg_size_usdc":     avg_size,
            "total_volume_usdc": total_volume,
            "recent_trades":     recent,
            "all_trades":        trades,
            "wallet_weight":     1.0,
            "wallet_roi":        0.0,
            "wallet_win_rate":   0.5,
            "wallet_reliable":   False,
            "resolved_count":    0,
            "category_weights":   {},
            "trades_7d":          0,
            "trades_14d":         0,
            "active_recently":    False,
        }

    def _passes_base_filter(self, m: Dict) -> bool:
        return (
            m["trades_48h"]    >= self.min_trades_48h and
            m["avg_size_usdc"] >= self.min_avg_size   and
            m["avg_size_usdc"] <= self.max_avg_size
        )

    def _extract_address(self, trade: Dict) -> Optional[str]:
        # Delegated to polymath_utils so address/timestamp/size parsing stays
        # consistent across every module (each previously had its own key list).
        return extract_address(trade)

    def _parse_timestamp(self, trade: Dict) -> Optional[datetime]:
        return parse_timestamp(trade)

    def _parse_size_usdc(self, trade: Dict) -> float:
        return parse_size_usdc(trade)

    def _wallet_source(self, trades: List[Dict]) -> str:
        for trade in trades:
            source = trade.get("_wallet_source")
            if source:
                return str(source)
        return "unknown"
