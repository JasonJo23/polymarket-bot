"""
=============================================================================
analyzer.py – WalletAnalyzer  (v5.0 – Integroitu wallet scoring)
=============================================================================
KORJAUKSET v4.0 → v5.0:

  BUG #4  analyze() ei käyttänyt wallet_scorer.py:n painoja mitenkään
          → Scorer laskettiin mutta tuloksia ei koskaan sovellettu
          → Nyt analyze() ottaa wallet_scores-parametrin ja:
              a) Lisää score-tiedot jokaiseen qualified-walletiin
              b) Järjestää lompakot painotetulla volyymilla (volume × weight)
                 eikä pelkällä volyymilla
              c) Suodattaa pois lompakot joiden paino on alle min_weight

  LISÄYS  Loki kertoo nyt selvästi miten scoring vaikutti järjestykseen
          → Helpompi debugata miksi tietty lompakko valittiin/hylättiin

STRATEGIA (ennallaan):
  1. Aktiivisuus: ≥3 kauppaa viimeisen 48h aikana
  2. Kauppakoko: keskiarvo 200–5000 USDC
  3. Scorer-paino: ≥ min_weight (oletus 0.7 — suodattaa selvästi huonot)
  4. Järjestys: total_volume_usdc × wallet_weight (isoimmat + parhaat ensin)
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
        min_win_rate:   float = 0.60,   # Ei käytetä — yhteensopivuuden vuoksi
        min_trades_48h: int   = 3,
        min_avg_size:   float = 200.0,
        max_avg_size:   float = 5000.0,
        min_weight:     float = 0.7     # UUSI: suodattaa selvästi huonot lompakot
    ):
        self.min_trades_48h = min_trades_48h
        self.min_avg_size   = min_avg_size
        self.max_avg_size   = max_avg_size
        self.min_weight     = min_weight

    def analyze(
        self,
        raw_trades:    List[Dict[str, Any]],
        history_cache: Dict = None,
        wallet_scores: Dict = None   # BUG #4 KORJAUS: otetaan scorer-tulokset vastaan
    ) -> List[Dict[str, Any]]:
        """
        Ryhmittelee kaupat lompakoittain, suodattaa aktiivisuuden,
        kauppakoon JA wallet scorer -painon perusteella.

        Args:
            raw_trades:    Viimeisimmät kaupat fetcheriltä
            history_cache: Koko historia per lompakko (fetcheriltä)
            wallet_scores: Scorer-tulokset per lompakko (wallet_scorer.py:ltä)
                           Jos None, kaikki lompakot saavat neutraalin painon 1.0
        """
        history_cache = history_cache or {}
        wallet_scores = wallet_scores or {}

        # Ryhmittele kaupat lompakoittain
        wallet_trades: Dict[str, List[Dict]] = defaultdict(list)
        for trade in raw_trades:
            addr = self._extract_address(trade)
            if addr:
                wallet_trades[addr].append(trade)

        log.info(f"Uniikit lompakot raakakauppaistossa: {len(wallet_trades)}")

        cutoff_48h = datetime.now(timezone.utc) - timedelta(hours=48)
        qualified  = []
        filtered_low_weight = 0

        for address, recent_trades in wallet_trades.items():
            metrics = self._calculate_metrics(address, recent_trades, cutoff_48h)
            if not metrics:
                continue

            # BUG #4 KORJAUS: Liitä scorer-tiedot metriikoihin
            score = wallet_scores.get(address) or wallet_scores.get(address.lower()) or {}
            metrics["wallet_weight"]   = score.get("weight",       1.0)
            metrics["wallet_roi"]      = score.get("weighted_roi", 0.0)
            metrics["wallet_win_rate"] = score.get("win_rate",     0.5)
            metrics["wallet_reliable"] = score.get("reliable",     False)
            metrics["resolved_count"]  = score.get("resolved_count", 0)

            # Perussuodatus (aktiivisuus + koko)
            if not self._passes_base_filter(metrics):
                continue

            # BUG #4 KORJAUS: Hylkää selvästi huonot lompakot scorer-painon mukaan
            # HUOM: Epäluotettavat lompakot (ei tarpeeksi dataa) läpäisevät tämän
            #       neutraalilla painolla 1.0 — ei rangaista tietämättömyydestä
            if metrics["wallet_reliable"] and metrics["wallet_weight"] < self.min_weight:
                filtered_low_weight += 1
                log.debug(
                    f"Hylätty matalan painon takia: {address[:10]} "
                    f"w={metrics['wallet_weight']} roi={metrics['wallet_roi']:+.1%}"
                )
                continue

            qualified.append(metrics)

        # BUG #4 KORJAUS: Järjestä painotetulla volyymilla
        # → Lompakko jolla suuri volyymi JA hyvä track record nousee ylös
        qualified.sort(
            key=lambda x: x["total_volume_usdc"] * x["wallet_weight"],
            reverse=True
        )

        log.info(
            f"Kvalifioituja lompakoita: {len(qualified)} "
            f"(hylätty matalan painon takia: {filtered_low_weight})"
        )

        # Loki top-5:stä selkeyden vuoksi
        for w in qualified[:5]:
            reliable_str = f"roi={w['wallet_roi']:+.1%}" if w["wallet_reliable"] else "ei dataa"
            log.info(
                f"  ✅ {w['address'][:10]}... | "
                f"48h={w['trades_48h']} kauppaa | "
                f"avg={w['avg_size_usdc']:.0f} USDC | "
                f"weight={w['wallet_weight']} | {reliable_str}"
            )

        return qualified

    # ------------------------------------------------------------------
    # Metriikat
    # ------------------------------------------------------------------

    def _calculate_metrics(
        self,
        address:    str,
        trades:     List[Dict],
        cutoff_48h: datetime
    ) -> Optional[Dict[str, Any]]:
        """Laskee perustilastot lompakosta viimeisen 48h kauppojen perusteella."""

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
            "win_rate":          0.0,       # Legacy-kenttä — käytä wallet_win_rate
            "trades_48h":        trades_48h,
            "avg_size_usdc":     avg_size,
            "total_volume_usdc": total_volume,
            "recent_trades":     recent,
            "all_trades":        trades,
            # Scorer-kentät täytetään analyze():ssa
            "wallet_weight":     1.0,
            "wallet_roi":        0.0,
            "wallet_win_rate":   0.5,
            "wallet_reliable":   False,
            "resolved_count":    0,
        }

    def _passes_base_filter(self, m: Dict) -> bool:
        """Perussuodatus: aktiivisuus ja kauppakoko."""
        return (
            m["trades_48h"]    >= self.min_trades_48h and
            m["avg_size_usdc"] >= self.min_avg_size   and
            m["avg_size_usdc"] <= self.max_avg_size
        )

    # ------------------------------------------------------------------
    # Parsinta
    # ------------------------------------------------------------------

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