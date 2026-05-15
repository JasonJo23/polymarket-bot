"""
=============================================================================
main.py – Scout  (v4.0 – Scorer integroitu analyzeriin)
=============================================================================
KORJAUKSET v3.0 → v4.0:

  BUG #4  wallet_scorer.py:n tulokset eivät kulkeutuneet analyzer.py:lle
          → score_wallets_batch() kutsuttiin mutta paluuarvo hylättiin
          → Nyt: scores = score_wallets_batch(...) → analyzer.analyze(..., wallet_scores=scores)

  LISÄYS  Scorer-statistiikka lokiin ennen signaaliajoa
          → Näet kuinka moni lompakko on luotettava / korkean painon
=============================================================================
"""

import os
import time
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv

from fetcher import GammaFetcher
from analyzer import WalletAnalyzer
from tracker import SignalTracker
from wallet_scorer import score_wallets_batch

load_dotenv()

_handlers = [logging.FileHandler("scout.log", encoding="utf-8")]
if os.isatty(1):
    _handlers.append(logging.StreamHandler())

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=_handlers
)
log = logging.getLogger("Scout")


def get_bankroll_usdc() -> float:
    try:
        from tracker import get_usdc_balance_v2
        return get_usdc_balance_v2()
    except Exception as e:
        log.warning(f"Saldon haku epäonnistui: {e}")
        return float(os.getenv("CURRENT_BANKROLL_USDC", 100.0))


def main():
    log.info("=" * 60)
    log.info("Polymarket CopyTrader Scout käynnistyy (v4.0)...")
    log.info("=" * 60)

    poll_interval        = int(os.getenv("POLL_INTERVAL_SECONDS", 1800))
    dry_run              = os.getenv("DRY_RUN", "true").lower() == "true"
    min_win_rate         = float(os.getenv("MIN_WIN_RATE", 0.60))
    min_trades_48h       = int(os.getenv("MIN_TRADES_48H", 3))
    min_avg_size         = float(os.getenv("MIN_AVG_SIZE_USDC", 200))
    max_avg_size         = float(os.getenv("MAX_AVG_SIZE_USDC", 5000))
    min_weight           = float(os.getenv("MIN_WALLET_WEIGHT", 0.4))
    smart_threshold      = int(os.getenv("SMART_FOLLOW_THRESHOLD", 5))
    min_signal_size      = float(os.getenv("MIN_SIGNAL_SIZE_USDC", 50000))
    max_orders_per_cycle = int(os.getenv("MAX_ORDERS_PER_CYCLE", 3))
    min_bankroll         = float(os.getenv("MIN_BANKROLL_USDC", 80))
    max_daily_loss       = float(os.getenv("MAX_DAILY_LOSS_USDC", 30))
    position_check_interval = int(os.getenv("POSITION_CHECK_SECONDS", 300))

    log.info(
        f"Asetukset: DRY_RUN={dry_run} | Poll={poll_interval}s | "
        f"Threshold={smart_threshold} | Trades48h>={min_trades_48h} | "
        f"MinWeight={min_weight}"
    )

    if dry_run:
        log.warning("⚠️  DRY RUN -tila PÄÄLLÄ – oikeita ostoja EI tehdä.")
    else:
        log.warning("🔴 LIVE-TILA PÄÄLLÄ – OIKEAT OSTOT KÄYTÖSSÄ!")

    fetcher  = GammaFetcher()
    analyzer = WalletAnalyzer(
        min_win_rate=min_win_rate,
        min_trades_48h=min_trades_48h,
        min_avg_size=min_avg_size,
        max_avg_size=max_avg_size,
        min_weight=min_weight
    )
    tracker = SignalTracker(smart_threshold=smart_threshold, dry_run=dry_run)

    today_str        = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    daily_spent_usdc = 0.0

    import json as _json
    _spending_file = "daily_spending.json"
    try:
        with open(_spending_file) as f:
            _data = _json.load(f)
            if _data.get("date") == today_str:
                daily_spent_usdc = float(_data.get("spent", 0))
                log.info(f"Ladattu päiväkulut: {daily_spent_usdc:.2f} USDC")
    except Exception:
        pass

    def save_spending():
        try:
            with open(_spending_file, "w") as f:
                _json.dump({"date": today_str, "spent": daily_spent_usdc}, f)
        except Exception:
            pass

    while True:
        try:
            current_day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            if current_day != today_str:
                today_str        = current_day
                daily_spent_usdc = 0.0
                log.info("Uusi päivä – päivittäinen kulukello nollattu.")

            log.info("--- Uusi skannaus alkaa ---")
            cycle_start = time.time()

            # Position check syklin alussa
            if not dry_run:
                try:
                    from position_manager import check_and_exit_positions
                    check_and_exit_positions()
                except Exception as e:
                    log.warning(f"Position check epäonnistui: {e}")

            # Bankroll-tarkistus
            if not dry_run:
                bankroll = get_bankroll_usdc()
                log.info(f"💰 Kassa: {bankroll:.2f} USDC | Päiväkulut: {daily_spent_usdc:.2f} USDC")

                if bankroll < min_bankroll:
                    log.error(f"🛑 KASSA LIIAN MATALA: {bankroll:.2f} < {min_bankroll:.0f} USDC — DRY RUN päälle!")
                    tracker.dry_run = True
                    dry_run = True

                if daily_spent_usdc >= max_daily_loss:
                    log.error(f"🛑 PÄIVÄRAJA TÄYNNÄ: {daily_spent_usdc:.2f} >= {max_daily_loss:.0f} USDC — ostot pysäytetty!")
                    tracker.dry_run = True

            # 1. Haku
            raw_trades = fetcher.fetch_recent_trades()
            log.info(f"Haettu {len(raw_trades)} kauppaa.")

            if not raw_trades:
                log.warning("Ei dataa – odotetaan.")
            else:
                history_cache = fetcher.get_wallet_history_cache() \
                    if hasattr(fetcher, "get_wallet_history_cache") else {}

                # NOPEUTUS: Aja analyzer ensin ilman scoreja (perussuodatus)
                # → scorataan vain kvalifioituneet lompakot, ei kaikkia 260+
                log.info("Esikarsinta ennen scoringta...")
                pre_qualified = analyzer.analyze(
                    raw_trades,
                    history_cache=history_cache,
                    wallet_scores={}   # Ei scoreja vielä — pelkkä perussuodatus
                )
                log.info(f"Esikarsinta: {len(pre_qualified)} lompakkoa scoringiin (kaikista {len(history_cache)})")

                # 2. Wallet scoring — vain esikarsitut lompakot
                log.info("Lasketaan wallet scoret...")
                scores = score_wallets_batch(pre_qualified, history_cache)

                reliable_count    = sum(1 for s in scores.values() if s["reliable"])
                high_weight_count = sum(1 for s in scores.values() if s["weight"] >= 1.5)
                log.info(
                    f"Wallet scoring: {len(scores)} lompakkoa | "
                    f"{reliable_count} luotettavaa | "
                    f"{high_weight_count} korkean painon"
                )

                # 3. Analyysi uudelleen — nyt scoret mukana järjestykseen
                qualified_wallets = analyzer.analyze(
                    raw_trades,
                    history_cache=history_cache,
                    wallet_scores=scores
                )
                log.info(f"Kvalifioituja lompakoita: {len(qualified_wallets)}")

                # 4. Tyhjennä edge detector singleton cache — uusi sykli
                try:
                    import tracker as _tracker_module
                    if hasattr(_tracker_module, "_edge_detector_instance") and _tracker_module._edge_detector_instance:
                        _tracker_module._edge_detector_instance.clear_cache()
                except Exception:
                    pass

                # 5. Signaalit
                signals = tracker.process(qualified_wallets, raw_trades, wallet_scores=scores)

                if signals:
                    log.info(f"🔥 Smart Follow -signaaleja: {len(signals)}")
                    for sig in signals[:20]:
                        log.info(
                            f"  🎯 {sig.get('question','')[:45]} | "
                            f"Tuki: {sig['support_count']} lompakon | "
                            f"Outcome: {sig['outcome']} | "
                            f"Koko: {sig['total_size_usdc']:.0f} USDC"
                        )

                    strong_signals = [
                        s for s in signals
                        if s["support_count"] >= smart_threshold
                        and s["total_size_usdc"] >= min_signal_size
                    ]

                    log.info(f"Vahvoja signaaleja: {len(strong_signals)} – {'ostetaan' if not dry_run else 'simuloidaan'}")

                    orders_this_cycle = 0
                    for sig in strong_signals:
                        if orders_this_cycle >= max_orders_per_cycle:
                            log.info(f"Max {max_orders_per_cycle} ostoa per sykli — lopetetaan.")
                            break

                        success = tracker.execute_order(sig)

                        if success and not dry_run:
                            actual_size = sig.get(
                                "_actual_order_size",
                                float(os.getenv("MAX_ORDER_SIZE_USDC", 5))
                            )
                            daily_spent_usdc += actual_size
                            orders_this_cycle += 1
                            save_spending()
                            log.info(f"Päiväkulut: {daily_spent_usdc:.2f} USDC")
                        elif success and dry_run:
                            orders_this_cycle += 1
                else:
                    log.info("Ei signaaleja tällä syklillä.")

            elapsed = time.time() - cycle_start
            log.info(f"Sykli valmis {elapsed:.1f}s. Odotetaan {poll_interval}s...")

        except KeyboardInterrupt:
            log.info("Scout sammuu.")
            break
        except Exception as e:
            log.error(f"Virhe pääsilmukassa: {e}", exc_info=True)

        # Position check odotuksen aikana
        elapsed_wait = 0
        while elapsed_wait < poll_interval:
            sleep_chunk = min(position_check_interval, poll_interval - elapsed_wait)
            time.sleep(sleep_chunk)
            elapsed_wait += sleep_chunk
            if not dry_run and elapsed_wait < poll_interval:
                try:
                    from position_manager import check_and_exit_positions
                    check_and_exit_positions()
                except Exception as e:
                    log.warning(f"Position check epäonnistui: {e}")


if __name__ == "__main__":
    main()