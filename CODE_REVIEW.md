# Polybottinen — Senior Engineering Review

*Polymarket copy-trading "Scout" bot. Review focus: code functionality end-to-end, from finding a market to placing an order, plus bottlenecks and improvements.*

---

## 1. What the system actually is

It is a **copy-trading scout for Polymarket**. The thesis (stated in `fetcher.py`) is *speed-based*: find wallets that just reacted to fresh information (a lineup, a news drop, an insider move) and follow them before the price fully adjusts. On top of that copy-trading core, a second, independent layer (`probability_engine.py` + `edge_detector.py`) asks Claude to compute its *own* probability and only lets an order through if Claude *also* sees an edge.

It runs as a single-threaded infinite loop (`main.py`) on a worker dyno (`Procfile`), polling every `POLL_INTERVAL_SECONDS` (default 600s), with all state in flat JSON files. There are ~70 tunable thresholds in `.env`.

The code is mature and defensively written — atomic JSON writes, disk caches, retry/backoff, dry-run safety, daily risk caps, bad-fill force-exit. The version comments show many bugs have already been hunted down. The issues below are the *next* layer.

---

## 2. End-to-end pipeline (find market → place order)

Each cycle runs these stages in order:

**Stage 0 — Risk gate (`main.py`).** Position exits checked, prediction calibration updated, bankroll fetched, daily spend/loss caps checked. Any breach flips the tracker into dry-run.

**Stage 1 — Market discovery (`fetcher._fetch_closing_soon_markets`).** Page through up to `MARKET_SCAN_LIMIT` (500) Gamma markets sorted by 24h volume, keep those closing within `CLOSING_DAYS` with volume ≥ `MIN_VOLUME_24H` and ≥1h left. Keep top `TOP_MARKETS` (10).

**Stage 2 — Wallet discovery (`fetcher._collect_wallets_from_holders`).** For each market: pull 2h "volume-spike" BUY-ers (the primary signal) and the top holders (fallback). Merge in the persistent `known_wallets.json` universe.

**Stage 3 — History fetch (`fetcher.fetch_recent_trades`).** Pull each wallet's trade history in parallel (`FETCH_WORKERS`=16), keep the last 48h for signal-building, keep the deeper history (up to `SCORING_HISTORY_LIMIT`=300) for scoring.

**Stage 4 — Pre-qualify (`analyzer.analyze`, no scores).** Group raw trades by wallet, compute 48h metrics, apply base filter (`trades_48h`, `avg_size` window). This narrows the set before the expensive scoring.

**Stage 5 — Wallet scoring (`wallet_scorer.score_wallets_batch`).** For each pre-qualified wallet: group its history by market, look up each market's resolved winner (CLOB then Gamma, cached to `market_cache.json`), compute a weighted ROI → a `weight` (0.4–2.0), plus per-category weights and recent-activity flags. Disk-cached with a fingerprint + TTL.

**Stage 6 — Re-qualify + signal building (`analyzer.analyze` with scores → `tracker.process`).** Build `market → outcome → [supporters]`, fetch live market info from CLOB in parallel, then per outcome compute `weighted_support`, `positive_roi_support`, `active_support`, `high_weight_support`, source breakdown, and price-move-since-first-seen. Reject late/volatile signals, then classify each as `smart_follow` or `fresh_spike`. Keep the best outcome per market, sort by `weighted_support`.

**Stage 7 — Execution (`tracker.execute_order`).** For each strong signal: dedupe (48h memory), opposite-position check, re-fetch market, match outcome → token, candidate-price check, edge cooldown, **Intelligence** order-book quality gate (`intelligence.py`), **EdgeDetector** Claude-probability gate (`edge_detector.py` + `probability_engine.py`), final price rules, order sizing, profit-floor check, then place the order — FOK marketable-limit for sports/esports, GTC limit for macro. Record fill → `add_position`, or track a pending/delayed order for later reconciliation.

**Stage 8 — Exit management (`position_manager.check_and_exit_positions`).** Runs every `POSITION_CHECK_SECONDS` (300s) including during the sleep window. Per-market-type exit rules (sports hold-to-resolution with stop/profit-lock; esports SL/TP; macro time-decayed TP). Sells via GTC limit 2% under market.

---

## 3. Bottlenecks

### 3.1 The strategy's edge is speed, but the architecture is slow — this is the core tension
The whole premise is reacting *faster than the market* to a fresh buy. But a single cycle is a long serial chain: discover markets → (serial) per-market holders/spike calls with a `REQUEST_DELAY_SECONDS` sleep each → fetch 200+ wallet histories → resolve dozens of markets per wallet for scoring → build signals → and finally, at the most time-critical moment, `execute_order` does **`time.sleep(1.0)` + `time.sleep(0.5)` + a tick-size GET + several sequential CLOB GETs + a synchronous Claude API call**, per signal. With a 600s poll interval, by the time you copy a spike buyer the price has almost certainly already moved. **You are architected to arrive late to a strategy that only pays if you arrive early.**

### 3.2 Stage 2 is serial
`_collect_wallets_from_holders` loops markets sequentially with two API calls plus `time.sleep(request_delay)` each. Fine for 10 markets, but it's pure latency on the critical path and should be parallelized like the history fetch already is.

### 3.3 First-run scoring cost
`score_wallets_batch` on a cold cache resolves up to `WALLET_SCORE_MAX_MARKETS` (50) markets per wallet × 200+ wallets — this is the documented "5–9 min" problem. The disk cache + TTL mostly fix steady state, but the prefetch uses `SCORING_WORKERS` and **every non-fetcher module opens a fresh `requests` connection** (no pooled `Session`), paying a TLS handshake per call. fetcher pools connections; `wallet_scorer`, `intelligence`, `position_manager`, `probability_engine` do not.

### 3.4 Redundant work per cycle
`analyzer.analyze` runs twice (full grouping + metrics recomputed). Per-order, a fresh `ClobClient` is constructed for every balance check, every buy, every sell. Worker-count default is inconsistent for the same env var (`FETCH_WORKERS` defaults to 16 in `fetcher`, 4 in `tracker.process`).

### 3.5 Unbounded files rewritten wholesale
`predictions_log.json` is fully re-serialized on every prediction *and* every calibration pass — O(n) writes that grow without bound. `signal_snapshots.jsonl` and `scout.log` grow forever (no rotation). These will quietly degrade over weeks/months.

---

## 4. Correctness / logic issues (highest-impact first)

### 4.1 Wallet ROI — the core ranking signal — is biased and hypothetical
This is the most important finding because *everything* downstream (weights, support counts, sizing) rests on it.

- **Missing price → wrong payoff.** In `_group_trades_by_market`, when a trade has no usable price, it does `shares += size` — i.e. it silently assumes a price of 1.0. `_calculate_market_roi` then computes ROI from those "shares." Any wallet with poor price coverage gets its winning payoff *understated*, dragging ROI and weight down. `price_coverage` is computed but **never used to discount or reject** the score.
- **Sells are ignored.** Only `side == "BUY"` trades are grouped. A wallet that bought then sold before resolution is scored *as if it held to resolution*. The "ROI" is a hypothetical hold-to-resolution number, not the wallet's realized performance — so "smart wallet" can be an artifact.
- **No confidence shrinkage.** A wallet with 5 resolved markets at +9% gets `weight = 2.0`, identical to one with 50 resolved markets at +9%. Weight should shrink toward a neutral prior by sample size (Wilson / Bayesian), not jump on tiny samples.

### 4.2 Copy-trading and independent-modeling are fighting each other
The bot first decides a wallet set is "smart," then `EdgeDetector` overrides them with Claude's own probability and **rejects unless Claude independently finds an edge** (fail-closed). For macro/crypto the prompt *explicitly tells Claude to trust the Polymarket price* → `our_prob ≈ price` → `edge ≈ 0` → reject. For sports/esports the gate needs `data_quality ≥ MIN_DATA_QUALITY` (0.3), which the context providers often can't reach. Net effect: the gate likely rejects the large majority of copy signals, and the bot only really fires through `fresh_spike`, `probe_mode`, or the late-quality override. You've built two different theses and required both to agree — which is the most restrictive possible combination and discards most of the copy edge you went to such lengths to detect.

### 4.3 The learning loop is measured but never used
`predictions_log.json` is logged and resolved, `analyze_calibration()` exists — but **nothing feeds win-rate back** into thresholds, sizing, or whether to trust the Claude edge at all. You are paying for the Claude calls and the instrumentation but never closing the loop, so you can't know if the edge layer is net-positive or net-negative.

### 4.4 "Freshness" can be a false reading
`price_move_since_first_seen` is measured against the *bot's* first sighting, which is persisted state. If the bot only scans every 10 min and a market first appears mid-move, `price_at_first_seen` = current price → move = 0 → it passes the late/volatile filter as "fresh" even though the real-world move already happened.

### 4.5 Two latent live-trading availability bugs
- `get_usdc_balance_v2` returns `0.0` on a transient CLOB error (when `allow_fallback=False`). `main` then sees `bankroll < min_bankroll` and **permanently flips the process into dry-run** — a momentary balance-fetch failure silently disables live trading until restart.
- When the **daily spend/loss cap** trips, `tracker.dry_run` is set `True`, but `reset_if_new_day()` never resets it. After hitting the cap once, the tracker stays in dry-run **across day boundaries** until the process is restarted.

### 4.6 Executed markets bypass the discovery filters
Signals are built from qualified wallets' *entire* 48h trade list across *any* market — not only the closing-soon, high-volume markets the scout selected (`FRESH_SPIKE_MIN_SCOUT_SCOPE_SUPPORT` defaults to 0). So you can place orders on illiquid or far-dated markets that never passed the `MIN_VOLUME_24H` / closing-soon gates. The order-book quality check in `intelligence.py` becomes your *only* liquidity guard at that point.

### 4.7 Position sizing doesn't use the edge it computes
You have `our_probability` and `edge` in hand but size by confidence buckets × multipliers, capped — not by a principled fraction of bankroll (fractional Kelly given odds). There's also no check of order size against *remaining bankroll or open exposure*, only the daily spend cap. With bankroll 190 and 3×40 per cycle you can commit a large fraction without an exposure model.

---

## 5. Code-quality / architecture issues

- **Duplicated parsing logic, with drift.** `_parse_timestamp` / `_ts`, `_parse_size_usdc`, `_extract_address`, `_normalize` are re-implemented in `fetcher`, `analyzer`, `wallet_scorer`, `tracker`, `position_manager` — and they **already disagree** (e.g. `fetcher._ts` checks four timestamp keys; `analyzer._parse_timestamp` checks only `"timestamp"`). Same data, different rules → silent inconsistencies. Centralize in one `polymath_utils.py`.
- **No tests, no type checking.** For software that moves real money, there is zero automated verification. The risk/sizing/exit math and the outcome-matching normalizer are exactly the kind of pure functions that should have unit tests.
- **Fragile cross-module coupling.** `main` reaches into `tracker._edge_detector_instance` to clear a cache; modules mutate global dict caches (`_market_result_cache`, `_wallet_score_cache`) from inside thread pools without locks (low risk in CPython, but unguarded).
- **`god-object` tracker.py (1,625 lines)** mixes signal scoring, gating rules, CLOB client construction, order placement, fill parsing, pending-order reconciliation, and persistence. Worth splitting (signal-scoring vs. order-execution vs. persistence).
- **Config sprawl.** ~70 env knobs with logic-bearing defaults scattered in code. No schema/validation; a typo'd float silently changes behavior. A typed config object loaded once would prevent drift and re-reading `os.getenv` in hot loops.
- **Observability.** Rich logs exist but in mixed Finnish/English, file-only, unrotated. No metrics emission (orders attempted/filled, reject reasons, realized PnL by type) beyond the Telegram summary.

---

## 6. The biggest opportunity (data-analytics lens)

You have already **instrumented everything** — `signal_snapshots.jsonl`, `predictions_log.json`, `resolved_positions.json`, daily metrics — but there is **no analytics layer consuming it.** Every threshold in `.env` appears to have been chosen anecdotally (the comments literally cite single examples: *"US-Iran NO +0.195"*, *"NaVi +0.185 conf=medium"*). That is the gap between this being a hand-tuned rule engine and a data-driven system.

Concretely, the highest-leverage work is a small offline research notebook/pipeline that:
1. Joins `signal_snapshots` (accepted *and* rejected) to `resolved_positions` to compute **realized PnL and hit-rate by `signal_type`, `market_type`, `source`, support bucket, and price-move bucket.** This tells you which gates earn money and which just block fills.
2. Runs `analyze_calibration()` and asks the decisive question: **is the Claude EdgeDetector net-positive vs. simply copying the wallets?** If copy-only beats copy+edge, you're paying for a filter that costs money.
3. Replaces the hand-set thresholds with values fit on this history, and re-derives wallet weights with **sample-size shrinkage** and **realized (sell-aware) ROI**.

You don't need ML — you need to close the loop you already built.

---

## 7. Prioritized recommendations

**P0 — protect capital / correctness**
1. Fix the two dry-run latches (4.5): retry/fallback the balance fetch instead of returning 0.0; reset the daily-cap dry-run flag on day rollover.
2. Make wallet ROI honest (4.1): discount or drop scores below a `price_coverage` floor; account for sells (realized ROI) or relabel the metric as "hold-to-resolution proxy"; add sample-size shrinkage on `weight`.
3. Decide the thesis (4.2): either copy-trade *or* model independently as the gate. Recommend running EdgeDetector in **shadow/log-only** mode for a few weeks and comparing copy-only vs copy+edge before letting it block orders.

**P1 — make the edge real**
4. Close the learning loop (4.3 / §6): build the offline PnL-attribution + calibration pipeline; let it set thresholds.
5. Add an exposure model and fractional-Kelly sizing using the `edge`/`our_probability` you already compute (4.7).
6. Constrain executed markets to vetted scope or add a hard liquidity/closing gate at execution (4.6).

**P2 — latency (only matters if the speed thesis survives §6)**
7. Strip the hardcoded sleeps and serialize fewer calls in `execute_order`; parallelize Stage 2; share a pooled `requests.Session` and a single reused `ClobClient` (3.1–3.4).
8. Shorten the poll interval or move spike-buyer detection to a lightweight fast path that doesn't wait for full scoring.

**P3 — maintainability**
9. Centralize parsing utilities (5.1) and add unit tests for sizing, exits, outcome-matching, and ROI.
10. Add log rotation + bounded prediction/snapshot files; load config once into a typed object.

---

### One-line summary
A genuinely sophisticated, defensively-coded copy-trading bot whose two biggest problems are conceptual, not cosmetic: **(1) the wallet-quality signal it ranks on is biased and hypothetical, and (2) it bolts an independent Claude-edge gate on top of copy-trading and requires both to agree — while never using the calibration data it already collects to check whether any of it makes money.** Fix the ROI math, close the measurement loop, and decide on a single thesis before chasing the (real but secondary) latency bottlenecks.
