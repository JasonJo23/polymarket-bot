#!/usr/bin/env python3
"""
=============================================================================
export_csv.py - Tidy CSV export for live Excel dashboards
=============================================================================
Flattens the bot's JSON state into clean, one-row-per-record CSV tables that
Excel Power Query can auto-refresh. Read-only: never trades or modifies state.

Outputs (default into ./excel_export/):
  summary.csv      - headline metrics as metric,value (one-glance dashboard)
  positions.csv    - one row per CLOSED position (real PnL) + signal_type/source
  predictions.csv  - one row per Claude edge prediction, with per-row Brier/ROI

Usage (run in the bot directory, e.g. via cron every few minutes):
    python3 export_csv.py
    python3 export_csv.py --dir /root/polymarket-bot --out /root/polymarket-bot/excel_export
=============================================================================
"""

import argparse
import csv
import json
import os
import re
from datetime import datetime, timezone
from collections import defaultdict


def load_json(path, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return default
    except Exception:
        return default


def load_jsonl(path):
    rows = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        rows.append(json.loads(line))
                    except Exception:
                        pass
    except FileNotFoundError:
        pass
    return rows


def norm(s):
    return re.sub(r"[^A-Z0-9]+", " ", str(s or "").upper()).strip()


def fnum(v, d=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return d


def write_csv(path, headers, rows):
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(headers)
        for r in rows:
            w.writerow(r)


def build_positions(resolved, snapshots):
    # accepted-snapshot lookup for signal_type + dominant source
    accepted = {}
    for s in snapshots:
        if s.get("status") == "accepted_candidate":
            accepted[(str(s.get("market_id", "")), norm(s.get("outcome")))] = s

    headers = [
        "market_id", "bought_at", "resolved_at", "question", "market_type",
        "outcome", "signal_type", "wallet_source", "support_count",
        "weighted_support", "edge", "edge_confidence", "our_probability",
        "buy_price", "amount", "cost_usdc", "proceeds_usdc",
        "realized_pnl_usdc", "roi", "win", "bad_fill", "close_status",
    ]
    rows = []
    raw = resolved.get("positions", []) if isinstance(resolved, dict) else []
    for p in raw:
        if not isinstance(p, dict):
            continue
        cost = fnum(p.get("cost_usdc"))
        if cost <= 0:
            cost = fnum(p.get("buy_price")) * fnum(p.get("amount"))
        pnl = fnum(p.get("realized_pnl_usdc"))
        roi = round(pnl / cost, 4) if cost > 0 else ""
        win = 1 if (str(p.get("close_status", "")).endswith("win") or pnl > 0) else 0
        snap = accepted.get((str(p.get("market_id", "")), norm(p.get("outcome"))))
        sig_type = (snap or {}).get("signal_type", "")
        src = ""
        if snap and snap.get("source_breakdown"):
            sb = snap["source_breakdown"]
            src = max(sb, key=lambda k: sb.get(k, 0))
        rows.append([
            p.get("market_id", ""), p.get("bought_at", ""), p.get("resolved_at", ""),
            str(p.get("question", ""))[:80], p.get("market_type", ""),
            p.get("outcome", ""), sig_type, src, p.get("support_count", ""),
            p.get("weighted_support", ""), p.get("edge", ""), p.get("edge_confidence", ""),
            p.get("our_probability", ""), p.get("buy_price", ""), p.get("amount", ""),
            round(cost, 2), p.get("proceeds_usdc", ""), round(pnl, 2), roi, win,
            1 if p.get("bad_fill") else 0, p.get("close_status", ""),
        ])
    # newest first
    rows.sort(key=lambda r: str(r[2]) or str(r[1]), reverse=True)
    return headers, rows


def build_predictions(preds):
    headers = [
        "timestamp", "resolved_at", "market_id", "question", "outcome", "sport",
        "confidence", "market_price", "our_probability", "market_implied",
        "edge", "should_bet", "resolved", "win", "roi_if_bet",
        "brier_claude", "brier_market",
    ]
    rows = []
    raw = preds.get("predictions", []) if isinstance(preds, dict) else []
    for p in raw:
        if not isinstance(p, dict):
            continue
        price = fnum(p.get("polymarket_price"))
        q = fnum(p.get("our_probability"))
        ar = p.get("actual_result")
        resolved = 0 if ar is None else 1
        win = "" if ar is None else (1 if ar else 0)
        roi = ""
        bc = bm = ""
        if resolved and price > 0:
            y = 1.0 if ar else 0.0
            roi = round((1.0 / price - 1.0) if ar else -1.0, 4)
            bc = round((q - y) ** 2, 4)
            bm = round((price - y) ** 2, 4)
        rows.append([
            p.get("timestamp", ""), p.get("resolved_at", ""), p.get("market_id", ""),
            str(p.get("question", ""))[:80], p.get("outcome", ""), p.get("sport", ""),
            p.get("confidence", ""), price, q, price,
            p.get("edge", ""), 1 if p.get("should_bet") else 0, resolved, win, roi, bc, bm,
        ])
    rows.sort(key=lambda r: str(r[1]) or str(r[0]), reverse=True)
    return headers, rows


def build_summary(pos_rows, pred_rows):
    # pos_rows/pred_rows are the data rows (no header)
    n = len(pos_rows)
    cost = sum(fnum(r[15]) for r in pos_rows)        # cost_usdc col
    pnl = sum(fnum(r[17]) for r in pos_rows)         # realized_pnl_usdc col
    wins = sum(1 for r in pos_rows if r[19] == 1)    # win col
    # predictions
    res = [r for r in pred_rows if r[12] == 1]       # resolved col
    pwins = sum(1 for r in res if r[13] == 1)
    bc = sum(fnum(r[15]) for r in res) / len(res) if res else 0.0
    bm = sum(fnum(r[16]) for r in res) / len(res) if res else 0.0
    bet = [r for r in res if r[11] == 1]             # should_bet
    block = [r for r in res if r[11] == 0]
    bet_roi = sum(fnum(r[14]) for r in bet) / len(bet) if bet else 0.0
    block_roi = sum(fnum(r[14]) for r in block) / len(block) if block else 0.0
    verdict = "Claude adds info" if (res and bc < bm) else ("not enough data" if not res else "no edge vs market")

    rows = [
        ["generated_at_utc", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")],
        ["closed_positions", n],
        ["total_cost_usdc", round(cost, 2)],
        ["net_realized_pnl_usdc", round(pnl, 2)],
        ["return_on_cost_pct", round(100 * pnl / cost, 2) if cost else ""],
        ["win_rate_pct", round(100 * wins / n, 1) if n else ""],
        ["resolved_predictions", len(res)],
        ["claude_accuracy_pct", round(100 * pwins / len(res), 1) if res else ""],
        ["claude_brier", round(bc, 4) if res else ""],
        ["market_brier", round(bm, 4) if res else ""],
        ["edge_verdict", verdict],
        ["bet_group_avg_roi", round(bet_roi, 4) if bet else ""],
        ["block_group_avg_roi", round(block_roi, 4) if block else ""],
    ]
    return ["metric", "value"], rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default=".")
    ap.add_argument("--out", default="excel_export")
    args = ap.parse_args()
    d, out = args.dir, args.out
    os.makedirs(out, exist_ok=True)

    resolved = load_json(os.path.join(d, "resolved_positions.json"), {"positions": []})
    snapshots = load_jsonl(os.path.join(d, "signal_snapshots.jsonl"))
    preds = load_json(os.path.join(d, "predictions_log.json"), {"predictions": []})

    ph, pr = build_positions(resolved, snapshots)
    dh, dr = build_predictions(preds)
    sh, sr = build_summary(pr, dr)

    write_csv(os.path.join(out, "positions.csv"), ph, pr)
    write_csv(os.path.join(out, "predictions.csv"), dh, dr)
    write_csv(os.path.join(out, "summary.csv"), sh, sr)

    print("Exported to %s/" % os.path.abspath(out))
    print("  summary.csv      (%d metrics)" % len(sr))
    print("  positions.csv    (%d closed positions)" % len(pr))
    print("  predictions.csv  (%d predictions)" % len(dr))


if __name__ == "__main__":
    main()
