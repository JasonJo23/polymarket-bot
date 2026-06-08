#!/usr/bin/env python3
"""
=============================================================================
analyze_edge.py - Offline edge & PnL analyzer for Polymarket Scout
=============================================================================
Reads the bot's own logs and answers one question: "Is there edge, and where?"

It NEVER trades, never calls an API, and never modifies state. It only reads:

  resolved_positions.json  -> realized PnL of closed positions (the ground truth)
  signal_snapshots.jsonl   -> signal_type + wallet source for each candidate
  predictions_log.json     -> every Claude edge call, with resolved outcomes

Usage (run in the bot directory):
    python3 analyze_edge.py
    python3 analyze_edge.py --dir /root/polymarket-bot
    python3 analyze_edge.py --md report.md      # also write a markdown report

Sections:
  1. Realized trading PnL (by market type, edge confidence, edge size, bad fills)
  2. Signal-type & wallet-source attribution (which discovery path earns money)
  3. Claude edge calibration (Brier vs market, gate value, calibration table)
=============================================================================
"""

import argparse
import json
import os
import re
from collections import defaultdict


# --------------------------------------------------------------------------
# Loading helpers
# --------------------------------------------------------------------------

def load_json(path, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return default
    except Exception as e:
        print("  (could not read %s: %s)" % (path, e))
        return default


def load_jsonl(path):
    rows = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except Exception:
                    pass
    except FileNotFoundError:
        pass
    return rows


def norm(s):
    return re.sub(r"[^A-Z0-9]+", " ", str(s or "").upper()).strip()


def fnum(v, default=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def money(v):
    return "%+.2f" % v


def pct(n, d):
    return "%.1f%%" % (100.0 * n / d) if d else "  n/a"


# --------------------------------------------------------------------------
# Output buffering (so we can print AND optionally write markdown)
# --------------------------------------------------------------------------

LINES = []
def out(s=""):
    LINES.append(s)
    print(s)


def hr(title):
    out("")
    out("=" * 70)
    out(title)
    out("=" * 70)


def table(headers, rows, aligns=None):
    cols = len(headers)
    widths = [len(str(h)) for h in headers]
    for r in rows:
        for i in range(cols):
            widths[i] = max(widths[i], len(str(r[i])))
    aligns = aligns or ["<"] * cols
    def fmt(r):
        return "  ".join(
            ("%-*s" % (widths[i], r[i])) if aligns[i] == "<"
            else ("%*s" % (widths[i], r[i]))
            for i in range(cols)
        )
    out(fmt(headers))
    out("  ".join("-" * widths[i] for i in range(cols)))
    for r in rows:
        out(fmt(r))


# --------------------------------------------------------------------------
# Section 1 + 2: realized positions
# --------------------------------------------------------------------------

def pnl_block(rows):
    """rows = list of dicts each with cost, pnl, win(bool)."""
    n = len(rows)
    cost = sum(r["cost"] for r in rows)
    pnl = sum(r["pnl"] for r in rows)
    wins = sum(1 for r in rows if r["win"])
    roi = pct(pnl, cost) if cost else "  n/a"
    return n, cost, pnl, wins, roi


def grouped_pnl(positions, keyfn, label):
    groups = defaultdict(list)
    for p in positions:
        groups[keyfn(p)].append(p)
    rows = []
    for k in sorted(groups, key=lambda x: -sum(r["pnl"] for r in groups[x])):
        n, cost, pnl, wins, roi = pnl_block(groups[k])
        rows.append([str(k), n, pct(wins, n), "%.2f" % cost, money(pnl), roi])
    out("")
    out("By %s:" % label)
    table(["group", "n", "win%", "cost", "pnl", "roi"], rows,
          aligns=["<", ">", ">", ">", ">", ">"])


def edge_bucket(e):
    e = fnum(e)
    if e <= 0:      return "edge<=0"
    if e < 0.08:    return "0-0.08"
    if e < 0.15:    return "0.08-0.15"
    return "0.15+"


def support_bucket(s):
    s = int(fnum(s))
    if s < 5:   return "sup<5"
    if s < 10:  return "sup5-9"
    if s < 20:  return "sup10-19"
    return "sup20+"


def analyze_positions(resolved, snapshots):
    hr("1. REALIZED TRADING PnL  (resolved_positions.json)")
    raw = resolved.get("positions", []) if isinstance(resolved, dict) else []
    positions = []
    for p in raw:
        if not isinstance(p, dict):
            continue
        cost = fnum(p.get("cost_usdc"))
        pnl = fnum(p.get("realized_pnl_usdc"))
        if cost <= 0 and pnl == 0:
            # fall back to buy_price*amount if cost not recorded
            cost = fnum(p.get("buy_price")) * fnum(p.get("amount"))
        positions.append({
            "raw": p,
            "cost": cost,
            "pnl": pnl,
            "win": str(p.get("close_status", "")).endswith("win") or pnl > 0,
            "market_type": p.get("market_type") or "unknown",
            "edge_conf": p.get("edge_confidence") or "none",
            "edge": p.get("edge"),
            "support": p.get("support_count"),
            "bad_fill": bool(p.get("bad_fill")),
            "market_id": str(p.get("market_id", "")),
            "outcome": p.get("outcome", ""),
            "question": p.get("question", ""),
        })

    if not positions:
        out("No resolved positions yet. Once trades close, this fills in.")
        return

    n, cost, pnl, wins, roi = pnl_block(positions)
    out("")
    out("Closed positions : %d" % n)
    out("Total cost       : %.2f USDC" % cost)
    out("Net realized PnL : %s USDC" % money(pnl))
    out("Return on cost   : %s" % roi)
    out("Win rate         : %s  (%d/%d)" % (pct(wins, n), wins, n))

    grouped_pnl(positions, lambda p: p["market_type"], "market type")
    grouped_pnl(positions, lambda p: p["edge_conf"], "edge confidence")
    grouped_pnl(positions, lambda p: edge_bucket(p["edge"]), "edge size")
    grouped_pnl(positions, lambda p: support_bucket(p["support"]), "support count")
    grouped_pnl(positions, lambda p: "bad_fill" if p["bad_fill"] else "clean_fill",
                "fill quality")

    # worst / best
    s = sorted(positions, key=lambda p: p["pnl"])
    out("")
    out("Worst 5 trades:")
    for p in s[:5]:
        out("  %s USDC | %-9s | %s" % (money(p["pnl"]), p["market_type"], str(p["question"])[:45]))
    out("Best 5 trades:")
    for p in reversed(s[-5:]):
        out("  %s USDC | %-9s | %s" % (money(p["pnl"]), p["market_type"], str(p["question"])[:45]))

    # ---- Section 2: join with snapshots for signal_type + source ----
    hr("2. SIGNAL-TYPE & WALLET-SOURCE ATTRIBUTION  (snapshots x resolved)")
    accepted = {}
    for snap in snapshots:
        if snap.get("status") != "accepted_candidate":
            continue
        key = (str(snap.get("market_id", "")), norm(snap.get("outcome")))
        accepted[key] = snap  # keep last seen

    if not accepted:
        out("No accepted-candidate snapshots found (signal_snapshots.jsonl empty?).")
        return

    matched = 0
    by_type = defaultdict(list)
    by_source = defaultdict(list)
    for p in positions:
        snap = accepted.get((p["market_id"], norm(p["outcome"])))
        if not snap:
            continue
        matched += 1
        by_type[snap.get("signal_type") or "unknown"].append(p)
        sb = snap.get("source_breakdown") or {}
        dom = max(sb, key=lambda k: sb.get(k, 0)) if sb else "unknown"
        by_source[dom].append(p)

    out("")
    out("Matched %d of %d closed positions to their signal snapshot." % (matched, n))
    if matched:
        rows = []
        for k in sorted(by_type, key=lambda x: -sum(r["pnl"] for r in by_type[x])):
            nn, cc, pp, ww, rr = pnl_block(by_type[k])
            rows.append([k, nn, pct(ww, nn), money(pp), rr])
        out("")
        out("By signal type:")
        table(["signal_type", "n", "win%", "pnl", "roi"], rows,
              aligns=["<", ">", ">", ">", ">"])
        rows = []
        for k in sorted(by_source, key=lambda x: -sum(r["pnl"] for r in by_source[x])):
            nn, cc, pp, ww, rr = pnl_block(by_source[k])
            rows.append([k, nn, pct(ww, nn), money(pp), rr])
        out("")
        out("By dominant wallet source (spike / known / holder):")
        table(["source", "n", "win%", "pnl", "roi"], rows,
              aligns=["<", ">", ">", ">", ">"])


# --------------------------------------------------------------------------
# Section 3: Claude edge calibration
# --------------------------------------------------------------------------

def analyze_predictions(preds):
    hr("3. CLAUDE EDGE CALIBRATION  (predictions_log.json)")
    rows = preds.get("predictions", []) if isinstance(preds, dict) else []
    resolved = [p for p in rows if isinstance(p, dict) and p.get("actual_result") is not None]
    out("")
    out("Logged predictions : %d" % len(rows))
    out("Resolved (scored)  : %d" % len(resolved))
    if len(resolved) < 5:
        out("Not enough resolved predictions yet for a verdict (need ~30+ to trust).")
        return

    # Brier: Claude prob vs market price, lower is better
    brier_c = brier_m = 0.0
    claude_correct = 0
    for p in resolved:
        y = 1.0 if p.get("actual_result") else 0.0
        q = fnum(p.get("our_probability"))
        m = fnum(p.get("polymarket_price"))
        brier_c += (q - y) ** 2
        brier_m += (m - y) ** 2
        if (q >= 0.5) == (y >= 0.5):
            claude_correct += 1
    nb = len(resolved)
    out("")
    out("Claude directional accuracy : %s  (%d/%d)" % (pct(claude_correct, nb), claude_correct, nb))
    out("Brier score  Claude         : %.4f   (lower = better)" % (brier_c / nb))
    out("Brier score  market price   : %.4f" % (brier_m / nb))
    verdict = "Claude ADDS info" if brier_c < brier_m else "Claude does NOT beat market"
    out("Verdict                     : %s (%.4f vs %.4f)" % (verdict, brier_c / nb, brier_m / nb))

    # Gate value: ROI if you bet at market price, split by should_bet
    def roi_if_bet(group):
        tot = 0.0
        for p in group:
            price = fnum(p.get("polymarket_price"))
            if price <= 0:
                continue
            tot += (1.0 / price - 1.0) if p.get("actual_result") else -1.0
        return tot, (tot / len(group)) if group else 0.0

    bet = [p for p in resolved if p.get("should_bet")]
    nobet = [p for p in resolved if not p.get("should_bet")]
    bt, ba = roi_if_bet(bet)
    nt, na = roi_if_bet(nobet)
    out("")
    out("Gate value (theoretical, betting 1 unit at market price each):")
    table(
        ["edge gate said", "n", "avg ROI/bet", "total units"],
        [["BET (should_bet)", len(bet), "%+.3f" % ba, "%+.2f" % bt],
         ["BLOCK (no edge)", len(nobet), "%+.3f" % na, "%+.2f" % nt]],
        aligns=["<", ">", ">", ">"],
    )
    if nobet and na > ba:
        out(">> WARNING: blocked predictions outperformed bet ones - the edge gate")
        out("   may be filtering out winners. This is the shadow-mode question.")
    elif bet and ba > 0:
        out(">> The BET group is profitable on paper; the edge gate looks additive.")

    # by confidence
    by_conf = defaultdict(list)
    for p in resolved:
        by_conf[p.get("confidence", "?")].append(p)
    rows2 = []
    for c in ("high", "medium", "low"):
        g = by_conf.get(c, [])
        if not g:
            continue
        wins = sum(1 for p in g if p.get("actual_result"))
        avg_edge = sum(fnum(p.get("edge")) for p in g) / len(g)
        rows2.append([c, len(g), pct(wins, len(g)), "%+.3f" % avg_edge])
    if rows2:
        out("")
        out("By confidence bucket:")
        table(["confidence", "n", "win%", "avg edge"], rows2, aligns=["<", ">", ">", ">"])

    # calibration table: predicted prob bucket vs actual frequency
    buckets = [(0.0, 0.2), (0.2, 0.4), (0.4, 0.6), (0.6, 0.8), (0.8, 1.01)]
    rows3 = []
    for lo, hi in buckets:
        g = [p for p in resolved if lo <= fnum(p.get("our_probability")) < hi]
        if not g:
            continue
        actual = sum(1 for p in g if p.get("actual_result")) / len(g)
        predmid = sum(fnum(p.get("our_probability")) for p in g) / len(g)
        rows3.append(["%.1f-%.1f" % (lo, hi), len(g), "%.2f" % predmid, "%.2f" % actual])
    if rows3:
        out("")
        out("Calibration (predicted prob vs actual win freq; should track closely):")
        table(["pred bucket", "n", "avg pred", "actual"], rows3, aligns=["<", ">", ">", ">"])


# --------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default=".", help="bot directory containing the json/jsonl files")
    ap.add_argument("--md", default="", help="optional path to also write a markdown report")
    args = ap.parse_args()
    d = args.dir

    out("Polymarket Scout - edge & PnL report")
    out("dir: %s" % os.path.abspath(d))

    resolved = load_json(os.path.join(d, "resolved_positions.json"), {"positions": []})
    snapshots = load_jsonl(os.path.join(d, "signal_snapshots.jsonl"))
    preds = load_json(os.path.join(d, "predictions_log.json"), {"predictions": []})

    analyze_positions(resolved, snapshots)
    analyze_predictions(preds)

    hr("HOW TO READ THIS")
    out("- Section 1/2: real money. Positive net PnL and win% > break-even = working.")
    out("- Section 3 Brier: if Claude's Brier < market's, the edge model truly predicts.")
    out("- Gate value: if BLOCKED bets beat BET ones, the edge gate is costing you;")
    out("  that's the signal to run EDGE_DETECTOR_SHADOW_MODE=true and re-check here.")

    if args.md:
        try:
            with open(args.md, "w", encoding="utf-8") as f:
                f.write("# Polymarket Scout edge & PnL report\n\n```\n")
                f.write("\n".join(LINES))
                f.write("\n```\n")
            print("\n[written markdown: %s]" % args.md)
        except Exception as e:
            print("could not write markdown: %s" % e)


if __name__ == "__main__":
    main()
