#!/usr/bin/env python3
"""Pass 2: latency-shifted re-rank of the buy-and-hold edge-floor candidates (#536).

The pass-1 ranker (`rank_72hr_buyandhold.py`) scores each first-buy at the LEADER's
entry price. When we copy, we observe the leader's trade ~Δ seconds late and enter at
whatever the market is then. This pass re-prices every candidate position at the
CLOB minute historical-price reference: the LATEST sample at-or-before `entry+Δ`
(at-or-before per repository precedent — forward selection would be look-ahead),
staleness bounded by the window, strictly before actual resolution. Positions with no
fresh-enough sample are NOT REPRICED and count against the wallet's repricing
coverage (the `fill_rate` wire column, name retained for compatibility). The prior
print-tape proxy was retired at the #536 owner-gated cutover: it measured a
wallet-sampled trade tape (side-blind, up to 120s late) instead of the market's
state at our entry; the pinned batch-70 experiment on the issue quantified the
difference (survivors 299 → 558; late-print drift ≈ +1-2¢ against winners).

Fail-closed publication gate: every candidate pair's needed reference window must be
terminal in `ranker_price_pages` ('complete' or valid-'empty', written by
`pe-bootstrap prices-history --targets-csv`); any un-terminal remainder exits 75 —
the supervised tempfail lane — so a partially fetched cycle can never publish a
selectively biased batch. Selection statistics are never rewritten by that
operational gate: validated-empty, unmapped, stale, and invalid-price positions
simply count as not repriced.

Inputs: pass-1 `ranked_72hr_buyandhold.csv` + `qualifying_positions_72hr.csv` (must
include `outcome_id`) + the cache's `ranker_price_points`/`ranker_price_pages` and
`token_conditions`. Outputs: `latency_shift_ranked.csv`, `latency_shift_basket.txt`,
`oracle_outcomes.csv` (per-position chosen sample or typed not-repriced reason — the
replay/provenance artifact), and `oracle_manifest.json` (the versioned run manifest
whose canonical hash the publisher stores in `ranking_batches.config_hash`).
"""
from __future__ import annotations

import argparse
import bisect
import csv
import hashlib
import json
import math
import os
import sqlite3
import statistics
import sys
import time

from ranker_decay import (
    DEFAULT_HALF_LIFE_DAYS,
    decay_weights,
    parse_as_of,
    weighted_stats,
)

# Versioned oracle identity for the run manifest. Bump ORACLE_VERSION on any change to
# the lookup rule or repricing semantics; the fetch side's parser identity is
# `pe_bootstrap::prices_history::RANKER_PRICE_PARSER_VERSION` (mirrored here).
ORACLE_NAME = "clob-minute-reference"
ORACLE_VERSION = 1
ORACLE_FIDELITY_MINUTES = 1
ORACLE_PARSER_VERSION = 1


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="data/wallet_cache.db")
    p.add_argument("--ranked-csv", required=True, help="pass-1 ranked_72hr_buyandhold.csv")
    p.add_argument("--positions-csv", required=True,
                   help="pass-1 qualifying_positions_72hr.csv (must include outcome_id)")
    p.add_argument("--out-dir", default="data/eval-results")
    p.add_argument("--latency-shift-secs", type=float, default=2.0,
                   help="Δ: re-price at the latest reference sample at <= entry+Δ "
                        "(2s = the measured websocket-path copy latency, #530)")
    p.add_argument("--emit-targets", default=None, metavar="PATH",
                   help="write the per-token merged backward fetch windows "
                        "(token_id,start_ts,end_ts) for the candidate positions and exit "
                        "(consumed by `pe-bootstrap prices-history --targets-csv`).")
    p.add_argument("--fill-window-secs", type=float, default=120.0,
                   help="reference-sample staleness bound: the chosen sample must be at "
                        "most this many seconds before entry+Δ; older => NOT REPRICED. "
                        "Must be positive (the bound also anchors the fetch windows).")
    p.add_argument("--slip-cents", type=float, default=1.0,
                   help="entry slippage in cents on the repriced fill basis")
    p.add_argument("--half-life-days", type=float, default=DEFAULT_HALF_LIFE_DAYS,
                   help="exponential recency-decay half-life in days for the net edge/t-stat "
                        "(a fill one half-life old weighs 0.5). <= 0 disables decay (flat = legacy). "
                        "coverage / hit_rate / activity gates stay raw. Same weight as pass-1.")
    p.add_argument("--as-of", default=None,
                   help="decay age anchor (ISO date/datetime or unix epoch). Default: max repriced entry_ts.")
    p.add_argument("--floor-tstat", type=float, default=2.0,
                   help="pass-1 candidate floor AND pass-2 survival floor on net t-stat")
    p.add_argument("--min-fill-rate", type=float, default=0.5,
                   help="drop wallets whose repriced fraction (repricing coverage; the "
                        "`fill_rate` wire column) is below this")
    p.add_argument("--min-active-months", type=int, default=3)
    p.add_argument("--min-avg-per-month", type=float, default=20.0)
    p.add_argument("--min-trl", type=int, default=0,
                   help="minimum track-record length: survival requires >= this many REPRICED "
                        "positions (docs/_GLOSSARY ranker_prod_min_trl). 0 = off. Mirrors pass-1; "
                        "the run28 production shape uses 20 with the per-month gates zeroed.")
    p.add_argument("--target-n", type=int, default=25)
    p.add_argument("--git-sha", default="unknown",
                   help="code revision recorded in the run manifest (the wrapper passes it)")
    return p.parse_args()


def load_candidates(ranked_csv: str, floor_tstat: float) -> set[str]:
    """Edge-floor candidates from pass 1: eligible & tstat_net>=floor & mean_net>0."""
    out: set[str] = set()
    with open(ranked_csv, newline="") as f:
        for r in csv.DictReader(f):
            try:
                if (r.get("eligible", "").strip().lower() in ("true", "1")
                        and float(r["tstat_net"]) >= floor_tstat
                        and float(r["mean_net"]) > 0):
                    out.add(r["wallet"])
            except (ValueError, KeyError):
                continue
    return out


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


# ─── Reference-oracle helpers (#536) ─────────────────────────────────────────────

def map_pair_tokens(db: str, pairs: list[tuple[str, str]]) -> dict[tuple[str, str], str]:
    """(market_id, outcome_id) → CLOB token_id via token_conditions (positional
    outcome_index; NULL rows skipped, never mispriced). Unmapped pairs are simply
    absent — their positions are not repriceable (honest)."""
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    con.execute("PRAGMA busy_timeout=30000;")
    out: dict[tuple[str, str], str] = {}
    markets = sorted({m for m, _ in pairs})
    want = set(pairs)
    for i in range(0, len(markets), 800):
        chunk = markets[i:i + 800]
        ph = ",".join("?" * len(chunk))
        for cid, oi, tid in con.execute(
                f"SELECT condition_id, outcome_index, token_id FROM token_conditions "
                f"WHERE condition_id IN ({ph}) AND outcome_index IS NOT NULL", chunk):
            key = (cid, str(int(oi)))
            if key in want:
                out[key] = tid
    con.close()
    return out


def merge_ranges(ranges: list[tuple[int, int]]) -> list[tuple[int, int]]:
    """Sorted disjoint union of inclusive integer ranges (adjacent ranges merge)."""
    merged: list[tuple[int, int]] = []
    for lo, hi in sorted(ranges):
        if merged and lo <= merged[-1][1] + 1:
            merged[-1] = (merged[-1][0], max(merged[-1][1], hi))
        else:
            merged.append((lo, hi))
    return merged


def subtract_ranges(needed: list[tuple[int, int]],
                    covered: list[tuple[int, int]]) -> list[tuple[int, int]]:
    """`needed` minus the union of `covered`, all bounds inclusive — the python twin of
    the fetch side's range algebra, used only for the fail-closed publication gate."""
    out: list[tuple[int, int]] = []
    covered = merge_ranges(covered)
    for lo, hi in merge_ranges(needed):
        cursor = lo
        for c_lo, c_hi in covered:
            if c_hi < cursor:
                continue
            if c_lo > hi:
                break
            if c_lo > cursor:
                out.append((cursor, c_lo - 1))
            cursor = max(cursor, c_hi + 1)
            if cursor > hi:
                break
        if cursor <= hi:
            out.append((cursor, hi))
    return out


def pair_windows(positions: list[dict], shift: float, fill_window: float) -> list[tuple[int, int]]:
    """Merged backward decision windows for one pair's positions, padded one second
    each side (the venue documents after/before filtering; the lookup clamps locally)."""
    return merge_ranges([
        (int(p["entry_ts"] + shift - fill_window) - 1, int(p["entry_ts"] + shift) + 1)
        for p in positions
    ])


def main() -> int:
    a = parse_args()
    if a.fill_window_secs <= 0:
        log("FATAL: --fill-window-secs must be positive — the staleness bound also "
            "anchors the backward fetch windows (#536 review)")
        return 1
    os.makedirs(a.out_dir, exist_ok=True)
    slip = a.slip_cents / 100.0
    cand = load_candidates(a.ranked_csv, a.floor_tstat)
    log(f"pass-1 edge-floor candidates: {len(cand)} wallets")
    if not cand:
        log("no candidates; nothing to re-rank")
        return 1

    # Group candidate positions by (market, outcome) — one reference series each.
    by_mo: dict[tuple[str, str], list[dict]] = {}
    npos = 0
    with open(a.positions_csv, newline="") as f:
        rd = csv.DictReader(f)
        if "outcome_id" not in rd.fieldnames:
            log("FATAL: positions CSV lacks outcome_id — re-run pass 1 with the "
                "rank_72hr_buyandhold.py ranker (it writes outcome_id).")
            return 1
        for r in rd:
            w = r["wallet"]
            if w not in cand:
                continue
            key = (r["market_id"], r["outcome_id"])
            # The positions file's `price` column (the leader's entry) is retained in
            # the file format but not loaded: repricing uses only the reference sample.
            by_mo.setdefault(key, []).append({
                "wallet": w,
                "entry_ts": int(r["entry_ts"]),
                "resolved_at": int(r["resolved_at"]),
                "payoff": float(r["payoff"]),
            })
            npos += 1
    log(f"candidate positions: {npos} across {len(by_mo)} (market,outcome) pairs")

    conn = sqlite3.connect(f"file:{a.db}?mode=ro", uri=True)
    conn.execute("PRAGMA busy_timeout=30000;")

    token_of = map_pair_tokens(a.db, list(by_mo.keys()))
    unmapped = sum(1 for k in by_mo if k not in token_of)
    if unmapped:
        log(f"token mapping missing for {unmapped}/{len(by_mo)} pairs — those positions "
            f"are not repriceable")

    if a.emit_targets:
        rows_out = 0
        with open(a.emit_targets, "w", newline="") as tf:
            tf.write("token_id,start_ts,end_ts\n")
            per_token: dict[str, list[tuple[int, int]]] = {}
            for key, positions in by_mo.items():
                tok = token_of.get(key)
                if tok is None:
                    continue
                per_token.setdefault(tok, []).extend(
                    pair_windows(positions, a.latency_shift_secs, a.fill_window_secs))
            for tok in sorted(per_token):
                for lo, hi in merge_ranges(per_token[tok]):
                    tf.write(f"{tok},{lo},{hi}\n")
                    rows_out += 1
        log(f"wrote {rows_out} merged target window(s) for {len(per_token)} token(s) "
            f"to {a.emit_targets}")
        conn.close()
        return 0

    # ── Fail-closed publication gate (module docstring) ──
    shift = a.latency_shift_secs
    fill_window = a.fill_window_secs
    page_cov: dict[str, list[tuple[int, int]]] = {}
    for tok in sorted(set(token_of.values())):
        page_cov[tok] = [(int(l), int(h)) for l, h in conn.execute(
            "SELECT start_ts, end_ts FROM ranker_price_pages "
            "WHERE token_id = ? AND fidelity_minutes = ?",
            (tok, ORACLE_FIDELITY_MINUTES))]
    uncovered_pairs = 0
    for key, positions in by_mo.items():
        tok = token_of.get(key)
        if tok is None:
            continue  # unmapped: honestly not repriceable, no coverage requirement
        needed = pair_windows(positions, shift, fill_window)
        if subtract_ranges(needed, page_cov.get(tok, [])):
            uncovered_pairs += 1
    if uncovered_pairs:
        log(f"TEMPFAIL(75): {uncovered_pairs} pair(s) have un-terminal reference "
            f"coverage — the targeted fetch stage retries the remainder next attempt")
        conn.close()
        return 75
    log(f"reference coverage terminal for all {len(by_mo)} candidate pairs — scoring")

    # Per-wallet accumulators (repriced positions only feed the statistics).
    net_ls: dict[str, list[float]] = {}
    entry_ts_ls: dict[str, list[int]] = {}
    months: dict[str, set] = {}
    n_total: dict[str, int] = {}
    n_filled: dict[str, int] = {}       # repriced count (wire column n_filled/n_trades)
    payoffs: dict[str, list[float]] = {}
    staleness: list[float] = []

    outcomes_path = os.path.join(a.out_dir, "oracle_outcomes.csv")
    outcomes_fh = open(outcomes_path, "w", newline="")
    outcomes = csv.writer(outcomes_fh)
    outcomes.writerow(["wallet", "market_id", "outcome_id", "token_id", "entry_ts",
                       "payoff", "resolved_at", "sample_t", "sample_price", "outcome"])

    i = 0
    for (mid, oid), positions in by_mo.items():
        tok = token_of.get((mid, oid))
        if tok is None:
            ts_arr, px_arr = [], []
        else:
            lo = int(min(p["entry_ts"] for p in positions) + shift - fill_window) - 1
            hi = int(max(p["entry_ts"] for p in positions) + shift) + 1
            rows_pts = conn.execute(
                "SELECT t, price FROM ranker_price_points "
                "WHERE token_id = ? AND t >= ? AND t <= ? ORDER BY t",
                (tok, lo, hi),
            ).fetchall()
            ts_arr = [int(t) for t, _ in rows_pts]
            px_arr = [px for _, px in rows_pts]
        for pos in positions:
            w = pos["wallet"]
            n_total[w] = n_total.get(w, 0) + 1
            target = pos["entry_ts"] + shift
            # Latest sample at-or-before entry+Δ (no look-ahead), staleness bounded,
            # strictly before actual resolution.
            idx = bisect.bisect_right(ts_arr, target) - 1
            reason = None
            sample_t = sample_px = ""
            if tok is None:
                reason = "unmapped"
            elif idx < 0:
                reason = "future_only" if ts_arr else "no_sample"
            else:
                t_smp = ts_arr[idx]
                if fill_window > 0 and target - t_smp > fill_window:
                    reason = "stale"
                elif not (t_smp < pos["resolved_at"]):
                    reason = "post_resolution"
                else:
                    try:
                        fill_price = float(px_arr[idx])
                    except (TypeError, ValueError):
                        fill_price = None
                    if fill_price is None or not (0.0 < fill_price < 1.0):
                        reason = "invalid_price"
                    else:
                        eff = min(fill_price + slip, 0.999)
                        net = (pos["payoff"] - eff) / eff
                        net_ls.setdefault(w, []).append(net)
                        entry_ts_ls.setdefault(w, []).append(pos["entry_ts"])
                        g = time.gmtime(pos["entry_ts"])
                        months.setdefault(w, set()).add((g.tm_year, g.tm_mon))
                        n_filled[w] = n_filled.get(w, 0) + 1
                        payoffs.setdefault(w, []).append(pos["payoff"])
                        staleness.append(target - t_smp)
                        reason = "repriced"
                        sample_t, sample_px = t_smp, px_arr[idx]
            # token_id + sample_t uniquely locate the covering validated page in
            # ranker_price_pages — the per-position provenance chain (#536 review).
            outcomes.writerow([w, mid, oid, tok or "", pos["entry_ts"], pos["payoff"],
                               pos["resolved_at"], sample_t, sample_px, reason])
        i += 1
        if i % 2000 == 0:
            log(f"  {i}/{len(by_mo)} market-outcomes processed")
    outcomes_fh.close()
    conn.close()

    # Shared decay anchor: explicit --as-of (parsed identically to pass-1), else the
    # latest repriced entry, so every wallet decays against one anchor.
    as_of = parse_as_of(a.as_of)
    if as_of is None:
        all_entry = [t for lst in entry_ts_ls.values() for t in lst]
        as_of = max(all_entry) if all_entry else 0
    decay = "flat (no decay)" if a.half_life_days <= 0 else f"half_life={a.half_life_days}d"
    log(f"scoring: {decay}, as_of={as_of}")

    # Per-wallet repriced stats + survival gate. Wire columns keep their historical
    # names (`fill_rate` = repricing coverage, `n_filled` = repriced count) for
    # publisher/schema compatibility.
    rows = []
    for w in n_total:
        nf = n_filled.get(w, 0)
        nets = net_ls.get(w, [])
        ets = entry_ts_ls.get(w, [])
        am = len(months.get(w, set()))
        fr = nf / n_total[w] if n_total[w] else 0.0
        mean, _, n_eff, t = weighted_stats(nets, decay_weights(ets, as_of, a.half_life_days))
        hr = statistics.fmean(payoffs.get(w, [])) if payoffs.get(w) else float("nan")
        eligible = (
            nf > 1 and am >= a.min_active_months
            and (nf / am if am else 0) >= a.min_avg_per_month
            and nf >= a.min_trl
            and fr >= a.min_fill_rate
            and not math.isnan(t) and t >= a.floor_tstat and mean > 0
        )
        rows.append({
            "wallet": w, "n_total": n_total[w], "n_filled": nf,
            "fill_rate": round(fr, 4), "active_months": am,
            "mean_net_ls": round(mean, 6) if not math.isnan(mean) else "",
            "tstat_net_ls": round(t, 4) if not math.isnan(t) else "",
            "n_eff": round(n_eff, 4),
            "hit_rate": round(hr, 4) if not math.isnan(hr) else "",
            "survives": eligible,
        })
    rows.sort(key=lambda r: (r["survives"], r["tstat_net_ls"] if r["tstat_net_ls"] != "" else -9),
              reverse=True)

    ranked_path = os.path.join(a.out_dir, "latency_shift_ranked.csv")
    # Static fieldnames: never index rows[0] (empty when candidates had no positions
    # overlapping the positions CSV — still write a header-only file, don't crash).
    fields = ["wallet", "n_total", "n_filled", "fill_rate", "active_months",
              "mean_net_ls", "tstat_net_ls", "n_eff", "hit_rate", "survives"]
    with open(ranked_path, "w", newline="") as f:
        wcsv = csv.DictWriter(f, fieldnames=fields)
        wcsv.writeheader()
        wcsv.writerows(rows)

    # Versioned run manifest: canonical JSON whose sha256 the publisher stores in
    # `ranking_batches.config_hash` (#536 replay binding). The publish request is
    # deliberately NOT part of the manifest (it would hash-cycle through config_hash).
    manifest = {
        "oracle": ORACLE_NAME,
        "oracle_version": ORACLE_VERSION,
        "fidelity_minutes": ORACLE_FIDELITY_MINUTES,
        "parser_version": ORACLE_PARSER_VERSION,
        "lookup": "latest sample at-or-before entry+shift",
        "latency_shift_secs": a.latency_shift_secs,
        "staleness_bound_secs": a.fill_window_secs,
        "slip_cents": a.slip_cents,
        "half_life_days": a.half_life_days,
        "as_of": as_of,
        "floor_tstat": a.floor_tstat,
        "min_coverage": a.min_fill_rate,
        "min_trl": a.min_trl,
        "min_active_months": a.min_active_months,
        "min_avg_per_month": a.min_avg_per_month,
        "git_sha": a.git_sha,
        "inputs": {
            "ranked_csv_sha256": sha256_file(a.ranked_csv),
            "positions_csv_sha256": sha256_file(a.positions_csv),
            # The cycle's target file (stage 2a) when present — binds which windows
            # the reference store was asked to cover (#536 review).
            "oracle_targets_sha256": (
                sha256_file(os.path.join(a.out_dir, "oracle_targets.csv"))
                if os.path.exists(os.path.join(a.out_dir, "oracle_targets.csv"))
                else None
            ),
        },
        "outputs": {
            "latency_shift_ranked_sha256": sha256_file(ranked_path),
            "oracle_outcomes_sha256": sha256_file(outcomes_path),
        },
    }
    manifest_path = os.path.join(a.out_dir, "oracle_manifest.json")
    rendered = json.dumps(manifest, sort_keys=True, separators=(",", ":"))
    with open(manifest_path, "w") as f:
        f.write(rendered + "\n")
    log(f"manifest sha256={hashlib.sha256(rendered.encode()).hexdigest()[:16]}… "
        f"written to {manifest_path}")

    if not rows:
        log("no candidate positions overlapped the positions CSV — wrote empty ranking")
        return 0
    survivors = [r for r in rows if r["survives"]]
    log(f"repriced survivors (coverage>={a.min_fill_rate}, t>={a.floor_tstat}, "
        f"mean>0, >={a.min_active_months}mo, >={a.min_avg_per_month}/mo): {len(survivors)}")
    basket = survivors[: a.target_n]
    basket_path = os.path.join(a.out_dir, "latency_shift_basket.txt")
    with open(basket_path, "w") as f:
        f.write(f"# latency_shift_basket — Δ={shift}s slip={slip} floor_t={a.floor_tstat} "
                f"half_life={a.half_life_days}d min_coverage={a.min_fill_rate} "
                f"candidates={len(cand)} survivors={len(survivors)}\n")
        for r in basket:
            f.write(r["wallet"] + "\n")
    if staleness:
        fd = sorted(staleness)
        log(f"reference-sample staleness (s before entry+Δ): p50={fd[len(fd)//2]:.0f} "
            f"p90={fd[int(len(fd)*0.9)]:.0f} max={fd[-1]:.0f}")
    if basket:
        log(f"basket={len(basket)}  mean coverage={statistics.fmean([r['fill_rate'] for r in basket]):.3f}  "
            f"mean tstat_ls={statistics.fmean([r['tstat_net_ls'] for r in basket]):.2f}  "
            f"mean meanNet_ls={statistics.fmean([r['mean_net_ls'] for r in basket]):+.4f}")
    log(f"wrote {ranked_path}  and  {basket_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
