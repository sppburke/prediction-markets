#!/usr/bin/env python3
"""
Holdout baseline harness — Phase 1 of the 4-phase validation plan (issue #162).

Builds a clean train<=2026-03 / test=2026-04 holdout baseline on the current
candidate universe, in two sizing modes (Kelly and flat_usd_per_trade), and
records the exact additive PnL decomposition for each.

WHY THIS EXISTS
---------------
Every edge measurement so far overlaps the parameter-tuning period -- there is
no honest out-of-sample number. The production backtest is already walk-forward:
on each simulation day D the ranker only sees trades with timestamp < D
(`ranker_cutoff_unix` in crates/backtest/src/simulation.rs). So positions
*entered* in April 2026 were selected by a ranker trained only on <=March data.
The train-side split is therefore structurally enforced in Rust; this harness
does not (and cannot) re-assert it -- it asserts only the test-side split.

This harness runs the backtest, slices the round-trips *entered* in the test
window, and applies the exact additive decomposition (the same identity as
scripts/pnl_decomposition.py):

    Total realized PnL = SELECTION + SIZING + EXIT-TIMING        (exact, additive)
      SELECTION   = cbar * sum(r_i - e_i)        equal-weight, hold-to-resolution
      SIZING      = sum((c_i - cbar)(r_i - e_i)) cov(size, per-contract edge)
      EXIT-TIMING = sum(c_i (x_i - r_i))         leader early-sell vs holding

TWO MODES
---------
#161 (PR #163) landed `WinnerFollowConfig.flat_usd_per_trade`. The harness runs
the backtest twice -- Kelly (the baseline --config) and flat (the same config
with `[strategy].flat_usd_per_trade` set) -- and decomposes each independently.
The two runs' position sets may legitimately diverge (different sizing ->
different bankroll path -> different liquidity-clamp / insufficient-bankroll
outcomes); neither is assumed identical to the other.

Pure stdlib. Run:
  python3 scripts/holdout_baseline.py --config <kelly.toml> \\
    --flat-usd-per-trade 25.00 --cache-path /path/to/wallet_cache.db \\
    --output-dir /path/to/out
"""

import argparse
import json
import math
import sqlite3
import statistics
import subprocess
import sys
import tomllib
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path

HARNESS_VERSION = "1"

# Annualisation factor for the daily Sharpe (sqrt of 365). crates/backtest's
# report.rs hard-codes 19.105; sqrt(365) is the same constant to more digits.
SHARPE_ANNUALISATION = math.sqrt(365.0)


# ─────────────────────────────────────────────────────────────────────────────
# Pure data helpers (unit-tested)
# ─────────────────────────────────────────────────────────────────────────────


def parse_day(s):
    """Parse an ISO date or datetime string to a `date`.

    Backtest fills stamp `simulated_at` as `sim_date.midnight()` -> always
    `YYYY-MM-DDT00:00:00Z`, so taking the first 10 chars is sufficient and
    avoids tz-parsing fragility. Also accepts a bare `YYYY-MM-DD`.
    """
    return date.fromisoformat(s[:10])


def read_fills(ndjson_path):
    """Read trades.ndjson into a flat list of fill dicts, preserving line order.

    Line order matters: the backtest writes fills sequentially in simulation
    order (the per-day resolution sweep emits `resolution` rows before that
    day's `buy`/`sell` rows), so per-key line order is already causal. We do
    NOT re-sort -- sorting by `simulated_at` would be ambiguous because a
    same-day resolution and buy share an identical midnight timestamp.
    """
    fills = []
    with open(ndjson_path) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            t = json.loads(line)
            fills.append(
                {
                    "side": t["side"],
                    "leader_wallet": t["leader_wallet"],
                    "operator_id": t.get("operator_id"),
                    "market_id": t["market_id"],
                    "outcome_id": int(t["outcome_id"]),
                    "contracts": int(t["contracts"]),
                    "fill_price": float(t["fill_price"]),
                    "simulated_at": t["simulated_at"],
                }
            )
    return fills


def load_resolutions(db_path):
    """Return {market_id: (winning_outcome_id, resolved_at_unix)} from the cache.

    `winning_outcome_id` is NULL for voided / non-binary markets -- those map to
    `(None, resolved_at_unix)` and are treated as unresolved by the decomposition
    (matches scripts/pnl_decomposition.py, which skips `win is None`).
    """
    con = sqlite3.connect(db_path)
    if int(con.execute("PRAGMA user_version").fetchone()[0]) == -2:
        con.close()
        raise ValueError("unfinished bulk root (schema -2); resume cache-populate-activity-v2 --bulk-root before any other command")
    try:
        rows = con.execute(
            "SELECT market_id, winning_outcome_id, resolved_at_unix "
            "FROM market_resolutions"
        ).fetchall()
    finally:
        con.close()
    out = {}
    for market_id, winning_outcome_id, resolved_at_unix in rows:
        out[market_id] = (winning_outcome_id, resolved_at_unix)
    return out


def pair_round_trips(fills):
    """Pair fills into discrete round-trips per (leader, market, outcome) key.

    The backtest's Buy guard (`open_positions.contains_key`) forces strict
    `buy -> close -> buy -> close` alternation per key, where a close is one
    `sell` OR one `resolution` row. We walk each key's fills in line order:
    a `buy` opens a round-trip; the next `sell`/`resolution` closes it. A
    trailing `buy` with no following close is a round-trip the sim left open at
    its data horizon (`close` is None).

    Raises ValueError if the strict alternation is violated -- that would mean
    the ndjson format or the sim's invariants changed, and silently
    mis-pairing would corrupt every downstream number.
    """
    by_key = defaultdict(list)
    for f in fills:
        key = (f["leader_wallet"], f["market_id"], f["outcome_id"])
        by_key[key].append(f)

    round_trips = []
    for key, key_fills in by_key.items():
        open_buy = None
        for f in key_fills:
            if f["side"] == "buy":
                if open_buy is not None:
                    raise ValueError(
                        f"alternation violated for {key}: two consecutive buys "
                        f"(buy at {open_buy['simulated_at']} then "
                        f"{f['simulated_at']}) -- expected a close between them"
                    )
                open_buy = f
            elif f["side"] in ("sell", "resolution"):
                if open_buy is None:
                    raise ValueError(
                        f"alternation violated for {key}: {f['side']} at "
                        f"{f['simulated_at']} with no open buy"
                    )
                round_trips.append({"key": key, "buy": open_buy, "close": f})
                open_buy = None
            else:
                raise ValueError(f"unknown fill side {f['side']!r} for {key}")
        if open_buy is not None:
            round_trips.append({"key": key, "buy": open_buy, "close": None})
    return round_trips


def classify_round_trip(rt, resolutions):
    """Resolve a paired round-trip into a decomposable record, or mark unresolved.

    Resolution is CACHE-DRIVEN, not sim-horizon-driven: a round-trip closed by a
    `sell` is `sold`; one closed by a `resolution` row is `held`; one with no
    close row is `held_open` IFF the cache knows the outcome (a position entered
    in the test month that resolves after the sim's data horizon is still
    knowable retrospectively, and IS counted). Only a round-trip whose market is
    absent from `market_resolutions` -- or voided (NULL winner) -- is genuinely
    `unresolved` and excluded from the decomposition.

    Returns a dict with keys: leader, operator_id, market, outcome, buy_dt,
    e, c, r, x, exit_kind, close_dt, within_sim. `within_sim` is True iff the
    sim itself closed the position (a `sell` or `resolution` row exists) --
    used by the reconciliation against report.json.
    """
    buy = rt["buy"]
    close = rt["close"]
    leader, market, outcome = rt["key"]
    e = buy["fill_price"]  # already slippage-loaded: signal_price * (1 + slippage)
    c = buy["contracts"]
    buy_dt = parse_day(buy["simulated_at"])

    base = {
        "leader": leader,
        "operator_id": buy.get("operator_id"),
        "market": market,
        "outcome": outcome,
        "buy_dt": buy_dt,
        "e": e,
        "c": c,
    }

    res = resolutions.get(market)
    winning_outcome = res[0] if res is not None else None
    if winning_outcome is None:
        # Market absent from cache, or voided/non-binary (NULL winner).
        return {**base, "exit_kind": "unresolved", "r": None, "x": None,
                "close_dt": None, "within_sim": close is not None}

    r = 1.0 if int(winning_outcome) == outcome else 0.0

    if close is not None and close["side"] == "sell":
        return {**base, "exit_kind": "sold", "r": r, "x": close["fill_price"],
                "close_dt": parse_day(close["simulated_at"]), "within_sim": True}
    if close is not None and close["side"] == "resolution":
        return {**base, "exit_kind": "held", "r": r, "x": r,
                "close_dt": parse_day(close["simulated_at"]), "within_sim": True}
    # No close row in the ndjson: the sim left it open at its horizon, but the
    # cache resolves it. Count it; close date comes from the cache.
    resolved_at_unix = res[1]
    close_dt = datetime.fromtimestamp(resolved_at_unix, tz=timezone.utc).date()
    return {**base, "exit_kind": "held_open", "r": r, "x": r,
            "close_dt": close_dt, "within_sim": False}


def select_test_window(records, test_start, test_end):
    """Round-trips whose BUY date is within [test_start, test_end] inclusive.

    Slices on the buy (entry) date only -- never the sell/resolution date,
    which carry close dates and would wrongly pull in earlier-entered positions.
    """
    return [rec for rec in records if test_start <= rec["buy_dt"] <= test_end]


def decompose(records):
    """Exact additive decomposition over a list of *resolved* round-trip records.

    Records with `exit_kind == 'unresolved'` MUST be filtered out before calling
    this -- they have no `r`. Returns a dict; on an empty input returns a
    well-formed zero result (n == 0) rather than dividing by zero.
    """
    resolved = [rec for rec in records if rec["exit_kind"] != "unresolved"]
    n = len(resolved)
    if n == 0:
        return {"n": 0, "total_contracts": 0, "cbar": 0.0, "selection": 0.0,
                "sizing": 0.0, "exit_timing": 0.0, "total": 0.0,
                "net_per_contract_edge": 0.0}

    total_contracts = sum(rec["c"] for rec in resolved)
    cbar = total_contracts / n

    total = sum(rec["c"] * (rec["x"] - rec["e"]) for rec in resolved)
    selection = cbar * sum(rec["r"] - rec["e"] for rec in resolved)
    sizing = sum((rec["c"] - cbar) * (rec["r"] - rec["e"]) for rec in resolved)
    exit_timing = sum(rec["c"] * (rec["x"] - rec["r"]) for rec in resolved)
    net_per_contract_edge = statistics.fmean(rec["r"] - rec["e"] for rec in resolved)

    return {
        "n": n,
        "total_contracts": total_contracts,
        "cbar": cbar,
        "selection": selection,
        "sizing": sizing,
        "exit_timing": exit_timing,
        "total": total,
        "net_per_contract_edge": net_per_contract_edge,
    }


def decompose_by_operator(records):
    """Per-operator decomposition. Winner-Follow ranks at the operator level
    (CLAUDE.md), so the baseline carries an operator-keyed breakdown. Wallets
    with no resolved operator identity bucket under "unknown".
    """
    groups = defaultdict(list)
    for rec in records:
        if rec["exit_kind"] == "unresolved":
            continue
        groups[rec["operator_id"] or "unknown"].append(rec)
    return {op: decompose(recs) for op, recs in sorted(groups.items())}


def daily_pnl_series(records):
    """Realized PnL per close date, as a sorted list of (date, pnl).

    Each resolved round-trip contributes `c * (x - e)` on its close date (the
    `sell`/`resolution` date, or the cache `resolved_at_unix` date for
    `held_open`). The series spans past test_end because test-entered positions
    resolve over the following weeks.
    """
    by_day = defaultdict(float)
    for rec in records:
        if rec["exit_kind"] == "unresolved":
            continue
        by_day[rec["close_dt"]] += rec["c"] * (rec["x"] - rec["e"])
    return sorted(by_day.items())


def sharpe(daily_pnls):
    """Annualised Sharpe from a daily-PnL series. Mirrors report.rs semantics:
    sample stdev, annualised by sqrt(365); returns 0.0 for < 2 points or zero
    variance. SECONDARY / low-confidence metric for an n=1-entry-month slice.
    """
    values = [pnl for _, pnl in daily_pnls]
    if len(values) < 2:
        return 0.0
    mean = statistics.fmean(values)
    stdev = statistics.stdev(values)
    if stdev == 0.0:
        return 0.0
    return (mean / stdev) * SHARPE_ANNUALISATION


def max_drawdown_usd(daily_pnls):
    """Largest peak-to-trough drop (in USD) of the cumulative realized-PnL curve.

    Reported in absolute dollars rather than percent: for a single entry-month
    slice there is no meaningful bankroll base to take a percentage of, and the
    cumulative-PnL curve crosses zero. SECONDARY / low-confidence metric.
    """
    values = [pnl for _, pnl in daily_pnls]
    if len(values) < 2:
        return 0.0
    cumulative = 0.0
    peak = 0.0
    max_dd = 0.0
    for v in values:
        cumulative += v
        peak = max(peak, cumulative)
        max_dd = max(max_dd, peak - cumulative)
    return max_dd


def reconcile(all_records, report_total_pnl_usd):
    """Methodology reconciliation against report.json (run once per mode, on the
    FULL fill set -- not the test slice).

    `total` decomposes every resolved round-trip (cache-driven, includes
    positions the sim left open at its horizon). `total_within_sim` decomposes
    only round-trips the sim itself closed (`within_sim` -- a `sell` or
    `resolution` row exists); those are exactly the positions report.json's
    `total_pnl_usd` counts, so the two MUST agree to rounding. The
    `open_past_horizon` delta is the contribution of cache-resolved positions
    the sim left open.

    Returns (result_dict, ok: bool). `ok` is False if the within-sim identity
    breaks beyond tolerance -- the caller turns that into a hard failure.
    """
    resolved = [r for r in all_records if r["exit_kind"] != "unresolved"]
    within_sim = [r for r in resolved if r["within_sim"]]

    total = decompose(resolved)["total"]
    total_within_sim = decompose(within_sim)["total"]
    delta = total_within_sim - report_total_pnl_usd
    # Relative tolerance, with an absolute floor so a near-zero report total
    # does not make the relative check explode.
    tolerance = max(0.001 * abs(report_total_pnl_usd), 1.0)
    ok = abs(delta) <= tolerance

    return (
        {
            "total": total,
            "total_within_sim": total_within_sim,
            "report_total_pnl_usd": report_total_pnl_usd,
            "within_sim_delta": delta,
            "within_sim_tolerance": tolerance,
            "open_past_horizon": total - total_within_sim,
            "reconciles": ok,
        },
        ok,
    )


def derive_flat_config(base_toml_text, flat_usd_per_trade):
    """Return `base_toml_text` with `[strategy].flat_usd_per_trade` set.

    rust_decimal's default serde reads a Decimal from a quoted string in TOML
    (verified: crates/backtest/tests/scenario_config.rs uses
    `slippage_rate = "0.02"`), so the value is written quoted. The transform is
    line-based to preserve the rest of the file byte-for-byte -- a dict
    round-trip would have to re-serialise tagged enums like `PerTradeCap` and is
    far more fragile. The result is validated by `tomllib.loads` before return.
    """
    value_line = f'flat_usd_per_trade = "{flat_usd_per_trade}"'
    lines = base_toml_text.splitlines()

    def is_table_header(s):
        return s.strip().startswith("[") and s.strip().endswith("]")

    strategy_idx = None
    for i, line in enumerate(lines):
        if line.strip() == "[strategy]":
            strategy_idx = i
            break

    if strategy_idx is None:
        # No [strategy] table -- append one.
        new_lines = lines + ["", "[strategy]", value_line]
    else:
        # Find the table body span and look for an existing assignment.
        body_end = len(lines)
        for j in range(strategy_idx + 1, len(lines)):
            if is_table_header(lines[j]):
                body_end = j
                break
        replaced = False
        new_lines = list(lines)
        for j in range(strategy_idx + 1, body_end):
            stripped = new_lines[j].strip()
            if stripped.startswith("flat_usd_per_trade") and "=" in stripped:
                new_lines[j] = value_line
                replaced = True
                break
        if not replaced:
            new_lines.insert(strategy_idx + 1, value_line)

    result = "\n".join(new_lines) + "\n"
    tomllib.loads(result)  # validate -- raises tomllib.TOMLDecodeError on garbage
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Orchestration (subprocess boundary -- exercised end-to-end, not unit-tested)
# ─────────────────────────────────────────────────────────────────────────────


def run_backtest(backtest_bin, config_path, output_dir, cache_path):
    """Invoke `pe-backtest` with `config_path`, directing output to `output_dir`.

    `pe-backtest` takes the config path as its first positional arg; output_dir
    and the cache path are overridden via the verified `PE_BACKTEST_OUTPUT_DIR`
    and `PE_BOOTSTRAP_CACHE_PATH` env aliases so the harness controls them
    without editing the pinned TOML.
    """
    import os

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ)
    env["PE_BACKTEST_OUTPUT_DIR"] = str(output_dir)
    env["PE_BOOTSTRAP_CACHE_PATH"] = str(cache_path)
    subprocess.run(
        [str(backtest_bin), str(config_path)],
        env=env,
        check=True,
    )
    return output_dir / "trades.ndjson", output_dir / "report.json"


def analyse_run(ndjson_path, report_path, cache_path, test_start, test_end):
    """Full post-processing for one backtest run: pair -> classify -> reconcile
    -> window -> decompose. Returns the per-mode result dict.
    """
    resolutions = load_resolutions(cache_path)
    fills = read_fills(ndjson_path)
    round_trips = pair_round_trips(fills)
    records = [classify_round_trip(rt, resolutions) for rt in round_trips]

    report = json.loads(Path(report_path).read_text())
    report_total = float(report["total_pnl_usd"])
    recon, recon_ok = reconcile(records, report_total)

    test_records = select_test_window(records, test_start, test_end)
    resolved_test = [r for r in test_records if r["exit_kind"] != "unresolved"]
    unresolved_test = [r for r in test_records if r["exit_kind"] == "unresolved"]

    series = daily_pnl_series(resolved_test)

    return {
        "reconciliation": recon,
        "reconciles": recon_ok,
        "test_window": {
            "n_round_trips": len(test_records),
            "n_resolved": len(resolved_test),
            "n_unresolved": len(unresolved_test),
            "copy_count": len(resolved_test),
            "decomposition": decompose(resolved_test),
            "by_operator": decompose_by_operator(resolved_test),
            "sharpe": sharpe(series),
            "max_drawdown_usd": max_drawdown_usd(series),
        },
    }


def format_summary(artifact):
    """Render the stdout summary table (precedent: the existing analysis scripts)."""
    lines = []
    p = artifact["params"]
    lines.append("=== HOLDOUT BASELINE (issue #162, Phase 1) ===")
    lines.append(
        f"  train<= {p['train_cutoff']}   test=[{p['test_start']}, {p['test_end']}]"
        f"   universe={p['cache_path']}"
    )
    lines.append("")
    header = (
        f"  {'mode':<8}{'N':>6}{'SELECTION':>14}{'SIZING':>14}"
        f"{'EXIT':>14}{'TOTAL':>14}{'net edge':>11}{'Sharpe':>9}{'maxDD$':>13}"
    )
    lines.append(header)
    lines.append("  " + "-" * (len(header) - 2))
    for mode in ("kelly", "flat"):
        m = artifact["modes"][mode]
        d = m["test_window"]["decomposition"]
        tw = m["test_window"]
        lines.append(
            f"  {mode:<8}{d['n']:>6}{d['selection']:>+14,.0f}{d['sizing']:>+14,.0f}"
            f"{d['exit_timing']:>+14,.0f}{d['total']:>+14,.0f}"
            f"{d['net_per_contract_edge']:>+11.4f}{tw['sharpe']:>9.2f}"
            f"{tw['max_drawdown_usd']:>13,.0f}"
        )
    lines.append("")
    cm = artifact["cross_mode"]
    lines.append("  cross-mode check (flat total should track Kelly SELECTION,")
    lines.append("  not Kelly total -- flat sizing is de-risked):")
    lines.append(
        f"    Kelly SELECTION = {cm['kelly_selection']:>+14,.0f}   "
        f"flat TOTAL = {cm['flat_total']:>+14,.0f}   "
        f"Kelly TOTAL = {cm['kelly_total']:>+14,.0f}"
    )
    lines.append("")
    for mode in ("kelly", "flat"):
        r = artifact["modes"][mode]["reconciliation"]
        status = "OK" if r["reconciles"] else "FAIL"
        lines.append(
            f"  reconcile[{mode}]: within-sim {r['total_within_sim']:>+14,.0f} "
            f"vs report {r['report_total_pnl_usd']:>+14,.0f} "
            f"(delta {r['within_sim_delta']:+.2f}) [{status}]   "
            f"open-past-horizon {r['open_past_horizon']:>+14,.0f}"
        )
    return "\n".join(lines)


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="Holdout baseline harness (issue #162, Phase 1)."
    )
    p.add_argument("--config", required=True,
                   help="Baseline Kelly backtest TOML (pinned into the artifact).")
    p.add_argument("--flat-usd-per-trade", required=True,
                   help="Flat-mode stake; sets [strategy].flat_usd_per_trade.")
    p.add_argument("--cache-path", required=True,
                   help="wallet_cache.db -- read for market_resolutions and "
                        "passed to the backtest via PE_BOOTSTRAP_CACHE_PATH.")
    p.add_argument("--output-dir", required=True,
                   help="Directory for the two run dirs and the JSON artifact.")
    p.add_argument("--backtest-bin", default="./target/release/pe-backtest",
                   help="Path to the pe-backtest binary.")
    p.add_argument("--train-cutoff", default="2026-03-31",
                   help="Documentary: the Rust-side ranker cutoff (not enforced "
                        "here -- structurally enforced by ranker_cutoff_unix).")
    p.add_argument("--test-start", default="2026-04-01")
    p.add_argument("--test-end", default="2026-04-30")
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    test_start = parse_day(args.test_start)
    test_end = parse_day(args.test_end)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    base_toml_text = Path(args.config).read_text()
    # Validate the baseline parses before spending two backtest runs on it.
    tomllib.loads(base_toml_text)

    flat_toml_text = derive_flat_config(base_toml_text, args.flat_usd_per_trade)
    flat_config_path = out_dir / "flat_config.toml"
    flat_config_path.write_text(flat_toml_text)

    mode_configs = {
        "kelly": Path(args.config),
        "flat": flat_config_path,
    }

    modes = {}
    for mode, config_path in mode_configs.items():
        run_dir = out_dir / mode
        print(f"[holdout] running backtest: mode={mode} config={config_path}",
              file=sys.stderr)
        ndjson_path, report_path = run_backtest(
            args.backtest_bin, config_path, run_dir, args.cache_path
        )
        modes[mode] = analyse_run(
            ndjson_path, report_path, args.cache_path, test_start, test_end
        )

    kelly_d = modes["kelly"]["test_window"]["decomposition"]
    flat_d = modes["flat"]["test_window"]["decomposition"]
    artifact = {
        "harness_version": HARNESS_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "params": {
            "config": str(Path(args.config).resolve()),
            "flat_config": str(flat_config_path.resolve()),
            "flat_usd_per_trade": args.flat_usd_per_trade,
            "cache_path": str(Path(args.cache_path).resolve()),
            "backtest_bin": args.backtest_bin,
            "train_cutoff": args.train_cutoff,
            "test_start": args.test_start,
            "test_end": args.test_end,
        },
        "modes": modes,
        "cross_mode": {
            "kelly_total": kelly_d["total"],
            "kelly_selection": kelly_d["selection"],
            "flat_total": flat_d["total"],
        },
        "limitations": [
            "OOS-for-selection, in-sample-for-config: ranking is held out "
            "(<=March) but config params were swept over a window including April.",
            "n = 1 entry-month -- Sharpe and max-DD are secondary, low-confidence.",
            "Universe is the known mis-constructed (favorite-filtered) Dune set -- "
            "this is a comparison anchor for Phase 3, not a standalone verdict.",
            "Kelly-mode total dollar PnL is bankroll-inflated; SELECTION, net "
            "per-contract edge, and the flat-mode total are the size-agnostic reads.",
            "Cost model is slippage-only (no explicit fee); net edge = mean(r - e).",
            "flat mode is equal-dollar with the per-trade cap + risk gate still "
            "active -- a de-risked cross-check, not an identity with Kelly SELECTION.",
        ],
    }

    artifact_path = out_dir / "holdout_baseline.json"
    artifact_path.write_text(json.dumps(artifact, indent=2, default=str))

    print(format_summary(artifact))
    print(f"\n[holdout] artifact written: {artifact_path}", file=sys.stderr)

    # Reconciliation is a methodology gate: a broken within-sim identity means
    # the reconstruction is wrong, so fail loudly rather than emit a bad baseline.
    failed = [m for m, r in modes.items() if not r["reconciles"]]
    if failed:
        print(f"[holdout] RECONCILIATION FAILED for: {', '.join(failed)}",
              file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
