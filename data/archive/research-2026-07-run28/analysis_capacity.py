#!/usr/bin/env python3
"""run28 deliverables: capacity-feasible $/week leaderboard + TTR marginal analysis.

Inputs (same dir): leaderboard.csv, return_matrix.csv, mtm_coverage.json, decision.json
Outputs: capacity_leaderboard.csv + stdout summary.

Pre-registered metric: cap_feasible_wk = min(pos_per_week, CAP=588) * net_per_pos
where 588 = floor(14702 / 25) concurrent $25 slots. NW paired-t vs baseline uses
Bartlett kernel, lag 2 (floor(4*(24/100)^(2/9)) == 2).
"""
import json
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
BANKROLL = 14702.0
STAKE = 25.0
CAP = int(BANKROLL // STAKE)  # 588
NW_LAG = 2

lb = pd.read_csv(HERE / "leaderboard.csv")
rm = pd.read_csv(HERE / "return_matrix.csv", index_col=0)
mtm = json.load(open(HERE / "mtm_coverage.json"))["by_config_overall"]
dec = json.load(open(HERE / "decision.json"))
BASELINE = dec["baseline_key"]
WINNER = dec["winner"]

n_weeks = rm.shape[0]
assert n_weeks == 24, f"expected 24 weeks, got {n_weeks}"
assert rm.shape[1] == 768, f"expected 768 config columns, got {rm.shape[1]}"
assert BASELINE in rm.columns, "baseline missing from return matrix"

# --- consistency gate: leaderboard cum_return must equal column sums of return_matrix
lb_cum = lb.set_index("config")["cum_return"]
col_sums = rm.sum(axis=0)
join = pd.concat([lb_cum, col_sums.rename("rm_sum")], axis=1).dropna()
assert len(join) == 768, f"config-key mismatch between leaderboard and return_matrix: {len(join)}"
max_abs_diff = (join["cum_return"] - join["rm_sum"]).abs().max()
print(f"[gate] leaderboard cum_return vs return_matrix col-sum: max|diff| = {max_abs_diff:.3e}")
assert max_abs_diff < 1e-6, "leaderboard and return_matrix disagree"


def nw_tstat(diff: np.ndarray, lag: int = NW_LAG) -> float:
    """Newey-West (Bartlett) t-stat for mean(diff) != 0."""
    d = np.asarray(diff, dtype=float)
    n = len(d)
    m = d.mean()
    e = d - m
    s = e @ e / n
    for k in range(1, lag + 1):
        w = 1.0 - k / (lag + 1.0)
        s += 2.0 * w * (e[k:] @ e[:-k]) / n
    se = np.sqrt(s / n)
    return float(m / se) if se > 0 else np.nan


base_ret = rm[BASELINE].to_numpy()

rows = []
for cfg in rm.columns:
    r = rm[cfg].to_numpy()
    cum = r.sum()
    cov = mtm.get(cfg, {})
    pos = cov.get("positions_in_window", np.nan)
    ppw = pos / n_weeks if pos == pos else np.nan
    npp = cum / pos if pos and pos == pos and pos > 0 else np.nan
    raw_wk = cum / n_weeks
    cap_wk = min(ppw, CAP) * npp if npp == npp else np.nan
    diff = r - base_ret
    open_frac = cov.get("open_at_horizon_frac", np.nan)
    marked = cov.get("marked_at_horizon", np.nan)
    open_h = cov.get("open_at_horizon", np.nan)
    mtm_pct = 1.0 - open_frac + (marked / pos if pos and pos > 0 else 0.0) if open_frac == open_frac else np.nan
    rows.append({
        "config": cfg,
        "cum_return": cum,
        "positions": pos,
        "pos_per_week": ppw,
        "net_per_pos": npp,
        "raw_per_week": raw_wk,
        "cap_feasible_wk": cap_wk,
        "weekly_sd": r.std(ddof=1),
        "nw_t_vs_baseline": nw_tstat(diff) if cfg != BASELINE else 0.0,
        "weeks_won_vs_baseline": int((diff > 0).sum()),
        "mtm_marked_pct": mtm_pct,
        "open_at_horizon_frac": open_frac,
    })

df = pd.DataFrame(rows).sort_values("cap_feasible_wk", ascending=False).reset_index(drop=True)
df.index += 1
df.to_csv(HERE / "capacity_leaderboard.csv", index_label="rank")

pd.set_option("display.width", 250)
pd.set_option("display.max_colwidth", 95)
fmt = df.copy()
for c in ["cum_return", "pos_per_week", "net_per_pos", "raw_per_week", "cap_feasible_wk", "weekly_sd", "nw_t_vs_baseline", "mtm_marked_pct"]:
    fmt[c] = fmt[c].map(lambda v: f"{v:.3f}" if v == v else "nan")

print("\n=== TOP 20 by capacity-feasible units/week (pre-registered metric) ===")
print(fmt.head(20).to_string())

brow = df[df.config == BASELINE]
wrow = df[df.config == WINNER]
print(f"\n=== decision.json WINNER rank: {wrow.index[0]}  |  BASELINE rank: {brow.index[0]} of 768 ===")
print(fmt.loc[[wrow.index[0], brow.index[0]]].to_string())

print("\n=== BOTTOM 5 ===")
print(fmt.tail(5).to_string())

# --- capacity binding check
n_capped = int((df.pos_per_week > CAP).sum())
print(f"\n[capacity] configs where pos_per_week > {CAP} (cap binds): {n_capped} / 768")
print(f"[capacity] pos_per_week range: {df.pos_per_week.min():.1f} .. {df.pos_per_week.max():.1f}")

# ================= TTR marginal analysis =================
print("\n" + "=" * 80)
print("TTR MARGINAL: within-config paired groups (identical except ttr)")
TTR_RE = re.compile(r"ttr(\d+(?:\.\d+)?)")


def ttr_of(cfg: str) -> float:
    return float(TTR_RE.search(cfg).group(1))


def strip_ttr(cfg: str) -> str:
    return TTR_RE.sub("ttrX", cfg)


groups: dict[str, dict[float, str]] = {}
for cfg in rm.columns:
    groups.setdefault(strip_ttr(cfg), {})[ttr_of(cfg)] = cfg

ttrs_seen = sorted({t for g in groups.values() for t in g})
print(f"ttr values in grid: {ttrs_seen}")
complete = {k: g for k, g in groups.items() if set(g) >= {24.0, 48.0, 72.0}}
print(f"groups: {len(groups)} total, {len(complete)} complete 24/48/72 triples")

for (a, b) in [(48.0, 72.0), (24.0, 72.0), (24.0, 48.0)]:
    # per-week mean difference across all complete triples
    diffs = np.zeros(n_weeks)
    cum_diffs = []
    for g in complete.values():
        d = rm[g[a]].to_numpy() - rm[g[b]].to_numpy()
        diffs += d
        cum_diffs.append(d.sum())
    diffs /= len(complete)
    cd = np.array(cum_diffs)
    frac_pos = (cd > 0).mean()
    print(f"\nttr{a:.0f} - ttr{b:.0f}:  mean weekly diff {diffs.mean():+.3f} units/wk, "
          f"NW-t {nw_tstat(diffs):+.2f}, weeks won {(diffs > 0).sum()}/24, "
          f"triples favoring ttr{a:.0f}: {frac_pos:.0%} (n={len(cd)}), "
          f"median cum diff {np.median(cd):+.1f}")

# per-estimator TTR breakdown (cum_return means)
print("\n=== mean cum_return by estimator x ttr (all configs) ===")
meta = pd.DataFrame({
    "config": rm.columns,
    "estimator": [c.split("|")[0] for c in rm.columns],
    "ttr": [ttr_of(c) for c in rm.columns],
    "cum": [rm[c].sum() for c in rm.columns],
})
pv = meta.pivot_table(index="estimator", columns="ttr", values="cum", aggfunc="mean")
print(pv.round(1).to_string())
print("\n=== BEST single config per ttr ===")
for t in ttrs_seen:
    sub = meta[meta.ttr == t].sort_values("cum", ascending=False).iloc[0]
    print(f"ttr{t:.0f}: {sub.cum:+9.1f}  {sub.config}")

# capacity-feasible by ttr
dft = df.copy()
dft["ttr"] = dft.config.map(ttr_of)
print("\n=== capacity-feasible units/week by ttr (mean / median / best) ===")
print(dft.groupby("ttr")["cap_feasible_wk"].agg(["mean", "median", "max"]).round(2).to_string())
