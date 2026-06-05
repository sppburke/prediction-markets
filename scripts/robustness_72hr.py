#!/usr/bin/env python3
"""
Robustness battery for the 72hr buy-and-hold wallet cohort — band vs no-band.

Consumes a long-window qualifying-positions CSV (from rank_72hr_buyandhold.py with a
12-month window) and, for a given population (no-band, or a position-level price band),
evaluates how *robust* each wallet's net edge is, so the surviving COUNT is an output of
evidence rather than a target.

Tests (all on net = (payoff-eff)/eff, eff = min(price+slip, 0.999); buy-and-hold => no exit cost):
  1. In-sample net t-stat + Benjamini-Hochberg FDR  -> honest count of defensible winners.
  2. Walk-forward persistence: train (entries <= 2026-01) vs test (entries >= 2026-02).
     Spearman(train,test) edge rank-corr; fraction of train-winners that stay test-positive;
     quartile monotonicity (does train-edge rank predict test-edge?).
  3. Edge concentration: top-5 positions' share of total positive net contribution (fragility).
  4. Slippage stress: re-evaluate net under a harsher 2-cent slip; survival of positive edge.
A wallet SURVIVES if: FDR(q=.10) significant AND test-net-positive (with enough test n)
AND not concentration-fragile AND slippage-robust.  Survivors -> group-Sharpe (within survivors).

Run once per population; compare the printed/JSON summaries.
"""
from __future__ import annotations
import argparse, json, math, os
import numpy as np
import pandas as pd

try:
    from scipy import stats as _st
    def t_sf(t, dfree):  # one-sided upper-tail p-value
        return float(_st.t.sf(t, dfree))
    def spearman(a, b):
        if len(a) < 3: return float("nan")
        return float(_st.spearmanr(a, b).correlation)
except Exception:  # normal approx fallback
    def t_sf(t, dfree):
        return 0.5 * math.erfc(t / math.sqrt(2.0))
    def spearman(a, b):
        a = pd.Series(a); b = pd.Series(b)
        return float(a.corr(b, method="spearman"))

WEEK = 7 * 86400
TEST_FROM = os.environ.get("PE_TEST_FROM", "2026-02")  # entries >= this month = TEST; earlier = TRAIN


def net_return(price: np.ndarray, payoff: np.ndarray, slip: float) -> np.ndarray:
    eff = np.minimum(price + slip, 0.999)
    return (payoff - eff) / eff


def tstat(x: np.ndarray):
    n = len(x)
    if n < 2: return (float("nan"), float("nan"), n)
    m = float(np.mean(x)); sd = float(np.std(x, ddof=1))
    t = (m / sd * math.sqrt(n)) if sd > 0 else float("nan")
    return (m, t, n)


def bh_fdr(pvals: dict, q: float) -> set:
    """Benjamini-Hochberg: return set of keys rejected (significant) at FDR=q."""
    items = [(k, p) for k, p in pvals.items() if p == p]  # drop NaN
    items.sort(key=lambda kv: kv[1])
    m = len(items)
    kmax = 0
    for i, (_, p) in enumerate(items, start=1):
        if p <= (i / m) * q:
            kmax = i
    return {items[i][0] for i in range(kmax)}


def concentration_top5(g: np.ndarray) -> float:
    """Share of total POSITIVE net contribution coming from the top-5 positions."""
    pos = g[g > 0]
    if pos.sum() <= 0: return 1.0
    top = np.sort(pos)[::-1][:5].sum()
    return float(top / pos.sum())


def per_wallet(df: pd.DataFrame, slip: float, slip_stress: float) -> pd.DataFrame:
    df = df.copy()
    df["net"] = net_return(df["price"].to_numpy(), df["payoff"].to_numpy(), slip)
    df["net_stress"] = net_return(df["price"].to_numpy(), df["payoff"].to_numpy(), slip_stress)
    df["month"] = pd.to_datetime(df["entry_ts"], unit="s").dt.to_period("M").astype(str)
    is_test = df["month"] >= TEST_FROM
    rows = []
    for w, g in df.groupby("wallet", sort=False):
        net = g["net"].to_numpy()
        m, t, n = tstat(net)
        ms, ts_, _ = tstat(g["net_stress"].to_numpy())
        gtr = g[~is_test.loc[g.index]]; gte = g[is_test.loc[g.index]]
        mtr, ttr_, ntr = tstat(gtr["net"].to_numpy()) if len(gtr) else (float("nan"),)*2+(0,)
        mte, tte_, nte = tstat(gte["net"].to_numpy()) if len(gte) else (float("nan"),)*2+(0,)
        rows.append(dict(
            wallet=w, n=n, mean_net=m, tstat_net=t, p_one_sided=t_sf(t, n-1) if (t==t and n>1) else float("nan"),
            hit_rate=float(g["payoff"].mean()), avg_price=float(g["price"].mean()),
            conc_top5=concentration_top5(net),
            mean_net_stress=ms, n_train=ntr, mean_train=mtr, n_test=nte, mean_test=mte,
            active_months=g["month"].nunique(),
        ))
    return pd.DataFrame(rows)


def eval_population(df: pd.DataFrame, name: str, prm) -> dict:
    pw = per_wallet(df, prm.slip, prm.slip_stress)
    # eligibility: frequent & active (same spirit as deployment gate, full window)
    elig = pw[(pw["n"] >= prm.min_n) & (pw["active_months"] >= prm.min_active_months)].copy()
    pmap = {r.wallet: r.p_one_sided for r in elig.itertuples()}
    fdr10 = bh_fdr(pmap, 0.10); fdr05 = bh_fdr(pmap, 0.05)
    elig["fdr10"] = elig["wallet"].isin(fdr10)
    elig["fdr05"] = elig["wallet"].isin(fdr05)

    # walk-forward persistence (wallets with enough train & test positions)
    wf = elig[(elig["n_train"] >= prm.min_split_n) & (elig["n_test"] >= prm.min_split_n)].copy()
    sp = spearman(wf["mean_train"].to_numpy(), wf["mean_test"].to_numpy()) if len(wf) >= 5 else float("nan")
    train_winners = wf[wf["mean_train"] > 0]
    persist_hit = float((train_winners["mean_test"] > 0).mean()) if len(train_winners) else float("nan")
    # quartile monotonicity: test-edge of top vs bottom train-quartile
    q_mono = float("nan")
    if len(wf) >= 8:
        wf2 = wf.sort_values("mean_train")
        qb = wf2.head(len(wf2)//4)["mean_test"].mean()
        qt = wf2.tail(len(wf2)//4)["mean_test"].mean()
        q_mono = float(qt - qb)

    # survivor gate
    elig["test_ok"] = (elig["n_test"] >= prm.min_split_n) & (elig["mean_test"] > 0)
    elig["conc_ok"] = elig["conc_top5"] < prm.max_conc
    elig["slip_ok"] = elig["mean_net_stress"] > 0
    elig["survivor"] = elig["fdr10"] & elig["test_ok"] & elig["conc_ok"] & elig["slip_ok"]
    surv = elig[elig["survivor"]].copy()

    summary = dict(
        population=name, n_positions=int(len(df)), n_eligible=int(len(elig)),
        fdr10_count=int(elig["fdr10"].sum()), fdr05_count=int(elig["fdr05"].sum()),
        wf_spearman=sp, wf_persist_hitrate=persist_hit, wf_quartile_mono=q_mono,
        wf_n_wallets=int(len(wf)),
        median_conc_top5=float(elig["conc_top5"].median()),
        slip_survival_rate=float((elig["mean_net_stress"] > 0).mean()),
        survivors=int(len(surv)),
        surv_mean_net=float(surv["mean_net"].mean()) if len(surv) else float("nan"),
        surv_hit_rate=float(surv["hit_rate"].mean()) if len(surv) else float("nan"),
        surv_avg_price=float(surv["avg_price"].mean()) if len(surv) else float("nan"),
        surv_median_n=int(surv["n"].median()) if len(surv) else 0,
    )
    return dict(summary=summary, elig=elig, survivors=surv)


def group_sharpe_select(df: pd.DataFrame, wallets: list[str], slip: float, target_n: int):
    d = df[df["wallet"].isin(set(wallets))].copy()
    d["net"] = net_return(d["price"].to_numpy(), d["payoff"].to_numpy(), slip)
    d["week"] = (d["resolved_at"].to_numpy() // WEEK).astype(int)
    weeks = np.sort(d["week"].unique()); wi = {w: i for i, w in enumerate(weeks)}
    al = {w: i for i, w in enumerate(wallets)}
    sm = np.zeros((len(wallets), len(weeks))); ct = np.zeros((len(wallets), len(weeks)))
    for (w, wk), gg in d.groupby(["wallet", "week"]):
        sm[al[w], wi[wk]] = gg["net"].sum(); ct[al[w], wi[wk]] = len(gg)

    def gs(s, c):
        a = c > 0
        if a.sum() < 2: return -np.inf
        r = s[a] / c[a]; sd = r.std(ddof=1)
        return float(r.mean()/sd*math.sqrt(len(r))) if sd > 0 else -np.inf

    sel = []; insel = np.zeros(len(wallets), bool); st = np.zeros(len(weeks)); cc = np.zeros(len(weeks))
    # seed with highest-n wallet for stability
    first = int(np.argmax(ct.sum(axis=1))); sel.append(first); insel[first] = True
    st += sm[first]; cc += ct[first]
    tgt = min(target_n, len(wallets))
    while len(sel) < tgt:
        bi, bs = -1, -np.inf
        for c in np.where(~insel)[0]:
            s = gs(st + sm[c], cc + ct[c])
            if s > bs: bs, bi = s, c
        if bi < 0: break
        sel.append(bi); insel[bi] = True; st += sm[bi]; cc += ct[bi]
    return [wallets[i] for i in sel], gs(st, cc)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--positions", default="data/eval-results/robust/qualifying_positions_72hr.csv")
    ap.add_argument("--out-dir", default="data/eval-results/robust")
    ap.add_argument("--band-lo", type=float, default=None)
    ap.add_argument("--band-hi", type=float, default=None)
    ap.add_argument("--slip", type=float, default=0.01)
    ap.add_argument("--slip-stress", type=float, default=0.02)
    ap.add_argument("--min-n", type=int, default=30)
    ap.add_argument("--min-active-months", type=int, default=3)
    ap.add_argument("--min-split-n", type=int, default=10)
    ap.add_argument("--max-conc", type=float, default=0.5)
    ap.add_argument("--target-n", type=int, default=250)
    ap.add_argument("--tag", default="")
    a = ap.parse_args()

    class P: pass
    prm = P()
    for k in ("slip","slip_stress","min_n","min_active_months","min_split_n","max_conc"):
        setattr(prm, k, getattr(a, k))

    df = pd.read_csv(a.positions)
    name = a.tag or ("no-band" if a.band_lo is None else f"band_{a.band_lo}-{a.band_hi}")
    if a.band_lo is not None:
        df = df[(df["price"] >= a.band_lo) & (df["price"] <= a.band_hi)].copy()
    r = eval_population(df, name, prm)
    s = r["summary"]
    print("=" * 72)
    print(f"POPULATION: {name}")
    for k, v in s.items():
        print(f"  {k:24s}: {v}")
    surv = r["survivors"].sort_values("tstat_net", ascending=False)
    os.makedirs(a.out_dir, exist_ok=True)
    safe = name.replace("/", "_")
    r["elig"].to_csv(os.path.join(a.out_dir, f"robust_eligible_{safe}.csv"), index=False)
    if len(surv):
        sel, gsharpe = group_sharpe_select(df, surv["wallet"].tolist(), a.slip, a.target_n)
        s["group_sharpe_survivors"] = gsharpe
        out = os.path.join(a.out_dir, f"survivors_{safe}.txt")
        with open(out, "w") as f:
            f.write(f"# survivors ({name}): FDR10 & test-positive & conc<{a.max_conc} & slip-robust\n")
            f.write(f"# n_survivors={len(surv)} group_sharpe={gsharpe:.3f}\n")
            for w in sel:
                f.write(w + "\n")
        print(f"  group_sharpe_survivors  : {gsharpe:.3f}")
        print(f"  wrote {out} ({len(sel)} wallets)")
    with open(os.path.join(a.out_dir, f"summary_{safe}.json"), "w") as f:
        json.dump(s, f, indent=2)
    print(f"  wrote summary_{safe}.json")


if __name__ == "__main__":
    main()
