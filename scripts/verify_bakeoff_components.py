#!/usr/bin/env python3
"""Independent verification of every ranker bake-off component (issue #436 follow-up).

This is the executable form of the bake-off runbook's **Phase 0 gate**. Each statistic is
cross-checked against a SECOND, authoritative method — scipy / statsmodels / a published
closed-form / a Monte-Carlo ground-truth recovery — NOT the harness's own drift-guard tests.
A drift-guard asserts the harness agrees with itself; this asserts it agrees with an external
reference, so a shared misreading of a formula cannot pass both.

Run:
    .venv-analysis/bin/python3 scripts/verify_bakeoff_components.py

Exit 0 = every component verified GREEN; non-zero = at least one component failed independent
verification (do NOT trust a bake-off winner — or freeze it via #417 — until this is green).

Determinism: every check is seeded; Monte-Carlo checks use fixed seeds + tolerant bands so they
are reproducible. The end-to-end byte-determinism check (run the real bake-off twice) needs the
cache and lives in the runbook's Phase 0.11, not here (this script runs with no cache / no network).
"""
import math
import sys
import traceback
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.optimize import brentq
from scipy.stats import fisher_exact, norm, truncnorm, ttest_1samp

sys.path.insert(0, str(Path(__file__).resolve().parent))

from ranker.bakeoff import _churn  # noqa: E402
from ranker.deflation import DeflatedSharpe, deflated_sharpe_ratio, expected_max_sharpe  # noqa: E402
from ranker.demotion import EmpiricalBernsteinDemoter  # noqa: E402
from ranker.estimators import (  # noqa: E402
    _EB_PRIOR_VAR_FLOOR,
    _EB_TAU2_FLOOR_FRAC,
    _SD_FLOOR,
    REGISTRY as EST,
)
from ranker.oos_validation import (  # noqa: E402
    PBO,
    HansenSPA,
    RomanoWolf,
    akm_inference_on_winners,
    brown_goetzmann_cpr,
    fcr_selected_ci,
    mrsw_rank_cs,
)
from ranker_decay import weighted_stats  # noqa: E402

try:
    from statsmodels.stats.meta_analysis import combine_effects

    _HAVE_SM = True
except Exception:  # pragma: no cover - statsmodels is a hard dep but degrade gracefully
    _HAVE_SM = False

_EULER = 0.5772156649015329
_CHECKS: "list" = []


def check(name: str):
    """Register an independent-verification check. The body returns a one-line PASS detail or
    raises AssertionError (or any exception) to FAIL."""

    def deco(fn):
        _CHECKS.append((name, fn))
        return fn

    return deco


def _skill_ss(rows) -> pd.DataFrame:
    """Build a suff_stats frame for the skill estimators from (wallet, payoff, _eff) rows, with a
    contiguous RangeIndex so `weights[g.index]` aligns (the harness's indexing contract)."""
    return pd.DataFrame(rows, columns=["wallet", "payoff", "_eff"]).reset_index(drop=True)


# --------------------------------------------------------------------------------------------------
# Shared statistic
# --------------------------------------------------------------------------------------------------
@check("weighted_stats == scipy.ttest_1samp (uniform) + hand-derived Kish (weighted)")
def _c_weighted_stats():
    rng = np.random.default_rng(1)
    x = rng.normal(0.3, 1.0, 40)
    wmean, wstd, n_eff, tstat = weighted_stats(x, np.ones(40))
    t_ref = float(ttest_1samp(x, 0.0).statistic)
    assert abs(tstat - t_ref) < 1e-9, f"uniform t {tstat} != scipy {t_ref}"
    assert abs(wmean - x.mean()) < 1e-12 and abs(wstd - x.std(ddof=1)) < 1e-9
    # Weighted path: recompute Kish n_eff + unbiased weighted variance independently.
    w = rng.uniform(0.2, 2.0, 40)
    _, wstd2, neff2, t2 = weighted_stats(x, w)
    big_w, v2 = w.sum(), (w * w).sum()
    m = (w * x).sum() / big_w
    neff_ref = big_w * big_w / v2
    var_ref = (big_w / (big_w * big_w - v2)) * (w * (x - m) ** 2).sum()
    t_ref2 = m / math.sqrt(var_ref) * math.sqrt(neff_ref)
    assert abs(neff2 - neff_ref) < 1e-9 and abs(wstd2 - math.sqrt(var_ref)) < 1e-9
    assert abs(t2 - t_ref2) < 1e-9
    return f"uniform t={tstat:.6f}==scipy; weighted Kish n_eff={neff2:.3f}, t matches hand-derived"


# --------------------------------------------------------------------------------------------------
# Estimators
# --------------------------------------------------------------------------------------------------
@check("TStatBaseline score == scipy.ttest_1samp on net edge, per wallet")
def _c_tstat():
    rng = np.random.default_rng(2)
    rows = []
    for w, wr, n in [("a", 0.70, 40), ("b", 0.55, 50), ("c", 0.62, 35)]:
        rows += [(w, 1.0 if rng.random() < wr else 0.0, 0.5) for _ in range(n)]
    ss = _skill_ss(rows)
    sc = EST["t_stat_baseline"]().score(ss, as_of=0, weights=np.ones(len(ss)))
    for w in ("a", "b", "c"):
        g = ss[ss.wallet == w]
        net = ((g.payoff - g._eff) / g._eff).to_numpy()
        t_ref = float(ttest_1samp(net, 0.0).statistic)
        assert abs(float(sc.loc[w, "score"]) - t_ref) < 1e-9, f"{w}: {sc.loc[w,'score']} != {t_ref}"
    return "3 wallets: harness t-stat == scipy.ttest_1samp to 1e-9"


@check("EBShrinkageSkill posterior == independent EB pipeline INCLUDING the C1 finite-se2 mask")
def _c_eb():
    rng = np.random.default_rng(3)
    specs = ([(f"s{i}", 0.76, 50) for i in range(4)]
             + [(f"n{i}", 0.50, 50) for i in range(4)]
             + [(f"m{i}", 0.62, 40) for i in range(4)]
             + [("zero", 1.0, 50)])  # all-win -> zero dispersion -> MUST be masked out (C1 / A10)
    rows = []
    for w, wr, n in specs:
        rows += [(w, 1.0 if rng.random() < wr else 0.0, 0.5) for _ in range(n)]
    ss = _skill_ss(rows)
    sc = EST["eb_shrinkage_skill"]().score(ss, as_of=0, weights=np.ones(len(ss)))
    # The zero-dispersion wallet must DROP (NaN posterior) — this is the C1/A10 behaviour under test.
    assert "zero" not in sc.index, "zero-dispersion wallet must be masked out, not scored"
    # Independent per-wallet mean / sd over ALL wallets (uniform weights -> numpy mean/std).
    means, sds, ns = {}, {}, {}
    for w, gg in ss.groupby("wallet", sort=False):
        net = ((gg.payoff - gg._eff) / gg._eff).to_numpy()
        means[w], sds[w], ns[w] = float(net.mean()), float(net.std(ddof=1)), len(net)
    allw = list(means)
    mu0 = float(np.mean([means[w] for w in allw]))            # harness: prior mean over ALL candidates
    valid = [w for w in allw if sds[w] > _SD_FLOOR]           # C1: tau^2 over the finite-se^2 subset
    assert set(valid) == set(sc.index), f"valid subset {set(valid)} != scored {set(sc.index)}"
    se2 = {w: sds[w] ** 2 / ns[w] for w in valid}
    xv = np.array([means[w] for w in valid])
    s2 = np.array([se2[w] for w in valid])
    # DerSimonian-Laird tau2 over the valid subset — independent closed form, vs statsmodels.
    pw = 1.0 / s2
    sw = pw.sum()
    xb = (pw * xv).sum() / sw
    q = (pw * (xv - xb) ** 2).sum()
    c = sw - (pw * pw).sum() / sw
    tau2_dsl = max((q - (xv.size - 1)) / c, 0.0)
    sm_note = "statsmodels NA"
    if _HAVE_SM:
        tau2_sm = float(combine_effects(xv, s2, method_re="dl").tau2)
        assert abs(tau2_dsl - tau2_sm) <= 1e-6 + 1e-6 * abs(tau2_sm), \
            f"DL tau2 closed-form {tau2_dsl} != statsmodels {tau2_sm}"
        sm_note = f"DL tau2 {tau2_dsl:.4g}==statsmodels"
    var_means = float(np.var(xv, ddof=1))
    floor = (_EB_TAU2_FLOOR_FRAC * var_means
             if np.isfinite(var_means) and var_means > 0 else _EB_PRIOR_VAR_FLOOR)
    tau2 = max(tau2_dsl, floor)
    for w in valid:
        a = tau2 / (tau2 + se2[w])
        ref = float(norm.cdf((mu0 + a * (means[w] - mu0)) / math.sqrt(a * se2[w])))
        assert abs(float(sc.loc[w, "score"]) - ref) < 1e-6, \
            f"{w}: harness EB {sc.loc[w,'score']} != independent EB {ref}"
    return f"{len(valid)} valid wallets + zero-disp dropped; harness EB == independent EB (C1 mask); {sm_note}"


@check("GuKoenkerNPMLE recovers a known 2-point skill mixture (P(theta>0) separates populations)")
def _c_npmle():
    rng = np.random.default_rng(4)
    rows, truth = [], {}
    k, n = 300, 80
    for i in range(k):
        w = f"w{i}"
        skilled = rng.random() < 0.30
        truth[w] = skilled
        wr = 0.78 if skilled else 0.50
        rows += [(w, 1.0 if rng.random() < wr else 0.0, 0.5) for _ in range(n)]
    ss = _skill_ss(rows)
    sc = EST["gu_koenker_npmle"]().score(ss, as_of=0, weights=np.ones(len(ss)))
    sk = np.array([float(sc.loc[w, "score"]) for w in sc.index if truth.get(w)])
    nu = np.array([float(sc.loc[w, "score"]) for w in sc.index if not truth.get(w)])
    assert sk.mean() > nu.mean() + 0.15, f"skilled {sk.mean():.3f} not > null {nu.mean():.3f}+0.15"
    assert nu.mean() < 0.62, f"null mean score {nu.mean():.3f} not ~0.5 (mis-calibrated)"
    n_top = int(0.30 * len(sc))
    top = sc.sort_values("rank").head(n_top).index
    prec = float(np.mean([truth.get(w, False) for w in top]))
    assert prec > 0.60, f"top-30% precision {prec:.2f} not >0.6 (NPMLE failed to recover the skilled)"
    return f"skilled {sk.mean():.3f} > null {nu.mean():.3f}; top-30% precision {prec:.2f}"


@check("ProxyCLV / TrueCLV score == scipy.ttest_1samp on (close - price), per wallet")
def _c_clv():
    rng = np.random.default_rng(5)
    rows = []
    for w in ("a", "b", "c"):
        for _ in range(30):
            price = float(rng.uniform(0.3, 0.7))
            close = price + float(rng.normal(0.03, 0.1))
            rows.append((w, price, close, close))
    ss = pd.DataFrame(rows, columns=["wallet", "price", "close_proxy", "true_clv_close"]).reset_index(drop=True)
    for est, col in (("proxy_clv", "close_proxy"), ("true_clv", "true_clv_close")):
        sc = EST[est]().score(ss, as_of=0, weights=np.ones(len(ss)))
        for w in ("a", "b", "c"):
            g = ss[ss.wallet == w]
            clv = (g[col] - g.price).to_numpy()
            t_ref = float(ttest_1samp(clv, 0.0).statistic)
            assert abs(float(sc.loc[w, "score"]) - t_ref) < 1e-9, f"{est} {w}: != scipy"
    return "proxy + true CLV t-stat == scipy.ttest_1samp to 1e-9"


# --------------------------------------------------------------------------------------------------
# Deflation
# --------------------------------------------------------------------------------------------------
@check("DeflatedSharpe == Bailey-Lopez de Prado closed form + Monte-Carlo null calibration")
def _c_dsr():
    def emax_ref(n_trials, sr_var):
        if n_trials <= 1 or sr_var <= 0:
            return 0.0
        return math.sqrt(sr_var) * ((1 - _EULER) * norm.ppf(1 - 1.0 / n_trials)
                                    + _EULER * norm.ppf(1 - 1.0 / (n_trials * math.e)))

    def dsr_ref(sr, n, sk, ku, sr0):
        var = max(1.0 - sk * sr + (ku - 1.0) / 4.0 * sr * sr, 1e-12)
        return float(norm.cdf((sr - sr0) * math.sqrt(n - 1.0) / math.sqrt(var)))

    for nt, srv in [(10, 0.01), (100, 0.04), (500, 0.02)]:
        assert abs(expected_max_sharpe(nt, srv) - emax_ref(nt, srv)) < 1e-9, f"SR0 mismatch nt={nt}"
    for sr, n, sk, ku, sr0 in [(0.4, 300, 0.0, 3.0, 0.1), (0.2, 250, -0.5, 5.0, 0.05)]:
        assert abs(deflated_sharpe_ratio(sr, n, sk, ku, sr0=sr0) - dsr_ref(sr, n, sk, ku, sr0)) < 1e-9
    s0 = [expected_max_sharpe(nt, 0.02) for nt in (2, 10, 100, 1000)]
    assert all(a < b for a, b in zip(s0, s0[1:])), "SR0 not monotonically increasing in n_trials"
    # Monte-Carlo: empirical max in-sample Sharpe over N null strategies brackets SR0 (calibration).
    rng = np.random.default_rng(6)
    n_trials, n_obs, reps = 150, 250, 120
    maxes = []
    for _ in range(reps):
        srs = [(r := rng.normal(0, 1, n_obs)).mean() / r.std(ddof=1) for _ in range(n_trials)]
        maxes.append(max(srs))
    emp = float(np.mean(maxes))
    sr0 = emax_ref(n_trials, 1.0 / n_obs)  # null per-obs Sharpe ~ N(0, 1/n_obs)
    assert 0.5 * sr0 < emp < 1.6 * sr0, f"MC max-Sharpe {emp:.3f} not bracketing SR0 {sr0:.3f}"
    return f"SR0+DSR == closed form to 1e-9; MC null max-Sharpe {emp:.3f} brackets SR0 {sr0:.3f}"


# --------------------------------------------------------------------------------------------------
# Honesty layer
# --------------------------------------------------------------------------------------------------
@check("PBO/CSCV known-answer: persistent skill -> low PBO; anti-persistent rank-reversal -> elevated")
def _c_pbo():
    # CSCV splits T into contiguous groups (PBO.assess uses np.array_split). Two theory-clean cases:
    #  - a strictly dominant config is IS-best AND OOS-best in every split -> logit>0 -> PBO ~ 0;
    #  - a rank-reversal (config j worth +j in the first half of periods, -j in the second) makes the
    #    IS-best the OOS-worst on every unbalanced split -> logit<0 -> PBO elevated.
    # NB the naive "i.i.d. noise -> 0.5" is NOT a valid known-answer: fixed finite columns have a
    # realized per-column mean that BOTH disjoint halves estimate, so IS-rank predicts OOS-rank.
    t, n = 120, 8
    dom = pd.DataFrame({f"c{j}": ([1.0] * t if j == 0 else list(np.linspace(0, 0.1, t))) for j in range(n)})
    pbo_dom = float(PBO(s_groups=8).assess(dom, n_configs=n)["pbo"].iloc[0])
    assert pbo_dom < 0.2, f"persistently-dominant config PBO {pbo_dom} not < 0.2"
    half = t // 2
    # Asymmetric reversal: +j first half, -0.9j second half. The half-ranks still reverse (OOS-worst =
    # IS-best on unbalanced splits) but the full-sample column means are 0.05j -> distinct, so the A7
    # near-zero-dispersion degeneracy guard does not (correctly) NaN it out.
    rev = pd.DataFrame({f"c{j}": [float(j)] * half + [float(-0.9 * j)] * half for j in range(n)})
    pbo_rev = float(PBO(s_groups=8).assess(rev, n_configs=n)["pbo"].iloc[0])
    assert pbo_rev > 0.3 and pbo_rev > pbo_dom + 0.25, \
        f"anti-persistent PBO {pbo_rev} not elevated vs dominant {pbo_dom}"
    return f"persistent PBO={pbo_dom:.3f}(<0.2); anti-persistent PBO={pbo_rev:.3f}(IS-best=OOS-worst)"


@check("RomanoWolf/HansenSPA known-answer: a clear edge -> superior+SPA-GO; all-null -> none+NO-GO")
def _c_spa_stepm():
    def lb(seed, edge):
        rng = np.random.default_rng(seed)
        t = 250
        bench = rng.normal(0.0, 1.0, t)
        cols = {"baseline": bench}
        if edge:
            cols["edge"] = bench + 0.5 + rng.normal(0.0, 1.0, t)
        for i in range(3):
            cols[f"null{i}"] = bench + rng.normal(0.0, 1.0, t)
        return pd.DataFrame(cols)

    le = lb(1, True)
    rw = RomanoWolf("baseline", reps=200, seed=1).assess(le, n_configs=le.shape[1])
    sup = set(rw[rw["beats_benchmark"]]["config"])
    assert "edge" in sup, f"RomanoWolf failed to flag the edge config: {sup}"
    p_edge = float(HansenSPA("baseline", reps=200, seed=1).assess(le, n_configs=le.shape[1])
                   ["spa_pvalue_consistent"].iloc[0])
    assert p_edge < 0.10, f"SPA p {p_edge} not < 0.10 with a clear edge"
    ln = lb(2, False)
    rw0 = RomanoWolf("baseline", reps=200, seed=2).assess(ln, n_configs=ln.shape[1])
    assert not rw0["beats_benchmark"].any(), "RomanoWolf flagged a null as superior"
    p_null = float(HansenSPA("baseline", reps=200, seed=2).assess(ln, n_configs=ln.shape[1])
                   ["spa_pvalue_consistent"].iloc[0])
    assert p_null > 0.10, f"SPA p {p_null} not > 0.10 for an all-null grid"
    return f"edge: RW flags 'edge', SPA p={p_edge:.3f}; null: none flagged, SPA p={p_null:.3f}"


@check("AKM median-unbiased == scipy.truncnorm inversion + winner's-curse correction direction")
def _c_akm():
    est = np.array([3.0, 1.0, 0.5])
    out = akm_inference_on_winners(est, np.ones(3), winner=0)
    y, s, lower = 3.0, 1.0, 1.0  # runner-up = truncation bound

    def cdf(theta):
        return float(truncnorm.cdf(y, (lower - theta) / s, np.inf, loc=theta, scale=s))

    mu_ref = brentq(lambda th: cdf(th) - 0.5, y - 20 * s, y + 20 * s, xtol=1e-10)
    assert abs(out["median_unbiased"] - mu_ref) < 1e-4, \
        f"AKM median {out['median_unbiased']} != truncnorm inversion {mu_ref}"
    near = akm_inference_on_winners(np.array([1.05, 1.0, 0.5]), np.ones(3), winner=0)
    assert near["median_unbiased"] < near["naive_estimate"] - 0.1, \
        "no winner's-curse correction when the winner barely beats the runner-up"
    return f"median_unbiased {out['median_unbiased']:.4f} == truncnorm {mu_ref:.4f}; correction confirmed"


@check("MRSW rank-CS / FCR CI == scipy.norm recompute (Bonferroni z / Benjamini-Yekutieli level)")
def _c_mrsw_fcr():
    cs = mrsw_rank_cs(np.array([5.0, 1.0, 1.0, -3.0, -3.0]), np.full(5, 0.3), tau=1)
    in_top = set(int(i) for i in cs[cs["in_top_tau_cs"]]["index"])
    assert in_top == {0}, f"MRSW top-1 confidence set {in_top} != {{0}} (clear leader)"
    est, ses = np.array([2.0, 1.5, 1.0, 0.5]), np.ones(4)
    fc = fcr_selected_ci(est, ses, np.array([True, False, False, False]))
    row = fc.iloc[0]
    r, m, q = 1, 4, 0.05
    assert abs(float(row["fcr_level"]) - (1.0 - r * q / m)) < 1e-9, "FCR level != 1 - R*q/m (BY)"
    z = norm.ppf((1.0 + float(row["fcr_level"])) / 2.0)
    half = float(row["ci_hi"]) - float(row["estimate"])
    assert abs(half - 1.0 * z) < 1e-6, "FCR CI half-width != se * z(fcr_level)"
    assert z > norm.ppf(0.975), "FCR CI not wider than the unadjusted 95% CI"
    return f"MRSW top-1 CS={{0}}; FCR level={row['fcr_level']:.4f}==1-(R/m)q, CI==se*z, wider than 95%"


@check("Brown-Goetzmann CPR odds ratio == scipy.fisher_exact on the persistence contingency table")
def _c_cpr():
    rng = np.random.default_rng(8)
    n = 200
    idx = [f"w{i}" for i in range(n)]
    skill = rng.normal(0, 1, n)  # shared latent skill -> genuine persistence
    p1 = pd.Series(skill + rng.normal(0, 0.3, n), index=idx)
    p2 = pd.Series(skill + rng.normal(0, 0.3, n), index=idx)
    res = brown_goetzmann_cpr(p1, p2)
    m1, m2 = p1.median(), p2.median()
    keep = (p1 != m1) & (p2 != m2)  # C3: drop wallets at either median
    ww = int(((p1 > m1) & (p2 > m2) & keep).sum())
    wl = int(((p1 > m1) & (p2 < m2) & keep).sum())
    lw = int(((p1 < m1) & (p2 > m2) & keep).sum())
    ll = int(((p1 < m1) & (p2 < m2) & keep).sum())
    assert (ww, wl, lw, ll) == (res["ww"], res["wl"], res["lw"], res["ll"]), \
        f"contingency {ww,wl,lw,ll} != harness {(res['ww'],res['wl'],res['lw'],res['ll'])}"
    odds, _ = fisher_exact([[ww, wl], [lw, ll]])
    assert abs(res["cpr"] - float(odds)) < 1e-9, f"CPR {res['cpr']} != fisher odds ratio {odds}"
    # Perfect-persistence branch (C3-hardened): identical periods -> WL=LW=0 -> go=True only on genuine
    # ww>0 & ll>0. fisher_exact's odds ratio is inf here; the harness reports it as a deterministic GO.
    s = pd.Series(np.arange(n, dtype=float), index=idx)
    perf = brown_goetzmann_cpr(s, s)
    assert perf["wl"] == 0 and perf["lw"] == 0 and perf["ww"] > 0 and perf["ll"] > 0, "not perfect persistence"
    assert perf["go"] is True, "perfect persistence (WL=LW=0, WW>0, LL>0) must be go=True"
    return (f"contingency {ww}/{wl}/{lw}/{ll} matches; CPR={res['cpr']:.4f} == fisher OR; "
            f"perfect-persistence go branch exercised")


# --------------------------------------------------------------------------------------------------
# Demotion
# --------------------------------------------------------------------------------------------------
@check("EmpiricalBernsteinDemoter == Maurer-Pontil closed form + one-sided coverage >= 1-delta")
def _c_bernstein():
    d = EmpiricalBernsteinDemoter(delta=0.05, min_periods=5)
    ln = math.log(2.0 / 0.05)
    pnl = np.array([-2.0, -1.0, -3.0, -0.5, -2.5, -1.5])
    lp = pd.DataFrame({"wallet": ["x"] * len(pnl), "period_end": range(len(pnl)), "realized_pnl": pnl})
    got = d.should_demote("x", lp, as_of=100)
    n = len(pnl)
    upper = pnl.mean() + math.sqrt(2 * pnl.var(ddof=1) * ln / n) + 3 * (pnl.max() - pnl.min()) * ln / n
    ref = (pnl.mean() < 0) and (pnl.sum() < 0) and (upper < 0) and (n >= 5)
    assert got == ref, f"should_demote {got} != closed-form gate {ref} (upper={upper:.3f})"
    winner = pd.DataFrame({"wallet": ["y"] * 6, "period_end": range(6), "realized_pnl": [1, 2, -1, 3, 2, 1]})
    assert not d.should_demote("y", winner, as_of=100), "a profitable wallet must never be demoted"
    # Coverage: the one-sided EB upper bound covers the true mean at >= 1-delta over Monte-Carlo.
    rng = np.random.default_rng(9)
    reps, nn, true_mean, covered = 400, 20, 0.5, 0
    for _ in range(reps):
        s = rng.uniform(0.0, 1.0, nn)  # bounded support, true mean 0.5
        rng_s = (s.max() - s.min()) or 1.0
        up = s.mean() + math.sqrt(2 * s.var(ddof=1) * ln / nn) + 3 * rng_s * ln / nn
        covered += up >= true_mean
    cov = covered / reps
    assert cov >= 0.95, f"EB upper-bound coverage {cov:.3f} < nominal 0.95"
    return f"should_demote == closed-form gate; EB upper-bound coverage {cov:.3f} >= 0.95"


# --------------------------------------------------------------------------------------------------
# Policy turnover
# --------------------------------------------------------------------------------------------------
@check("_churn == positive-part L1 weight movement (hard-set admission + soft-set ramp)")
def _c_churn():
    prev = pd.DataFrame({"wallet": ["a", "b"], "weight": [1.0, 1.0]})
    follow = pd.DataFrame({"wallet": ["b", "c"], "weight": [1.0, 1.0]})
    assert abs(_churn(prev, follow) - 1.0) < 1e-12, "hard-set: drop a / keep b / add c must charge +1"
    prev2 = pd.DataFrame({"wallet": ["a", "b"], "weight": [1.5, 0.5]})
    follow2 = pd.DataFrame({"wallet": ["b", "c"], "weight": [1.5, 1.0]})  # b +1.0, c +1.0, a -1.5 (clamp 0)
    assert abs(_churn(prev2, follow2) - 2.0) < 1e-12, "soft-set: ramp-up + admission must charge +2"
    return "hard-set admission == 1.0; soft-set positive-part L1 == 2.0 (weight decreases clamped to 0)"


def main() -> int:
    print("=" * 96)
    print("Independent verification of ranker bake-off components (external-reference cross-checks)")
    print(f"statsmodels external check: {'available' if _HAVE_SM else 'UNAVAILABLE (closed-form only)'}")
    print("=" * 96)
    failures = 0
    for name, fn in _CHECKS:
        try:
            detail = fn() or ""
            print(f"[PASS] {name}\n       {detail}")
        except Exception as exc:  # noqa: BLE001 - report any failure mode
            failures += 1
            print(f"[FAIL] {name}\n       {type(exc).__name__}: {exc}")
            print(textwrap_indent(traceback.format_exc()))
    print("=" * 96)
    total = len(_CHECKS)
    print(f"{total - failures}/{total} components verified GREEN against an independent reference.")
    if failures:
        print(f"{failures} FAILED — do NOT trust a bake-off winner until these are resolved.")
    return 1 if failures else 0


def textwrap_indent(s: str) -> str:
    return "\n".join("       " + ln for ln in s.rstrip().splitlines())


if __name__ == "__main__":
    raise SystemExit(main())
