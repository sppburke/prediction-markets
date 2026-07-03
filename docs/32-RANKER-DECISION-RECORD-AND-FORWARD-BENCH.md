# 32 — Ranker adjudication decision record + the pre-registered forward bench

**Status: BINDING.** This document is the repo-committed decision record required by item 3.3
of the 2026-07-01 decision on issue #417 (full audit trail in the #417 comments of 2026-07-01),
and the **pre-registration** of the forward paper-trade bench and its promotion gate (item 3.5).
The registration is effective at this file's first merge commit, BEFORE bench T0. Numeric
defaults live in `docs/_GLOSSARY.md` (`bench_gate_*`); this file holds the design and the
rationale. Amendments A0–A8 referenced below are the audit's protocol amendments, quoted in the
#417 decision-record comment.

## 1. Decision record (backtest certification: CLOSED)

- **run25 is the adjudication; verdict NO-GO.** The pre-registered 13-config confirmatory grid
  (`scripts/ranker/confirmatory_grid.py`) on the full `[20,∞)` copyable universe (208,670
  wallets / 49,487,802 first-buy positions), forward-criteria-filter ON, 22 clean periods of
  24: no challenger is Romano-Wolf superior and beats the baseline; Hansen-SPA p = 0.075;
  PBO = 0.145; baseline cum **+299** (profitable). Artifacts:
  `runs/run25-confirmatory-full/`.
- **run24 is formally DE-CERTIFIED** (its "WINNER" was a panel artifact). The pre-#475
  rectangular cleaning made the evaluation panel — and the baseline's cumulative return — a
  function of which challenger configs were in the grid: the same baseline returns summed to
  **−62** on run24's 8-of-24-period panel, **+299** on run25's 22, **+1,156** zero-filled over
  all 24. run24 additionally carried a 614/960-config indistinguishable top-1 set and a
  degenerate near-tie AKM CI. run24 may not be cited as certification evidence. (Panel
  semantics fixed by amendment A2 in #475; near-tie CI by A5.)
- **run26 was cancelled, not relaunched** (full-history variant): the registered arms have
  **zero eligible MinTRL-20 wallets at every as_of before 2024-07** (verified by direct query),
  so all 12 added periods drop under the cleaning and the panel is bit-identical to run25's.
  The pre-mid-2024 cache cannot support the registered test — see the purge bound below.
- **The overnight full-grid re-run (run27 / protocol branch C) was cancelled** (amendment A7):
  anti-informative under the cleaning mechanism and a re-opened selection surface.
- **Both CLV diagnostics are struck from decision narratives, in both directions** (amendment
  A3): `proxy_clv` is payoff-contaminated (corr ≈ 0.987 with the payoff itself); `true_clv`
  was an hourly-fidelity stale-tick artifact until the #475 tick-ordering guard, and remains
  bench-ineligible until minute-fidelity history exists for short markets.
- **Every backtest number from this cache is bench-composition evidence only, never
  certification** (amendment A4): the #385 purge hard-deleted **85,406 wallets / ~57M trades**
  — era-asymmetric (worst 2023–2024), estimator-correlated, and unreconstructable — an
  optimistic survivorship bias no analysis of this cache can quantify. #474's
  archive-before-DELETE ends further destruction; it cannot resurrect what is gone.
- **The certification program is TERMINATED** (amendment A1): no new exploratory grids, no new
  certification runs, no re-rolls. The only permitted backtest work is the closed list of
  frozen-matrix secondary analyses below, all labeled descriptive/bench-composition:
  1. per-policy-family SPA/RW on variance-homogeneous strata;
  2. zero-filled fixed-panel per-config paired-t vs baseline (the bench-composition statistic,
     `scripts/ranker/bench_composition.py`);
  3. last-12-periods regime sub-panel of every gate statistic;
  4. per-candidate market-duration-class (<2 h / 2–26 h / >26 h) P&L decomposition from the
     frozen `ss_snapshot`.
- **No production-ranker change on backtest evidence, and no single-config argmax freeze,
  ever** (amendments A4/A5). The forward bench below is the sole promotion instrument.
- **run28 (2026-07-03)** was an explicit operator-ordered exception to A1's run
  moratorium; the exception does not reopen the program, and run28's evidence class is
  A4 bench-composition/descriptive, never certification. Results and a *proposed*
  pre-T0 amendment to §2's online_weighting arm are in
  `docs/33-RUN28-2026-WEEKLY-BAKEOFF-RESULTS.md`. This registration is unchanged until
  that amendment is explicitly adopted here.
- **OPERATOR DECISION 2026-07-03 — single-system cutover ADOPTED (explicit operator
  override of this section's A4 no-cutover clause, on operator authority; paper trading
  only).** The production ranker moves to the run28 default shape (`t_stat`, full-re-rank
  policy semantics, TTR 48h, band 0.15–0.85, MinTRL-20 — `docs/33` §9) with 4-hourly
  ranking pushes, paper P&L archived and reset to a fresh T0, and the forward gauge
  becomes the ABSOLUTE forward paper P&L of the single new system. This supersedes §2's
  incumbent-control bench composition and §3's paired gate (the incumbent ceases to
  exist at reset). run28's evidence class is unchanged (A4 bench-composition/descriptive
  — the operator decision, not the backtest, authorizes the cutover); 4h cadence is a
  data-freshness choice, not an evidence-backed one (weekly was the tested cadence,
  `docs/33` §5). The full §2/§3 rewrite lands in the cutover epic's docs PR; the shape
  change itself ships in the same PR as this entry (Part of #417).

## 2. Bench composition (item 3.4 — computed from the FROZEN run25 matrix)

Rule (committed in `scripts/ranker/bench_composition.py`, run with
`PYTHONPATH=scripts python -m ranker.bench_composition runs/run25-confirmatory-full/return_matrix.csv
--baseline 't_stat_baseline|none|policy_full_rerank|ttr72.0_pb0.15-0.85_act0_trl0_hl0.0|churn0.0'`):
zero-fill the panel; paired per-period t vs the baseline; `true_clv`-ranked arms ineligible;
**slots fixed by family** (no global argmax): one `policy_hybrid_displacement` arm (low
variance → fastest forward convergence), one `policy_online_weighting` arm (the registered
family bet; one slot only — its per-period sd is ~20× the hybrids', maximizing
time-to-signal), plus the incumbent production ranker as control.

Result (24 zero-filled periods, run25 matrix):

| bench slot | config | paired t | positive | cum | period sd |
|---|---|---|---|---|---|
| hybrid | `t_stat_baseline\|none\|policy_hybrid_displacement\|ttr72.0_pb0.15-0.85_act0_trl20_hl0.0\|churn0.75` | **4.90** | 22/24 | +22,203 | 867 |
| online_weighting | `eb_shrinkage_skill\|none\|policy_online_weighting\|ttr72.0_pb0.15-0.85_act0_trl20_hl0.0\|churn0.75` | **2.21** | 23/24 | +210,930 | 19,364 |
| control | the live production ranker (`t_stat` ≥ 2.0 greedy, MinTRL-0) | — | — | — | — |

These paired-t values are descriptive (post-hoc on the same panel); they compose the bench and
certify nothing. The forward gate below is the only selector among the benched arms.

## 3. The pre-registered forward gate (item 3.5 — registered BEFORE T0)

- **Design.** Each challenger arm runs as its own paper pe-service instance beside the live
  incumbent (deployment layout is item 3.6, held pending operator review). Identical flat
  sizing (`sizing_dollar_usd`), identical entry gates. **Unit of inference: the selection
  SYSTEM** (ranker + maintenance + demotion), not a frozen wallet list (amendment A8) — the
  challenger's top-25 refreshes by its own rule, exactly as the incumbent's does.
- **Primary metric:** paired weekly dollar P&L difference (challenger − incumbent), markets
  clustered; pooled per-share edge secondary; **co-primary excluding each cohort's single
  top-profit wallet** (the goose guard).
- **Evaluation:** a SINGLE look at **T0 + `bench_gate_eval_periods` × 30 d** using the fixed-n
  empirical-Bernstein bound (valid at one look; the #440 anytime-valid upgrade is the
  continuous-monitoring path, not a blocker). One pre-registered extension to
  `bench_gate_max_periods` iff the CI straddles 0. **Forced decision at the max horizon;
  default = keep incumbent** and reallocate slots. No other interim looks.
- **Error control:** `bench_gate_alpha` split evenly across the challenger arms (Bonferroni;
  two arms → 0.025 each). Adopt a challenger iff its lower confidence bound on the paired
  difference is > 0 AND it beats the incumbent on the primary AND on the goose-excluded
  co-primary AND its trailing-`bench_gate_regime_guard_days` difference is ≥ 0 (regime guard).
  A challenger looking worse never demotes the incumbent by itself.
- **Power (published at registration; recompute from live `paper_fills` telemetry at T0):**
  from run25's hybrid paired-difference series, pair sd ≈ $956 per 30 d period → MDE ≈ **$780
  /period at T = 6**, ≈ **$552/period at T = 12**, vs the frozen-panel point effect of ≈
  **+$1,095/period** — detectable at 6 periods; a 50 %-haircut effect at 12.
- **Validity conditions (amendment A8, all pre-T0):** ranker-input parquet snapshot pinned at
  freeze (the purge cron is armed — the candidate distribution otherwise drifts under a
  "frozen" config); `price_impact_cap_bps > 0` set identically in every instance; daily
  config-parity hash (each instance's `service_config` + binary SHA) logged into the P&L
  record, a parity break pauses the clock; paper fill price vs next CLOB trade/mid logged
  (fill-fidelity check — the flat-slippage fill model is least valid for sub-hour markets);
  the #473 windowed-dollar demotion gate deployed to ALL instances at T0 (a fair systems
  test). Forward verdict scope: paper → `live_tiny` only; capacity gates `live_tiny` → scale
  separately per `docs/19-`.

## 4. What will not happen

No new certification grids or runs; no production cutover on backtest evidence from this
cache; no single-config freeze; no CLV-column narratives (either direction) until the
minute-fidelity fix; no continuous monitoring under fixed-n bounds (optional stopping is the
exact error class this program exists to kill); no open-ended forward experiment — the gate
decides at its registered horizon, and the default is the incumbent.
