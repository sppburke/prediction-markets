# 33 — run28: the 2026 weekly full-grid bake-off (bench-composition evidence)

**Status: DESCRIPTIVE / BENCH-COMPOSITION.** Under `docs/32` §1 (amendment A4), every
backtest number from this cache — including everything below — is bench-composition
evidence, never certification. run28 was an operator-ordered post-registration run
("definitive runs, no shortcuts", 2026-07-02); it does not reopen the certification
program, and the `docs/32` §3 forward gate remains the sole promotion instrument.
What run28 adds: the first artifact-free full-grid panel (see §2), the capacity-feasible
ranking at the live bankroll, and the TTR / set-transition-policy / cadence answers the
bench design needs. Artifacts: `data/archive/research-2026-07-run28/` (inventory in its
README); working copies in gitignored `runs/run28-2026-weekly-lambda/`.
(Point-in-time note: rows labeled "current prod shape" / "current live behavior" below
describe PRE-cutover production; the 2026-07-03 single-system cutover — `docs/32` §1 —
moved production to ttr48/trl20 + `full_rerank` membership.)

## 1. Design

| axis | value |
|---|---|
| window | 2026-only: 24 Saturday-to-Saturday weekly steps from 2026-01-03 (`--start-unix 1767398400`, `--step-days 7 --horizon-days 7 --steps 24`), 180 d train |
| universe | full copyable `[20,∞)`: 208,670 wallets / 49,487,802 first-buy positions |
| grid | 768 = 4 estimators × 2 deflators × 4 policies × 12 criteria (TTR {24,48,72} h × band {0.15–0.85, 0.30–0.70} × MinTRL {0,20}) × 2 churn ({0, 0.75}) |
| eval | `--forward-criteria-filter` ON (selection and deployment share the filter, #468) |
| true_clv | excluded by preflight: position-level CLOB-close coverage 25% < 30% gate — a permanent data ceiling (#429), not a prep gap |
| provenance | Lambda 80-core/440 GB box, repo @ `a1d7601`; process executor, `--workers 48` requested but effective parallelism 12 — the grid partitions into 12 criteria chunks, one process each (`progress.w0..w11`, box load steady at 12); materialize 227 s, screen 692 s, grid 34,165 s (9.49 h), honesty layer 24.6 s; component gate `verify_bakeoff_components.py` 16/16 GREEN on the box before launch |

A thread-executor + numba-`nogil` speed variant was raced and **failed** (GIL-bound at
~1.2 cores — kernel calls are short relative to Python dispatch); it was killed and the
box edits reverted. Recorded in the archive so nobody retries it.

## 2. Harness verdict — and how to read it

`decision.json`: **status WINNER** — the first non-artifact WINNER across all nine runs.
PBO **0.0103**, Hansen-SPA p **0.005**, CPR go, and — decisive for panel validity —
**24 of 24 clean periods, zero dropped** (`raw_periods == clean_periods == 24`,
`dropped_configs=[]`). The #475/#477 A2 zero-fill semantics eliminated the
panel-endogeneity mechanism that de-certified run24 (`docs/32` §1): the baseline's
cumulative return is no longer a function of which challengers share the grid.

The crowned config is `gu_koenker_npmle|none|policy_full_rerank|ttr48.0_pb0.15-0.85_act0_trl20_hl0.0|churn0.0`
(cum +$209,867 / 24 wk). **Do not read the crown as a single-config identity claim:**

- The MRSW top-1 confidence set holds **122 of 768 configs** — a 122-way statistical tie
  for first place at α=0.05.
- The winner leads the runner-up by 6.1 weekly-mean units = **0.003·SE** (SE $1,885,
  n=24): the argmax identity is selection noise.
- The AKM block (`median_unbiased = ci_lo = ci_hi = −6889`) is the known
  truncated-normal CDF **underflow artifact** (`oos_validation.py` `_truncated_normal_cdf`
  den≤0 branch): the winner/runner-up gap sits above the `AKM_NEAR_TIE_SIGMA = 1e-3`
  guard but inside the numeric underflow zone (any gap below ~0.08·SE lands there).
  The honest post-selection statement is the unconditional CI, mean ± 1.96·SE.
  Hygiene follow-up: widen the near-tie guard so the fallback fires in this zone.
- `grid_dsr_advisory_low = true` (DSR ≈ 0.0004) — advisory by design (`docs/31`), not a
  gate; the RW/SPA/PBO panel already deflates.

## 3. Units (code-verified)

Every dollar figure below is at the **live trading shape**: each weekly cell of
`return_matrix.csv` is one config's one-week USD P&L following ≤25 wallets (`k=25`) at
**$25 flat per position** (`flat_usd` default, threaded as `PE_BACKTEST_FLAT_USD`), net
of 100 bps slippage on entry and leader-sell exit, resolution close at $1/$0, **no
taker-fee model** (the churn axis charges $0.75 per newly-admitted wallet as the fee
proxy; churn 0 vs 0.75 twins differ ≲0.1%, so the results are fee-insensitive).
Per-config position counts come from `mtm_coverage.json` `by_config_overall.positions_in_window`
("positions alive in the window" — includes pre-as_of entries that closed in-window).

## 4. Capacity-feasible ranking (the deployable-money answer)

Metric (fixed before results): `cap_feasible_wk = min(positions/week, 588) × net$/position`,
where 588 = ⌊live bankroll $14,702 / $25⌋ concurrent slots — a conservative no-recycling
cap (derived from live values, not a new config default). The cap binds for 184/768
configs; no config sits near the boundary. Computed by `analysis_capacity.py`; full
ranking in `capacity_leaderboard.csv`.

| rank | config | $/wk | pos/wk | net $/pos | NW-t vs prod | weeks won |
|---|---|---|---|---|---|---|
| 1 | `proxy_clv\|deflated_sharpe\|online_weighting\|ttr48\|pb0.15-0.85\|trl20\|churn0` | **3,586** | 134 | 26.72 | 3.27 | 18/24 |
| 3 | `t_stat_baseline\|deflated_sharpe\|online_weighting\|ttr48\|pb0.15-0.85\|trl20\|churn0` | 3,251 | 130 | 25.06 | 3.30 | 17/24 |
| 5 | `t_stat_baseline\|none\|full_rerank\|ttr24\|pb0.15-0.85\|trl20\|churn0` | 3,193† | 1,489 | 5.43 | 3.16 | 24/24 |
| 7 | `t_stat_baseline\|none\|full_rerank\|ttr48\|pb0.15-0.85\|trl20\|churn0` | 3,050† | 1,591 | 5.19 | 3.30 | 24/24 |
| 29 | `gu_koenker_npmle\|none\|full_rerank\|ttr48\|pb0.15-0.85\|trl20\|churn0` (harness winner) | 1,766† | 2,911 | 3.00 | 3.34 | 24/24 |
| 577 | **current prod shape** `t_stat\|none\|full_rerank\|ttr72\|pb0.15-0.85\|trl0\|churn0` | **−17.8** | 11.75 | −1.52 | — | — |

† capacity-capped at 588 signals/week (raw uncapped $8.1–8.7k/wk needs >$37k/wk notional).

Robustness of the #1 config: ranked **#3 in each independent 12-week half** of 2026
(grid-wide split-half Spearman 0.475; some rival configs that topped half 1 fell to
rank ~745 in half 2 — the curse is real, this family survives it); it sits inside the
122-member MRSW set; its `t_stat_baseline` twin at #3 shows the **family** (deflated-gate
+ online_weighting + ttr48 + MinTRL-20), not the estimator, carries the edge. An
equal-weight portfolio of the top 8 makes $5,791/wk, t=5.14, positive 23/24 weeks.

**The current production shape ranks 577/768 and lost $428 over 2026** (11.75
signals/week admitted). MinTRL-20 is the single dominant fix (median cap-feasible
$/wk, MinTRL-20 vs 0: full_rerank +1,195 vs −9; online_weighting +994 vs +25).

## 5. TTR, set-transition policy, cadence

**TTR (256 complete within-config triples, everything else identical):**
48 h ≈ 72 h — mean diff +$3.99/wk, NW-t 0.11, 45% of triples: a coin flip, so the
capital-velocity preference for 48 h is free. 24 h is worse: −$62.2/wk vs 72 h,
NW-t −1.94, only 30% of triples favor it. (8/256 triples have identical 48/72 columns;
NW = Bartlett lag 2; all values independently reproduced.)

**Set-transition policy (capacity-feasible $/wk across all 768):**

| policy | median | best config | best rank |
|---|---|---|---|
| full_rerank (top-25 replaces the set each re-rank) | **601** | 3,193 | 5 |
| online_weighting (soft weights) | 262 | **3,586** | 1 |
| hybrid_displacement | 24 | 1,305 | 75 |
| knockout_backfill (**current live behavior**) | **−5** | 739 | 168 |

The live hold-until-knockout policy is the worst tested — median config loses money,
best config ranks 168th. Wallets should re-earn their slot at every re-rank. Weekly
churn is effectively free (churn-twin identity above). full_rerank is the most robust
and the operationally simplest with flat sizing; online_weighting wins only in its best
configs and needs weight-proportional sizing or probabilistic copying to implement.

**Cadence:** weekly is the tested cadence (the walk-forward step). Faster cadences were
not tested; scores are built from resolved positions (median resolution lag ~8.1 d per
`mtm_coverage.json`), so the ranking input moves on resolution timescales — no tested or
structural support for intra-day re-ranking. N was fixed at k=25 (not swept).

## 6. Verification record

- Pre-run: `scripts/verify_bakeoff_components.py` 16/16 GREEN on the box.
- Post-run, three independent adversarial verifiers, all CONFIRMED:
  NW t-stats reproduced by statsmodels OLS-HAC (`maxlags=2, use_correction=False`) to
  ≤1e-15; all 768×6 capacity fields recomputed from raw inputs with zero mismatches
  (worst rel-diff 8.5e-14) including the 184-capped count with no boundary sensitivity;
  TTR pairing re-derived via an independent structural parse (`verify_ttr_pairing.py`) —
  zero grouping collisions, all claimed statistics reproduced.
- Panel consistency: `leaderboard.cum_return` == `return_matrix` column sums (max |diff|
  3e-11); zero never-live (all-zero) columns.

## 7. Standing caveats (unchanged in kind from docs/32)

Survivorship is optimistic and unquantifiable (#385 purge; `survivorship.json`); there
is no taker-fee model; the sim's net $26.7/position (~107% on $25) is far above live
experience (~$0.8/fill) — treat levels as upper bounds and the **ranking** as the
reliable output. `clv_diagnostic.json` (proxy_clv t=9.46) remains struck from narratives
per amendment A3 — payoff contamination applies to the *diagnostic*; the proxy_clv
*estimator-as-ranker* is unaffected. Nothing here authorizes a production-ranker change;
the `docs/32` §3 forward gate decides.

## 8. Proposed pre-T0 amendment to docs/32 §2 (operator decision — NOT yet in force)

The bench is not yet deployed (item 3.6 pending), so arm composition may be amended
before T0. run28 motivates re-picking the **online_weighting slot** from the run28 panel
instead of run25's. The capacity-metric #1 config is ranked by `proxy_clv`, which is
**bench-ineligible** under the committed eligibility rule
(`scripts/ranker/bench_composition.py` `INELIGIBLE_ESTIMATORS = ("true_clv", "proxy_clv")`
— the A3 artifact classification applies to the estimator's slot eligibility, not just
the diagnostic), so the proposed arm is its rule-compliant twin:

`t_stat_baseline|deflated_sharpe|policy_online_weighting|ttr48.0_pb0.15-0.85_act0_trl20_hl0.0|churn0.0`
(capacity rank #3, $3,251/wk, 130 signals/week).

Rationale: weekly sd $3,453 vs the registered run25 arm's $19,364 (≈5–6× faster forward
convergence — directly serving the gate's time-to-signal constraint), capacity-fit
(no truncation at the live bankroll), ttr48 (proven free, §5), t = 4.61 vs zero, and
split-half positive in both halves (top-6% in weeks 1–12, #1 in weeks 13–24). The
hybrid slot and incumbent control are unchanged; the gate itself (docs/32 §3) is
untouched. If adopted, edit docs/32 §2's table and note the amendment date there.

## 9. What this changes / what it does not

- **Changes:** the bench arm menu (§8); the ranker-shape default for any future
  freeze candidate (MinTRL-20 + ttr48 + weekly full re-rank, §4–5); closes the TTR and
  cadence questions for the bench design.
- **Does not change:** the certification-closed status, the forward gate, the
  no-cutover-on-backtest-evidence rule, the true_clv ineligibility, or the CLV-narrative
  strike (all `docs/32`).
