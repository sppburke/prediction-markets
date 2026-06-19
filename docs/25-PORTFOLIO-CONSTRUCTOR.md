# 25 — Stage-2 Portfolio Constructor (archived — removed in #370)

> **Status: ARCHIVE.** Nothing here describes live code. The `scripts/portfolio_constructor/`
> package and the GBM walk-forward it consumed were removed in issue #370 (PR3). Cohort
> selection is now the 72hr ranker's own eligibility filters over the full trade universe
> (`scripts/rank_and_push.sh` → Supabase `latest_ranking`) — see
> `docs/26-DATA-REFRESH-AND-REOPTIMIZATION-RUNBOOK.md`. Retained for design history.

**Issue:** #276  
**Status:** v1 shipped (greedy max-edge / min-overlap) — superseded; see banner above.

## Why

The Stage-1 GBM ranker scores each wallet's standalone forward edge, but has no notion of how wallets combine. For a capital-constrained copy-trading account the binding constraint is portfolio variance: `Var(portfolio) = σ²·[ρ + (1−ρ)/N]`, so adding correlated wallets leaves the variance floor `ρ` unchanged regardless of N. Stage-2 selects a jointly-diversified, edge-weighted subset to follow given a capital budget.

## Architecture

Sub-package `scripts/portfolio_constructor/` mirrors `composite_tuner/`'s one-job-per-module structure. No ABC or Protocol interfaces in v1 (zero in `composite_tuner`; deferred to the second selector method).

| Module | Deps | CI-testable |
|---|---|---|
| `overlap.py` | stdlib | yes |
| `selector.py` | stdlib; no sibling imports (overlap injected) | yes |
| `sizing.py` | stdlib (`statistics`) | yes |
| `data.py` | numpy, composite_tuner.data | local only |
| `edge.py` | lightgbm, monthly_rerank_gbm | local only |
| `validate.py` | numpy, gbm_walkforward, composite_tuner.pbo | local only |
| `constructor.py` | all of the above | local only |
| `cli.py` | all of the above | local only |

The CI test (`scripts/test_portfolio_constructor.py`) imports the stdlib trio directly via `sys.path.insert(.../portfolio_constructor)`, bypassing `__init__.py` — the same pattern as `test_haircut_constants.py:37`. `selector.py` imports no sibling (overlap is constructor-injected) so it loads standalone cleanly.

## Algorithm

Greedy max-edge / min-overlap: at each step select the wallet maximising

    score × (1 − λ · marginal_Jaccard_overlap(candidate, selected_union))

where `selected_union` is the union of `(market_id, outcome_id)` sets of all already-selected wallets, `score` is the GBM edge score from `gbm_scores_at`, and λ is `portfolio_overlap_lambda` (default 1.0).

## Forward-validity contract

| Variable | Computed as-of | Used for |
|---|---|---|
| `gbm_scores_at(cutoff)` | `cutoff` (train cutoffs strictly `c + fwd_secs <= cutoff`) | candidate universe + edge μ |
| `load_wallet_market_sets(cutoff, lookback)` | `(cutoff − lookback, cutoff]` | overlap (Jaccard) |
| greedy `select(...)` | `cutoff` | portfolio choice |
| `load_oos_positions(cutoff, fwd_end)` | `(cutoff, fwd_end]` forward window | return measurement only |
| ex-ante `exante_kelly_fraction(prior_returns)` | prior window `(cutoff − lookback, cutoff]` | sizing fraction |
| `--watchlist` filter | applied **only** at the deploy cutoff | deploy set, not validation |

## Sizing

Two modes (`--sizing-mode`):

- **`fractional`** (default): stake = `applied_f × current_bankroll` per position, compounding. Applied fraction = `portfolio_sizing_kelly_fraction × exante_kelly_fraction(prior_returns)`.
- **`flat`**: fixed stake = `applied_f × starting_bankroll`, no compounding.

Both skip positions whose stake < `portfolio_sizing_min_position_usd`.

`exante_kelly_fraction` is the single pinned estimator: `clip(μ/σ², [0,1]) × t²/(t²+1)`, returning 0 for n<5.  
`portfolio_sizing_kelly_fraction = 0.25` is the research default; **distinct from** the Winner-Follow Kelly fractions in `docs/19-`.

## Validation

Walk-forward via `gbm_walkforward.eligible_anchors` → per-anchor greedy portfolio → PBO on (seeds × anchors) matrix. Outputs schema_version=2 JSON matching `gbm_walkforward.py`'s format plus `n_eligible_anchors` and `credible` fields.

- `portfolio_pbo_min_anchors = 4`: minimum for a committed PBO verdict (reuses the existing rule).
- `portfolio_min_credible_anchors = 4`: minimum for deploy credibility. Below this, `credible=false` and the deploy set is flagged "insufficient evidence".

## Open risks

- **Edge must be real.** Diversification reduces variance, not bias. A non-positive `mean_of_mean_edge` → "not deployable."
- **Low statistical power.** At `fwd=30d`, few eligible anchors → PBO `undefined` below 4. Use `--fwd-days 7` or `14`.
- **Overlap is historical.** Trailing-window Jaccard reflects recent specialisation; does not guarantee future market disjointness.
- **Sparse ex-ante Kelly.** Tight portfolios → few prior-window positions → noisy f*; `exante_kelly_fraction` returns 0 for n<5.
- **Reimplemented sizing math.** `sizing.py` reimplements `full_kelly_fraction`/`sim_*` in stdlib (CI Blocking forced this). Numbers mirror `sizing_april_sim.py:95-138` / `forward_curated_selector.py:68-80`.

## Defaults

All numeric defaults in `docs/_GLOSSARY.md` §"Portfolio constructor defaults".

## Deferred (out of scope v1)

- HRP and convex-MVO selectors — seam deferred to second method.
- Return-covariance overlap signal.
- Wallet-level optimized weights.
- Cron/automation for refresh cadence.
