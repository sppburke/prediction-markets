# 31 — Ranker Bake-off Methodology

> See `_BASELINE.md` for the toolchain pin, lints, and common acceptance gate.
> See `_GLOSSARY.md` for vocabulary (wallet/trader/leader/candidate), type aliases, and **all** ranker configuration defaults — this doc references those keys by name rather than restating values.
> See `16-RUST-WORKSPACE-ARCHITECTURE.md` for the `pe-backtest` seam and `26-DATA-REFRESH-AND-REOPTIMIZATION-RUNBOOK.md` for the production ranking refresh this harness evaluates.

**This file is the canonical description of the modular ranker evaluation harness (issue #421) — the swappable-module bake-off that decides which wallets the copy-trader should follow.** It documents *what is implemented* in `scripts/ranker/` at the close of the #421 PR sequence (PR1–PR5), the honesty guards that keep its verdict trustworthy, and the boundary between the live menu and the reserved seams. The harness produces a **grid-deflated leaderboard → a recommended winning config (or an explicit NO-GO)** that feeds productionization in #417. It deploys nothing on its own: the bake-off **run** is a separate operator compute step (§Running the bake-off), and a NO-GO is a valid deliverable.

## Why the redesign

The previous ranker selected on raw net t-stat ≥ 2.0 plus a greedy max-group-Sharpe portfolio step. Per the issue #421 forensics this is a textbook **winner's-curse** failure: the top-ranked wallets bled in live paper trading, the #8-ranked wallet was the single worst live performer, and ~82% of realized P&L traced to one wallet. Ranking on an unshrunk, un-deflated point estimate selects the luckiest sample, not the most skilled wallet. The redesign keeps trading mechanics and buy-and-hold-to-resolution execution **fixed** and changes only the *ranking math*, behind a permanent modular harness so every estimator, selector, criterion, and transition policy is a swappable module measured on our own data.

The target question every experiment must answer is narrow and operational: **"which wallets should we follow right now?"** — enforced by forward walk-forward scoring, the `resolved_at ≤ as_of` look-ahead guard, and an activity-recency eligibility gate (a dormant wallet is not a follow-now candidate).

## Unit of evaluation: the set-transition policy

The deployed system does not pick a point-in-time snapshot — it **maintains a followed set over time** (currently incremental knockout + backfill in `crates/service/src/watchlist_maintenance.rs`). A ranking that looks excellent at a single cutoff but behaves badly under the real maintenance policy is a false positive. So the bake-off's unit of evaluation is the **set-transition policy**: each candidate config is run as a walk-forward *trajectory* —

```
set_t  →  pe-backtest(set_t)  →  live_pnl_t  →  policy.step(...)  →  set_{t+1}
```

— and scored by **cumulative forward copy P&L over the trajectory, net of churn cost**. The four policies are a first-class axis (§The modular menu).

## The shared substrate — `suff_stats`

Every module reads one materialized frame, `scripts/ranker/suff_stats.py`, so no module can drift from the production position definition: `materialize` calls `ranker_duck.duck_extract_positions` (the same first-buy join the live ranker uses) over a permissive superset, and the criteria slice happens downstream in pandas. One row per qualifying first-buy position, twelve columns: `wallet`, `market`, `outcome_id`, `entry_ts`, `ttr_ref` (absolute per-market scheduled close), `resolved_at`, `price`, `payoff`, `dollar_size`, `_eff` (slip-adjusted effective entry), `c_t` (entry-instant concurrency), and `close_proxy` (last pre-resolution trade price on the bought outcome — added in PR5 for the CLV estimator; `NaN` when the outcome never traded pre-resolution).

`close_proxy` is derived in SQL (`arg_max(price, timestamp)` over `trades` strictly before `resolved_at`) and left-joined on; the `proxy_clv` estimator reads it as a column and never touches the database. The slip default and effective-entry cap mirror `rank_72hr_buyandhold.py`.

## The modular menu

Each axis is a `typing.Protocol` in `scripts/ranker/__init__.py`; every concrete module sets a class-level `name` string and is registered in its module's `*_REGISTRY` keyed by `.name`. The bake-off composes a config by naming one module per axis. **Implemented in v1** vs **reserved seams** (Protocol declared, no v1 concrete — deliberate extension points, not active axes):

| Axis | Protocol | Implemented in v1 (`.name`) | Reserved seams |
|---|---|---|---|
| **Estimator** (per-wallet skill) | `Estimator.score` | `eb_shrinkage_skill`, `t_stat_baseline` (the winner's-curse baseline every challenger must beat), `gu_koenker_npmle`, `proxy_clv` | r-value, Brier/CRPS, and the rest of the §menu drop-ins |
| **Signal combiner** | `SignalCombiner.combine` | *(none — single-signal in v1)* | multi-signal blends |
| **Deflation / calibration** | `Deflator.deflate` | `deflated_sharpe` (the deflation axis also accepts the literal `"none"` passthrough sentinel — a bypass recognised by the driver, not a `*_REGISTRY` class) | — |
| **Selector** | `Selector.select` | `top_k`, `online_exp_weights` | corr-aware, weighted-conformal, TTTS-online, full BOA |
| **Set-transition policy** | `SetTransitionPolicy.step` | `policy_full_rerank` (baseline), `policy_knockout_backfill` (the current live behaviour), `policy_hybrid_displacement`, `policy_online_weighting` | — |
| **Integrity filter** | `IntegrityFilter.mask` | *(none)* — overlap is handled continuously by the uniqueness weights (§Honesty), not a threshold | overlap-ratio / wash / coordination filters |
| **Capacity filter** | `CapacityFilter.haircut` | *(none)* | liquidity-at-fill haircut |
| **Demotion** | `Demoter.should_demote` | `empirical_bernstein` (demote iff the empirical-Bernstein upper CB on per-period P&L < 0 **and** cumulative realized P&L < 0) | `wallet_skill_cpd` change-point |
| **Criteria** (frozen dataclass, swept in the grid) | `Criteria` | `active_within_secs`, `ttr_hours`, `price_min`/`price_max` (entry band), `half_life_days` (recency decay), `min_trl` (minimum track-record length) | — |

The estimators all compute per-position net edge `(payoff − _eff) / _eff` and reuse `ranker_decay.weighted_stats` so the AFML uniqueness weights and #366 recency decay apply uniformly. `eb_shrinkage_skill` is **empirical** Bayes: the normal-normal prior mean μ0 and variance τ² are estimated from the cross-section at every scoring step — only the variance floor is a fixed numeric (`ranker_eb_prior_var_floor`), which keeps a degenerate <2-candidate input deterministic. (This is why the prior here is named distinctly from the *fixed* Kelly sizing priors `kelly_p_prior_*` in `19-WINNER-FOLLOW-STRATEGY.md`: those are fixed Beta(α,β) parameters; the EB ranker prior is data-derived.)

The selector axis is **coupled into the policy** in v1 (`policy_online_weighting` *is* the `online_exp_weights` selector carried across steps), so the swept grid is `estimator × deflator × policy × criteria × churn_cost`.

## The honesty layer

The harness's value is entirely in its guards; `scripts/ranker/oos_validation.py` and `scripts/ranker/deflation.py` hold them. Per `oos_validation.py`'s own header, the numerics inside the validators are *statistical-method parameters* (bootstrap reps, RNG seeds, CSCV group counts) or honest sentinels — not strategy thresholds — so they stay as code constants; only the strategy-level levels are glossary'd (§Configuration defaults).

- **AFML uniqueness weights** (`uniqueness_weights`, López de Prado AFML Ch.4) — the true time-averaged uniqueness `ū_i = (1/|span_i|) ∫ (1/c_t) dt`, where `c_t` is the count of *that wallet's* labels live at `t`. A wallet whose positions heavily overlap is down-weighted continuously; this is how "overlap ratio" is handled — a weight, not a cutoff. Weights normalize to mean 1 per wallet, concatenated across wallets.
- **Look-ahead guard** (LANDMINE-2; `split_walkforward` + `assert_no_lookahead`) — at cutoff `as_of`, in-sample = labels entered before `as_of` **and** already `resolved_at ≤ as_of`; forward = labels entered in `[as_of + embargo, as_of + horizon)`. The embargo (`ranker_embargo_secs`, default 0) shifts the forward start forward to break leakage across the cutoff. It is a per-`as_of` row filter over the once-materialized frame, not a per-cutoff rebuild of the join.
- **Grid-level deflation** (LANDMINE-1) — the trial count `N_GRID` is **pre-registered** before any scoring (§The bake-off driver), so the multiple-testing correction is honest. `deflated_sharpe` (Bailey & López de Prado 2014) deflates each config's Sharpe by the expected maximum under `N_GRID` nulls; `PBO` (CSCV probability of backtest overfitting), `romano_wolf` (StepM FWER), and `hansen_spa` (SPA) form the grid-level validator panel.
- **Is following viable at all?** — `brown_goetzmann_cpr` (contingency-table cross-product ratio) tests whether winners persist period-to-period; a no-persistence population is grounds for a NO-GO regardless of any single config's apparent edge.
- **Cross-check vs live** — `paper_fills_crosscheck` compares the harness's leaderboard against realized live P&L from Supabase `paper_fills`, flagging any wallet the harness ranks well but that is bleeding live.
- **Winner's-curse uncertainty on the selected arm** — `akm_inference_on_winners` (Andrews-Kitagawa-McCloskey, median-unbiased conditional estimate + CI for the argmax), `mrsw_rank_cs` (Mogstad-Romano-Shaikh-Wilhelm top-τ rank confidence set; hand-rolled because the cited `csranks` package is not on PyPI), and `fcr_selected_ci` (Benjamini-Yekutieli false-coverage-rate-adjusted CIs for the selected set).

## The bake-off driver

`scripts/ranker/bakeoff.py` composes the modules into a staged sweep:

1. **8a — estimator screen** (`screen_estimators`): score each estimator at each cutoff on a cheap point-in-time forward-copy-P&L proxy (no `pe-backtest`), keep the best `screen_keep`, and **always retain `t_stat_baseline`** so every later comparison has its baseline. This only ever shrinks the grid before `N_GRID` is frozen.
2. **8b — pre-register `N_GRID`** (`pre_register_grid`): after 8a prunes the estimator axis, enumerate `estimator × deflator × policy × criteria × churn_cost` and emit a manifest `{created_at, n_grid, axes, grid_keys}`. `n_grid` is the frozen trial count fed to every deflator/validator (LANDMINE-1). The baseline config key is asserted present.
3. **8c — trajectories** (`run_trajectories` → `run_trajectory`): each grid point runs the full walk-forward trajectory above. Per step: split, eligibility (recency + `ranker_min_trl`), uniqueness weights, estimator score, deflation gate, `policy.step`, then a real `pe-backtest` run via `SubprocessBacktestRunner` (shelling to the Rust injected-set path, §Data pipeline). Churn cost is subtracted per newly-admitted wallet. The per-step deflation gate's `n_trials` is the **candidate count**, not `N_GRID` (the grid-level correction is applied once, at the leaderboard).
4. **Leaderboard + winner/NO-GO** (`grid_deflate` → `select_winner_or_nogo`): a config is declared **WINNER** only if it (a) beats `t_stat_baseline` cumulative return, (b) is Romano-Wolf superior **and** Hansen-SPA significant at `ranker_fdr_q` **and** has `PBO < ranker_pbo_max`, and (c) the `brown_goetzmann_cpr` persistence premise holds. Otherwise it returns an explicit **NO-GO** with the failing-reason list. The winner carries its AKM/MRSW/FCR uncertainty, and `main` runs the `paper_fills` cross-check.

## Data pipeline and backtest seam

The harness needs two pieces of Rust plumbing, both additive and idempotent on the cache, both deploying nothing live:

- **`pe-backtest` injected-set path (PR3).** `injected_wallets_path` makes the backtester follow an explicit wallet list (ranker bypassed) and emit `pnl_by_period.ndjson` — one row per `(wallet, UTC-day)` with `{wallet, period_end, realized_pnl, unrealized_pnl, n_fills, notional}` (money fields serialized as JSON floats for the Python harness; never fed back into P&L math). The load is **bounded** — only the injected wallets' trades — so `max_trade_count = 0` is safe on the production cache without disabling any guard code. `unrealized_pnl` is a documented `0.0` sentinel: true mark-to-market needs PR4's price series, and marking still-open positions to their future resolution would be look-ahead. Injected mode requires `flat_usd` and is incompatible with `kelly_sweep_fractions`.
- **CLV / Gamma pipeline (PR4).** A dedicated CLOB `GET /prices-history` client (`crates/source-polymarket-public/src/clob_prices_history.rs`, throttled by `clob_prices_history_min_interval_ms` on its own fetcher so it does not loosen the Data-API gate), a `market_price_history` table, a `start_date_unix` column backfilled from Gamma `createdAt`, the `pe-bootstrap prices-history` subcommand (pass 1 = `start_date_unix` on decided-outcome markets; pass 2 = the coarse pre-resolution series at `clob_prices_history_fidelity_minutes` over `prices_history_window_secs`), and an optional `market_price_history` parquet/DuckDB view. The proxy-CLV axis runs without the parquet; true-CLV consumes `market_price_history`. The batch `POST` variant and order-flow consumers are deferred to #418.

## Configuration defaults

Per the `AGENTS.md` rule, every strategy-level threshold has a single canonical home in `_GLOSSARY.md`; this doc references the keys and never restates the values.

- **Selection / honesty levels (issue #421 PR6):** `ranker_fdr_q` (family significance / FCR target shared by the StepM, SPA, AKM, MRSW, and winner gates), `ranker_pbo_max` (PBO overfit ceiling), `ranker_dsr_min` (per-step deflated-Sharpe survival gate), `ranker_eb_prior_var_floor` (EB prior variance floor; μ0/τ² are data-estimated), `ranker_min_trl` (minimum track-record-length gate, swept in the criteria grid; 0 = no gate), `ranker_embargo_secs` (walk-forward embargo, swept; 0 = no-op sentinel).
- **Recency / OOS decay anchor:** the criteria axis's `half_life_days` is anchored on the existing `ranker_half_life_days` (#366); `0` = flat weights.
- **Data pipeline (issue #421 PR4):** `clob_prices_history_min_interval_ms`, `clob_prices_history_fidelity_minutes`, `prices_history_window_secs`, `prices_history_token_limit`, and the reused `bootstrap_clob_concurrency`.
- **Method-internal parameters** (bootstrap reps and seeds, CSCV group count, the NPMLE solver grid, the online-selector temperature/EWMA) are *not* strategy thresholds; per `oos_validation.py`'s header they stay as code constants and are intentionally **not** glossary'd.

The criteria-grid *values* themselves (which TTR horizons, entry bands, half-lives, and MinTRL the operator sweeps) are settled empirically by the bake-off, not fixed defaults — the winning config's values feed #417.

## Running the bake-off

The bake-off is an **operator compute step**, not part of any PR's CI (CI exercises the harness with a fake `BacktestRunner`). It runs where DuckDB and the local `data/wallet_cache.db` live — the cache box — because `main` shells out to the real `pe-backtest` over the full cache. Prerequisites: the Python deps in `scripts/requirements.txt` (`scipy`, `arch`, `statsmodels`) plus `duckdb`; the additive DDL (`market_price_history`, `start_date_unix`) applied to the cache; and, for the true-CLV axis, the `prices-history` backfill run. See `26-DATA-REFRESH-AND-REOPTIMIZATION-RUNBOOK.md` for the cache-refresh procedure. The output is the §Deliverable: a grid-deflated leaderboard and a recommended winning config or NO-GO, both feeding #417.

## Scope and reasoned dead-ends

**Out of scope** (tracked elsewhere): position sizing (→ `SizingMode`/#398); maker/taker and order-flow-leads-price, which need on-chain `OrderFilled` and the order-flow consumer of `market_price_history` (→ #418); productionizing the winner (→ #417).

**Reasoned dead-ends — do not re-propose** (full rationale in issue #421): OCBA / indifference-zone (needs resampling — observational data); James-Stein linear shrinkage (dominated by `gu_koenker_npmle`); CORRAL / discounted-UCB / EXP3.S / universal-portfolios (no exploration trade-off — we copy every awake wallet); comomentum / factor-crowding (no co-moving residual panel); spoofing / counterparty-graph / Benford (need depth, counterparties, or deleted data); reverse-line-movement (data we lack); VPIN/PIN/Hawkes as a wallet ranker (market-level = wrong unit); adaptive-conformal as a selection fix (interval coverage, not FDR-over-top-k).
