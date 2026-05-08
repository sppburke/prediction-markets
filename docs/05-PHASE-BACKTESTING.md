# 05 — Phase Backtesting

> See [`_BASELINE.md`](_BASELINE.md) for the Rust-only implementation rule and common acceptance gate.
> See [`_GLOSSARY.md`](_GLOSSARY.md) for the "close to simulation" definition (KS p-value, mean-PnL z-score) and configuration defaults, including `kelly_p_prior_alpha_default`, `kelly_p_prior_beta_default`, and `kelly_p_k_per_market_default` (Bayesian shrinkage on leader win-rate `p` with N_eff scaling; set via `PE_BACKTEST_KELLY_P_PRIOR_ALPHA` / `PE_BACKTEST_KELLY_P_PRIOR_BETA` / `PE_BACKTEST_KELLY_P_K_PER_MARKET`).

## Objective

Replay the world exactly as the Rust production system would have seen it. The backtest must answer: could this code, with this config, from these events, have made this decision and settled correctly after costs?

## Golden rule

Backtest against the same source family used for settlement. Do not use generic weather history when the market resolves to a station report. Do not use spot exchange price when the market resolves to Chainlink or a benchmark average. Do not use news recaps when official releases exist.

## Replay engine

```rust
pub struct ReplayConfig {
    pub start: OffsetDateTime,
    pub end: OffsetDateTime,
    pub speed: ReplaySpeed,
    pub seed: u64,
    pub code_version: String,
    pub config_hash: blake3::Hash,
}

pub enum ReplaySpeed {
    AsFastAsPossible,
    WallClock,
    Scaled(f64),
    Step,
}
```

Replay rebuilds source state, venue books, resolver state, model output, strategy decisions, risk decisions, order lifecycle, fills, and settlement.

## Backtest modes

1. **Source-only replay:** parser, latency, schema drift, source availability.
2. **Model replay:** fair value calibration and signal existence.
3. **Venue replay:** book reconstruction, queue, fees, stale book, fills.
4. **Full replay:** source + model + strategy + risk + fill + settlement.

## Fill models

Implement conservative fill models:

- immediate top-of-book;
- queue-position model;
- book-delta model;
- partial fill model;
- adverse-selection model;
- cancel-race model;
- stale-book failure model.

Reports must label which fill model was used.

## Property and concurrency testing

Use `proptest` for timing, rounding, tie rules, benchmark averages, weather windows, compatibility classes, price conversion, and risk limits.

Use `loom` for hot shared state: order book deltas, order journal transitions, cancel/ack races, and hot cache swaps. Use `shuttle` or deterministic simulation for larger actor systems.

## Reports

Base report:

```rust
pub struct BacktestReport {
    pub run_id: uuid::Uuid,
    pub strategy_id: StrategyId,
    pub code_version: String,
    pub config_hash: blake3::Hash,
    pub event_log_hash: blake3::Hash,
    pub gross_pnl: Decimal,
    pub net_pnl: Decimal,
    pub max_drawdown: Decimal,
    pub brier_score: Decimal,
    pub fill_rate_ppm: ProbabilityPpm,
    pub false_edge_count: u64,
}
```

Winner-Follow extension (emitted alongside the base report by `strategy-winner-follow` runs):

```rust
pub struct WinnerFollowReport {
    pub base: BacktestReport,
    pub mode: WinnerFollowMode,                      // leader_follow | inherited_prior_first_trade | cluster_coordination

    // Compounding and exposure
    pub expected_log_growth_per_day: Decimal,
    pub realized_log_growth_per_day: Decimal,
    pub lcb_5pct_log_growth_per_day: Decimal,
    pub turnover_bankroll_per_day: Decimal,

    // Latency and fill
    pub copy_delay_p50_ms: u32,
    pub copy_delay_p95_ms: u32,
    pub copy_delay_p99_ms: u32,
    pub edge_decay_by_delay_bps: BTreeMap<DelayBucket, i32>,
    pub fill_rate_simulated_vs_realized: (ProbabilityPpm, ProbabilityPpm),

    // Hold and concentration
    pub hold_p50_seconds: u32,
    pub hold_p75_seconds: u32,
    pub max_single_market_pnl_pct: Decimal,
    pub uncopiable_pnl_pct: Decimal,

    // Watchlist dynamics
    pub leader_churn_rate_per_day: Decimal,
    pub demotion_count_by_cause: BTreeMap<DemotionCause, u32>,

    // Operator-aware
    pub exposure_by_operator_bps: BTreeMap<OperatorId, i32>,
    pub wallet_to_operator_confidence_p50: ProbabilityPpm,
    pub inherited_prior_effective_n_p50: u32,
    pub fresh_wallet_outcomes: ModeOutcomes,
    pub cluster_coordination_outcomes: ModeOutcomes,
    pub anti_gaming_flag_counts: BTreeMap<AntiGamingFlag, u32>,
    pub onchain_source_lag_p95_blocks: u32,

    // Promotion-relevant
    pub paper_vs_backtest_ks_pvalue: Decimal,
    pub paper_vs_backtest_mean_z: Decimal,
}
```

`DelayBucket` matches the survivability buckets in `19-WINNER-FOLLOW-STRATEGY.md`.

## Simulation-level trade filters

These six gates live in `crates/backtest/src/simulation.rs` and fire in the backtest harness only. They do **not** exist in production and do not contribute to `false_edge_count` in `BacktestReport`. Each entry skips the trade via `continue` and moves on to the next signal.

| # | Gate | Condition | Source | Rationale |
|---|---|---|---|---|
| 1 | Not on watchlist | `!watchlisted.contains(&trade.wallet)` | `simulation.rs:457` | Restricts copies to active or incubator wallets at that historical timestamp. Without this, any trader in the raw dataset would be copied, including wallets that were never ranked — survivorship and look-forward bias. |
| 2 | Duplicate open position | `open_positions.contains_key(&wallet_pos_key)` (Buy only) | `simulation.rs:476` | Each leader's position in a `(wallet, market, outcome)` triple is tracked once. Copying a second buy while one is already open would double-enter the same thesis. Prevents position doubling that real risk caps would block in production. |
| 3 | Expiry filter | `resolved_at_unix − sim_date_unix > max_hours` (only when `max_hours_to_expiry` is set) | `simulation.rs:485–495` | Skips trades in markets resolving too far out. Markets with unknown resolution (`None`) are **always allowed** — at simulation time the future resolution date was unknown, so excluding them would be survivorship bias. |
| 4 | Slippage ceiling | `fill_price = leader_price + slippage; if fill_price >= 1.0 { continue }` | `simulation.rs:497–504` | A Buy at `price + slippage ≥ 1.00¢` has no remaining upside even if the outcome resolves YES. Simulating a fill at a price ≥ $1.00 is nonsensical for a binary outcome. |
| 5 | No shrunk-p | `leader_win_rate_p_shrunk(...) == None` | `simulation.rs:506–513` | The Bayesian-shrunk win-rate `p` is `None` when the leader has zero trades — no prior evidence, no size. Copying without a `p` estimate would require Kelly to assert a probability with no data. |
| 6 | No quality score | `quality_by_wallet.get(&leader) == None` | `simulation.rs:516–518` | Every watchlisted wallet must have a quality score from the ranker. A missing score means the watchlist and ledger data are inconsistent — a data integrity guard, not a real eligibility criterion. |

Gates 1–6 are evaluated sequentially inside the trade loop (not as part of any `Err` variant). After them, the signal proceeds to `strategy.evaluate()`, which applies the five strategy-level gates documented in `docs/19-WINNER-FOLLOW-STRATEGY.md`.

## Winner-Follow backtesting and validation

Winner-Follow backtesting must be **walk-forward** and **follower-realistic**. A historical leader trade is not copied at the leader's price unless the follower could have filled there after discovery delay, API delay, decision delay, order routing, queue position, and slippage.

### Required replay modes

1. **Leader reconstruction replay:** rebuild each candidate's historical positions from public trades/activity/positions.
2. **Ranking replay:** at each historical time `t`, rank candidates using only data available before `t`.
3. **Follower replay:** copy eligible trades after simulated latency and with book-aware fill assumptions.
4. **Portfolio replay:** apply Kelly sizing, caps, correlated exposure limits, exits, and drawdown stops (caps in `19-`).
5. **Live-vs-backtest drift replay:** compare paper/live outcomes against simulated expectations using the "close to simulation" definition in `_GLOSSARY.md`.
6. **Operator graph replay:** rebuild funding/collateral graph state and operator identities exactly as known at historical time `t`.

### Bias controls

- No future leaderboard membership.
- No future realized PnL in rank features.
- No using final market outcome before the historical timestamp.
- No assuming fills inside the spread unless book depth supports it.
- No ignoring missed exits.
- No ignoring market delistings, disputes, or stale prices.
- No treating Kalshi public market trades as trader-attributed signals unless identity is public/authorized.
- No using future funding edges, future cluster members, future labels, or future operator PnL to identify a funder as skilled at historical time `t`.
- No treating CrowdIntel or other opaque third-party cluster scores as replayable production truth unless the exact input/export is logged and licensed.

### Acceptance gate

Winner-Follow can enter live-tiny only if the gates in `19-WINNER-FOLLOW-STRATEGY.md` ("Promotion ladder") and `_GLOSSARY.md` ("Promotion criteria — quantified") all pass for ordinary leader-follow. `inherited_prior_first_trade` and `cluster_coordination` modes have separate walk-forward acceptance reports and are not promoted because ordinary leader-follow passed.
