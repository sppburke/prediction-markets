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
| 4 | Slippage ceiling | `fill_price = leader_price × (1 + slippage_rate); if fill_price >= 1.0 { continue }` | `simulation.rs:497–504` | A Buy at fill ≥ $1.00 has no remaining upside even if YES. Proportional slippage (`backtest_slippage_bps = 100`) is also added to `c` in Kelly sizing for consistency. |
| 5 | No shrunk-p | `leader_win_rate_p_shrunk(...) == None` | `simulation.rs:506–513` | The Bayesian-shrunk win-rate `p` is `None` when the leader has zero trades — no prior evidence, no size. Copying without a `p` estimate would require Kelly to assert a probability with no data. |
| 6 | No quality score | `quality_by_wallet.get(&leader) == None` | `simulation.rs:516–518` | Every watchlisted wallet must have a quality score from the ranker. A missing score means the watchlist and ledger data are inconsistent — a data integrity guard, not a real eligibility criterion. |

Gates 1–6 are evaluated sequentially inside the trade loop (not as part of any `Err` variant). After them, the signal proceeds to `strategy.evaluate()`, which applies the five strategy-level gates documented in `docs/19-WINNER-FOLLOW-STRATEGY.md`.

### Flat-stake mode (`PE_BACKTEST_FLAT_USD`)

A research lever (issue #134) that short-circuits the entire sizing pipeline. Setting `BacktestConfig::flat_usd = Some(stake)` (env: `PE_BACKTEST_FLAT_USD=<decimal>`) opens each copy at `floor(stake / fill_price).max(1)` contracts and skips Kelly, the per-trade cap, mode clamping, `risk-engine`, and the liquidity clamp. The all-or-nothing bankroll guard (`bankroll < notional → continue`) is the only remaining filter on the size path.

| Aspect | Behaviour with `flat_usd = Some(stake)` |
|---|---|
| Sizing | `contracts = floor(stake / fill_price).max(1)`; degenerate `fill_price > stake` opens 1 contract (best-effort, notional > stake) |
| Skipped pipeline | Kelly, per-trade cap, mode clamp, `risk-engine`, liquidity clamp, snapshot-aware prior |
| Bankroll floor | `bankroll < notional → continue` (all-or-nothing) |
| Preserved | Gates 1–6 (watchlist, duplicate-open, expiry, slippage ceiling, p, quality); sell path |
| `total_signals_evaluated`, `snapshot_prior_*`, `liquidity_*` counters | Stay at 0 (those code paths never execute) |
| `kelly_sweep_fractions` interaction | Sweep is suppressed in `main.rs`; one `tracing::warn!` is emitted and a single run executes |

`BacktestConfig` is not imported by the live service crate — the flag cannot leak into production by construction. Operator-level scenario coverage lives in `crates/backtest/tests/scenario_flat_usd.rs`.

### Per-market position cap (`PE_BACKTEST_MAX_POSITIONS_PER_MARKET`)

A structural gate (issue #138) that caps concurrent open positions on any `market_id`. `BacktestConfig::max_positions_per_market = Some(n)` blocks new BUY signals once `n` positions are open on that market across every leader and outcome; the slot reopens when positions close via SELL or resolution sweep. The cap is keyed strictly on `market_id` — leader A on outcome 0 and leader B on outcome 1 of the same binary market count against the same slot, preventing simultaneous exposure to both sides of one contract. Default `Some(1)`; `None` disables. `NonZeroU32` rejects `0` at deserialize time so a typo cannot silently block every BUY.

Operator-level scenario coverage lives in `crates/backtest/tests/scenario_market_position_cap.rs`.

### Scenario-test contributor note

`BacktestConfig` has two fields whose production-correct defaults are restrictive: `require_known_expiry: true` (target post #137 Sub-PR 3) and `max_positions_per_market: Some(1)`. **Scenario tests under `crates/backtest/tests/scenario_*.rs` opt out by setting `require_known_expiry: false` and `max_positions_per_market: None`** unless the test is specifically exercising one of those gates. The defaults are intentional for production correctness; scenario tests opt out, never opt in. Forgetting either field in a new scenario will produce confusing fewer-than-expected BUY fills (cap) or fewer-than-expected through-fills (require_known_expiry).

## Ranker-level presets

These presets operate at the **trader-index layer** during watchlist construction, before any simulation-level filter runs. They tune the set of candidate leaders the simulation sees, not the per-signal copy decisions. Use them when the simulation-level gates are insufficient to suppress copies from statistically underpowered leaders.

### Tightened-ranker preset (`ACTIVE_MIN_CLOSED=30`, `INCUBATOR_MIN_CLOSED=10`)

A runbook preset (issue #143) that tightens two ranker thresholds to filter thin-history leaders — wallets with too few closed trades to be statistically credible. The preset is **opt-in** and **does not change the canonical backtest defaults** (`_GLOSSARY.md` `backtest_active_min_closed_trades=10`, `backtest_incubator_min_closed_trades=3`).

**Invocation:**

```
PE_BACKTEST_ACTIVE_MIN_CLOSED=30 PE_BACKTEST_INCUBATOR_MIN_CLOSED=10 pe-backtest <toml>
```

**Hypothesis:** thin-history leaders are statistically underpowered and are expected to contribute disproportionately to negative PnL in the unknown-operator bucket. The preset's lift is **not yet empirically confirmed**; a post-merge sweep against the current cache is the confirmation step. Run the sweep after the cap-the-bleed simulation-level gates land (issues #141 `skip_unknown_operator`, #142 `max_signal_price`) so the baseline reflects the final filtered behaviour. If the sweep refutes the hypothesis, update this section to flag the preset as "don't use" — do not remove it, the negative result is itself valuable.

**Values are starting points subject to empirical tuning.** 3× the current backtest defaults (`ACTIVE_MIN_CLOSED`: 10 → 30; `INCUBATOR_MIN_CLOSED`: 3 → 10) is a reasonable opening bid: stricter than the current live default of 15 (`_GLOSSARY.md` `active_min_closed_trades`) and well short of the historical pre-N_eff live default of 60. If the sweep shows the optimum lies elsewhere, update the values in this section directly rather than opening a new issue.

**Deliberate exclusions:**

| Env var | Why excluded |
|---|---|
| `PE_BACKTEST_MIN_QUALITY` | `reconstruction_quality` is closed contracts as a fraction of total contracts (`crates/trader-index/src/reconstruction.rs:290`). The Polymarket CLOB API does not return market-resolution redemption events, so buy-and-hold-to-resolution traders score quality=0 regardless of actual performance. Tightening `MIN_QUALITY` would filter conviction operators for structural data reasons unrelated to history depth — the wrong lever. |
| `PE_BACKTEST_ACTIVE_MIN_MARKETS` / `PE_BACKTEST_INCUBATOR_MIN_MARKETS` | The min-markets filter is a hard gate in the ranker (`crates/trader-index/src/ranker.rs:60,87`). N_eff shrinkage in Kelly sizing (`docs/19-WINNER-FOLLOW-STRATEGY.md` § p estimation, `kelly_p_k_per_market`) prices specialists with narrow market breadth correctly via shrinkage. The current design deliberately lowered the hard filter to 1 *because* N_eff downstream does that work. Tightening here would re-introduce the hard cliff that the N_eff design explicitly chose to avoid — it excludes specialists rather than down-weights them. |

**Expected effect** (to be confirmed by post-merge sweep):

- Drops thin-history leaders from the watchlist before they enter the simulation.
- Reduces `total_copies`.
- Expected improvement in per-copy PnL.

The ranker thresholds operate at the trader-index layer (watchlist construction), structurally distinct from the BUY-arm per-signal suppression in `simulation.rs`. The two layers compose: tightening the ranker shrinks the candidate pool; the simulation-level gates suppress per-signal copies within that pool.

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
