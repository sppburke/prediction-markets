# 03 — Phase Model Engine

> See [`_BASELINE.md`](_BASELINE.md) for the Rust-only implementation rule and common acceptance gate.
> See [`_GLOSSARY.md`](_GLOSSARY.md) for type aliases and configuration defaults.

## Objective

Turn source and venue state into fair values that match exact settlement. The model engine is a Rust runtime for resolver-card evaluation, nowcasting, finite-state machines, benchmark accumulators, calibration, and uncertainty.

## Model input

A model never receives a title alone. It receives:

- `ResolverCard` (sub-types defined in `_GLOSSARY.md`);
- normalized source snapshots;
- venue book snapshots;
- source health state;
- latency metadata;
- strategy/risk context.

## Model trait

```rust
pub trait FairValueModel: Send + Sync {
    type Input;
    type Output;
    type Error;

    fn model_id(&self) -> ModelId;
    fn version(&self) -> semver::Version;
    fn evaluate(&self, input: &Self::Input) -> Result<Self::Output, Self::Error>;
}
```

## Output

```rust
pub struct FairValueSnapshot {
    pub resolver_card_id: ResolverCardId,
    pub model_id: ModelId,
    pub model_version: semver::Version,
    pub probability_ppm: ProbabilityPpm,
    pub confidence_ppm: ProbabilityPpm,
    pub source_staleness_ms: u64,
    pub expected_edge_bps_before_costs: i32,
    pub explanation: ModelExplanation,
    pub generated_at: OffsetDateTime,
}
```

## Model families

### Finalizer models

Used for final NWS reports, Wunderground station history, official sports final pages, official chart pages, macro release tables, and official event feeds. Implemented as state machines and typed parsers.

### Benchmark-window models

Used for Kalshi-style crypto averages or other windowed outcomes. Implement explicit `WindowAccumulator<T>` with `SamplePolicy`, missing-sample policy, finality, and exact arithmetic. The `WindowSpec` and `SamplePolicy` types are in `_GLOSSARY.md`.

### Nowcaster models

Used when upstream sources lead the resolver: exchange microstructure, station observations, sports play-by-play, page publication watchers. Start with interpretable Rust rules and calibration. Add ML only after replay proves it helps.

### Cross-venue compatibility models

Classify whether a Kalshi and Polymarket pair are same resolver, same underlying/different resolver, correlated, title-only, or incompatible. See `09-CROSS-VENUE-MISMATCHES-AND-HEDGES.md` for the `CompatibilityClass` enum.

### Document/text models

For company mentions, earnings calls, filings, or rule extraction: use deterministic parsers first. Use `candle`, `burn`, or `ort` only with constrained outputs and deterministic validators. LLM/classifier output alone cannot trigger high-risk live orders.

## Rust analytics

- `polars` for feature generation and local research.
- `datafusion` with Arrow/Parquet for historical replay queries.
- `linfa` for classical ML/calibration.
- `burn`, `candle`, or `ort` for production inference in Rust.

## Calibration

Each model family reports Brier score, log loss, expected calibration error, settlement accuracy, false-positive rate, edge capture by latency bucket, and net return after execution costs. Do not pool unrelated verticals into one calibration result.

## Artifact metadata

```rust
pub struct ModelArtifactMetadata {
    pub model_id: ModelId,
    pub version: semver::Version,
    pub git_sha: String,
    pub training_data_hash: blake3::Hash,
    pub feature_schema_hash: blake3::Hash,
    pub created_at: OffsetDateTime,
}
```

## Winner-Follow model engine

Winner-Follow models **leader skill, copy survivability, fill cost, and expected log growth**. Risk caps and Kelly fractions used by the engine come from `19-WINNER-FOLLOW-STRATEGY.md`.

### Trader skill model

For each candidate trader, estimate conditional copied-trade win probability and payoff quality using resolved outcomes, entry price bucket, market family, side/outcome, holding-period bucket, trade size percentile, recency, action type, post-trade drift after realistic follower delays, and market liquidity at copied entry time.

Use Bayesian shrinkage so a trader with 15 lucky trades cannot outrank a trader with hundreds of robust trades. Default: a hierarchical beta-binomial layer for win probability plus a payoff/edge model for realized log return. Output is a distribution, not a point estimate.

### Copy survivability model

A leader can be profitable while uncopiable. For every leader and market family:

```text
survivability(delay) = P(follower_fill_within_budget | delay)
                     * E(edge_after_delay | filled, delay)
```

Measured at delay buckets `{250 ms, 1 s, 3 s, 10 s, 30 s, 120 s}`. A trader is demoted if edge disappears before the follower can execute within the production latency budget (`_GLOSSARY.md`).

### Ranking model

The rank score is the lower-confidence estimate of follower compounding, not raw PnL:

```text
score = LCB_5pct(expected_log_growth_per_day)
      + 0.20 * recency_quality
      + 0.15 * latency_survivability
      + 0.10 * capacity_score
      + 0.10 * exit_clarity
      - penalties
```

The numeric weights above are **illustrative starting values**. They are tuned by walk-forward optimization, persisted with the model artifact, and not enforced by code as constants. Replay reproduces the version of the weights active at decision time. The LCB term is in log-growth/day units; bonus and penalty terms are pre-scaled to comparable units before summation.

Penalties include profit concentration, low sample size, illiquidity, uncopyable entries, excessive drawdown, market-family crowding, and strategy drift.

### Probability for Kelly sizing

`p` is the calibrated probability that **the copied follower trade**, entered at the current follower price and latency, resolves profitably. It is never a naive leaderboard win rate.
