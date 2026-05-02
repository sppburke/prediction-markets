# 03 — Phase Model Engine

> **Rust-only implementation rule:** all first-party production services, clients, parsers, models, replay tools, CLIs, and test harnesses are implemented in **Rust 2024 Edition pinned to stable Rust 1.95.0**. Non-Rust components are permitted only as external infrastructure daemons, vendor APIs, operating-system services, managed databases, or public data sources. No production hot-path Python, Node, or browser automation is allowed.

## Objective

Turn source and venue state into fair values that match exact settlement. The model engine is a Rust runtime for resolver-card evaluation, nowcasting, finite-state machines, benchmark accumulators, calibration, and uncertainty.

## Model input

A model never receives a title alone. It receives:

- `ResolverCard`
- normalized source snapshots
- venue book snapshots
- source health state
- latency metadata
- strategy/risk context

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

Used for Kalshi-style crypto averages or other windowed outcomes. Implement explicit `WindowAccumulator<T>` with sample policy, missing-sample policy, finality, and exact arithmetic.

### Nowcaster models

Used when upstream sources lead the resolver: exchange microstructure, station observations, sports play-by-play, page publication watchers. Start with interpretable Rust rules and calibration. Add ML only after replay proves it helps.

### Cross-venue compatibility models

Classify whether a Kalshi and Polymarket pair are same resolver, same underlying/different resolver, correlated, title-only, or incompatible.

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


## Common acceptance gate

This file is complete only when the implementation:
1. compiles as Rust 2024;
2. uses typed IDs, prices, probabilities, quantities, timestamps, and resolver states;
3. writes replayable events with raw payload hashes;
4. has fixture tests and deterministic replay;
5. blocks live execution when source, resolver, venue, or risk state is invalid.


## Winner-Follow model engine

Winner-Follow models **leader skill, copy survivability, fill cost, and expected log growth**.

### Trader skill model

For each candidate trader, estimate conditional copied-trade win probability and payoff quality using resolved outcomes, entry price bucket, market family, side/outcome, holding-period bucket, trade size percentile, recency, action type, post-trade drift after realistic follower delays, and market liquidity at copied entry time.

Use Bayesian shrinkage so a trader with 15 lucky trades cannot outrank a trader with hundreds of robust trades. The default is a hierarchical beta-binomial layer for win probability plus a payoff/edge model for realized log return. The output is a distribution, not a point estimate.

### Operator identity model

Wallets are not assumed independent. `operator-graph` produces deterministic identity snapshots that the model can use as features:

- `operator_id` and identity confidence;
- funder root, funding hop count, wallet age, cluster size, and cluster rule version;
- operator-level track record by market family and odds bucket;
- inherited-prior mean, standard error, effective sample size, and shrinkage source;
- seeding velocity and cluster-membership instability;
- same-cluster co-movement on the same market/outcome/side within a configured window.

The model must keep wallet-level and operator-level evidence separate. An inherited funder prior is a prior over a fresh wallet's copied trade, not proof that the fresh wallet has the operator's posterior skill.

### Copy survivability model

A leader can be profitable while uncopiable. For every leader and market family, estimate:

```text
survivability = P(follower_fill_within_budget) * E(edge_after_delay | filled)
```

Measure this at delay buckets such as 250ms, 1s, 3s, 10s, 30s, and 120s. A trader is demoted if edge disappears before the follower can execute.

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

Penalties include profit concentration, low sample size, illiquidity, uncopyable entries, excessive drawdown, market-family crowding, and strategy drift.

When operator identity is confident, ranking starts from the operator-level distribution and keeps wallet-level results as sub-aggregations. Add penalties for uncertain membership, abnormal seeding velocity, narrow market-family specialization, and unstable funding/collateral paths.

### Probability for Kelly sizing

`p` for Kelly sizing is the calibrated probability that **the copied follower trade**, entered at the current follower price and latency, resolves profitably. It is never a naive leaderboard win rate.

For `FreshWalletFirstTrade`, `p` is built from a heavily shrunk inherited prior toward the category baseline and must carry an explicit `effective_n` cap. For `ClusterCoordination`, `p` includes the coordination feature only after walk-forward evidence shows that same-cluster co-movement survives latency, fees, and adverse selection.
