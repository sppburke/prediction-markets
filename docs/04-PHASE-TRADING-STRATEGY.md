# 04 — Phase Trading Strategy

> **Rust-only implementation rule:** all first-party production services, clients, parsers, models, replay tools, CLIs, and test harnesses are implemented in **Rust 2024 Edition pinned to stable Rust 1.95.0**. Non-Rust components are permitted only as external infrastructure daemons, vendor APIs, operating-system services, managed databases, or public data sources. No production hot-path Python, Node, or browser automation is allowed.

## Objective

Convert fair values into real, venue-aware order intents only when the edge survives costs, queue, latency, source health, resolver uncertainty, and risk limits.

## Pipeline

```text
FairValueSnapshot
  -> source health gate
  -> resolver confidence gate
  -> venue cost model
  -> order book / queue model
  -> latency budget gate
  -> risk engine
  -> OrderIntent
  -> execution router
```



## Strategy 0 — Winner-Follow comes first

Winner-Follow is the first strategy implemented and the first strategy allowed into live-tiny mode. Resolver/source-arbitrage strategies are Strategy 1+.

### Why it comes first

The strategy does not require building a proprietary weather station network, crypto oracle predictor, sports feed parser, or macro-release parser before it can produce signals. It requires public-market research, trader-performance reconstruction, continuous monitoring, copy latency, and risk sizing. That makes it the fastest path to a deployable system while the deeper source-arbitrage stack is built.

It still has real risk. The strategy's edge is not "free money"; it is a measurable empirical claim that selected public traders' future entries contain signal that survives copy delay and costs.

### End-to-end pipeline

```text
Candidate discovery
  -> public Polygon funding/collateral graph
  -> operator identity collapse
  -> public trader ledger reconstruction
  -> walk-forward follower simulation
  -> top-50 active operator/leader ranking
  -> continuous leader watch
  -> trade and signal-kind classification
  -> copy eligibility check
  -> calibrated p and fractional Kelly
  -> risk-gated OrderIntent
  -> execution router
  -> fill/reconcile
  -> live decay monitoring
```

### Trader universe

Polymarket candidate discovery starts from:

- leaderboard by category and offset;
- top users by recent PnL and volume;
- users repeatedly appearing in profitable short-duration markets;
- users whose trades produce favorable post-entry drift;
- users with public profiles and sufficient trade/position data.
- fresh wallets funded by a known operator/funder with a strong, replayable track record;
- same-operator clusters whose member wallets coordinate on the same market/outcome/side within a short window.

Wallets are collapsed into operators only when funding/collateral evidence is public, reproducible, and confidence-scored. Polymarket proxy-wallet, pUSD, deposit, and bridge flows must be verified before using funding identity for sizing.

Kalshi candidate discovery is not equivalent because public trades do not identify traders. Kalshi copy trading is enabled only for public/authorized trader data. Otherwise Kalshi signals remain market-flow and source/resolver signals.

### Eligibility thresholds

Use these initial thresholds, then tune with walk-forward optimization:

| Filter | Default |
|---|---:|
| Rolling audit window | 180 days |
| Minimum closed trades | 60 |
| Minimum resolved markets | 30 |
| Minimum closed trades in last 30 days | 12 |
| Median capital-weighted hold | <= 72 hours |
| 75th percentile hold | <= 7 days |
| Max profit from one market | <= 20% |
| Max uncopiable profit contribution | <= 35% |
| Minimum lower 5% daily log-growth | > 0 after costs |
| Active watchlist size | top 50 leaders |
| Incubator watchlist size | up to 250 candidates |
| Inherited-prior first-trade mode | paper by default |
| Cluster-coordination mode | shadow by default |

These replace the arbitrary `average hold < 5 days` and `>= 15 trades` rule. Shorter holding periods are good only when the edge survives copy latency; more trades are useful only when they are independent and reproducible.

### Trade classification

Each observed leader trade is classified as:

- `Entry`: opens a new position or reopens a flat market.
- `Add`: increases an existing position in the same direction.
- `Trim`: reduces but does not close.
- `Exit`: closes or near-closes.
- `Flip`: changes net direction.
- `Unknown`: insufficient state; never copy as an entry.

Only `Entry` and high-confidence `Add` trades can initiate follower exposure. `Exit` and `Trim` events can reduce follower exposure if the follower has a mirrored position and liquidity is acceptable.

Signal kind is separate from trade action:

```rust
pub enum WinnerFollowSignalKind {
    NormalLeaderFollow,
    FreshWalletFirstTrade,
    ClusterCoordination,
}
```

`FreshWalletFirstTrade` requires a fresh wallet, low prior trade count, known operator/funder track record, low funding hop count, sane cluster size, low seeding velocity, and no anti-gaming flags. `ClusterCoordination` requires at least `K` member wallets of the same operator entering the same market/outcome/side within window `W`; it is emitted once per `(operator, market, outcome, side, window)`.

### Copy eligibility

Do not copy unless all are true:

1. leader is active top-50 at decision time;
2. the event is `Entry` or approved `Add`;
3. current price is within the max slippage budget from leader's observed entry;
4. market liquidity can fill the follower order without exceeding adverse-selection limits;
5. market is not in a blocked category, settlement dispute state, or stale metadata state;
6. leader's family-specific model has positive lower-confidence expected log growth;
7. portfolio risk caps permit new exposure;
8. order can be represented as an idempotent `OrderIntent` and replayed.

Additional gates for inherited-prior and cluster-coordination signals:

1. operator identity confidence is above threshold;
2. `source-onchain-polygon` is healthy and within block-lag limits;
3. proxy-wallet/funder/collateral mapping is proven for the wallet class;
4. inherited prior is shrinkage-adjusted with capped `effective_n`;
5. anti-gaming flags are absent or configured to demote rather than block;
6. mode-specific promotion state permits the order mode.

### Fractional Kelly sizing

For a binary contract with current follower entry price `c` and calibrated follower win probability `p`:

```text
f_full = max(0, (p - c) / (1 - c))
f_live = f_full * kelly_fraction
stake_dollars = bankroll * f_live
contracts = floor(stake_dollars / c)
```

Default `kelly_fraction`:

- 0.10x in dry-run sanity checks;
- 0.25x in live-tiny;
- 0.50x only after live data proves stability;
- never above 0.50x without explicit human approval.

Mode-specific defaults:

```toml
[winner_follow.kelly]
fraction_leader_promoted = 0.25
fraction_leader_live_tiny = 0.10
fraction_inherited_prior = 0.05
fraction_cluster_coordination = 0.15
```

Use net `c` after fees, expected slippage, and adverse-selection buffer. Reject trades where `f_live` is positive only because of stale or uncalibrated `p`.

### Risk caps

| Cap | Default |
|---|---:|
| Live-tiny max per copied trade | 0.25% bankroll |
| Promoted max per copied trade | 1.00% bankroll |
| Max per leader | 3.00% bankroll |
| Max per operator | 3.00% bankroll |
| Max per market | 2.00% bankroll |
| Max per market family | 8.00% bankroll |
| Max all copy exposure | 25.00% bankroll |
| Max inherited-prior exposure | 1.00% bankroll |
| Max cluster-coordination exposure | 2.00% bankroll |
| Max per operator per market | 0.75% bankroll |
| Max inherited-prior trades per funder per day | 3 |
| Intraday new-entry stop | -2.00% bankroll |
| Rolling 7-day new-entry stop | -6.00% bankroll |
| Absolute kill-switch drawdown | -10.00% bankroll |

### Strategy trait implementation

`strategy-winner-follow` implements the same strategy trait as every other strategy but receives `LeaderSignal` and `TraderRankSnapshot` in context. It emits `OrderIntent` only; it never submits orders directly.

```rust
pub struct WinnerFollowStrategy {
    pub strategy_id: StrategyId,
    pub min_rank: TraderRankCutoff,
    pub kelly_fraction: KellyFraction,
    pub max_copy_slippage: PriceDelta,
    pub risk_caps: CopyRiskCaps,
    pub modes: WinnerFollowModes,
}
```

```toml
[winner_follow.modes]
leader_follow = "live_tiny"
inherited_prior_first_trade = "paper"
cluster_coordination = "shadow"
```

### Promotion path

1. Historical reconstruction.
2. Walk-forward backtest.
3. Paper-copy live signals.
4. Live-tiny with 0.10x-0.25x Kelly and small bankroll.
5. Promotion only after the realized follower distribution matches the simulated distribution.

Inherited-prior first-trade and cluster-coordination modes have separate promotion ladders and cannot inherit validation from ordinary leader-follow. They share ingestion and execution infrastructure, not statistical approval.

## Strategy trait

```rust
pub trait Strategy: Send + Sync {
    type Error;
    fn id(&self) -> StrategyId;
    fn evaluate(&self, ctx: StrategyContext<'_>) -> Result<Vec<OrderIntent>, Self::Error>;
}
```

Strategies do not submit orders. They emit typed order intents.

## Order lifecycle typestate

```rust
pub struct DraftOrder;
pub struct RiskChecked;
pub struct Submitted;
pub struct Live;
pub struct Filled;
pub struct Cancelled;
pub struct Rejected;

pub struct Order<S> {
    pub local_id: OrderLocalId,
    pub intent: OrderIntent,
    pub state: S,
}
```

The execution router owns transitions. This prevents invalid state transitions from compiling.

## Strategy classes

### Exact reprice

Trade after official source/page/feed updates but before venue fully reprices. Best for chart publication, official sports status, final weather reports, macro release parsing, and oracle/benchmark prints.

### Predictive upstream

Trade when upstream data predicts resolver state before final publication. Best for crypto microstructure, weather observations, sports live state, and official file watchers.

### Cross-venue mismatch

Trade divergence only after compatibility classification. Do not call it a hedge unless resolver rules prove it.

### Passive microstructure

Quote when fair value is stable and venue economics support it. Use Kalshi queue data and Polymarket maker/reward economics where applicable.

## Cost model

```rust
pub struct ExpectedTradeEconomics {
    pub gross_edge_bps: i32,
    pub taker_fee_bps: i32,
    pub maker_rebate_bps: i32,
    pub expected_slippage_bps: i32,
    pub latency_decay_bps: i32,
    pub resolver_uncertainty_bps: i32,
    pub net_edge_bps: i32,
}
```

No strategy trades on gross edge.

## Risk decision

```rust
pub enum RiskDecision {
    Allow,
    Resize { new_qty: ContractQty, reason: String },
    Block { reason: String },
    KillSwitch { reason: String },
}
```

Risk checks are pure, deterministic Rust functions. Inputs are explicit snapshots: positions, orders, balances, source health, venue health, limits, and resolver confidence.

## Execution safety

- idempotency key per submission;
- local journal before network call;
- stale intent expiration;
- reconnect/reconcile before new orders;
- cancel-on-disconnect policy;
- venue maintenance awareness;
- account/position reconciliation;
- per-venue rate-limit handling.


## Common acceptance gate

This file is complete only when the implementation:
1. compiles as Rust 2024;
2. uses typed IDs, prices, probabilities, quantities, timestamps, and resolver states;
3. writes replayable events with raw payload hashes;
4. has fixture tests and deterministic replay;
5. blocks live execution when source, resolver, venue, or risk state is invalid.
