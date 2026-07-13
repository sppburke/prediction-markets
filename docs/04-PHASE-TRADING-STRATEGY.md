# 04 — Phase Trading Strategy

> See [`_BASELINE.md`](_BASELINE.md) for the Rust-only implementation rule and common acceptance gate.
> See [`_GLOSSARY.md`](_GLOSSARY.md) for type aliases, latency budget, and configuration defaults.
> See [`19-WINNER-FOLLOW-STRATEGY.md`](19-WINNER-FOLLOW-STRATEGY.md) for canonical risk caps, Kelly fractions, eligibility thresholds, mode definitions, and promotion ladders. **All Winner-Follow numeric values in this file are pointers to that file.**

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

Winner-Follow is the first strategy implemented and the first allowed into live-tiny mode. Resolver/source-arbitrage strategies are Strategy 1+.

### Why it comes first

The strategy does not require building a proprietary weather station network, crypto oracle predictor, sports feed parser, or macro-release parser before producing signals. It requires public-market research, trader-performance reconstruction, continuous monitoring, copy latency, and risk sizing. That makes it the fastest path to a deployable system while the deeper source-arbitrage stack is built.

It still has real risk: the strategy's edge is not "free money"; it is a measurable empirical claim that selected public traders' future entries contain signal that survives copy delay and costs (see edge-claim inequality in `19-WINNER-FOLLOW-STRATEGY.md`).

### End-to-end pipeline

```text
Candidate discovery
  -> public trader ledger reconstruction
  -> walk-forward follower simulation
  -> top-N active leader ranking                   (N = active_watchlist_size, _GLOSSARY.md)
  -> continuous leader watch
  -> trade classification
  -> copy eligibility check
  -> calibrated p and fractional Kelly             (see 19-)
  -> risk-gated OrderIntent                        (see 19-)
  -> execution router
  -> fill/reconcile
  -> live decay monitoring
```

### Trader universe

Polymarket candidate discovery starts from sources listed in `19-WINNER-FOLLOW-STRATEGY.md` ("Candidate discovery"). Each wallet is evaluated independently (the wallet→operator collapse was removed in #326).

Kalshi candidate discovery is not equivalent because public trades do not identify traders. Kalshi copy trading is enabled only for public/authorized trader data; otherwise Kalshi signals remain market-flow and source/resolver signals.

### Eligibility thresholds

See the canonical eligibility table in `19-WINNER-FOLLOW-STRATEGY.md` ("Eligibility thresholds"). Watchlist sizes are `active_watchlist_size = 50` and `incubator_watchlist_size = 250` (`_GLOSSARY.md`).

### Trade classification

Each observed leader trade is classified as `Entry`, `Add`, `Trim`, `Exit`, `Flip`, or `Unknown` (`LeaderAction` in `core-types`). The current production copy scope admits only first-ever BUY `Entry` signals and holds them to resolution; the other actions remain classified for ledger/replay and future profiles. Action eligibility and the confidence thresholds (`add_high_confidence_threshold_ppm`, `exit_high_confidence_threshold_ppm`) are defined canonically in `19-WINNER-FOLLOW-STRATEGY.md` ("Signal classification").

### Copy eligibility

Do not copy unless ALL are true:

1. leader is in active top-`active_watchlist_size` at decision time;
2. the event is a first-ever BUY `Entry`; SELLs and all later actions are blocked by the production copy-entry gate;
3. current price is within `max_slippage_from_leader_bps` of leader's observed entry;
4. market liquidity can fill the follower order without exceeding adverse-selection limits;
5. market is not in a blocked category, settlement dispute, or stale-metadata state;
6. leader's family-specific model has positive lower-confidence expected log growth;
7. portfolio risk caps permit new exposure (caps in `19-`);
8. order can be represented as an idempotent `OrderIntent` and replayed (key in `_GLOSSARY.md` "Idempotency").

### Fractional Kelly sizing

Formula and Kelly fractions per mode are in `19-WINNER-FOLLOW-STRATEGY.md` ("Kelly sizing"). Use net `c` (after fees, expected slippage, adverse-selection buffer). Reject trades where `f_live > 0` only because `p` is stale or uncalibrated.

### Risk caps

The canonical cap TOML lives in `19-WINNER-FOLLOW-STRATEGY.md` ("Canonical risk caps"). This file does not restate the values.

### Strategy trait implementation

`strategy-winner-follow` implements the same strategy trait as every other strategy but receives `LeaderSignal` and `TraderRankSnapshot` in context. It emits `OrderIntent` only.

```rust
pub struct WinnerFollowStrategy {
    pub strategy_id: StrategyId,
    pub min_rank: TraderRankCutoff,
    pub kelly_fraction: KellyFraction,
    pub max_copy_slippage: PriceDelta,
    pub risk_caps: CopyRiskCaps,                  // loaded from `19-` canonical TOML
    pub modes: WinnerFollowModes,
}
```

### Promotion path

The full per-mode ladder, with quantified gates (KS p-value, mean-PnL z-score, observation length, fill-rate match, latency match, demotion-resets-clock rule), is in `19-WINNER-FOLLOW-STRATEGY.md` ("Promotion ladder", "Promotion and demotion criteria") and `_GLOSSARY.md` ("Promotion criteria — quantified", "Demotion criteria"). All "matches simulation" / "close to" / "stable" qualifiers in this file resolve through those tables.

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

The execution router owns transitions. Invalid state transitions do not compile.

## Strategy classes

### Exact reprice

Trade after official source/page/feed updates but before venue fully reprices. Best for chart publication, official sports status, final weather reports, macro release parsing, and oracle/benchmark prints.

### Predictive upstream

Trade when upstream data predicts resolver state before final publication. Best for crypto microstructure, weather observations, sports live state, and official file watchers.

### Cross-venue mismatch

Trade divergence only after compatibility classification. Do not call it a hedge unless the resolver rules prove it (see `09-CROSS-VENUE-MISMATCHES-AND-HEDGES.md`).

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
    Block { reason: WinnerFollowRiskBlock },                  // taxonomy in 19-
    KillSwitch { reason: WinnerFollowRiskBlock },
}
```

Risk checks are pure, deterministic Rust functions. Inputs are explicit snapshots: positions, orders, balances, source health, venue health, limits, resolver confidence, and the approval flags `flip_human_approved` / `kelly_fraction_above_default_human_approved` (`_GLOSSARY.md`).

## Execution safety

- idempotency key per submission (key in `_GLOSSARY.md`);
- local journal before network call;
- stale intent expiration after `order_validity_seconds` (`19-`);
- reconnect/reconcile before new orders;
- cancel-on-disconnect policy;
- venue maintenance awareness;
- account/position reconciliation;
- per-venue rate-limit handling using `_GLOSSARY.md` budgets.

Market vs limit: the engine submits limit orders by default. Market orders are only used when `prefer_market_order = true` AND the market passes the "very liquid" gate in `_GLOSSARY.md`.
