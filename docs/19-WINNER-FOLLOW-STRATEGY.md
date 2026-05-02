# 19 — Winner-Follow Strategy

> **Rust-only implementation rule:** all first-party production services, clients, parsers, models, replay tools, CLIs, and test harnesses are implemented in **Rust 2024 Edition pinned to stable Rust 1.95.0**. Non-Rust components are permitted only as external infrastructure daemons, vendor APIs, operating-system services, managed databases, or public data sources.

## Objective

Make Winner-Follow the first deployable strategy. The system continuously identifies the fastest-compounding public traders, selects the subset whose future trades are likely to remain profitable after copy delay and costs, and mirrors qualifying entries using risk-capped fractional Kelly.

The goal is not to copy famous accounts. The goal is to maximize **follower bankroll log-growth per day** while minimizing ruin risk, overfitting, copy slippage, hidden liquidity risk, and false skill.

## Important correction

The strategy is lower infrastructure complexity than weather/crypto/source arbitrage, but it does not have "no edge requirement." Its edge is empirical and must be proven:

```text
leader skill + public detectability + speed + liquidity + risk sizing > fees + slippage + adverse selection + decay
```

If that inequality fails, the strategy is disabled.

## Venue support

### Polymarket — primary

Polymarket is the primary Winner-Follow venue because public data supports user-level research:

- leaderboard snapshots;
- user-filtered trades;
- current positions;
- closed positions;
- user activity;
- market/orderbook websocket state;
- transaction hashes for timing validation.

### Kalshi — conditional

Kalshi public trades are useful for market-flow analysis, but public trade events do not identify the trader. Kalshi Winner-Follow requires one of:

1. official public trader-level data sufficient for attribution;
2. a trader who explicitly consents and provides API/portfolio access;
3. a future endpoint that lawfully exposes public user-level trade history.

Until then, Kalshi copy-following is disabled by default and Kalshi remains a source/resolver and market-flow venue.

## System pipeline

```text
Discover candidates
  -> hydrate public profiles/trades/positions
  -> reconstruct trader ledgers
  -> label entries/adds/exits/flips
  -> settle historical outcomes
  -> simulate follower execution walk-forward
  -> rank by lower-confidence daily log growth
  -> watch top 50 leaders continuously
  -> classify new trade events
  -> estimate current copied-trade p
  -> fractional Kelly sizing
  -> risk gates
  -> idempotent OrderIntent
  -> execution and reconciliation
  -> live decay feedback
```

## Candidate discovery

Run discovery on rolling intervals:

- every 5 minutes: active watchlist health;
- every 30 minutes: category leader refresh;
- every 6 hours: full candidate refresh;
- daily: full rank rebuild and backtest report.

Candidate sources:

1. Polymarket overall leaderboard.
2. Polymarket category leaderboards: politics, sports, crypto, culture, mentions, weather, and any newly documented category.
3. Wallets that appear repeatedly in profitable short-duration markets.
4. Wallets whose trades lead favorable post-trade drift.
5. Wallets with strong realized performance in markets resolving within 1–7 days.
6. Incubator accounts with too little sample but unusually strong live drift.

## Ledger reconstruction

For every candidate, build a `TraderLedger`:

```rust
pub struct TraderLedger {
    pub trader: TraderId,
    pub venue: VenueId,
    pub events: Vec<TraderLedgerEvent>,
    pub positions: BTreeMap<MarketOutcomeId, PositionState>,
    pub closed_trades: Vec<ClosedTrade>,
    pub reconstruction_quality: ReconstructionQuality,
}
```

A ledger event is one of buy/open, add, trim, exit, flip, settlement, merge/split/redemption/accounting action, or unknown. Unknown events are never copied. Low reconstruction quality demotes the leader.

## Eligibility thresholds

Initial production thresholds:

| Metric | Active leader threshold | Incubator threshold |
|---|---:|---:|
| Audit window | 180 days | 90 days |
| Closed trades | >= 60 | >= 20 |
| Resolved markets | >= 30 | >= 10 |
| Closed trades in last 30 days | >= 12 | >= 5 |
| Median capital-weighted hold | <= 72h | <= 96h |
| p75 hold | <= 7d | <= 10d |
| Lower 5% daily log-growth | > 0 | not required |
| Max profit from one market | <= 20% | <= 35% |
| Max uncopiable profit | <= 35% | <= 50% |
| Max drawdown in follower sim | configured by bankroll tier | observation only |

The incubator list is for research and paper-copying. It cannot receive meaningful live capital until promoted.

## Ranking objective

Rank by walk-forward lower-confidence daily compounding:

```text
leader_score = LCB_5pct(E[log(B_t / B_{t-1}) per day after costs])
             + recency_bonus
             + latency_survivability_bonus
             + capacity_bonus
             + exit_clarity_bonus
             - concentration_penalty
             - drawdown_penalty
             - reconstruction_penalty
             - drift_penalty
```

Why log growth: it directly optimizes compounding and penalizes overbetting. A trader who doubles once and then sits illiquid may have high PnL but poor daily compounding for a follower.

## Copy survivability

For every trader, compute edge decay after the leader's observed trade:

| Delay bucket | Question |
|---|---|
| 250ms | Could colocated/fast polling copy near leader price? |
| 1s | Could a Rust service copy through normal API latency? |
| 3s | Could AWS + public API copy? |
| 10s | Does the signal persist for realistic production jitter? |
| 30s | Is it still profitable for slower polling? |
| 120s | Is the trade based on durable research rather than immediate repricing? |

A leader with high historical PnL but negative edge after 3–10 seconds is not a good copy target unless the infrastructure can reliably act faster.

## Signal classification

```rust
pub enum LeaderAction {
    Entry,
    Add,
    Trim,
    Exit,
    Flip,
    Unknown,
}

pub struct LeaderSignal {
    pub leader: TraderId,
    pub venue: VenueId,
    pub market: MarketId,
    pub outcome: OutcomeId,
    pub action: LeaderAction,
    pub leader_side: Side,
    pub leader_price: Price,
    pub leader_size: Quantity,
    pub observed_at: OffsetDateTime,
    pub received_at: OffsetDateTime,
    pub reconstruction_quality: ReconstructionQuality,
    pub source_trade_id: SourceTradeId,
}
```

Copyable actions:

- `Entry`: normally eligible.
- `Add`: eligible only if the leader is already profitable in this market or the new add passes independent eligibility.
- `Trim`/`Exit`: can reduce mirrored exposure.
- `Flip`: requires human-approved strategy setting; default block.
- `Unknown`: block.

## Kelly sizing

For a binary contract priced at `c`, paying `1` if correct, with calibrated probability `p`:

```text
b = (1 - c) / c
f_full = (b * p - (1 - p)) / b
       = (p - c) / (1 - c)
f_live = max(0, f_full) * kelly_fraction
```

Use net price including fees/slippage/adverse-selection buffer.

Recommended fractions:

| Mode | Kelly fraction |
|---|---:|
| Backtest sanity | 0.10 |
| Paper | 0.10 |
| Live-tiny | 0.25 |
| Promoted | 0.25–0.50 |
| Maximum without explicit approval | 0.50 |

`p` source:

```text
p = calibrated_probability(
      leader,
      market_family,
      odds_bucket,
      action_type,
      side,
      current_copy_price,
      liquidity_bucket,
      copy_delay_bucket,
      recency_state,
      resolver_confidence
    )
```

## Risk caps

Default hard caps:

```toml
[winner_follow.risk]
max_trade_live_tiny_bps = 25
max_trade_promoted_bps = 100
max_leader_bps = 300
max_market_bps = 200
max_family_bps = 800
max_total_copy_bps = 2500
intraday_stop_bps = -200
rolling_7d_stop_bps = -600
kill_switch_drawdown_bps = -1000
```

## Execution rules

1. Submit limit orders, not blind market orders, unless explicitly configured for very liquid markets.
2. Use idempotency keys so duplicate signals cannot double-enter.
3. Cancel if the order is not filled within the signal's validity window.
4. Do not chase beyond max copy slippage.
5. Recheck risk after partial fills.
6. Reconcile against venue state before the next order.
7. Follow exits only when the follower has mirrored exposure and the exit classification is high-confidence.

## Backtest acceptance

Winner-Follow can go to live-tiny only when:

- ranking is walk-forward;
- follower fills are conservative;
- lower 5% daily log growth is positive after costs;
- max drawdown is below the bankroll tier limit;
- at least 30 days of paper-copying produces behavior close to simulation;
- every copied/passed trade has a replayable decision record.

## Live monitoring

Demote or disable a leader when live copied trades underperform simulation by 2 standard errors, signal decay worsens materially, reconstruction quality drops, market family changes abruptly, profit concentration increases, the trader becomes inactive, copied exits become unreliable, or venue API/data quality degrades.
