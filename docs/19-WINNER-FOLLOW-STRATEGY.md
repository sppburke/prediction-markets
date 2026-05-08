# 19 — Winner-Follow Strategy

> See `_BASELINE.md` for the Rust-only implementation rule, toolchain pin, lints, and common acceptance gate.
> See `_GLOSSARY.md` for vocabulary (wallet/trader/operator/leader/candidate), type aliases, latency budget, rate limits, anti-gaming flag thresholds, and configuration defaults.

**This file is the canonical source of truth for Winner-Follow risk caps, Kelly fractions, eligibility thresholds, mode definitions, and promotion ladders.** Other files reference this file rather than restating these values.

## Objective

Make Winner-Follow the first deployable strategy. The system continuously identifies the fastest-compounding public traders/operators, selects the subset whose future trades are likely to remain profitable after copy delay and costs, and mirrors qualifying entries using risk-capped fractional Kelly.

The goal is not to copy famous accounts. The goal is to maximize **follower bankroll log-growth per day** while minimizing ruin risk, overfitting, copy slippage, hidden liquidity risk, and false skill.

## Edge claim (must hold; otherwise the strategy is disabled)

```text
leader skill + public detectability + speed + liquidity + risk sizing
    > fees + slippage + adverse selection + decay
```

If walk-forward evidence cannot support that inequality at the configured Kelly fraction, the affected leader/operator is demoted to paper or off (see "Demotion criteria" in `_GLOSSARY.md`).

## Venue support

### Polymarket — primary

Polymarket is the primary Winner-Follow venue because public data supports user-level reconstruction:

- leaderboard snapshots;
- user-filtered trades;
- current positions;
- closed positions;
- user activity;
- market/orderbook websocket state;
- transaction hashes for timing validation.

Polymarket also uses proxy wallets, pUSD collateral, deposit addresses, and bridge/onramp flows. Funder identity is therefore derived from verified public proxy/funder/collateral evidence in `source-onchain-polygon` and `operator-graph`, never from a naive first-USDC-sender heuristic.

### Kalshi — conditional

Kalshi public trades are useful for market-flow analysis, but public trade events do not identify the trader. Kalshi Winner-Follow requires one of:

1. official public trader-level data sufficient for attribution;
2. a trader who explicitly consents and provides API/portfolio access;
3. a future endpoint that lawfully exposes public user-level trade history.

Until then, Kalshi copy-following is disabled by default; Kalshi remains a source/resolver and market-flow venue.

## Operator identity and funding graph

Winner-Follow follows public economic actors, not isolated wallet strings. The wallet-level ledger remains the base observation; `operator-graph` collapses wallets into a deterministic `OperatorId` when public funding/collateral evidence is strong enough (`identity_confidence ≥ funder_root_min_confidence_ppm`; see `_GLOSSARY.md`).

Identity layer:

```rust
pub struct OperatorIdentity {
    pub operator_id: OperatorId,
    pub funder_root: Option<FunderRootId>,
    pub member_wallets: BTreeSet<WalletAddress>,
    pub cluster_rule_version: semver::Version,
    pub identity_confidence: ProbabilityPpm,
}

pub struct InheritedPriorPpm {
    pub mean_p: ProbabilityPpm,
    pub stderr_ppm: u32,
    pub effective_n: u32,                     // capped at inherited_prior_max_effective_n
}

pub struct OperatorTrackRecord {
    pub operator_id: OperatorId,
    pub closed_trades: u32,
    pub realized_pnl_ppm: i64,
    pub category_log_growth_lcb_bps: BTreeMap<MarketFamily, i32>,
    pub cluster_size_history: Vec<(time::Date, ClusterSize)>,
    pub seeding_velocity_per_week: Decimal,
}
```

The graph is built natively from public Polygon data and public/official Polymarket data. CrowdIntel-style clusters are research references; CrowdIntel UI data, proprietary scores, and non-replayable labels are not live decision inputs unless an authorized, stable, replayable export/API exists and has been reviewed.

## System pipeline

```text
Discover candidates
  -> hydrate public profiles/trades/positions
  -> ingest public Polygon funding/collateral events
  -> build operator identity and inherited-prior snapshots
  -> collapse wallet ledgers where identity confidence is high
  -> reconstruct trader ledgers
  -> label entries/adds/exits/flips
  -> settle historical outcomes
  -> simulate follower execution walk-forward
  -> rank by lower-confidence daily log growth
  -> watch top-`active_watchlist_size` leaders continuously
  -> classify new trade events and signal kind
  -> estimate current copied-trade p
  -> fractional Kelly sizing
  -> risk gates
  -> idempotent OrderIntent
  -> execution and reconciliation
  -> live decay feedback
```

## Candidate discovery

### Bootstrap quality filters (also applied continuously in live stream)

The `pe-bootstrap` Dune query seeds the initial watchlist using four quality filters. These same
criteria **must be mirrored as continuous live eligibility gates** in `operator-graph` /
`strategy-winner-follow` so that wallets that degrade after bootstrap are demoted or excluded:

| Filter | Canonical default | Env override |
|---|---|---|
| Distinct resolved binary markets traded | `> bootstrap_dune_min_closed_markets` (15) | `PE_BOOTSTRAP_DUNE_MIN_MARKETS` |
| Win rate on those markets | `> bootstrap_dune_min_win_rate_pct` (95%) | `PE_BOOTSTRAP_DUNE_MIN_WIN_RATE_PCT` |
| At least one trade on a resolved market within recent window | within `bootstrap_dune_active_window_days` (30 days) | `PE_BOOTSTRAP_DUNE_ACTIVE_DAYS` |
| Average hours from first entry to market resolution | `< bootstrap_dune_max_avg_hours_to_resolution` (72 h) | `PE_BOOTSTRAP_DUNE_MAX_AVG_HOURS` |

Canonical default values are in `_GLOSSARY.md` "Bootstrap defaults" table. The live stream gate
uses the same thresholds computed over rolling windows; a wallet falling below any threshold
transitions to the incubator tier and stops receiving copy signals until it recovers.

Discovery cadences:

| Job | Interval |
|---|---|
| Active watchlist health | 5 minutes |
| Category leader refresh | 30 minutes |
| Full candidate refresh | 6 hours |
| Full rank rebuild + backtest report | 24 hours |

Candidate sources:

1. Polymarket overall leaderboard.
2. Polymarket category leaderboards: politics, sports, crypto, culture, mentions, weather, and any newly documented category.
3. Wallets that appear repeatedly in profitable short-duration markets.
4. Wallets whose trades lead favorable post-trade drift.
5. Wallets with strong realized performance in markets resolving within 1–7 days.
6. Incubator accounts with too little sample but unusually strong live drift.
7. Fresh wallets linked to known high-quality operators/funders, eligible only for inherited-prior incubator mode (`fresh_wallet_max_closed_trades`, `fresh_wallet_max_age_seconds`).
8. Same-operator clusters with `≥ cluster_coord_min_members_K` member wallets entering the same `(market, outcome, side)` within `cluster_coord_window_seconds_W`.

## Ledger reconstruction

```rust
pub struct TraderLedger {
    pub trader: TraderId,
    pub venue: VenueId,
    pub operator_id: Option<OperatorId>,
    pub funder_root: Option<FunderRootId>,
    pub funding_hop_count: Option<FundingHopCount>,
    pub wallet_age: Option<WalletAgeSeconds>,
    pub cluster_size_at_observation: Option<ClusterSize>,
    pub events: Vec<TraderLedgerEvent>,
    pub positions: BTreeMap<MarketOutcomeId, PositionState>,
    pub closed_trades: Vec<ClosedTrade>,
    pub reconstruction_quality: ReconstructionQuality,
}
```

A ledger event is one of buy/open, add, trim, exit, flip, settlement, merge/split/redemption/accounting action, or unknown. `Unknown` events are never copied. `reconstruction_quality < 60` demotes the leader to research-only.

Operator identity does not overwrite wallet history. The ledger keeps wallet-level facts and adds identity annotations with confidence and rule version so replay can reproduce exactly why a wallet was or was not collapsed into an operator.

## Eligibility thresholds

| Metric | Active leader | Incubator |
|---|---:|---:|
| Audit window | 180 days | 90 days |
| Closed trades | ≥ 60 | ≥ 20 |
| Resolved markets | ≥ 30 | ≥ 10 |
| Closed trades in last 30 days | ≥ 12 | ≥ 5 |
| Median capital-weighted hold | ≤ 72 h | ≤ 96 h |
| p75 hold | ≤ 7 d | ≤ 10 d |
| Lower 5 % daily log-growth | > 0 (after costs) | not required |
| Max profit from one market | ≤ 20 % | ≤ 35 % |
| Max uncopiable profit | ≤ 35 % | ≤ 50 % |
| Max drawdown in follower sim | per bankroll tier | observation only |
| Min simulated follower turnover | ≥ 0.35 bankroll/day | not required |
| Watchlist size | top `active_watchlist_size` (= 50) | up to `incubator_watchlist_size` (= 250) |

The incubator list is for research and paper-copying; it cannot receive meaningful live capital until promoted.

Inherited-prior incubator additionally requires:

- operator has active-leader-level sample size and positive lower-confidence family-specific log-growth;
- fresh wallet meets `fresh_wallet_max_closed_trades` and `fresh_wallet_max_age_seconds`;
- funding/collateral hop count ≤ `funding_max_hops`;
- proxy/funder/collateral mapping is proven for this wallet class (see `21-RESEARCH-AND-SOURCE-DISCOVERY.md`);
- cluster size, seeding velocity, membership stability within configured bands;
- no `BaitWalletSuspect`, `DilutionAttack`, `LaunderedFunder`, `WashCluster`, or equivalent flag blocks the signal.

## Ranking objective

Walk-forward lower-confidence daily compounding:

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

When operator identity is confident:

```text
operator_score = LCB_5pct(E[operator follower log-growth per day after costs])
               + latency_survivability_bonus
               + capacity_bonus
               + exit_clarity_bonus
               - membership_uncertainty_penalty
               - seeding_velocity_penalty
               - concentration_penalty
               - anti_gaming_penalty
```

The bonus/penalty weights in `03-PHASE-MODEL-ENGINE.md` are illustrative starting values; final weights are tuned by walk-forward optimization and persisted with the model artifact.

Why log growth: it directly optimizes compounding and penalizes overbetting. A trader who doubles once and then sits illiquid may have high PnL but poor daily compounding for a follower.

## Copy survivability

Edge decay measured at `latency-attribution-profiler` buckets:

| Delay bucket | Question |
|---|---|
| 250 ms | Could colocated/fast polling copy near leader price? |
| 1 s | Could a Rust service copy through normal API latency? |
| 3 s | Could AWS + public API copy? |
| 10 s | Does the signal persist for realistic production jitter? |
| 30 s | Is it still profitable for slower polling? |
| 120 s | Is the trade based on durable research rather than immediate repricing? |

A leader with high historical PnL but negative edge after 3–10 s is not a good copy target unless the infrastructure can reliably act faster (see production latency budget in `_GLOSSARY.md`).

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

pub enum WinnerFollowSignalKind {
    NormalLeaderFollow,
    FreshWalletFirstTrade,
    ClusterCoordination,
}

pub struct LeaderSignal {
    pub leader: TraderId,
    pub operator_id: Option<OperatorId>,
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
    pub signal_kind: WinnerFollowSignalKind,
    pub inherited_prior: Option<InheritedPriorPpm>,
    pub source_trade_id: SourceTradeId,
    pub action_confidence_ppm: ProbabilityPpm,
}
```

Action eligibility:

| Action | Live copy? | Notes |
|---|---|---|
| `Entry` | yes | Normally eligible |
| `Add` | conditional | Eligible only if `action_confidence_ppm ≥ add_high_confidence_threshold_ppm` AND (leader's existing position in this market is currently profitable OR the add itself satisfies all `Entry` gates independently). "Profitable" = unrealized PnL > 0 at observed price. |
| `Trim` | reduces only | Reduces mirrored exposure when `action_confidence_ppm ≥ exit_high_confidence_threshold_ppm` and the follower has matching exposure |
| `Exit` | reduces only | Same gate as Trim |
| `Flip` | requires `flip_human_approved = true` | Default-deny |
| `Unknown` | block | Always |

Signal-kind rules:

- `NormalLeaderFollow`: wallet/operator already qualifies under standard active-leader ranking.
- `FreshWalletFirstTrade`: a fresh wallet (≤ `fresh_wallet_max_closed_trades`, age ≤ `fresh_wallet_max_age_seconds`) enters a position ≥ `inherited_prior_min_position_usd`, inherits a heavily shrunk prior from a known operator/funder, defaults to paper.
- `ClusterCoordination`: ≥ `cluster_coord_min_members_K` member wallets of the same operator enter the same `(market, outcome, side)` within `cluster_coord_window_seconds_W` and aggregate notional ≥ `cluster_coord_min_aggregate_usd`. Debounced once per `(operator, market, outcome, side, dedup_window)`; defaults to shadow.

Idempotency key: `(leader, source_trade_id, market, outcome, side, observed_at_bucket)` where `observed_at_bucket = floor(observed_at_ms / 1_000)` (1-second buckets). Cluster-coordination adds `operator_id`.

## Kelly sizing

For a binary contract priced at `c`, paying `1` if correct, with calibrated probability `p`:

```text
b       = (1 - c) / c
f_full  = (b * p - (1 - p)) / b = (p - c) / (1 - c)
f_live  = max(0, f_full) * kelly_fraction
stake   = bankroll * f_live
contracts = floor(stake / c)
```

`c` is the **net** price (after fees, expected slippage, adverse-selection buffer). Reject trades where `f_live > 0` only because `p` is stale or uncalibrated.

Kelly fractions:

| Mode | Kelly fraction |
|---|---:|
| Backtest sanity | 0.10 |
| Paper | 0.10 |
| Live-tiny | 0.25 |
| Promoted | 0.25 |
| Maximum (with `kelly_fraction_above_default_human_approved`) | 0.50 |
| Inherited-prior first trade | 0.05 |
| Cluster coordination | 0.15 |

Above 0.50 requires a separate, signed config change.

`p` estimation: the backtest uses a Bayesian Beta(α, β) shrinkage prior on the leader's empirical win-rate with N_eff (effective sample size) scaling — see `_GLOSSARY.md` `kelly_p_prior_alpha_default` / `kelly_p_prior_beta_default` / `kelly_p_k_per_market_default`. `(α=0, β=0, k=0)` reproduces raw empirical rate; the default `(α=10, β=10, k=6)` shrinks small-sample extremes toward 0.5 and down-weights specialists with narrow market breadth. N_eff = min(total_trades, distinct_markets × k); scaled_wins = wins × N_eff / total; shrunk_p = (scaled_wins + α) / (N_eff + α + β).

`p` source (live system):

```text
p = calibrated_probability(
      leader,
      operator_identity,
      signal_kind,
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

For `FreshWalletFirstTrade`:

```text
p_effective = shrink(
    inherited_operator_prior_by_family_and_odds_bucket,
    toward = category_baseline,
    cap_effective_n = inherited_prior_max_effective_n
)
```

This is a prior over the copied follower trade, not a posterior on the new wallet.

## Per-trade cap configuration

After Kelly sizing, the strategy clamps the contract count to a per-trade size cap before the risk gate. This keeps single-trade notional within a configurable fraction of bankroll regardless of Kelly fraction.

`PerTradeCap` variants (set in `WinnerFollowConfig.per_trade_cap`):

| Variant | Resolved cap | Use |
|---|---|---|
| `ModeDefault` (default) | 25 bps LiveTiny / 100 bps Promoted | Production |
| `Bps(n)` | `n` bps of bankroll | Research / tuning |
| `Unlimited` | 10 000 bps (full bankroll) | Backtest Kelly-fraction study |

Clamp formula: `max_contracts = floor(bankroll × cap_bps / 10_000 / price)`. When `max_contracts == 0` (bankroll < price), the strategy returns `NoEdge`.

The risk engine retains `PerTradeSizeExceeded` as a defense-in-depth gate. Under normal flow the clamp prevents it from firing; it fires only on a programming error (e.g. `clamp_contracts_to_cap` bypassed).

Backtest override: `PE_BACKTEST_PER_TRADE_CAP=unlimited` (or `bps:N` / `mode_default`). Canonical defaults: `per_trade_cap_default` and `per_trade_cap_unlimited_resolved_bps` in `_GLOSSARY.md`.

## Anti-gaming flags

`operator-graph` computes deterministic flags from public funding/collateral and trading history. Concrete thresholds are in `_GLOSSARY.md`.

| Flag | Default action |
|---|---|
| `BaitWalletSuspect` | Demote inherited prior; keep paper |
| `DilutionAttack` | Shrink prior by membership uncertainty |
| `LaunderedFunder` | Block fresh-wallet first-trade |
| `WashCluster` | Block cluster-coordination signal |
| `MarketNarrowness` | Category-only prior; do not generalize |

## Canonical risk caps (single source of truth)

All values in basis points (1 bp = 0.01 %). Comments show the percent equivalent.

```toml
# winner-follow.toml — canonical risk caps. Other docs reference this block.

[winner_follow.modes]
leader_follow                = "live_tiny"   # paper -> live_tiny -> promoted (see promotion criteria below)
inherited_prior_first_trade  = "paper"
cluster_coordination         = "shadow"

[winner_follow.kelly]
fraction_backtest_sanity     = 0.10
fraction_paper               = 0.10
fraction_leader_live_tiny    = 0.25          # = 0.25x Kelly
fraction_leader_promoted     = 0.25          # raise to 0.50 only with kelly_fraction_above_default_human_approved
fraction_inherited_prior     = 0.05
fraction_cluster_coordination = 0.15
fraction_hard_max            = 0.50          # absolute ceiling without separate signed config change

[winner_follow.risk]
# Per-trade caps
max_trade_live_tiny_bps              = 25    # 0.25 % bankroll
max_trade_promoted_bps               = 100   # 1.00 %

# Concentration caps
max_leader_bps                       = 300   # 3.00 % per leader
max_operator_bps                     = 300   # 3.00 % per operator
max_per_operator_per_market_bps      = 75    # 0.75 % per (operator, market)
max_market_bps                       = 200   # 2.00 % per market
max_family_bps                       = 800   # 8.00 % per MarketFamily
max_total_copy_bps                   = 2500  # 25.00 % total open copy exposure

# Mode-specific exposure
max_funder_inherited_bps             = 100   # 1.00 % all inherited-prior across funders
max_inherited_prior_per_funder_per_day_count = 3
max_cluster_coord_bps                = 200   # 2.00 % all cluster-coordination

# Drawdown stops
intraday_stop_bps                    = -200  # halt new entries at -2.00 % intraday
rolling_7d_stop_bps                  = -600  # halt at -6.00 % over rolling 7d
inherited_prior_kill_if_drawdown_bps = -150  # -1.50 % inherited-prior PnL kills the mode
kill_switch_drawdown_bps             = -1000 # -10.00 % bankroll absolute kill
copy_latency_kill_switch_ms          = 3000  # fire when p95 > 1.5× the 2000 ms p95 budget

[winner_follow.copy]
max_slippage_from_leader_bps         = 75    # 0.75 % from leader observed price
order_validity_seconds               = 30    # cancel if unfilled within window
prefer_market_order                  = false # only true when "very liquid" gate passes (see _GLOSSARY.md)
```

The TOML above is the only authoritative copy. README, `04-PHASE-TRADING-STRATEGY.md`, and `14-COMPLIANCE-AND-RISK.md` reference this block by file path.

## Strategy-level trade gates

These five gates live in `crates/strategy-winner-follow/src/evaluate.rs` and fire in both backtest and production. They are checked before the risk engine is called. A gate returning `Err(WinnerFollowError::*)` means `evaluate_risk()` is never reached for that signal.

| # | Gate | Condition | Source | `WinnerFollowError` | Rationale |
|---|---|---|---|---|---|
| 1 | Flip not approved | `signal.action == Flip && !config.flip_human_approved` | `evaluate.rs:64–66` | `FlipNotApproved` | Position flips (exit + re-enter opposite side) are high-risk and require a manual signed config change. The flag prevents automated flip copying until an operator explicitly approves it. |
| 2 | Shadow mode | `effective_mode == Shadow` | `evaluate.rs:72–74` | `ShadowMode` | Shadow is record-only; no order is emitted. Returned as `Err` (not a hard failure) so callers can distinguish "intentionally suppressed" from "legitimately blocked". |
| 3 | Price invalid after fee | `Price::new(leader_price + fee_per_share)` fails | `evaluate.rs:92` | `NoEdge` | `c` (cost = leader price + taker fee) must be a valid `Price` in `(0, 1)`. If the leader traded at a price that after fees would round to ≥ $1.00 there is no upside — the trade has no edge. |
| 4 | Kelly sizes to zero | `size_contracts(kelly_input) == 0` | `evaluate.rs:102–104` | `NoEdge` | Kelly sizing returned zero contracts — the bankroll is too small to buy even one contract at this price with the configured fraction. Not an error; the signal is valid but unsizeable. |
| 5 | Cap-clamp to zero | `clamp_contracts_to_cap(...) == 0` | `evaluate.rs:111–113` | `NoEdge` | After applying the per-trade cap (basis points of bankroll), available bankroll is smaller than the price of a single contract. Fractional contracts are not supported; skip this trade. |

Gates 1–5 fire in order. Gate 6 onward is the risk-engine (`evaluate_risk`), which returns `RiskDecision::Blocked(reason)` → `Err(WinnerFollowError::Blocked(reason))`. See the risk-block taxonomy below for the 16 risk-engine block reasons.

**Relationship between layers:**

```
simulation.rs gates (backtest only, lines 457-518)
  → strategy-level gates 1-5 (evaluate.rs, both backtest and production)
      → risk-engine evaluate_risk() → 16 RiskBlock variants (production and backtest)
```

## Risk-block taxonomy and halt scope

`risk-engine` returns one of these block reasons; halt scope is recorded inline:

| Block reason | Halt scope |
|---|---|
| `OperatorConcentrationExceeded` | this trade |
| `LeaderConcentrationExceeded` | this trade |
| `MarketConcentrationExceeded` | this trade |
| `FamilyConcentrationExceeded` | this trade |
| `TotalCopyExposureExceeded` | this trade |
| `FunderInheritedExposureExceeded` | this trade + this funder for the day |
| `FunderSeedingRateSuspicious` | this funder + all dependent inherited-prior signals |
| `ClusterMembershipUnstable` | this cluster + cluster-coordination mode for `cluster_membership_stability_window_d` |
| `FunderHopCountExcessive` | this trade |
| `OnchainSourceUnhealthy` | inherited-prior + cluster-coordination modes |
| `ProxyFunderMappingUnproven` | inherited-prior + cluster-coordination modes |
| `AntiGamingFlagActive` | per-flag default action (see `_GLOSSARY.md`) |
| `IntradayDrawdownStop` | strategy-wide new entries until calendar reset |
| `Rolling7dDrawdownStop` | strategy-wide new entries until 7-day window clears |
| `KillSwitchDrawdown` | strategy-wide; manual review required to resume |
| `CopyLatencyKillSwitch` | strategy-wide new entries until p95 returns under budget |

## Execution rules

1. Submit limit orders, not market orders, unless `prefer_market_order = true` AND the market passes the "very liquid" gate (`_GLOSSARY.md`).
2. Use the idempotency key above so duplicate signals cannot double-enter.
3. Cancel if the order is not filled within `order_validity_seconds`.
4. Do not chase beyond `max_slippage_from_leader_bps`.
5. Recheck risk after partial fills.
6. Reconcile against venue state before the next order.
7. Follow exits only when the follower has mirrored exposure and the exit's `action_confidence_ppm ≥ exit_high_confidence_threshold_ppm`.
8. Block inherited-prior and cluster-coordination live orders when `source-onchain-polygon` is unhealthy or operator identity is unstable (see "Risk-block taxonomy" above).

## Promotion ladder

Each mode has its own ladder. Promotion of one mode does not promote another.

### Ordinary leader-follow

```
historical reconstruction
  -> walk-forward backtest passes (LCB_5pct > 0)
  -> paper-copy ≥ 30 days, ≥ 90 closed trades, drift within `_GLOSSARY.md` "close behavior" definition
  -> live-tiny (kelly = 0.25, max_trade = 25 bps)
  -> promoted (same kelly, max_trade = 100 bps) after another 30-day live-tiny window passes the gates
```

### Fresh-wallet inherited-prior

```
historical reconstruction (paper, kelly = 0.05)
  -> walk-forward backtest with separate report
  -> paper ≥ 30 days, ≥ 60 closed inherited-prior trades, drift within "close behavior"
  -> shadow (orders generated and recorded but not submitted) ≥ 14 days
  -> live-tiny (max_inherited_prior_per_funder_per_day_count = 3, max_funder_inherited_bps = 100)
  -> promoted only after a fresh 30-day live-tiny window AND no `BaitWalletSuspect` / `LaunderedFunder` flags fired in window
```

### Cluster coordination

```
historical reconstruction (shadow, kelly = 0.15)
  -> walk-forward backtest with separate report
  -> shadow ≥ 30 days, ≥ 60 cluster signals, drift within "close behavior"
  -> paper ≥ 14 days
  -> live-tiny (max_cluster_coord_bps = 200) only after `WashCluster` flag is absent for entire window
```

A demotion in any mode resets that mode's promotion clock.

## Promotion and demotion criteria

See `_GLOSSARY.md` for the quantified gates ("Promotion criteria — quantified" and "Demotion criteria"). They apply uniformly across all three modes.

## Backtest acceptance

Winner-Follow can go to live-tiny only when:

- ranking is walk-forward;
- follower fills are conservative (see `05-PHASE-BACKTESTING.md`);
- LCB_5pct of daily log-growth is positive after costs;
- max drawdown is below the bankroll-tier limit;
- paper-copy behavior is "close to simulation" (`_GLOSSARY.md` definition);
- every copied/passed trade has a replayable decision record;
- inherited-prior and cluster-coordination modes have separate walk-forward reports and promote on their own ladders.

## Live monitoring

Demote or disable a leader/operator when any demotion criterion in `_GLOSSARY.md` triggers. Demotion is automatic; promotion requires the gates above plus a manual review.
