# 19 — Winner-Follow Strategy

> See `_BASELINE.md` for the Rust-only implementation rule, toolchain pin, lints, and common acceptance gate.
> See `_GLOSSARY.md` for vocabulary (wallet/trader/leader/candidate), type aliases, latency budget, rate limits, and configuration defaults.

**This file is the canonical source of truth for Winner-Follow risk caps, Kelly fractions, eligibility thresholds, execution modes, and the promotion ladder.** Other files reference this file rather than restating these values.

## Objective

Make Winner-Follow the first deployable strategy. The system continuously identifies the fastest-compounding public traders, selects the subset whose future trades are likely to remain profitable after copy delay and costs, and mirrors qualifying entries using risk-capped fractional Kelly.

The goal is not to copy famous accounts. The goal is to maximize **follower bankroll log-growth per day** while minimizing ruin risk, overfitting, copy slippage, hidden liquidity risk, and false skill.

## Edge claim (must hold; otherwise the strategy is disabled)

```text
leader skill + public detectability + speed + liquidity + risk sizing
    > fees + slippage + adverse selection + decay
```

If walk-forward evidence cannot support that inequality at the configured Kelly fraction, the affected leader is demoted to paper or off (see "Demotion criteria" in `_GLOSSARY.md`).

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

### Kalshi — conditional

Kalshi public trades are useful for market-flow analysis, but public trade events do not identify the trader. Kalshi Winner-Follow requires one of:

1. official public trader-level data sufficient for attribution;
2. a trader who explicitly consents and provides API/portfolio access;
3. a future endpoint that lawfully exposes public user-level trade history.

Until then, Kalshi copy-following is disabled by default; Kalshi remains a source/resolver and market-flow venue.

## System pipeline

> Winner-Follow follows public economic actors at the **wallet** level. The
> wallet→operator clustering layer (operator identity, funding graph, inherited
> priors, cluster coordination) was removed end-to-end in #326 — see
> `docs/28-OPERATOR-GRAPH-ARCHIVE.md` for the archived design and rationale. Copy
> decisions are now purely per-wallet deterministic criteria.

```text
Discover candidates
  -> hydrate public profiles/trades/positions
  -> reconstruct trader ledgers
  -> label entries/adds/exits/flips
  -> settle historical outcomes
  -> simulate follower execution walk-forward
  -> rank by lower-confidence daily log growth
  -> watch top-`active_watchlist_size` leaders continuously
  -> classify new trade events
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
criteria **must be mirrored as continuous live eligibility gates** in
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

## Ledger reconstruction

```rust
pub struct TraderLedger {
    pub wallet: WalletAddress,
    pub reconstruction_quality: ReconstructionQuality,
    pub closed_trades: Vec<ClosedTrade>,
    pub open_positions: Vec<OpenPosition>,
    pub audit_window_days: u32,
}
```

A ledger event is one of buy/open, add, trim, exit, flip, settlement, merge/split/redemption/accounting action, or unknown. `Unknown` events are never copied. `reconstruction_quality < 60` demotes the leader to research-only.

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
| Watchlist size | top `active_watchlist_size` (= 100 default) | up to `incubator_watchlist_size` (= 250) |

The incubator list is for research and paper-copying; it cannot receive meaningful live capital until promoted.

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

pub struct LeaderSignal {
    pub leader: TraderId,
    pub venue: VenueId,
    pub market_id: MarketId,
    pub outcome_id: OutcomeId,
    pub action: LeaderAction,
    pub leader_side: Side,
    pub leader_price: Price,
    pub leader_size: Quantity,
    pub observed_at: OffsetDateTime,
    pub received_at: OffsetDateTime,
    pub reconstruction_quality: ReconstructionQuality,
    pub source_trade_id: SourceTradeId,
    pub action_confidence_ppm: ProbabilityPpm,
}
```

Action eligibility:

| Action | Live copy? | Notes |
|---|---|---|
| `Entry` + `Buy` | yes | The only action/side admitted by the current production copy-scope gate |
| `Entry` + `Sell` | block | Always rejected as `GateReject::NotBuy`; production Winner-Follow never opens a short |
| `Add` | block | Classified for ledger/replay, but the current first-entry gate does not copy it |
| `Trim` | block | Classified for ledger/replay; the current strategy holds copied BUYs to resolution |
| `Exit` | block | Same as Trim |
| `Flip` | block | The current first-entry gate blocks it; any future copy profile must also require `flip_human_approved = true` |
| `Unknown` | block | Always |

The classifier, strategy types, and paper executor retain generic SELL/reduction support for replay
and a future inventory-backed exit profile. That lower-layer support does not make SELL reachable
from the current production orchestrator.

Idempotency key: `(leader, source_trade_id, market, outcome, side, observed_at_bucket)` where `observed_at_bucket = floor(observed_at_ms / 1_000)` (1-second buckets).

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

Above 0.50 requires a separate, signed config change.

`p` estimation: the backtest uses a Bayesian Beta(α, β) shrinkage prior on the leader's empirical win-rate with N_eff (effective sample size) scaling — see `_GLOSSARY.md` `kelly_p_prior_alpha_default` / `kelly_p_prior_beta_default` / `kelly_p_k_per_market_default`. `(α=0, β=0, k=0)` reproduces raw empirical rate; the default `(α=10, β=10, k=6)` shrinks small-sample extremes toward 0.5 and down-weights specialists with narrow market breadth. N_eff = min(total_trades, distinct_markets × k); scaled_wins = wins × N_eff / total; shrunk_p = (scaled_wins + α + extra) / (N_eff + α + β + 2·extra), where `extra` is the snapshot-aware additive prior (issue #129) — see `_GLOSSARY.md` `kelly_p_min_snapshots_default` / `kelly_p_extra_per_missing_snapshot_default`. Defaults `min=4, extra_per_missing=5`. As `extra` grows, the effective prior point migrates from `α/(α+β)` toward 0.5 (least-informative) — newly-entering leaders with thin visible history are shrunk harder than long-history leaders with the same observed win-rate.

`p` source (live system):

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

## Per-trade cap configuration

After Kelly sizing, the strategy clamps the contract count to a per-trade size cap before the risk gate.

**Owner decision (#508, recorded): the ordinary per-trade bps caps are retired in production.** The
production `service_config.per_trade_cap` row is `unlimited` after the #508 Phase-A cutover, and the
**price-impact cap** (`price_impact_cap_bps`, `_GLOSSARY.md`) becomes the sole policy order-size
limit, below the always-applying available-bankroll affordability bound: an order is automatically
downsized to what the CLOB ask book absorbs within the band (e.g. a $500 budget with $230 absorbable
→ a $230 order at the exact ladder VWAP). `sizing_dollar_usd` stays the sizing input, not a cap.

The `PerTradeCap` enum is retained for backtest/research and as the safe boot posture
(set in `WinnerFollowConfig.per_trade_cap`):

| Variant | Resolved cap | Use |
|---|---|---|
| `ModeDefault` (default) | 25 bps LiveTiny / 100 bps Promoted | Boot/outage posture; backtest default |
| `Bps(n)` | `n` bps of bankroll | Research / tuning |
| `Unlimited` | 10 000 bps (full bankroll) | **Production (#508)**; backtest Kelly-fraction study |

The service **pins `ModeDefault` at boot** regardless of TOML/env (#508 round-5): a config-fetch
outage provably runs the 25 bps clamp with the impact gate off until the first successful poll;
`unlimited` can take effect only through the Supabase KV row.

Clamp formula: `max_contracts = floor(bankroll × cap_bps / 10_000 / price)`. When `max_contracts == 0` (bankroll < price), the strategy returns `NoEdge`.

The risk engine retains `PerTradeSizeExceeded` as a defense-in-depth gate. Under normal flow the clamp prevents it from firing; it fires only on a programming error (e.g. `clamp_contracts_to_cap` bypassed).

Backtest override: `PE_BACKTEST_PER_TRADE_CAP=unlimited` (or `bps:N` / `mode_default`). Canonical defaults: `per_trade_cap_default` and `per_trade_cap_unlimited_resolved_bps` in `_GLOSSARY.md`.

### Sizing modes (issue #161, #398 WS2)

`WinnerFollowConfig.sizing_mode` (a `SizingMode` enum; replaced `flat_usd_per_trade` in #398 WS2) selects how `evaluate()` performs steps 4–5:

```text
Kelly                  → fractional-Kelly (the full c/p/bankroll math)
Dollar { usd }         → contracts = max(1, floor(usd / fill_price))      # flat path; fill_price = leader + haircut on the live copy path (#484)
Contract { contracts } → contracts = exactly N
```

Steps 1–3 (Flip gate, mode clamp, Shadow gate) and steps 5b–5c–6 (per-trade cap, price-impact book cap `price_impact_cap_bps`, risk gate) remain active in all modes. The book cap (#398 WS2, reworked in #508 Phase A) `min`s the size to the **budget-planned executable ask ladder**: one `/book` fetch per admitted signal feeds the band gate, the within-band whole-share quantity (`pe_venue_polymarket::plan_budget_buy`), and — in `clob_best_ask` paper mode — the exact ladder-VWAP fill basis, with the FOK limit at the worst accepted tick and `worst_case_debit = shares × limit`. With the gate enabled, an **unusable book fails CLOSED** (missing token / fetch error / timeout / stale / corrupt / empty ⇒ skip — the #398 fail-open posture is retired), and an in-band ladder affording no whole share skips. Edits are valid `1..=10_000` bps (`0` rejected, last-known-good retained; "effectively off" is an explicit `10_000`); gate-off exists only as the compiled boot default. This differs from the backtest's `PE_BACKTEST_FLAT_USD` lever, which bypasses all sizing layers and is a research-only path.

**When to use `Dollar`/`Contract`:** when the Kelly `p` input is a per-leader constant with no per-trade information (e.g. a blended historical win rate). A constant `p` collapses Kelly to a pure function of price, which is noise with respect to per-trade edge; the fixed modes eliminate that noise and also eliminate bankroll compounding — position size does not grow with bankroll.

Default: `Kelly`; the deployed service's boot default is `Dollar` (the USD amount is canonical in `_GLOSSARY.md` `sizing_mode_default`). The Supabase KV layer stores three flat keys (`sizing_mode`/`sizing_dollar_usd`/`sizing_contracts`) reassembled in `runtime_config::parse_config`.

## Anti-gaming flags

The operator-clustering anti-gaming flags (`BaitWalletSuspect`, `DilutionAttack`,
`LaunderedFunder`, `WashCluster`) were removed with the operator graph in #326 —
see `docs/28-OPERATOR-GRAPH-ARCHIVE.md`. No anti-gaming flag is computed in live
code. The surviving per-wallet concern, market narrowness, is enforced
structurally rather than as a flag: the eligibility table caps single-market
profit share ("Max profit from one market"), and the ranking objective subtracts
a `concentration_penalty`.

## Canonical risk caps (single source of truth)

All values in basis points (1 bp = 0.01 %). Comments show the percent equivalent.

```toml
# winner-follow.toml — canonical risk caps. Other docs reference this block.
#
# Enforcement status (#508 Phase A owner decision, recorded):
# - Per-trade bps caps: RETIRED in production (per_trade_cap = unlimited after the cutover;
#   ModeDefault stays the boot/outage posture). The price-impact cap is the sole policy size
#   limit — production default 100 bps of best ask (`price_impact_cap_bps`, _GLOSSARY.md).
# - Concentration caps: UN-ENFORCED BY DECISION on the production copy path. The values below
#   remain canonical for backtest/tests, carried as `RiskSnapshot.concentration_caps =
#   Some(ConcentrationCaps::CANONICAL)`; production passes `None` (typed, not accidental).
# - Drawdown stops + latency kill switch: code ARMED but currently inert in production — the
#   Phase-0B stub feeds zeroed PnL/latency inputs (real exposure tracking is future work).

[winner_follow.modes]
leader_follow                = "live_tiny"   # paper -> live_tiny -> promoted (see promotion criteria below)

[winner_follow.kelly]
fraction_backtest_sanity     = 0.10
fraction_paper               = 0.10
fraction_leader_live_tiny    = 0.25          # = 0.25x Kelly
fraction_leader_promoted     = 0.25          # raise to 0.50 only with kelly_fraction_above_default_human_approved
fraction_hard_max            = 0.50          # absolute ceiling without separate signed config change

[winner_follow.risk]
# Per-trade caps (ModeDefault resolution; production retired these — see header note)
max_trade_live_tiny_bps              = 25    # 0.25 % bankroll
max_trade_promoted_bps               = 100   # 1.00 %

# Concentration caps (backtest/tests; un-enforced by decision in production — header note)
max_leader_bps                       = 300   # 3.00 % per leader
max_market_bps                       = 200   # 2.00 % per market
max_family_bps                       = 800   # 8.00 % per MarketFamily
max_total_copy_bps                   = 2500  # 25.00 % total open copy exposure

# Drawdown stops (armed; inert via zeroed stub inputs — header note)
intraday_stop_bps                    = -200  # halt new entries at -2.00 % intraday
rolling_7d_stop_bps                  = -600  # halt at -6.00 % over rolling 7d
kill_switch_drawdown_bps             = -1000 # -10.00 % bankroll absolute kill
copy_latency_kill_switch_ms          = 3000  # fire when p95 > 1.5× the 2000 ms p95 budget

[winner_follow.copy]
max_slippage_from_leader_bps         = 75    # 0.75 % from leader observed price
order_validity_seconds               = 30    # cancel if unfilled within window
prefer_market_order                  = false # only true when "very liquid" gate passes (see _GLOSSARY.md)
```

The TOML above is the only authoritative copy. README, `04-PHASE-TRADING-STRATEGY.md`, and `14-COMPLIANCE-AND-RISK.md` reference this block by file path.

## Copy-scope gates (service-side, issue #290)

These gates live in `crates/service` (the orchestrator copy path), fire **before** the strategy-level gates below: a copied trade must be a **first-ever BUY entry** into a market, resolving **within `[min, max]` of now**, **held to resolution**, with a **currently-available market price** inside the `[min_fill_price, max_fill_price)` current-price band. They are production-only (the orchestrator path is not exercised in backtest, which uses `simulation.rs`). The leader-price band of the original 72hr cohort was **removed in #339**; a **current-price band floor returned at the 2026-07-03 run28 cutover** (`min_fill_price`, #468 selection↔deployment parity — the run28 eval filtered forward copies to the band) alongside the pre-existing `max_fill_price` cap. Live sizing stays re-based on the **current** market price (see "Current-price sizing basis" below).

Gate order in `orchestrator.rs::handle_trade`, after dedup → watchlist → classify:

| # | Gate | Condition (copy iff …) | Source | Rationale |
|---|---|---|---|---|
| A | Hold-to-resolution | `(market, outcome)` not already held | `orchestrator.rs` `filled_positions` set | The cohort wallets sell winners early; dropping every later signal on a held contract (including the leader's own exits) reproduces copy-and-hold, capturing the full move. Re-seeded from `paper_positions()` on restart. **No code change in #290** — pre-existing behavior. |
| B | BUY-only first-ever entry | `signal.leader_side == Buy` **and** `signal.action == Entry` **and** `signal.market_id ∉ history[leader]` | `entry_gate.rs::admit` → `GateReject::NotBuy` / `NotAnEntry` / `NotFirstEntry` | The cohort was selected on first-ever BUYs. Drops every SELL, `Add`/`Trim`/`Exit`/`Flip`, and re-entry. History is per leader wallet (`signal.leader.0`), backfilled at startup from the free Data API and a JSON sidecar (`wallet_market_history_path`); the startup map is conservatively any-trade, while `record_entry` records only same-session admitted BUY entries. |
| C | Resolution horizon | `now + min_resolution_horizon_secs ≤` market resolution `≤ now + max_resolution_horizon_secs` | `orchestrator.rs::check_resolution_horizon` | Too far out locks capital for months; too soon (< 60 s) cannot be filled and held (`docs/29` copy floor). Resolution time from `MarketEndCache` (Gamma). Each bound's `0` disables it; **unknown** resolution time **fails closed** (skipped). One lookup serves both bounds. |
| D | Fill-price band | a current mid for `(market, outcome)` exists (liveness) **and** the BUY **fill price** (leader price + paper haircut) is `≥ min_fill_price` **and** `< max_fill_price` | `orchestrator.rs` band block; sizing/gating via `PaperExecutor::fill_price` | The copy is sized and gated on the price it will ACTUALLY fill at (leader + haircut), matching the backtest's fill-price gate (#484; the Gamma mid via `MidPriceCache` is kept only for liveness + observability, no longer the sizing basis, because a stale/thin mid diverges from the fill). The `max_fill_price` cap (0.85) skips BUYs near $1 (issue-#142 parity); the `min_fill_price` floor (0.15, run28 cutover) skips BUYs below the entry band — the boundary value fills. **#486:** in the default `clob_best_ask` fill mode the BUY fill basis is the fresh CLOB best-ask (`resolve_fill_price`), so this band gate keys on the *ask* — a copy whose book ran to `≥ max_fill_price` is rejected even when the leader price was in-band; `leader_haircut` mode restores the haircut basis (`fill_mode`, `_GLOSSARY.md`). |

**Fail posture (gate B history unknown).** A leader absent from the merged history map (startup fetch failed **and** no stale sidecar) is governed by `entry_gate_fail_closed` (`_GLOSSARY.md`): default `false` = **fail-open** (treat a BUY entry as new, copy allowed) to preserve availability for paper-only operation; `true` = **fail-closed** (block its BUY `Entry` signals until history is known). SELL rejection is unconditional and does not depend on this posture. Either way the loader emits a per-wallet `warn!`, so on a first-ever run a fetch failure is visible rather than silent. **Accepted exposure:** under the default fail-open posture, a transient API outage on first run can admit a non-first-entry BUY as if it were a first entry; set `entry_gate_fail_closed = true` to trade availability for strictness.

A rejected copy-scope gate logs the typed reason and commits a no-fill (the leader ledger is still mirrored, matching the existing no-edge path). A rejected SELL does not call `record_entry`, so it cannot consume same-session first-entry history. Defaults for the gate config keys (`entry_gate_fail_closed`, `min_resolution_horizon_secs`, `max_resolution_horizon_secs`, `min_fill_price`, `max_fill_price`) live in `_GLOSSARY.md` "Copy-entry gate".

**Current-price sizing basis (#339).** Unlike backtest (`simulation.rs`, which sizes at the historical fill price), the live copy path computes the Kelly cost `c` and the flat-path contract count at the **current** market price fetched at copy time — not `signal.leader_price`. The leader's price still sets the order's `limit_price` (don't-chase: if the price rose, the limit simply won't fill). This closes the live/backtest divergence the old leader-price band papered over.

## Strategy-level trade gates

These five gates live in `crates/strategy-winner-follow/src/evaluate.rs` and fire in both backtest and production. They are checked before the risk engine is called. A gate returning `Err(WinnerFollowError::*)` means `evaluate_risk()` is never reached for that signal.

| # | Gate | Condition | Source | `WinnerFollowError` | Rationale |
|---|---|---|---|---|---|
| 1 | Flip not approved | `signal.action == Flip && !config.flip_human_approved` | `evaluate.rs:62–64` | `FlipNotApproved` | Position flips (exit + re-enter opposite side) are high-risk and default-deny. The flag prevents automated flip copying until an operator approves it via the audit-logged Supabase admin panel (`service_config`, #398 Decision #2). |
| 2 | Shadow mode | `effective_mode == Shadow` | `evaluate.rs:70–72` | `ShadowMode` | Shadow is record-only; no order is emitted. Returned as `Err` (not a hard failure) so callers can distinguish "intentionally suppressed" from "legitimately blocked". |
| 3 | Price invalid after fee | `Price::new(leader_price + fee_per_share)` fails | `evaluate.rs:104` | `NoEdge` | `c` (cost = leader price + taker fee) must be a valid `Price` in `(0, 1)`. If the leader traded at a price that after fees would round to ≥ $1.00 there is no upside — the trade has no edge. |
| 4 | Kelly sizes to zero | `size_contracts(kelly_input) == 0` | `evaluate.rs:112–114` | `NoEdge` | Kelly sizing returned zero contracts — the bankroll is too small to buy even one contract at this price with the configured fraction. Not an error; the signal is valid but unsizeable. |
| 5 | Cap-clamp to zero | `clamp_contracts_to_cap(...) == 0` | `evaluate.rs:124–126` | `NoEdge` | After applying the per-trade cap (basis points of bankroll), available bankroll is smaller than the price of a single contract. Fractional contracts are not supported; skip this trade. |

Gates 1–5 fire in order. Gate 6 onward is the risk-engine (`evaluate_risk`), which returns `RiskDecision::Blocked(reason)` → `Err(WinnerFollowError::Blocked(reason))`. See the risk-block taxonomy below for the 10 risk-engine block reasons.

**Relationship between layers:**

```
simulation.rs gates (backtest only, lines 457-518)
  → strategy-level gates 1-5 (evaluate.rs, both backtest and production)
      → risk-engine evaluate_risk() → 10 RiskBlock variants (production and backtest)
```

## Risk-block taxonomy and halt scope

`risk-engine` returns one of these block reasons; halt scope is recorded inline:

| Block reason | Halt scope |
|---|---|
| `KillSwitchDrawdown` | strategy-wide; manual review required to resume |
| `IntradayDrawdownStop` | strategy-wide new entries until calendar reset |
| `Rolling7dDrawdownStop` | strategy-wide new entries until 7-day window clears |
| `CopyLatencyKillSwitch` | strategy-wide new entries until p95 returns under budget |
| `OnchainSourceUnhealthy` | this trade (on-chain resolution source is Degraded or Dead) |
| `PerTradeSizeExceeded` | this trade |
| `LeaderConcentrationExceeded` | this trade |
| `MarketConcentrationExceeded` | this trade |
| `FamilyConcentrationExceeded` | this trade |
| `TotalCopyExposureExceeded` | this trade |

## Execution rules

1. Submit limit orders, not market orders, unless `prefer_market_order = true` AND the market passes the "very liquid" gate (`_GLOSSARY.md`).
2. Use the idempotency key above so duplicate signals cannot double-enter.
3. Cancel if the order is not filled within `order_validity_seconds`.
4. Do not chase beyond `max_slippage_from_leader_bps`.
5. Recheck risk after partial fills.
6. Reconcile against venue state before the next order.
7. Current production Winner-Follow ignores all SELL/Trim/Exit signals and holds copied BUYs to resolution. Any future exit-following profile must require matching inventory and `action_confidence_ppm ≥ exit_high_confidence_threshold_ppm`.
8. Block live orders when the on-chain resolution source is unhealthy (`OnchainSourceUnhealthy`; see "Risk-block taxonomy" above).

## Isolated Polymarket V2 canary

The canary is orthogonal to `ExecutionMode`, ordinary `live_tiny`, and promotion evidence. It
starts inactive, cannot be armed by Supabase/runtime configuration, and never advances the
promotion ladder. A separately reviewed campaign may consume at most three individually
authorized probes followed by five automatic ordinary Winner-Follow attempts over at most seven
days.

The campaign starts from exactly $200.000000 of freshly reconciled free pUSD cash, no positions or
orders, and one standard-V2-exchange allowance no greater than $8.000000. Each BUY-only,
fee-free, standard-exchange, binary Geopolitics attempt is a whole-share V2 limit FOK with exact
worst-case debit bounded by both $1.000000 and `floor_to_collateral_atomic(50 bps ×
canary_bankroll)`. The monotonic sum of reserved worst-case debits and the approved allowance are
each capped at $8.000000; rejection or a smaller realized debit restores neither a slot nor
commitment. The campaign accepts the entire $8.000000 commitment as loss exposure and has no
canary drawdown measure or drawdown stop; the dynamic per-order percentage cap and the remaining
free bankroll can still reduce executable aggregate debit below $8.000000.

`canary_bankroll` is freshly reconciled free deposit-wallet pUSD minus a pending reservation. No
conditional-token mark is included. Exposure basis points round positive values outward against
that denominator. Missing or stale resolver, account, geoblock, closed-only, watchlist/history,
position, fee/category/delay, or book evidence fails before reservation. Any post-reservation
reject, timeout, ambiguity, partial/live state, drift, external activity, expiry, or kill closes
admission.

For canary admission, geoblock means API order restriction, not frontend availability. The raw
same-egress response country must exactly match the authority-bound boot jurisdiction. IE, JP, MT,
and NL are treated as frontend-only close-only jurisdictions under the current official contract;
`blocked: true` for any other country remains API-blocked. Authenticated account `closed_only`
status is independent and always fails admission when true.

Organic attempts retain the ordinary eligible `LeaderSignal`, calibrated ranking `p`, canonical
idempotency key, BUY-entry/history/latency/resolution gates, and concentration checks. Their Kelly
cost is `leader_price × 1.0075`, quantized down to the venue tick, with proven zero fee, canonical
live Kelly fraction, and the caps above. Operator probes bypass leader/Kelly gates but bind one
campaign/ordinal, condition/outcome/token, exact whole shares, absolute worst price/debit,
resolver hash, and canonical authority hash. Both origins converge on one immutable quote,
exact-risk, V2 preparation, synced reservation, and exactly-one-POST path.

See [`36-POLYMARKET-V2-CANARY-RUNBOOK.md`](36-POLYMARKET-V2-CANARY-RUNBOOK.md) for inactive
installation and later authority boundaries.

## Ordinary live execution (#508)

Ordinary `pe-service` has per-account modes `off | live_tiny`. `promoted` is deferred because it
has no distinct financial contract after retirement of the per-trade cap; the promotion ladder
below remains canonical for when a distinct promoted contract is introduced. The account grammar,
armed-account bound, per-account price-impact default, dispatch retention, and redemption surfacing
threshold live only in `_GLOSSARY.md`.

The panel requests a mode, but the service is the sole writer of effective mode through the atomic
`account_set_effective_mode` control RPC. Arming requires a decryptable, correctly bound credential
bundle; venue state not `closed_only`; passing geoblock evidence; sufficient balance and allowance
for both V2 exchange spenders; and an unrevoked promotion review newer than the executor's
first-boot arming fence. Requested mode alone never authorizes an order. Invalid credentials, a
missing/revoked promotion record, or `closed_only` demote an armed account; transient probe failure
refuses orders without demotion. A pending, ambiguous, or failed redemption is the non-demoting
exception: it closes that account's new-BUY admission until confirmation.

Venue settlement is payoff authority (Decision 11). Ordinary live admission composes an automated
`VenueSettlementRecord` from `resolver-card` with fresh live-admission market evidence: condition,
outcome, and token identity must agree; NegRisk is admitted and carried; fee fields are recorded but
do not gate. Missing, stale, ambiguous, or disagreeing evidence fails closed. Hand-installed full
`ResolverCard`s remain canary-only.

Resolved positions redeem automatically (Decision 12). NegRisk evidence selects the V2 collateral
adapter, and custody kind selects the supported Relayer transport. Recovery reconciles before any
resubmission, applies no dust floor, and keeps new-BUY admission closed for that account after a
pending, ambiguous, or failed attempt until redemption is confirmed.

The dispatch aggregate is durably staged at shared signal admission, before the paper outcome can
be durable, with frozen ordered account targets and credential bindings. The seed moves
`pending_paper -> ready` atomically with a typed paper outcome. That outcome records sequencing and
recovery state only: paper fills and paper-only skips never gate an otherwise admitted live target.

## Promotion ladder

Winner-Follow has a single leader-follow promotion ladder.

### Ordinary leader-follow

```
historical reconstruction
  -> walk-forward backtest passes (LCB_5pct > 0)
  -> paper-copy ≥ 30 days, ≥ 90 closed trades, drift within `_GLOSSARY.md` "close behavior" definition
  -> live-tiny (kelly = 0.25; size bounded by the price-impact cap since #508 — the 25 bps
     per-trade cap is the retired ModeDefault resolution)
  -> promoted (same kelly; the 100 bps ModeDefault resolution likewise retired) after another
     30-day live-tiny window passes the gates
```

A demotion resets the promotion clock.

## Promotion and demotion criteria

See `_GLOSSARY.md` for the quantified gates ("Promotion criteria — quantified" and "Demotion criteria").

## Backtest acceptance

Winner-Follow can go to live-tiny only when:

- ranking is walk-forward;
- follower fills are conservative (see `05-PHASE-BACKTESTING.md`);
- LCB_5pct of daily log-growth is positive after costs;
- max drawdown is below the bankroll-tier limit;
- paper-copy behavior is "close to simulation" (`_GLOSSARY.md` definition);
- every copied/passed trade has a replayable decision record.

## Live monitoring

Demote or disable a leader when any demotion criterion in `_GLOSSARY.md` triggers. Demotion is automatic; promotion requires the gates above plus a manual review.

## Watchlist refresh

The followed-wallet set is refreshed from Supabase `latest_ranking` **filtered to the ranker's `survives` verdict** (#518 — the published batch is a pass/fail bench, and only survivors are Leaders), which the buy-and-hold ranker publishes via `scripts/rank_and_push.sh` (issue #370 — the sole ranking pipeline; TTR 48h since the 2026-07-03 run28 cutover, `ranker_ttr_hours`). `pe-service` reads `latest_ranking` on an interval (score-update-only); MEMBERSHIP is governed by `watchlist_membership_mode` (`_GLOSSARY.md`): `knockout` (legacy — the maintenance tick's knockout+backfill is the sole membership path) or `full_rerank` (each ranking-batch transition wholesale-replaces the live top-`active_watchlist_size` — the cutover production mode; the demotion gate remains the intra-cycle rail). `active_watchlist_size` is a Supabase `service_config` integer (default 100, valid `1..=200`) loaded at boot and hot-reloaded every 30 seconds. A grow preloads newly admitted wallets' market histories and current positions before the atomic membership swap; a failed fetch or preload retains the last-known-good target and membership for retry. The supporting deployment needs one normal service restart, but later valid size edits do not. Run28 fixed `k=25` and did not sweep watchlist width: the earlier top-50 paper experiment and its superseding 100-wallet default are operator-directed choices, not run28-backed N choices. See `docs/26-DATA-REFRESH-AND-REOPTIMIZATION-RUNBOOK.md`.
