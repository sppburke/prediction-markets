# 08 — Venue Playbook: Polymarket

> See [`_BASELINE.md`](_BASELINE.md) for the Rust-only implementation rule and common acceptance gate.
> See [`_GLOSSARY.md`](_GLOSSARY.md) for type aliases, idempotency-key definition (`observed_at_bucket`), and Polymarket rate-limit defaults.

## Objective

Implement Polymarket as a dedicated Rust venue adapter with CLOB data, signed orders, market/user/sports/RTDS sockets, market/event hierarchy, fees/rebates, and rule-specific resolution sources.

## Crate

`venue-polymarket`

Responsibilities:

- Gamma/Data/CLOB API client;
- market WebSocket;
- user WebSocket;
- sports WebSocket;
- (RTDS attributed-trade ingestion is a SOURCE concern, not a venue one: `source-polymarket-public::activity_ws` owns it — #530. The venue adapter keeps market/user/sports sockets and order paths only.)
- signed order submission;
- market and event discovery;
- book snapshots/deltas;
- sports metadata and resolution URLs;
- cost model;
- account/order reconciliation;
- restart/maintenance awareness;
- public proxy-wallet, signature/funder, bridge/deposit, and pUSD collateral documentation tracking for on-chain identity research.

## Types

Authoritative in `core-types`; illustrative subset:

```rust
pub struct PolymarketConditionId(pub String);
pub struct PolymarketTokenId(pub String);
pub struct PolymarketAssetId(pub String);
pub struct PolymarketOrderId(pub String);
pub struct PolymarketPrice(pub Decimal);
pub enum PolymarketOrderType { Gtc, Gtd, Fok, Fak }
```

## Signing and auth

The signing module is isolated. Golden tests, no secret logging, explicit L1/L2 authentication boundaries, and payload hashes. Strategies never call signing functions directly.

Polymarket uses signature types and a funder address that can be an EOA, proxy wallet, or Gnosis Safe. Public profile/trade data exposes `proxyWallet`; authenticated trading uses the configured funder. The venue adapter keeps these concepts typed and never assumes the displayed wallet is the same thing as the original funding EOA.

### Isolated V2 canary boundary

The retired V1 signing/submission implementation is absent. Ordinary `pe-service` now has a #508
per-account V2 execution path that ships dark until armed. The dedicated, initially inactive
`pe-service-live-canary` remains a separate standard-exchange-only role. It pins SDK 0.7.0 with the
`clob` feature only, the explicit production CLOB host, Polygon standard V2 exchange, `POLY_1271`,
and a deposit-wallet maker/signer/funder. Its reachable order surface is BUY-only limit FOK with
zero builder/metadata, `postOnly = false`, and explicit `deferExec = false`; batches, replacement,
market-order builders, hidden polling, and V1 are unreachable.

Admission requires strict agreement among Gamma canonical tag identity and direct market-tag
evidence, CLOB long/short metadata, the fresh book, resolver card, authenticated account reads,
standard-exchange contract identity, same-egress API jurisdiction evidence, and exact signed fields.
The raw geoblock country must exactly match the authority-bound uppercase ISO country code in the
boot config. A raw `blocked: true` for IE, JP, MT, or NL means close-only on the frontend and does
not block API orders; any other blocked country remains API-blocked. Missing, malformed, mismatched,
or unknown response shapes fail closed, and the authenticated `closed_only` check remains an
independent admission gate. The Gamma market query explicitly requests documented direct tags;
missing or mismatched Geopolitics ID/slug evidence fails closed. The supported market subset is
fee-free, zero-delay, non-Neg-Risk, binary Geopolitics. Current jurisdiction, Gamma tag-shape, and
CLOB `nr`/`fd`/`itode` observations and verification dates are recorded in
[`15-SOURCES.md`](15-SOURCES.md); operations are in
[`36-POLYMARKET-V2-CANARY-RUNBOOK.md`](36-POLYMARKET-V2-CANARY-RUNBOOK.md).

## Sports

Sports markets require resolver cards (sub-types in `_GLOSSARY.md`) with game ID, league, start time, official resolution URL if available, auto-cancel behavior, live-state source, and finality rule.

## Crypto

If market rules specify Chainlink stream data, the model is a Chainlink resolver-shadow model, not a generic BTC model. Pair Chainlink shadow with exchange microstructure, Polymarket book state, RTDS where useful, and latency analysis.

## Cost model

```rust
pub struct PolymarketCostModel {
    pub taker_fee_bps: i32,
    pub maker_rebate_bps: i32,
    pub expected_reward_bps: i32,
    pub chain_or_transfer_cost: Decimal,
}
```

Trade net edge, not gross edge.

## Winner-Follow implications for Polymarket

Polymarket is the primary Winner-Follow venue because its public data surfaces support trader-centric reconstruction:

- leaderboard rankings;
- public trades filtered by user/profile address;
- public current and closed positions;
- user activity;
- market data/orderbook websockets for watched markets;
- transaction hashes for timing verification.

The Rust adapter implements:

1. `LeaderboardSnapshot` ingestion across categories and pagination;
2. `TraderTradeEvent` ingestion by watched user;
3. `TraderPositionLedger` reconstruction from trades, activity, and positions;
4. proxy-wallet/funder fields as typed optional public Polymarket metadata;
5. `LeaderSignal` generation for entry/add/trim/exit/flip (definitions in `19-WINNER-FOLLOW-STRATEGY.md`);
6. market websocket subscription management for every watched leader's active markets;
7. copy-order idempotency keyed exactly as in `_GLOSSARY.md` ("Idempotency"): `(leader, source_trade_id, market, outcome, side, observed_at_bucket)` where `observed_at_bucket = floor(observed_at_ms / 1_000)`.

Do not rely on UI scraping. Use official APIs first, then documented public chain data for timing validation where needed.

The venue adapter supplies typed public Polymarket fields and transaction references only; the wallet→operator funding/collateral identity layer was removed in #326.
