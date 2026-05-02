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
- RTDS connector where useful;
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
4. proxy-wallet/funder fields as typed optional identity evidence for `operator-graph`;
5. `LeaderSignal` generation for entry/add/trim/exit/flip with separate signal-kind annotations (definitions in `19-WINNER-FOLLOW-STRATEGY.md`);
6. market websocket subscription management for every watched leader's active markets;
7. copy-order idempotency keyed exactly as in `_GLOSSARY.md` ("Idempotency"): `(leader, source_trade_id, market, outcome, side, observed_at_bucket)` where `observed_at_bucket = floor(observed_at_ms / 1_000)`. Cluster-coordination signals add `operator_id` to the key.

Do not rely on UI scraping. Use official APIs first, then documented public chain data for timing validation where needed.

Funding/collateral identity is not owned by `venue-polymarket`. It is produced by `source-onchain-polygon` and `operator-graph`; the venue adapter supplies typed public Polymarket fields and transaction references.
