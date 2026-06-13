# 28 — Operator-Graph Archive (removed in #326)

> **Status: ARCHIVE.** Nothing in this document describes live code. It preserves
> the design rationale, the anti-gaming taxonomy, and the unconfirmed issue-#141
> hypothesis from the operator/funder-graph machinery that was removed end-to-end
> in issue #326 (PRs 1–6). The canonical live strategy is now per-wallet
> `leader_follow`-only — see `docs/19-WINNER-FOLLOW-STRATEGY.md`.

## Why this was removed

Winner-Follow originally modelled a **wallet → operator** identity: a set of
trading wallets believed to be controlled by one entity, reconstructed from
on-chain funding/collateral edges (proxy-wallet deploys, pUSD/USDC funding,
bridge/onramp deposits). On top of that identity it ran **three signal modes**:

- `leader_follow` — ordinary per-wallet copy-trading.
- `inherited_prior_first_trade` — a fresh wallet linked to a known operator
  inherits a shrunk prior from that operator's history (defaulted to **paper**).
- `cluster_coordination` — same-operator wallets entering the same
  `(market, outcome, side)` within a window (defaulted to **shadow**).

In production the operator machinery only ever fed the two **never-promoted**
shadow/paper modes plus a latent live-misclassification hazard (a fresh wallet
could be reclassified `FreshWalletFirstTrade` one env-var away from corrupting
the paper tape). It carried a large cost: 3 crates (`operator-graph`,
`funding-graph`, `source-onchain-polygon`), 5 bootstrap subcommands, the live
Polygon connector + scheduler, and the `counterparty_edges`/funder tables that
were roughly **half** of the ~360 GB `wallet_cache.db`. Copy decisions are now
purely per-wallet deterministic criteria (time-to-resolution, risk-adjusted
returns, entry price band, buy-and-hold-to-resolution), so the entire apparatus
was purged.

## Deleted crates (full source in git history)

| Crate | Role |
|-------|------|
| `pe-source-onchain-polygon` | Polygon PoS `eth_getLogs` connector: CTF/exchange contract scans, `OrderFilled` decode, Etherscan funder lookups, wallet enumeration. |
| `pe-operator-graph` | Pure (no-I/O) wallet→operator clustering + operator identity (`OperatorId` = blake3 of the funding-root set). |
| `pe-funding-graph` | Time-ordered funding-edge accumulator (`FunderGraphTimeline`) for walk-forward operator reconstruction. |

Their last live state is at the commit immediately before #326 PR5
(`origin/main` history). The minimal Polygon-RPC primitives still needed by the
surviving market-resolution scan were relocated into
`crates/bootstrap/src/chain.rs` in #326 PR4 (`CTF`, `CTF_DEPLOY_BLOCK`,
`TOPIC_CONDITION_RESOLUTION`, `ALL_EXCHANGE_CONTRACTS`,
`ALL_ORDER_FILLED_TOPICS`, `eth_get_logs_bisect`).

Removed `core-types` types (#326 PR5): `OperatorId`, `FunderRootId`,
`FundingHopCount`, `WalletAgeSeconds`, `ClusterSize`, `InheritedPriorPpm`,
`WinnerFollowSignalKind`. (`ReconstructionQuality` and `LeaderAction` survive.)

## Anti-gaming taxonomy (operator-clustering flags — archived)

The operator path computed anti-gaming flags that gated promotion of the
operator-scoped modes. The flags specific to operator clustering — bait-wallet
suspicion, wash-cluster detection, funder-seeding-rate anomalies, cluster
membership instability, and unproven proxy-funder mappings — are archived here
because they only have meaning against an operator identity that no longer
exists. Per-wallet anti-gaming heuristics (thin wallet history, market
narrowness) that operate without an operator identity survive in
`docs/19-WINNER-FOLLOW-STRATEGY.md` where still applicable.

## Issue #141 — `skip_unknown_operator` gate (UNCONFIRMED hypothesis)

The backtest carried a signal-time gate (`PE_BACKTEST_SKIP_UNKNOWN_OPERATOR`,
issue #141) that suppressed BUY signals from watchlisted leaders whose wallet had
no resolved operator identity in the funder graph. `skip_unknown_operator = true`
caused the BUY arm to `continue` whenever `op_identity.is_none()`; the SELL arm
was unaffected. The discriminator was the already-computed
`wallet_to_operator.get(&leader)`, so the gate added no new graph traversal.
Placement: after the per-market cap and before the horizon-cooldown filter, so
both flat-USD and Kelly sizing paths honoured it. Suppression was reported as
`unknown_operator_suppression_pct` (global) and
`unknown_operator_suppression_by_quarter`.

**Motivation:** A1 oracle-lift analysis on the 2026-05-09 sweep attributed
**+$23.99** to skipping unmapped wallets — they were an oversized share of
negative PnL. The gate defaulted `true`, firing uniformly when the funder-edge
cache was empty/stale and for individual wallets the clustering did not attach
to any operator.

**Hypothesis (NOT empirically confirmed):** thin-history leaders are
statistically underpowered and are expected to contribute disproportionately to
negative PnL in the unknown-operator bucket. The preset's lift was never
confirmed; a post-merge sweep against the current cache was the planned
confirmation step (after the cap-the-bleed gates #141 / #142 landed). The code
was deleted in #326 PR2 before that sweep ran, so the hypothesis remains **open**
— not refuted. If revisited, re-express it as a per-wallet history-depth
criterion (which needs no operator graph), validate it, and only then re-add.

> Related dead lever (kept for context): tightening `PE_BACKTEST_MIN_QUALITY`
> would filter conviction operators for structural data reasons (the Polymarket
> CLOB API returns no redemption events, so buy-and-hold-to-resolution traders
> score `reconstruction_quality = 0` regardless of performance) — the wrong lever
> for the same goal.
