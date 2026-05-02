# 21 — Research and Source Discovery Protocol

> See [`_BASELINE.md`](_BASELINE.md) for the Rust-only implementation rule (research may read websites, APIs, PDFs, and official docs, but generated production code remains Rust).
> See [`15-SOURCES.md`](15-SOURCES.md) for the per-class TTL table and tracked links.

## Objective

Prevent stale assumptions. Before implementing or changing any venue adapter, source connector, or strategy logic, the coding agent must look up the latest official documentation and record what changed.

## Required lookup order

1. Official venue docs.
2. Official venue API reference or `llms.txt` if offered.
3. Official help-center market rules.
4. Official source/resolution pages named in market rules.
5. Public regulatory/terms pages where relevant.
6. Public chain data or public API examples only after official docs are reviewed.

## Per-class TTL

The per-class TTL table is the canonical decision rule for "is this fact stale?". It lives in `15-SOURCES.md` ("Per-class re-verification TTL"). Summary:

| Class | TTL |
|---|---:|
| Venue API reference | 60 days |
| Venue help-center market rules | 14 days |
| Public source resolver pages | 60 days |
| Public chain provider docs | 90 days |
| Rust ecosystem docs | 180 days |
| AWS service docs | 90 days |
| Third-party research references | re-check before each use |

If `today - last_checked > TTL` for any link about to be relied on, re-verify before coding. Update `15-SOURCES.md` `Last checked` and `Re-verify by` columns in the same PR.

## Winner-Follow source checks

Before coding trader-copy logic, verify:

- whether the venue exposes trader-level public history;
- whether public trade feeds identify users;
- leaderboard availability and participation rules;
- rate limits and allowed uses (update `_GLOSSARY.md` "Venue rate limits" if changed);
- fields available for trades, positions, activity, and settlement;
- whether the endpoint is official, stable, beta, deprecated, or third-party;
- for Polymarket: how `proxyWallet`, signature type, funder address, pUSD collateral, deposit addresses, and bridge/onramp flows map to public wallet identity;
- whether the proxy/funder/collateral mapping is derivable from public chain data and official docs for representative historical examples;
- whether any third-party data source has an authorized replayable API/export, or is only an opaque UI product.

## Operator graph source checks

Before coding `source-onchain-polygon` or `operator-graph`, verify:

- official Polymarket contract addresses and deployment/factory docs;
- public event signatures for proxy-wallet, pUSD, USDC/USDC.e, deposit/onramp, and collateral flows;
- chain provider archive access, rate limits, reorg behavior, and allowed trading use;
- exchange/bridge/hot-wallet label source and versioning policy;
- strict versus transitive cluster rule version and replay determinism;
- anti-gaming flags and whether each is computable from public data at time `t` (thresholds in `_GLOSSARY.md`);
- degradation behavior when the on-chain source is stale or unavailable (`onchain_block_lag_warn` / `onchain_block_lag_block`).

## Documentation output

Every research pass updates:

- `15-SOURCES.md` with links, `Last checked`, and `Re-verify by` dates;
- the relevant venue file (`07-`, `08-`);
- any affected resolver card template;
- `_GLOSSARY.md` if rate limits, freshness defaults, or thresholds changed;
- `AGENTS.md` if coding instructions change.

## Staleness rule

If a fact could have changed since the last `Last checked` date, do not rely on memory. Re-check the official source before coding.
