# 21 — Research and Source Discovery Protocol

> **Rust-only implementation rule:** all first-party production code is Rust 2024 on stable Rust 1.95.0. Research may read websites, APIs, PDFs, and official docs, but generated production code remains Rust.

## Objective

Prevent stale assumptions. Before implementing or changing any venue adapter, source connector, or strategy logic, the coding agent must look up the latest official documentation and record what changed.

## Required lookup order

1. Official venue docs.
2. Official venue API reference or `llms.txt` if offered.
3. Official help-center market rules.
4. Official source/resolution pages named in market rules.
5. Public regulatory/terms pages where relevant.
6. Public chain data or public API examples only after official docs are reviewed.

## Winner-Follow source checks

Before coding trader-copy logic, verify:

- whether the venue exposes trader-level public history;
- whether public trade feeds identify users;
- leaderboard availability and participation rules;
- rate limits and allowed uses;
- fields available for trades, positions, activity, and settlement;
- whether the endpoint is official, stable, beta, deprecated, or third-party.
- for Polymarket, how `proxyWallet`, signature type, funder address, pUSD collateral, deposit addresses, and bridge/onramp flows map to public wallet identity;
- whether the proxy/funder/collateral mapping is derivable from public chain data and official docs for representative historical examples;
- whether any third-party data source has an authorized replayable API/export, or is only an opaque UI product.

## Operator graph source checks

Before coding `source-onchain-polygon` or `operator-graph`, verify:

- official Polymarket contract addresses and deployment/factory docs;
- public event signatures for proxy-wallet, pUSD, USDC/USDC.e, deposit/onramp, and collateral flows;
- chain provider archive access, rate limits, reorg behavior, and allowed trading use;
- exchange/bridge/hot-wallet label source and versioning policy;
- strict versus transitive cluster rule version and replay determinism;
- anti-gaming flags and whether each is computable from public data at time `t`;
- degradation behavior when the on-chain source is stale or unavailable.

## Documentation output

Every research pass updates:

- `15-SOURCES.md` with links and dates checked;
- the relevant venue file;
- any affected resolver card template;
- `AGENTS.md` if coding instructions change.

## Staleness rule

If a fact could have changed after the last implementation pass, do not rely on memory. Re-check the official source before coding.
