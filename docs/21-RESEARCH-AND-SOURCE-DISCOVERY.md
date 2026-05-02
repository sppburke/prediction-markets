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

## Documentation output

Every research pass updates:

- `15-SOURCES.md` with links and dates checked;
- the relevant venue file;
- any affected resolver card template;
- `AGENTS.md` if coding instructions change.

## Staleness rule

If a fact could have changed after the last implementation pass, do not rely on memory. Re-check the official source before coding.
