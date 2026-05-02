# 14 — Compliance and Risk

> **Rust-only implementation rule:** all first-party production services, clients, parsers, models, replay tools, CLIs, and test harnesses are implemented in **Rust 2024 Edition pinned to stable Rust 1.95.0**. Non-Rust components are permitted only as external infrastructure daemons, vendor APIs, operating-system services, managed databases, or public data sources. No production hot-path Python, Node, or browser automation is allowed.

## Objective

Prevent false edges, source misuse, venue-rule mistakes, operational failures, and legal/contractual problems.

## Primary principle

Use official, permitted, licensed, or clearly public sources. Trade the rules as written. Do not bypass access controls, misuse restricted feeds, rely on non-public material, or manipulate venues.

## Compliance gates

Every live strategy requires:

- source access review;
- venue terms/API review;
- data license notes;
- rate-limit behavior;
- market-family legal risk note;
- non-public information exclusion;
- manipulation avoidance;
- jurisdiction/account eligibility check.

## Risk engine

`risk-engine` contains pure replayable checks:

- account-level max loss;
- venue-level exposure;
- market-level exposure;
- family-level exposure;
- source-health gate;
- resolver-card tradability gate;
- stale-model gate;
- order-rate gate;
- reject-rate gate;
- venue-reconciliation gate;
- cross-venue fake-hedge gate.

## False edge taxonomy

- **Wrong resolver:** predicted a real fact that does not settle the market.
- **Wrong timing window:** correct data but wrong window/time zone.
- **Wrong finality:** preliminary/revised/disputed source treated as final.
- **Microstructure miss:** edge erased by fees, slippage, queue, or fill.
- **Source failure:** stale, wrong, rate-limited, or schema-changed source.
- **Cross-venue fake hedge:** correlated markets treated as identical.

## Operational controls

- idempotent submissions;
- durable local order journal;
- startup/reconnect reconciliation;
- cancel-on-disconnect policy;
- manual and automatic kill switches;
- immutable audit log;
- replayable decisions;
- per-strategy capital caps.

## Security

- no secrets in logs/traces;
- separate paper/live keys;
- secret redaction in config;
- dependency audit;
- minimal containers;
- non-root runtime;
- signing modules isolated.

## Responsible scaling

Scale only when backtest, shadow, paper, and live-tiny behavior agree.


## Common acceptance gate

This file is complete only when the implementation:
1. compiles as Rust 2024;
2. uses typed IDs, prices, probabilities, quantities, timestamps, and resolver states;
3. writes replayable events with raw payload hashes;
4. has fixture tests and deterministic replay;
5. blocks live execution when source, resolver, venue, or risk state is invalid.


## Winner-Follow compliance and risk

Winner-Follow must be implemented as public-data analysis and user-authorized copying only.

Rules:

- Use official APIs, public blockchain data, public leaderboards, and authorized data.
- Respect venue terms, rate limits, API restrictions, and geographic/account restrictions.
- Do not attempt to deanonymize Kalshi traders from anonymous public trades.
- Do not use hacked, leaked, private, or access-controlled data.
- Do not market the system as guaranteed returns.
- Display drawdown, ruin, liquidity, and copy-delay risk in operator dashboards.
- Require explicit human approval before increasing Kelly fraction, bankroll, or venue permissions.

Risk controls specific to Winner-Follow:

- leader concentration caps;
- market-family concentration caps;
- crowding/correlation caps;
- automatic demotion after live underperformance;
- copy-latency kill switch;
- no copy entries during venue/API incident states;
- no copying if the leader's current position cannot be reconstructed confidently.
