# 14 — Compliance and Risk

> See [`_BASELINE.md`](_BASELINE.md) for the Rust-only implementation rule and common acceptance gate.
> See [`19-WINNER-FOLLOW-STRATEGY.md`](19-WINNER-FOLLOW-STRATEGY.md) for the canonical risk-block taxonomy with halt scope.

## Objective

Prevent false edges, source misuse, venue-rule mistakes, operational failures, and legal/contractual problems.

## Primary principle

Use official, permitted, licensed, or clearly public sources. Trade the rules as written. Do not bypass access controls, misuse restricted feeds, rely on non-public material, or manipulate venues.

## Compliance review and order eligibility

Record these reviews for each live strategy and apply source, venue, jurisdiction, and account
eligibility checks to each order where they belong. They do not control account mode: the owner's
requested mode is the only on/off switch for ordinary live execution.

- source access review;
- venue terms/API review;
- data license notes;
- rate-limit behavior matches budgets in `_GLOSSARY.md`;
- market-family legal risk note;
- non-public information exclusion;
- manipulation avoidance;
- jurisdiction/account eligibility check.

## Risk engine

`risk-engine` contains pure replayable checks:

- leader, market, family, and total-copy concentration;
- exact proposed-trade size against the resolved cap;
- intraday, rolling-seven-day, and absolute drawdown;
- the historical copy-latency field for replay of semantic-1 decisions; current paper and live evaluations set it to false.

Source health, resolver tradability, authenticated account state, jurisdiction, venue reconciliation,
and reservation/allowance checks remain admission or canary gates around the pure ordinary risk
snapshot; they are not fabricated financial fields.

## False edge taxonomy

- **Wrong resolver:** predicted a real fact that does not settle the market.
- **Wrong timing window:** correct data but wrong window/time zone.
- **Wrong finality:** preliminary/revised/disputed source treated as final.
- **Microstructure miss:** edge erased by fees, slippage, queue, or fill.
- **Source failure:** stale, wrong, rate-limited, or schema-changed source.
- **Cross-venue fake hedge:** correlated markets treated as identical.

## Operational controls

- idempotent submissions (key in `_GLOSSARY.md`);
- durable local order journal;
- startup/reconnect reconciliation;
- cancel-on-disconnect policy;
- manual and automatic kill switches;
- immutable audit log;
- replayable decisions;
- per-strategy capital caps (canonical in `19-`).

## Security

- no secrets in logs/traces;
- separate paper/live keys;
- secret redaction in config;
- dependency audit (`cargo deny`, `cargo audit`);
- minimal containers;
- non-root runtime;
- signing modules isolated;
- `flip_human_approved` and `kelly_fraction_above_default_human_approved` (`_GLOSSARY.md`) are exact hot-configuration inputs; changed economic configuration is hash-bound and seals the current qualification evidence before publication.

## Responsible scaling

The sealed paper stream and backtest remain measurement evidence. The owner's requested mode
controls ordinary live mode; source, resolver, venue, financial, and risk checks govern each order.
Live-tiny is not part of issue #545 qualification.

## Winner-Follow compliance and risk

Rules:

- Use official APIs, public blockchain data, public leaderboards, and authorized data.
- Respect venue terms, rate limits, API restrictions, and geographic/account restrictions.
- Do not attempt to deanonymize Kalshi traders from anonymous public trades.
- Do not use hacked, leaked, private, or access-controlled data.
- Do not use CrowdIntel UI output, opaque proprietary scores, or non-replayable third-party cluster labels in live decisions unless a reviewed license and replayable export/API exist.
- Do not market the system as guaranteed returns.
- Display drawdown, ruin, liquidity, and copy-delay risk in operator dashboards.
- Require explicit human approval before increasing Kelly fraction, bankroll, or venue permissions (via `kelly_fraction_above_default_human_approved`).

Risk controls specific to Winner-Follow are enforced via the canonical TOML in `19-`. Risk-block taxonomy (with halt scope per variant) is also in `19-` ("Risk-block taxonomy and halt scope"); the enum is shared with `risk-engine`:

```rust
// Mirrors `pe_risk_engine::RiskBlock` (operator/funder/cluster/anti-gaming
// variants were removed in #326).
pub enum RiskBlock {
    LeaderConcentrationExceeded,
    MarketConcentrationExceeded,
    FamilyConcentrationExceeded,
    TotalCopyExposureExceeded,
    PerTradeSizeExceeded,
    IntradayDrawdownStop,
    Rolling7dDrawdownStop,
    KillSwitchDrawdown,
    CopyLatencyKillSwitch, // historical replay meaning only
}
```

The halt scope for each variant (this trade, strategy-wide) is documented canonically in `19-`.
Source health remains a separate service-readiness/admission gate; it is not a fabricated field in
the pure risk snapshot. Absolute loss remains latched until its own audited
`risk_halt_release_hash` is synchronized. Intraday and rolling causes release mechanically.
Historical `CopyLatency` records retain their audited-release path, but current decisions ignore
that cause. One cause never releases another.
