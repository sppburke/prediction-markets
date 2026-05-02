# SKILLS.md — Project Skills and Quality Bars

## Skill: Rust 1.95.0 systems implementation

- Use Rust 2024 Edition.
- Pin `rust-toolchain.toml` to `1.95.0`.
- Use `tokio` for async services.
- Use strong newtypes for domain values.
- Use `rust_decimal`/integer ticks for financial math.
- Use typed errors in libraries and contextual errors in binaries.
- Use `tracing` for structured observability.

## Skill: Winner-Follow strategy implementation

Build a public trader-intelligence pipeline:

1. ingest public trader data;
2. reconstruct ledgers;
3. classify trades;
4. rank leaders by walk-forward lower-confidence daily log growth;
5. detect new leader entries quickly;
6. size using calibrated fractional Kelly;
7. emit risk-checked `OrderIntent`;
8. record everything for replay.

Quality bar:

- no future leakage;
- conservative fill modeling;
- robust sample-size shrinkage;
- latency/edge decay measured;
- Kalshi identity restrictions respected;
- live-tiny only after paper-copy validation.

## Skill: Venue adapter implementation

- Read latest official docs before coding.
- Encode venue-specific IDs and order lifecycle states.
- Keep venue adapters separate from strategies.
- Implement fake venue servers before live adapters.
- Reconcile local order journal against venue state.

## Skill: Event sourcing and deterministic replay

- Every source, model, strategy, risk, order, fill, and settlement event is replayable.
- Replay uses the same crates as production.
- Replay output includes deterministic decision hashes.
- CI runs a replay smoke test.

## Skill: Backtesting and anti-overfitting

- Walk-forward only for strategy selection.
- Use bootstrap confidence intervals.
- Penalize profit concentration.
- Include costs, slippage, missed fills, and copy delay.
- Compare live paper-copy against backtest distributions.

## Skill: AWS and Git operations

- Use protected branches.
- Use GitHub Actions OIDC for AWS access.
- Build immutable Docker images.
- Push to ECR.
- Deploy to ECS/EKS with least-privilege IAM.
- Store secrets in Secrets Manager/KMS.
- Emit OpenTelemetry metrics/logs/traces.

## Skill: Source research

- Use official docs first.
- Update `15-SOURCES.md` after each research pass.
- Treat API behavior as stale unless recently verified.
- Record checked date and endpoint status for production-critical assumptions.
