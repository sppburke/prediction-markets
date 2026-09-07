# SKILLS.md — Project Skills and Quality Bars

> See [`_BASELINE.md`](_BASELINE.md), [`_GLOSSARY.md`](_GLOSSARY.md), and [`AGENTS.md`](AGENTS.md) before any task.

## Skill: Rust 1.95.0 systems implementation

- Use Rust 2024 Edition.
- Pin `rust-toolchain.toml` to `1.95.0` (per `_BASELINE.md`).
- Use `tokio` for async services.
- Use strong newtypes for domain values (canonical list in `_GLOSSARY.md` "Type aliases").
- Use `rust_decimal`/integer ticks for financial math.
- Use typed errors in libraries and contextual errors in binaries.
- Use `tracing` for structured observability.

## Skill: Winner-Follow strategy implementation

Build a public trader-intelligence pipeline:

1. ingest public trader data;
2. reconstruct ledgers;
3. classify trades (rules in `19-`);
4. rank leaders by walk-forward LCB_5pct daily log growth;
5. detect new leader entries quickly (latency budget in `_GLOSSARY.md`);
6. size using calibrated fractional Kelly (fractions and caps in `19-`);
7. emit risk-checked `OrderIntent`;
8. record everything for replay.

Quality bar:

- no future leakage;
- conservative fill modeling;
- robust sample-size shrinkage (Bayesian prior, see `19-` § p estimation);
- latency/edge decay measured against the production budget in `_GLOSSARY.md`;
- Kalshi identity restrictions respected (`07-`);
- live-tiny only after the sealed observed-paper gate in `_GLOSSARY.md` and one post-`Pass` review.

## Skill: Venue adapter implementation

- Read latest official docs before coding (`21-`).
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
- Use predeclared uncertainty calculations appropriate to the research question; qualification
  reuses the existing pure lower-five-percent daily-growth bound.
- Penalize profit concentration.
- Include costs, slippage, missed fills, and copy delay.
- Keep backtests as research/regression evidence. Paper qualification replays one sealed observed
  system exactly and does not compare it with a simulator distribution.

## Skill: AWS and Git operations

- Use protected branches.
- Use GitHub Actions OIDC for AWS access.
- Build immutable Docker images.
- Push to ECR.
- Deploy to ECS Fargate by default (migration trigger in `06-`).
- Store secrets in Secrets Manager/KMS.
- Emit OpenTelemetry metrics/logs/traces.

## Skill: Source research

- Use official docs first.
- Update `15-SOURCES.md` `Last checked` and `Re-verify by` after each research pass.
- Treat API behavior as stale per the per-class TTLs in `15-SOURCES.md`.
- Record checked date and endpoint status for production-critical assumptions.
