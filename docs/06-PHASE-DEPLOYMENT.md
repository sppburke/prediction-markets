# 06 — Phase Deployment

> See [`_BASELINE.md`](_BASELINE.md) for the Rust-only implementation rule and common acceptance gate.
> See [`_GLOSSARY.md`](_GLOSSARY.md) for production latency budget and rate-limit defaults.
> See [`19-WINNER-FOLLOW-STRATEGY.md`](19-WINNER-FOLLOW-STRATEGY.md) for canonical risk caps and promotion ladders.
> See [`20-AWS-GIT-OPERATIONS.md`](20-AWS-GIT-OPERATIONS.md) for the canonical AWS deployment architecture (region, VPC, RDS engine).

## Objective

Deploy the Rust system as reliable, observable, replayable, low-latency services with fast failure detection and conservative risk controls.

## Service topology

```text
source-gateway-trader              Rust binary
trader-ledger-builder              Rust binary
leader-ranker                      Rust binary
copy-signal-engine                 Rust binary
source-gateway-weather             Rust binary
source-gateway-crypto              Rust binary
source-gateway-sports              Rust binary
source-gateway-macro               Rust binary
source-gateway-charts              Rust binary
venue-gateway-kalshi               Rust binary
venue-gateway-polymarket           Rust binary
resolver-worker                    Rust binary
model-worker                       Rust binary
strategy-winner-follow             Rust binary
strategy-worker                    Rust binary
execution-router                   Rust binary
risk-supervisor                    Rust binary
operator-api                       Rust axum service
operator-cli                       Rust CLI
replay-cli                         Rust CLI
```

## Runtime model

- Tokio multi-threaded runtime for I/O services.
- CPU-bound model work isolated from socket loops.
- Bounded channels everywhere.
- Cancellation tokens for graceful shutdown.
- Per-task `tracing` spans.
- Explicit timeouts on external calls.
- Startup reconciliation before live trading.

## Deployment modes

1. **record-only:** collect sources and venue data.
2. **shadow:** run models/strategies without orders.
3. **paper:** route intents to fake/sandbox venue.
4. **live-tiny:** live orders with strict caps from `19-`.
5. **scaled-live:** only after metrics prove stability per the promotion criteria in `_GLOSSARY.md`.

## Health endpoints

`axum`:

- `/health/live`
- `/health/ready`
- `/health/sources`
- `/health/venues`
- `/metrics`

Readiness blocks live trading if critical sources, venue sockets, account reconciliation, or risk state are unhealthy.

## Observability

Every order must be trace-linked:

```text
source event -> normalized event -> feature snapshot -> fair value -> strategy decision
            -> risk decision -> order submission -> venue ack -> fill -> settlement
```

Metrics: source staleness, source parse latency, schema drift, venue book age, delta gaps, order ack latency, cancel latency, model latency, risk block counts, replay/live mismatch count, p50/p95/p99 of the latency budget in `_GLOSSARY.md`.

The **copy-latency kill switch** described in `19-` is wired to the p95 metric: when running p95 over the prior hour exceeds budget by 50 % for two consecutive 5-minute windows, `risk-supervisor` raises `CopyLatencyKillSwitch` and `execution-router` blocks new entries until p95 returns under budget.

## Reconciliation

On startup or reconnect:

1. load local order journal;
2. fetch venue open orders;
3. fetch balances/positions;
4. reconcile unknown states;
5. cancel stale orders if configured;
6. block trading until reconciliation passes.

## Containers

Multi-stage Rust builds, minimal runtime image, non-root user, read-only filesystem where possible, embedded git SHA, explicit health check.

## AWS deployment

The full architecture (region, VPC, IaC tool, RDS engine choice, secrets) is in `20-AWS-GIT-OPERATIONS.md`. Default summary:

```text
GitHub/Git remote
  -> protected main branch
  -> pull request checks
  -> GitHub Actions with AWS OIDC
  -> cargo test/clippy/audit/deny/nextest
  -> Docker build with rust:1.95.0 builder image
  -> ECR immutable image tag by git SHA
  -> ECS Fargate (default; see migration trigger below)
  -> private subnets + Secrets Manager + CloudWatch/OpenTelemetry
  -> S3/Parquet event archive + RDS Aurora-Postgres metadata + optional NATS/Redpanda stream
```

### Compute placement

**Default: ECS Fargate** for operational simplicity.

**Migration trigger to ECS-on-EC2 or EKS:** any of these, sustained over 7 days, promotes a service:

- p95 venue-ack latency exceeds the budget in `_GLOSSARY.md` by ≥ 50 %, AND latency-attribution-profiler attributes ≥ 100 ms median to Fargate scheduling jitter (verified by comparison run on EC2);
- service requires features Fargate does not support (kernel module, host networking, GPU);
- aggregate Fargate cost exceeds equivalent EC2 cost by ≥ 30 % at sustained throughput.

Otherwise stay on Fargate. Use Graviton (`aarch64-unknown-linux-gnu`) where dependencies support it and benchmarks show no regression vs. x86.

### Winner-Follow deployment services

- `source-gateway-trader`: pulls public trader/profile/trade data and market websockets.
- `trader-ledger-builder`: reconstructs per-trader positions.
- `leader-ranker`: produces top-`active_watchlist_size` lists.
- `copy-signal-engine`: converts newly observed leader trades into classified actions.
- `strategy-winner-follow`: emits risk-checked order intents.
- `execution-router`: submits/cancels/reconciles venue orders.
- `risk-supervisor`: enforces bankroll, exposure, drawdown, kill-switch, and copy-latency limits.

### Secrets and keys

- Store venue API keys, wallet keys, and signing material in AWS Secrets Manager or KMS-backed secure stores.
- No secrets in Git, Docker images, task definitions, logs, traces, or panic messages.
- Production signing is isolated behind a minimal internal signing service with allowlisted order-intent schemas.

### Release gates

A deployment cannot reach live mode unless:

1. `rustc --version` is 1.95.0;
2. all tests pass under `cargo nextest`;
3. `cargo clippy --all-targets --all-features -- -D warnings` passes;
4. `cargo deny check` and `cargo audit` pass or have explicit reviewed exceptions;
5. replay determinism hash matches between CI and staging;
6. live-tiny bankroll caps are configured per the canonical TOML in `19-`;
7. kill switch has been manually tested in staging;
8. `flip_human_approved` and `kelly_fraction_above_default_human_approved` flags (`_GLOSSARY.md`) are explicitly false unless a signed config change has set them.
