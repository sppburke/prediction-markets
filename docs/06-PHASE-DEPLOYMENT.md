# 06 — Phase Deployment

> **Rust-only implementation rule:** all first-party production services, clients, parsers, models, replay tools, CLIs, and test harnesses are implemented in **Rust 2024 Edition pinned to stable Rust 1.95.0**. Non-Rust components are permitted only as external infrastructure daemons, vendor APIs, operating-system services, managed databases, or public data sources. No production hot-path Python, Node, or browser automation is allowed.

## Objective

Deploy the Rust system as reliable, observable, replayable, low-latency services with fast failure detection and conservative risk controls.

## Service topology

```text
source-gateway-trader       Rust binary
leader-ranker               Rust binary
copy-signal-engine          Rust binary
source-gateway-weather       Rust binary
source-gateway-crypto        Rust binary
source-gateway-sports        Rust binary
source-gateway-macro         Rust binary
source-gateway-charts        Rust binary
venue-gateway-kalshi         Rust binary
venue-gateway-polymarket     Rust binary
resolver-worker              Rust binary
model-worker                 Rust binary
strategy-winner-follow       Rust binary
strategy-worker              Rust binary
execution-router             Rust binary
risk-supervisor              Rust binary
operator-api                 Rust axum service
operator-cli                 Rust CLI
replay-cli                   Rust CLI
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
4. **live-tiny:** live orders with strict caps.
5. **scaled-live:** only after metrics prove stability.

## Health endpoints

Use `axum` for:

- `/health/live`
- `/health/ready`
- `/health/sources`
- `/health/venues`
- `/metrics`

Readiness must block live trading if critical sources, venue sockets, account reconciliation, or risk state are unhealthy.

## Observability

Every order must be trace-linked:

```text
source event -> normalized event -> feature snapshot -> fair value -> strategy decision -> risk decision -> order submission -> venue ack -> fill -> settlement
```

Metrics: source staleness, source parse latency, schema drift, venue book age, delta gaps, order ack latency, cancel latency, model latency, risk block counts, replay/live mismatch count.

## Reconciliation

On startup or reconnect:

1. load local order journal;
2. fetch venue open orders;
3. fetch balances/positions;
4. reconcile unknown states;
5. cancel stale orders if configured;
6. block trading until reconciliation passes.

## Containers

Use multi-stage Rust builds, minimal runtime image, non-root user, read-only filesystem where possible, embedded git SHA, explicit health check.


## Common acceptance gate

This file is complete only when the implementation:
1. compiles as Rust 2024;
2. uses typed IDs, prices, probabilities, quantities, timestamps, and resolver states;
3. writes replayable events with raw payload hashes;
4. has fixture tests and deterministic replay;
5. blocks live execution when source, resolver, venue, or risk state is invalid.


## AWS + Git deployment standard

The project is Git-hosted and deployed on AWS. The default production path is:

```text
GitHub/Git remote
  -> protected main branch
  -> pull request checks
  -> GitHub Actions with AWS OIDC
  -> cargo test/clippy/audit/deny/nextest
  -> Docker build with rust:1.95.0 builder image
  -> ECR immutable image tag by git SHA
  -> ECS service or EKS deployment
  -> private subnets + Secrets Manager + CloudWatch/OpenTelemetry
  -> S3/Parquet event archive + Postgres/Aurora metadata + optional NATS/Redpanda stream
```

### Winner-Follow deployment services

- `source-gateway-trader`: pulls public trader/profile/trade data and market websockets.
- `trader-ledger-builder`: reconstructs per-trader positions.
- `leader-ranker`: produces top-50 active and incubator lists.
- `copy-signal-engine`: converts newly observed leader trades into classified signals.
- `strategy-winner-follow`: emits risk-checked order intents.
- `execution-router`: submits/cancels/reconciles venue orders.
- `risk-supervisor`: enforces bankroll, exposure, drawdown, and kill-switch limits.

### AWS service placement

Start on ECS Fargate for operational simplicity. Promote latency-sensitive services to ECS on EC2 or EKS only when measurements show Fargate jitter is hurting fills. Use Graviton instances where dependencies support `aarch64-unknown-linux-gnu` and benchmark against x86.

### Secrets and keys

- Store venue API keys, wallet keys, and signing material in AWS Secrets Manager or KMS-backed secure stores.
- No secrets in Git, Docker images, task definitions, logs, traces, or panic messages.
- Production signing should be isolated behind a minimal internal signing service with allowlisted order-intent schemas.

### Release gates

A deployment cannot reach live mode unless:

1. `rustc --version` is 1.95.0;
2. all tests pass under `cargo nextest`;
3. `cargo clippy --all-targets --all-features -D warnings` passes;
4. `cargo deny` and `cargo audit` pass or have explicit reviewed exceptions;
5. replay determinism hash matches between CI and staging;
6. live-tiny bankroll caps are configured;
7. kill switch has been manually tested in staging.
