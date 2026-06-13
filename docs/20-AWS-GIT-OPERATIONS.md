# 20 — AWS and Git Operations

> See [`_BASELINE.md`](_BASELINE.md) for the Rust-only implementation rule, lints, and common acceptance gate.
> See [`19-WINNER-FOLLOW-STRATEGY.md`](19-WINNER-FOLLOW-STRATEGY.md) for canonical risk caps that production deployments load from config.

## Repository model

Git with protected branches and mandatory pull requests.

```text
main                 protected, deployable
release/*            optional release stabilization
feature/*            normal development
hotfix/*             emergency fixes
```

No direct pushes to `main`. Every change must pass CI and review.

## Required root files

- `README.md`
- `AGENTS.md`
- `SKILLS.md`
- `rust-toolchain.toml`
- `Cargo.toml`
- `Cargo.lock`
- `deny.toml`
- `.github/workflows/ci.yml`
- `.github/workflows/deploy.yml`
- `infra/` for IaC
- `docs/` for decision records, resolver cards, `_BASELINE.md`, `_GLOSSARY.md`

## CI gates

Every pull request runs:

```bash
rustc --version              # must contain 1.95.0
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --doc --workspace --all-features
cargo nextest run --workspace --all-features
cargo deny check
cargo audit
cargo metadata --locked
```

Additional gates:

- docs check: `_BASELINE.md`, `_GLOSSARY.md`, `AGENTS.md`, `SKILLS.md` exist and are linked from `README.md`;
- no-secrets check;
- Docker build check;
- deterministic replay smoke test;
- AWS IAM policy linter for infrastructure changes.

## AWS deployment architecture (canonical)

Default region: **us-east-1** (closest to Polymarket and Kalshi public endpoints based on observed latency; promote a service to a different region only after `latency-attribution-profiler` shows ≥ 50 ms regional advantage sustained over 7 days).

```text
GitHub Actions OIDC
  -> AWS IAM deploy role (least privilege per service)
  -> ECR immutable image push (tag = git SHA)
  -> ECS Fargate service update (default; see migration trigger in 06-)
  -> CloudWatch + OpenTelemetry collector -> hosted Grafana / OTLP backend
  -> S3 event archive (Parquet via DataFusion)
  -> RDS Aurora-Postgres (Aurora-PostgreSQL-compatible) for metadata
  -> AWS Secrets Manager / KMS for venue keys, signing material
```

VPC layout:

- one VPC per environment (`dev`, `staging`, `prod`);
- private subnets across two AZs;
- VPC endpoints for ECR, S3, Secrets Manager, KMS, CloudWatch;
- NAT for outbound public-API calls only on source/venue gateway services;
- security groups locked per service;
- no public ALB unless `operator-api` is explicitly exposed (use private API + bastion or AWS SSO).

IaC tool: **Terraform** is the default. OpenTofu is acceptable as a drop-in. Pulumi/CDK are not used in production. Manual click-build is forbidden except for emergency break-glass steps that are codified within 7 days.

Database choice: **RDS Aurora-Postgres** (PostgreSQL-compatible, cluster mode). Plain RDS-Postgres is used only for `dev`. Reasons: replay determinism benefits from Aurora's read-replica fan-out for backtest queries; failover is faster than single-AZ RDS.

## Runtime services

Winner-Follow MVP services:

1. `source-gateway-trader`
2. `trader-ledger-builder`
3. `leader-ranker`
4. `copy-signal-engine`
5. `strategy-winner-follow`
6. `risk-supervisor`
7. `execution-router`
8. `reconciler`
9. `ops-api`

Later resolver-source services add weather, crypto, sports, macro, charts, and event feeds.

## Environments

| Environment | Purpose | Trading permissions |
|---|---|---|
| local | fake venues/sources | none |
| dev | integration tests | none |
| staging | paper/shadow | no live orders |
| live-tiny | smallest real capital | capped per `19-` |
| production | promoted strategies only | capped per `19-` and monitored |

## Secrets

- Use AWS Secrets Manager or KMS-backed equivalent.
- Use GitHub Actions OIDC, not long-lived AWS keys.
- No venue secrets in `.env` files committed to Git.
- Production signing keys are separated from ordinary service credentials.
- Rotate keys after incident, employee/contractor departure, or suspicious activity.
- The `flip_human_approved` and `kelly_fraction_above_default_human_approved` flags (`_GLOSSARY.md`) require a signed config change; both transitions are audit-logged.

## Observability

Every service emits structured logs with `tracing`, OpenTelemetry metrics, health endpoints, lag/backlog metrics, venue API error rates, copy delay histograms (against the budget in `_GLOSSARY.md`), order lifecycle events, risk state, and kill-switch state.

## Incident response

Severity levels:

- SEV0: unauthorized order/signing activity, risk cap breach, secret leak.
- SEV1: strategy making live decisions from stale/corrupt data.
- SEV2: venue/source degradation causing paper/live divergence.
- SEV3: non-trading service issue.

SEV0 automatic actions:

1. cancel open orders;
2. disable new orders;
3. freeze execution-router;
4. snapshot state;
5. alert operator;
6. require human review to resume.
