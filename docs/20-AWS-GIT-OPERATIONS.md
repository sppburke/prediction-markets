# 20 — AWS and Git Operations

> **Rust-only implementation rule:** all first-party production services, clients, parsers, models, replay tools, CLIs, and test harnesses are implemented in **Rust 2024 Edition pinned to stable Rust 1.95.0**.

## Repository model

Use Git with protected branches and mandatory pull requests.

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
- `docs/` for decision records and resolver cards

## CI gates

Every pull request runs:

```bash
rustc --version              # must contain 1.95.0
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo nextest run --workspace --all-features
cargo deny check
cargo audit
cargo metadata --locked
```

Additional gates:

- docs check: `AGENTS.md` and `SKILLS.md` exist and are linked from `README.md`;
- no secrets check;
- Docker build check;
- deterministic replay smoke test;
- AWS IAM policy linter for infrastructure changes.

## AWS deployment architecture

Default:

```text
GitHub Actions OIDC
  -> AWS IAM deploy role
  -> ECR immutable image push
  -> ECS service update
  -> CloudWatch/OpenTelemetry dashboards
  -> S3 event archive
  -> Aurora/Postgres metadata
  -> Secrets Manager/KMS for secrets
```

Use Terraform, OpenTofu, Pulumi, or AWS CDK. Do not click-build production resources by hand except emergency break-glass steps that are later codified.

## Runtime services

Winner-Follow MVP services:

1. `source-gateway-trader`
2. `source-gateway-onchain-polygon`
3. `operator-graph-worker`
4. `trader-ledger-builder`
5. `leader-ranker`
6. `copy-signal-engine`
7. `strategy-winner-follow`
8. `risk-supervisor`
9. `execution-router`
10. `reconciler`
11. `ops-api`

Later resolver-source services add weather, crypto, sports, macro, charts, and event feeds.

## Environments

| Environment | Purpose | Trading permissions |
|---|---|---|
| local | fake venues/sources | none |
| dev | integration tests | none |
| staging | paper/shadow | no live orders |
| live-tiny | smallest real capital | capped |
| production | promoted strategies only | capped and monitored |

## Secrets

- Use AWS Secrets Manager or KMS-backed equivalent.
- Use GitHub Actions OIDC, not long-lived AWS keys.
- No venue secrets in `.env` files committed to Git.
- Production signing keys should be separated from ordinary service credentials.
- Rotate keys after incident, employee/contractor departure, or suspicious activity.

## Observability

Every service emits structured logs with `tracing`, OpenTelemetry metrics, health endpoints, lag/backlog metrics, venue API error rates, copy delay histograms, order lifecycle events, risk state, and kill-switch state.

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
