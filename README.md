# prediction-markets

Rust-only, resolver-first, cross-venue trading/research stack for Polymarket and Kalshi. The first deployable strategy is **Winner-Follow** (Strategy 0).

See [`docs/`](docs/) for the full specification:

- [`docs/README.md`](docs/README.md) — package index and v5 overview
- [`docs/_BASELINE.md`](docs/_BASELINE.md) — Rust toolchain, lints, common acceptance gate
- [`docs/_GLOSSARY.md`](docs/_GLOSSARY.md) — vocabulary, type aliases, latency budget, rate limits, configuration defaults
- [`docs/19-WINNER-FOLLOW-STRATEGY.md`](docs/19-WINNER-FOLLOW-STRATEGY.md) — canonical risk caps, Kelly fractions, eligibility thresholds, promotion ladders
- [`docs/AGENTS.md`](docs/AGENTS.md) — coding-agent operating instructions
- [`docs/SKILLS.md`](docs/SKILLS.md) — project skill definitions and quality gates

## Quick start

```bash
# 1. Install toolchain and cargo tools
rustup toolchain install 1.95.0
cargo install cargo-nextest cargo-deny cargo-audit

# 2. Configure environment
cp .env.example .env
# Fill in the env-driven inputs; ordinary live credentials use the sealed #508 custody path, not .env

# 3. Provision a seeded schema-two cache using docs/26 (linked below).
#    Requires a verified finalized format-three drops source and a complete seed-wallet file.

# 4a. Run backtest (requires a completed seeded cache)
cargo run --release --bin pe-backtest

# 4b. Or run the acceptance gate
cargo nextest run --workspace --all-features
```

Follow the [15/5-minute loop: seeded cache procedure](docs/26-DATA-REFRESH-AND-REOPTIMIZATION-RUNBOOK.md#155-minute-loop-seeded-cache-switch-and-revert),
including its seed, carried-drop and capacity prerequisites. Legacy `fetch`, `backfill`, `all`,
no-argument and positional-TOML bootstrap invocations refuse before cache creation (#842).
`cache-init` retains its seed and finalized format-three drops-source requirements.

See [`docs/22-ONBOARDING.md`](docs/22-ONBOARDING.md) for the full environment-variable catalogue, startup sequence, and first-run recipes (backtest-only, paper trading, live-tiny).

## Toolchain

Rust 2024 Edition pinned to stable Rust 1.95.0. See `_BASELINE.md`.
