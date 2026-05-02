# prediction-markets

Rust-only, resolver-first, cross-venue trading/research stack for Polymarket and Kalshi. The first deployable strategy is **Winner-Follow** (Strategy 0).

See [`docs/`](docs/) for the full specification:

- [`docs/README.md`](docs/README.md) — package index and v5 overview
- [`docs/_BASELINE.md`](docs/_BASELINE.md) — Rust toolchain, lints, common acceptance gate
- [`docs/_GLOSSARY.md`](docs/_GLOSSARY.md) — vocabulary, type aliases, latency budget, rate limits, configuration defaults
- [`docs/19-WINNER-FOLLOW-STRATEGY.md`](docs/19-WINNER-FOLLOW-STRATEGY.md) — canonical risk caps, Kelly fractions, eligibility thresholds, promotion ladders
- [`docs/AGENTS.md`](docs/AGENTS.md) — coding-agent operating instructions
- [`docs/SKILLS.md`](docs/SKILLS.md) — project skill definitions and quality gates

## Toolchain

Rust 2024 Edition pinned to stable Rust 1.95.0. See `_BASELINE.md`.
