# Repository Guidelines

## Project Structure & Module Organization

This repo hosts `prediction-edge`, a Rust 1.95.0 / Rust 2024 trading and research workspace for Polymarket and Kalshi. Root files: `README.md` overview, `CLAUDE.md` agent context, this file contributor guide. Phase 0 has shipped `crates/` (`pe-config`, `pe-service`), `Cargo.toml`, `Cargo.lock`, `rust-toolchain.toml`, `deny.toml`, and `.github/workflows/ci.yml`.

Primary material lives in `docs/`. Start with `docs/_BASELINE.md`, `docs/_GLOSSARY.md`, `docs/19-WINNER-FOLLOW-STRATEGY.md`, `docs/AGENTS.md`, `docs/SKILLS.md`, then the relevant phase, venue, or source doc.

## Build, Test, and Development Commands

The full acceptance gate (also run in CI):

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

## Coding Style & Naming Conventions

Production code must be Rust only. Use small crates, explicit boundaries, pure risk/ranking/sizing logic, and replayable event-sourced behavior. Avoid `unwrap`, `expect`, unchecked `panic!`, raw financial `f64`, unbounded hot-path channels, and `unsafe` unless reviewed.

Docs use numbered prefixes, such as `docs/03-PHASE-MODEL-ENGINE.md` and `docs/07-VENUE-KALSHI.md`. Authority: `_BASELINE.md` > `_GLOSSARY.md` > `19-WINNER-FOLLOW-STRATEGY.md` > phase/venue/source docs > `docs/18-CODEX-RUST-BOOTSTRAP-PROMPT.md`. Do not duplicate numeric thresholds; add defaults to `_GLOSSARY.md` or canonical TOML in `19-`.

## Architecture Notes

Crate rules: source must not depend on venue, venue must not depend on strategy, and strategies emit `OrderIntent`; only execution routers submit orders. Keep `risk-engine` and `operator-graph` pure and deterministic. Use glossary terms exactly: wallet, trader, operator, leader, and candidate are distinct.

## Testing Guidelines

When code is added, prefer deterministic fixtures, replay tests, and pure-function tests for ranking, sizing, risk, and classification. Target one test with `cargo nextest run -p <crate> <test_name>`. Document any gate that cannot be run.

## Commit & Pull Request Guidelines

Recent commits use concise, imperative summaries, sometimes with a scoped prefix: `Restructure docs: canonical baseline...`, `Update Winner-Follow operator graph plans`. Follow that style.

PRs must include summary, files changed, tests run, replay impact, risk impact, API docs checked, deployment impact, and rollback plan. Update `docs/15-SOURCES.md` `Last checked` entries when venue or source docs are re-verified. Link issues when available; add screenshots only for visual changes.

## Security & Configuration Tips

Do not commit secrets, API keys, credentials, private datasets, or live trading configuration. Prefer official APIs and official documentation over scraping or opaque third-party scores.
