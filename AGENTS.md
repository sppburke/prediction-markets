# Repository Guidelines

## Project Structure & Module Organization

This repository is currently specification-only for `prediction-edge`, a future Rust 1.95.0 / Rust 2024 prediction-market trading and research workspace. Root files provide entry points: `README.md` summarizes the project, `CLAUDE.md` gives coding-agent context, and this `AGENTS.md` is the contributor guide.

Primary project material lives in `docs/`. Start with `docs/README.md`, `docs/_BASELINE.md`, `docs/_GLOSSARY.md`, and `docs/19-WINNER-FOLLOW-STRATEGY.md`. When implementation begins, source crates are expected under `crates/`, workspace metadata at `Cargo.toml`, and CI under `.github/workflows/`.

## Build, Test, and Development Commands

There is no active build yet because no Rust workspace has been committed. For documentation edits, validate links and examples manually.

Once the workspace exists, use the project gate from `CLAUDE.md`:

```bash
rustc --version
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo nextest run --workspace --all-features
cargo deny check
cargo audit
cargo metadata --locked
```

## Coding Style & Naming Conventions

Production implementation must be Rust only. Use Rust 2024, stable Rust 1.95.0, small crates, explicit boundaries, and deterministic replayable logic. Avoid `unwrap`, `expect`, unchecked `panic!`, raw `f64` for financial values, unbounded hot-path channels, and `unsafe` unless separately reviewed.

Documentation files use numbered prefixes for sequence and scope, such as `docs/03-PHASE-MODEL-ENGINE.md` and `docs/07-VENUE-KALSHI.md`. Keep new docs focused and cross-reference canonical values instead of duplicating thresholds.

## Testing Guidelines

When code is added, prefer deterministic fixtures, replay tests, and pure-function tests for ranking, sizing, risk, and classification. Use `cargo test` for the workspace and `cargo nextest run -p <crate> <test_name>` for targeted runs. Document any gate that cannot be run.

## Commit & Pull Request Guidelines

Recent commits use concise, imperative summaries, sometimes with a scoped prefix: `Restructure docs: canonical baseline...`, `Update Winner-Follow operator graph plans`. Follow that style.

Pull requests should include a summary, files changed, tests run, replay impact, risk impact, API docs checked, deployment impact, and rollback plan. Link issues when available and add screenshots only for visual changes.

## Security & Configuration Tips

Do not commit secrets, API keys, credentials, private datasets, or live trading configuration. For external APIs, prefer official docs and record source checks in the relevant docs when behavior changes.
