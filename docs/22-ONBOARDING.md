# 22 — Local Onboarding Guide

> See [`_BASELINE.md`](_BASELINE.md) for the Rust toolchain pin and acceptance gate.
> See [`.env.example`](../.env.example) for the canonical env-var list with inline descriptions.

This guide covers everything needed to run the system locally: toolchain install, environment configuration, startup sequence, and three first-run recipes.

## Prerequisites

### Rust toolchain

```bash
rustup toolchain install 1.95.0
rustup override set 1.95.0   # optional: pins this directory
rustc --version              # must print "rustc 1.95.0 ..."
```

### Cargo tools

```bash
cargo install cargo-nextest   # fast test runner (required by gate)
cargo install cargo-deny      # dependency policy (required by gate)
cargo install cargo-audit     # advisory check (required by gate)
```

All three are also installed automatically in CI via `taiki-e/install-action`.

### External accounts (for data ingestion)

| Account | Purpose | Free tier |
|---|---|---|
| [Dune Analytics](https://dune.com) | Wallet discovery SQL | 2,500 credits/month |

Polymarket CLOB credentials are only needed for live order submission (not backtest or bootstrap). Market resolutions come from the public CLOB `/markets?closed=true` endpoint (key-free, #369) — no Polygon RPC / Alchemy account is required.

## Environment configuration

```bash
cp .env.example .env
# Open .env and fill in every [required] field.
```

`.env` is excluded from git (see `.gitignore`). Never commit real credentials.

### Full env-var catalogue

Variables are grouped by binary. Required fields are marked **[req]**.

#### Both binaries

| Variable | Description | Required | Default |
|---|---|:---:|---|
| `RUST_LOG` | Log level filter (e.g. `info`, `debug`, `pe_bootstrap=debug`) | | `warn` |

#### pe-bootstrap

| Variable | Description | Required | Default |
|---|---|:---:|---|
| `PE_WALLET_SOURCE` | Wallet discovery source (`dune`) | **[req]** | — |
| `PE_DUNE_API_KEY` | Dune Analytics API key | **[req]** | — |
| `PE_DUNE_NAMESPACE` | Dune query namespace (your username) | **[req]** | — |
| `PE_DUNE_SIM_API_KEY` | Second Dune key for activity queries | | same as `PE_DUNE_API_KEY` |
| `PE_ETHERSCAN_API_KEY` | Etherscan V2 API key (chain 137) | **[req]** | — |
| `PE_BOOTSTRAP_OUTPUT` | Path for bootstrap SQLite output | **[req]** | — |
| `PE_BOOTSTRAP_CACHE_PATH` | Path for wallet cache SQLite | **[req]** | — |
| `PE_BOOTSTRAP_WALLET_SET_PATH` | Path for wallet set JSON | **[req]** | — |
| `PE_BOOTSTRAP_FETCH_RESOLUTIONS` | `1` to populate `market_resolutions` table | | `0` |
| `PE_BOOTSTRAP_SKIP_TRADE_FETCH` | `1` to skip Polymarket trade fetch (use cached data) | | `0` |
| `PE_BOOTSTRAP_DUNE_MIN_MARKETS` | Min distinct resolved markets for Dune seed | | `15` |
| `PE_BOOTSTRAP_DUNE_MIN_WIN_RATE_PCT` | Min win rate % for Dune seed | | `95` |
| `PE_BOOTSTRAP_DUNE_ACTIVE_DAYS` | Recency window days for Dune seed | | `30` |
| `PE_BOOTSTRAP_DUNE_MAX_AVG_HOURS` | Max avg hours-to-resolution for Dune seed | | `72` |
| `PE_BOOTSTRAP_MIN_CLOSED_TRADES` | Post-filter: min closed trades | | `20` |
| `PE_BOOTSTRAP_MIN_WIN_RATE_PCT` | Post-filter: min win rate (0–1) | | `0.50` |
| `PE_BOOTSTRAP_AUDIT_WINDOW_DAYS` | Audit window for post-filter | | `180` |
| `PE_BOOTSTRAP_POST_FILTER_ACTIVE_DAYS` | Activity recency window (post-filter) | | `30` |
| `PE_BOOTSTRAP_POST_FILTER_MAX_AVG_HOURS` | Max avg hours-to-resolution (post-filter) | | `72` |
| `PE_BOOTSTRAP_POLYMARKET_CONCURRENCY` | Parallel Polymarket API requests | | `4` |
| `PE_WALLET_FROM_BLOCK` | Start block for on-chain scan | | `0` |
| `PE_WALLET_TO_BLOCK` | End block for on-chain scan | | `latest` |
| `PE_SEED_AS_OF_DATES` | Comma-separated ISO dates for leaderboard snapshots | | current date |
| `PE_GAMMA_BASE_URL` | Gamma API base URL override | | production URL |

#### pe-backtest

| Variable | Description | Required | Default |
|---|---|:---:|---|
| `PE_BOOTSTRAP_CACHE_PATH` | Path to wallet cache SQLite (read-only) | **[req]** | — |
| `PE_BACKTEST_OUTPUT_DIR` | Directory for backtest output files | **[req]** | — |
| `PE_BANKROLL_USD` | Starting bankroll in USD | | `10000` |
| `PE_BACKTEST_ACTIVE_MIN_CLOSED` | Active-tier min closed trades | | `15` |
| `PE_BACKTEST_ACTIVE_MIN_MARKETS` | Active-tier min distinct markets | | `5` |
| `PE_BACKTEST_INCUBATOR_MIN_CLOSED` | Incubator-tier min closed trades | | `5` |
| `PE_BACKTEST_INCUBATOR_MIN_MARKETS` | Incubator-tier min distinct markets | | `2` |
| `PE_BACKTEST_AUDIT_WINDOW_DAYS` | Audit window for ranker | | `180` |
| `PE_BACKTEST_KELLY_P_PRIOR_ALPHA` | Beta-prior alpha (Bayesian shrinkage) | | `2.0` |
| `PE_BACKTEST_KELLY_P_PRIOR_BETA` | Beta-prior beta (Bayesian shrinkage) | | `1.0` |
| `PE_BACKTEST_KELLY_P_K_PER_MARKET` | N_eff scaling factor k (specialist discount) | | `6` |
| `PE_BACKTEST_MAX_HOURS_TO_EXPIRY` | Skip trades expiring beyond this many hours | | _(disabled)_ |
| `PE_BACKTEST_MIN_QUALITY` | Min quality score to copy (0–100) | | `0` |
| `PE_BACKTEST_PER_TRADE_CAP` | Per-trade cap in basis points of bankroll | | _(from config)_ |
| `PE_BACKTEST_STEP_DAYS` | Walk-forward step size in days | | `1` |
| `PE_BACKTEST_KELLY_SWEEP` | `1` to run Kelly-fraction sweep instead of single run | | `0` |

#### Credentialed canary

Ordinary `pe-service` is paper-only and has no live credential environment variables. The
isolated V2 canary receives root-owned files through systemd `LoadCredential=` and is installed
inactive. See [`36-POLYMARKET-V2-CANARY-RUNBOOK.md`](36-POLYMARKET-V2-CANARY-RUNBOOK.md); never
place canary credentials in the ordinary `.env`.

## Startup sequence

The binaries have a dependency order:

```
1. pe-bootstrap  (reads from Dune, Etherscan, Polygon, Polymarket)
       ↓  writes wallet_cache.db
2. pe-backtest   (reads wallet_cache.db; self-contained — no live network)
   OR
   pe-service    (reads wallet_cache.db; streams live Polymarket events)
```

**Do not run pe-backtest while pe-bootstrap is writing to the same cache file.** Wait for bootstrap to finish (exit code 0) before starting a backtest.

## Three first-run recipes

### Recipe A — Backtest only

Runs the full walk-forward simulation on historical data. No live credentials needed.

```bash
# 1. Set required bootstrap vars in .env
#    PE_DUNE_API_KEY, PE_DUNE_NAMESPACE,
#    PE_ETHERSCAN_API_KEY, PE_BOOTSTRAP_OUTPUT, PE_BOOTSTRAP_CACHE_PATH,
#    PE_BOOTSTRAP_WALLET_SET_PATH, PE_BOOTSTRAP_FETCH_RESOLUTIONS=1,
#    PE_BOOTSTRAP_FETCH_FUNDER_GRAPH=1, PE_BACKTEST_OUTPUT_DIR

# 2. Run bootstrap (takes ~2-4 hours on first run; ~20-30 min on re-runs with cache)
cargo run --release --bin pe-bootstrap

# 3. Run backtest
cargo run --release --bin pe-backtest

# Results land in $PE_BACKTEST_OUTPUT_DIR as JSONL.
```

### Recipe B — Paper trading (backtest + live paper mode)

Paper mode runs the strategy logic end-to-end against live Polymarket data but emits no real orders. `leader_follow` defaults to paper after fresh bootstrap — no config change required.

```bash
# Complete recipe A first (bootstrap + historical backtest for calibration).

# Then start the service (paper is the default mode for leader_follow):
cargo run --release --bin pe-service

# Monitor: RUST_LOG=info logs every signal and its paper outcome.
```

Paper mode is the default for `leader_follow` after fresh bootstrap (see `docs/19-WINNER-FOLLOW-STRATEGY.md` "Promotion ladder"). Promotion to live-tiny requires a manual review and passing the walk-forward gate.

### Recipe C — Install the inactive canary

The implementation workflow may build and install the disabled unit only. It does not authorize
wallet creation, funding, allowance mutation, arming, or a POST. Follow the artifact and
permission checks in [`36-POLYMARKET-V2-CANARY-RUNBOOK.md`](36-POLYMARKET-V2-CANARY-RUNBOOK.md).

## Acceptance gate

Run this before pushing to confirm nothing is broken:

```bash
rustc --version | grep -F '1.95.0'
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --doc --workspace --all-features   # doctests only
cargo nextest run --workspace --all-features   # unit + integration + scenario
cargo deny check
cargo audit
cargo metadata --locked --format-version 1 > /dev/null
```

Single-test repro: `cargo nextest run -p <crate> <test_name>`.
