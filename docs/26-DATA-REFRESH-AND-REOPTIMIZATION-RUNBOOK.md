# 26 — Data Refresh and Re-optimization Runbook

Step-by-step operational procedures for (1) backfilling trade data for the wallets
already in the pile and (2) re-optimizing which wallets the paper trader follows.

This is the **operational how-to**. For the *policy* — when to re-evaluate, what
cohort size to target at each capital tier, when to drop a wallet — see
[`25-WALLET-EVALUATION-AND-SCALING.md`](25-WALLET-EVALUATION-AND-SCALING.md).

> **Golden rule — this all runs on the machine that holds `wallet_cache.db`.**
> The cache is multi-GB (≈360 GB) and lives on the local/analysis box, not the
> paper-trading VPS. The VPS only ever consumes the small output JSON
> (`data/watchlist-production-n<N>.json`), which you `scp` over at the end of
> Part 2. Do not attempt either procedure on the VPS.

## Prerequisites

```bash
# Build the two binaries used below (from repo root).
cargo build --release -p pe-bootstrap -p pe-skill-select
```

- **DB path.** Every command points at `data/wallet_cache.db`. `pe-bootstrap`
  defaults `cache_path` to `wallet_cache.db` (`crates/bootstrap/src/config.rs:469`),
  so either pass a config TOML with `cache_path = "data/wallet_cache.db"` or set
  `PE_BOOTSTRAP_CACHE_PATH=data/wallet_cache.db`. `pe-skill-select` uses
  `PE_SKILL_CACHE_PATH`.
- **Polygon RPC (optional but recommended).** `PE_BOOTSTRAP_POLYGON_RPC_URL`
  improves resolution coverage (the resolution pipeline tries Polygon RPC → Dune →
  CLOB → Gamma; without RPC it falls through to Gamma). Backfill of *trades* works
  without it.
- **Python analysis venv.** The ranking scripts run under `.venv-analysis/bin/python3`.

---

## Part 1 — Backfill tradeable wallets (no new wallet discovery)

**Goal:** bring trade history, resolutions, and event mappings current for the
wallets **already** marked tradeable in the pile, *without* discovering or adding
any new wallets.

### What this does and does not do

`pe-bootstrap backfill` (`crates/bootstrap/src/backfill.rs:1`):
- Selects `is_active = 1` wallets whose `last_polymarket_fetch_at` is NULL or older
  than 1 day (`backfill_limit = 0` = all due wallets).
- Incremental two-phase cursor walk per wallet — it appends new trades, it does not
  re-download history already in the cache.
- Refreshes `market_resolutions` / `market_schedules` for the cache's market set.
- Runs in `DeltaMode::Shadow` by default (`crates/bootstrap/src/lib.rs:88`): the
  on-chain delta scan is audit-only and **does not change the fetch set**.

It does **not** add wallets via chain enumeration or Dune. Those are the
`enumerate` and `discovery` subcommands — **do not run them** for a pure backfill.
(Backfill *can* flip an already-present pile wallet from `is_active=0→1` if it now
meets the activation thresholds — that is re-classification of an existing wallet,
not new discovery.)

### Commands

```bash
# 1. Refresh trades + resolutions for all stale active wallets (incremental).
PE_BOOTSTRAP_CACHE_PATH=data/wallet_cache.db \
PE_BOOTSTRAP_POLYGON_RPC_URL=<polygon-rpc-url> \   # optional; improves resolution coverage
PE_BOOTSTRAP_POLYMARKET_DELTA_MODE=off \           # REQUIRED on free-tier RPC — see Delta-mode note below
  ./target/release/pe-bootstrap backfill

# 2. Refresh condition→event + fee mappings (needed by the ranker's
#    distinct-events eligibility gate; backfill does not cover this).
PE_BOOTSTRAP_CACHE_PATH=data/wallet_cache.db \
  ./target/release/pe-bootstrap events

# 3. (Only if step 1 ran without RPC and you need fuller resolution coverage)
#    Run the resolution pipeline explicitly.
PE_BOOTSTRAP_CACHE_PATH=data/wallet_cache.db \
PE_BOOTSTRAP_FETCH_RESOLUTIONS=1 \
  ./target/release/pe-bootstrap resolutions

# 4. Backfill scheduled end_date for resolved markets missing a schedule row
#    (RPC-free; Gamma &closed=true). Required for any time-to-resolution analysis —
#    without it the TTR filter must fall back to on-chain resolved_at, which LEAKS
#    future info (see the end_date-coverage note below). Idempotent; safe to re-run.
PE_BOOTSTRAP_CACHE_PATH=data/wallet_cache.db \
  ./target/release/pe-bootstrap schedules
```

> **Delta-mode note (important on a free-tier Polygon RPC).** `backfill` defaults to
> `DeltaMode::Shadow` (`crates/bootstrap/src/lib.rs:88`), which runs an audit-only
> on-chain `eth_getLogs` scan over the CTF block range *before* fetching any trades.
> Free-tier Polygon RPC providers cap `eth_getLogs` at a **10-block** range, so that
> scan bisects down to 10-block windows and is impractical across the ~900k-block gap —
> it stalls the run before a single trade is fetched, while adding **no** wallets and
> **no** trades (the scan is audit-only; in Shadow it never changes the fetch set —
> `backfill.rs:108-112`). For a pure backfill on a free-tier RPC, set
> `PE_BOOTSTRAP_POLYMARKET_DELTA_MODE=off`: this skips the scan and fetches every due
> wallet directly, which is still "no new wallets". Reserve Shadow/Delta for a paid RPC
> that allows wide `getLogs` ranges. (Verified 2026-06-04: a Shadow run logged
> thousands of `response cap hit, bisecting` warnings and never reached trade-fetch;
> the `off` run started fetching immediately.)

> **end_date coverage note (look-ahead safety).** Any "time-to-resolution" / expiry
> analysis must reference the *scheduled* `market_schedules.end_date_unix` (known at
> entry), never `market_resolutions.resolved_at_unix` (on-chain settlement, known only
> *after* the fact). Using `resolved_at` as the cutoff admits post-event and
> early-resolution trades and inflates edge (verified 2026-06-05: on markets with both
> refs, the resolved_at cutoff added 43% "leaked-in" positions and flipped population
> edge −1.8% → 0). Closed markets never fetched while open have no schedule row, so
> stages 6d/6f miss them; **step 4 (`pe-bootstrap schedules`, stage 6g)** backfills them
> via Gamma `&closed=true`. For a large one-time backlog, `scripts/backfill_end_dates.py`
> does the same via batched requests (~1200 markets/s vs the Rust per-ID ~20 req/s).
> The 2026-06-05 backfill lifted resolved-market `end_date` coverage 45% → 90%; the
> residual ~10% are markets Gamma no longer lists (exclude them — never fall back to
> `resolved_at`).

**Exit codes** (all `pe-bootstrap` subcommands): `0` = success, `1` = fatal,
`2` = partial (some wallets failed — safe to re-run; it retries the failures).
Add `--strict` to turn a partial into a fatal if you want CI-style hard failure.

### Verify the backfill landed

```bash
sqlite3 data/wallet_cache.db "
  SELECT COUNT(*) AS active_wallets FROM active_tradeable_wallets;
  SELECT COUNT(*) AS trades_total   FROM trades;
  SELECT COUNT(*) AS resolved_mkts  FROM market_resolutions WHERE winning_outcome_id IS NOT NULL;
  SELECT datetime(MAX(timestamp_unix),'unixepoch') AS newest_trade FROM trades;
"
```

`newest_trade` should be within the last day or two. If `resolved_mkts` is low
relative to the markets your wallets traded, re-run step 3 with an RPC URL set.

---

## Part 2 — Re-optimize the followed wallets

**Goal:** recompute per-wallet skill features at a fresh cutoff, re-rank the
universe, validate a cohort, and produce the `watchlist-production-n<N>.json` the
paper trader consumes — **with real `win_rate_bps`** so Kelly sizing works (the
deployed leaderboard-derived watchlist has `win_rate_bps = 0`, which forces the
flat-sizing fallback).

Run Part 1 first so the features are computed on current data.

Pick the cutoff: `<cutoff_unix>` is the UTC second at the boundary between the
training window and the forward window — normally the end of the most recently
completed month.

### Step 1 — Extract features

```bash
PE_SKILL_CACHE_PATH=data/wallet_cache.db \
PE_SKILL_CUTOFF_UNIX=<cutoff_unix> \
PE_SKILL_EXTRACT_CLEAN_PRIOR=1 \
  ./target/release/pe-skill-select extract
```

Writes the `wallet_features` table at `cutoff_unix` (`crates/skill-select/src/db.rs`):
`win_rate_bps`, `sharpe_bps`, `hold_to_resolution_rate_bps`, `skill_pvalue_bps`, etc.
`PE_SKILL_EXTRACT_CLEAN_PRIOR=1` clears stale rows from a previous extract at the
same cutoff.

### Step 2 — Rank the universe (GBM)

```bash
.venv-analysis/bin/python3 scripts/monthly_rerank_gbm.py \
  --db-path data/wallet_cache.db \
  --strategy gbm_throughput_single \
  --top-n 5000 \
  --out data/production-watchlist-gbm.txt
```

Reads `wallet_features` at the latest cutoff; emits a ranked `.txt` of wallet hex
addresses (the candidate pool for the constructor).

### Step 3 — Construct + validate the cohort (PBO)

```bash
.venv-analysis/bin/python3 scripts/portfolio_constructor/cli.py \
  --db-path data/wallet_cache.db \
  --watchlist data/production-watchlist-gbm.txt \
  --max-n <target N from the scaling table in doc 25> \
  --haircut-bps 500 \
  --fwd-days 7 \
  --n-seeds 5 \
  --pbo-perms 100 \
  --output-dir data/eval-results
```

The tool greedily selects the cohort that maximizes net forward edge subject to the
PBO gates, prints a `Deploy watchlist: <path>` line pointing at the selected-wallet
`.txt`, and writes the eval JSON under `data/eval-results/`.
(`scripts/overnight_pipeline.sh` is a turnkey wrapper around this step.)

> Flag note: the constructor takes `--max-n` (cap) and `--output-dir`, **not**
> `--target-n`/`--out`. Earlier drafts of doc 25 listed the wrong flags.

### Step 4 — Review the gates

Open the eval JSON in `data/eval-results/` and confirm:

| Metric | Gate | Meaning |
|---|---|---|
| `credible` | `true` | PBO ≤ 0.5 AND 0 negative anchors |
| `mean_of_mean_edge` | ≥ $0.01/pos (net of haircut) | Below this the edge is noise |
| `n_anchors_negative` | `0` | Any negative anchor = regime risk |
| `pbo.pbo` | ≤ 0.5 | Above 0.5 = overfit; do not deploy |

If gating fails, try a smaller `--max-n`, or — if only the most recent anchor is
negative — hold the existing cohort and re-check in two weeks (possible regime
shift). Never force a size that fails gating.

### Step 5 — Export the service watchlist (populates `win_rate_bps`)

```bash
PE_SKILL_CACHE_PATH=data/wallet_cache.db \
PE_SKILL_CUTOFF_UNIX=<cutoff_unix> \
PE_SKILL_EXPORT_WATCHLIST_INPUT_PATH=<Deploy-watchlist .txt from step 3> \
PE_SKILL_EXPORT_WATCHLIST_OUTPUT_PATH=data/watchlist-production-n<N>.json \
  ./target/release/pe-skill-select export-watchlist
```

This is the **Rust** `export-watchlist` subcommand
(`crates/skill-select/src/export_watchlist.rs`). It looks up each wallet's features
at `cutoff_unix` and writes a valid `pe_trader_index::Watchlist` JSON with
`win_rate_bps` populated from the DB. Wallets missing from `wallet_features` at the
cutoff are skipped with a warning (re-run Part 1 + Step 1 if any are unexpectedly
missing).

### Step 6 — Deploy to the VPS

```bash
# From the local box:
scp data/watchlist-production-n<N>.json sean@<vps-ip>:~/prediction-markets/data/
```

On the VPS, point the config at the new file and restart:

```toml
# smoke-test/service.toml
seed_watchlist_path = "data/watchlist-production-n<N>.json"
```

```bash
sudo systemctl restart pe-service
```

Once the exported watchlist carries real `win_rate_bps`, remove the
`flat_usd_per_trade` override from `[strategy]` in `service.toml` so Kelly sizing
takes over. The startup position seeder corrects each leader's ledger baseline
automatically on restart.

---

## Artifact naming

See doc 25's "Artifact naming convention". Retire superseded artifacts to
`data/archive/`; never delete eval JSONs (they are the audit trail for why a cohort
was deployed).
