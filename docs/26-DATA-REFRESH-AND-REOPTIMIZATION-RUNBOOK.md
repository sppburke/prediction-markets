# 26 — Data Refresh and Re-optimization Runbook

Step-by-step operational procedures for (1) backfilling trade data for the wallets
already in the pile and (2) re-ranking the universe and publishing the result the
paper trader follows.

This is the **operational how-to** for the single ranking pipeline (issue #370):
`scripts/rank_and_push.sh` refreshes data, ranks the full trade universe, reranks,
and publishes to Supabase `latest_ranking`, which `pe-service` reads.

> **Golden rule — this all runs on the machine that holds `wallet_cache.db`.**
> The cache is multi-GB (≈360 GB) and lives on the local/analysis box, not the
> paper-trading VPS. The pipeline publishes its result to Supabase `latest_ranking`;
> the VPS `pe-service` reads that table on an interval — there is **no** watchlist file
> to `scp`, and `pe-service` has **no** seed/leaderboard fallback (Supabase is the sole
> wallet source, #370/#379). Do not run these procedures on the VPS.

## Prerequisites

```bash
# Build the bootstrap binary used below (from repo root).
cargo build --release -p pe-bootstrap
```

- **DB path.** Every command points at `data/wallet_cache.db`. `pe-bootstrap`
  defaults `cache_path` to `wallet_cache.db` (`crates/bootstrap/src/config.rs:469`),
  so either pass a config TOML with `cache_path = "data/wallet_cache.db"` or set
  `PE_BOOTSTRAP_CACHE_PATH=data/wallet_cache.db` (`rank_and_push.sh` exports this for you).
- **Market resolutions (no RPC).** The resolution pipeline is CLOB → Gamma:
  the Polymarket CLOB `/markets?closed=true` listing is the sole resolution
  source (#369; key-free), with Gamma supplying open-market schedules/liquidity.
  No Polygon RPC / Alchemy provider is required.
- **Supabase + Python.** `.env` must carry `SUPABASE_URL` + `SUPABASE_SECRET_KEY`
  (the push reads them). The ranking scripts run under `.venv-analysis/bin/python3`.
  The Radion discovery source needs `PE_BOOTSTRAP_RADION_API_KEY` (#373).

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
- Refreshes `market_resolutions` (CLOB) / `market_schedules` (Gamma) for the
  cache's market set.

It does **not** add wallets via chain enumeration or Dune. Those are the
`enumerate` and `discovery` subcommands — **do not run them** for a pure backfill.
(Backfill *can* flip an already-present pile wallet from `is_active=0→1` if it now
meets the activation thresholds — that is re-classification of an existing wallet,
not new discovery.)

### Commands

```bash
# 1. Refresh trades + resolutions for all stale active wallets (incremental).
PE_BOOTSTRAP_CACHE_PATH=data/wallet_cache.db \
  ./target/release/pe-bootstrap backfill

# 2. Refresh condition→event + fee mappings (needed by the ranker's
#    distinct-events eligibility gate; backfill does not cover this).
PE_BOOTSTRAP_CACHE_PATH=data/wallet_cache.db \
  ./target/release/pe-bootstrap events

# 3. (Optional) Run the resolution pipeline (CLOB → Gamma) explicitly.
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
relative to the markets your wallets traded, re-run step 3 (the CLOB → Gamma
resolution pipeline — no RPC required, #369/#372).

> **Backfill before pushing (issue #350 WS3).** The Supabase upload
> (`scripts/push_ranking_to_supabase.py`, invoked by `scripts/rank_and_push.sh`)
> now enforces this freshness. With `--db data/wallet_cache.db` (always passed by
> `rank_and_push.sh`) it:
> - **aborts the push** (non-zero exit, no Supabase write) when the cache's global
>   `MAX(timestamp_unix)` is older than `--max-cache-staleness-hours`
>   (`upload_max_cache_staleness_hours` = 24); and
> - **drops wallets** with no cached trade in the last `--active-window-hours`
>   (`upload_active_window_hours` = 72) so idle wallets never reach the live set
>   (the dropped count is logged; the comparison is case-insensitive).
>
> Each ranking row also carries `last_trade_unix` — the wallet's real last on-chain
> trade time — which `pe-service` uses both as the inactivity clock and as the
> candidate-freshness filter (`ACTIVE_WINDOW_HOURS = 72`, #357). It is a push-time
> snapshot of the cache, so a stale cache yields stale clocks; backfilling immediately
> before the push is mandatory.
>
> Because both checks read `wallet_cache.db`, **run Part 1 (backfill) immediately
> before Part 2's ranking push.** A stale cache would otherwise filter out every
> wallet, leaving the live ranking empty — the abort guard turns that silent
> failure into a loud one.

---

## Part 2 — Rank and publish (the one command)

**Goal:** re-rank the universe and publish the result to Supabase `latest_ranking`,
which `pe-service` reads. There is exactly one ranker
(`scripts/rank_72hr_buyandhold.py`); pass-2 (`latency_shift_rerank.py`) reranks; the
push (`scripts/push_ranking_to_supabase.py`) publishes (#370).

```bash
# The single, cron-ready entry point. Zero args. Runs on the cache box.
bash scripts/rank_and_push.sh
```

In order it runs: **Step 0** data refresh — `winner-discovery` (leaderboard +
datadash + radion → new wallets) → `backfill` (trades only) → `events` →
`resolutions` (which also backfills missing schedule `end_date`s — the
`resolutions` subcommand runs the full `fetch_resolutions_and_schedules`, so no
separate `schedules` stage is needed, #383); **Stage 1** rank the full trade
universe (`--universe-from-trades` —
have-data ⇒ in-universe; the ranker's own eligibility filters decide the cohort, so
there is no curated pre-gate); **Stage 2** rerank (adds `hit_rate`); **Stage 3** push
to Supabase and verify `latest_ranking` is populated.

Production defaults are baked in (override via flags): `--universe-from-trades`,
`HALF_LIFE_DAYS=30` (30-day recency decay, #366/#370), relative 180-day window,
mid-price band 0.15–0.85, TTR 72h, `--scheduled-only` (no resolved-at look-ahead),
`floor_tstat=2.0`, `top_n=200`.

> **First full-universe run — stage the half-life.** For the first run after moving to
> the full trade universe, override with `--half-life-days 0` (decay off) so a
> surprising cohort shift is attributable to the wider universe rather than the decay;
> drop the override on subsequent runs (#370).

> **Backfill is built in.** Step 0 backfills before ranking, and the push aborts if the
> cache's newest trade is >24 h old (the issue #350 WS3 freshness guard above). A
> standalone Part-1 run is only needed for a faster iteration loop or to debug.

### Overrides (research / re-push)

- `--universe <file>` — rank a curated wallet file instead of all-trade-wallets.
- `--half-life-days N` — override the production decay half-life.
- `--skip-discovery` / `--skip-backfill` — skip Step-0 stages.
- `--skip-rank` — reuse existing CSVs in `--out-dir`; just (re-)push.
- Pure re-push: `--skip-discovery --skip-backfill --skip-rank --out-dir <prior run>`.

### Cron (operator-installed)

```cron
# daily 06:00 UTC on the box holding wallet_cache.db (NOT the VPS):
0 6 * * *  cd /home/sean/git/prediction-markets && bash scripts/rank_and_push.sh >> data/eval-results/cron.log 2>&1
```

`pe-service` on the VPS picks up the new `latest_ranking` on its next refresh
(score-update-only; the maintenance tick handles eviction/backfill of live-set
membership).

---

## Output artifacts

`rank_and_push.sh` writes each run's ranked CSVs under an auto-timestamped
`data/eval-results/cron-<UTC>/` (override with `--out-dir`). Retire superseded
artifacts to `data/archive/`; never delete eval outputs — they are the audit trail
for what was published to `latest_ranking` and when.

---

## Adding new wallets

New-wallet discovery is **Step 0** of `rank_and_push.sh` (`pe-bootstrap
winner-discovery`: Polymarket leaderboard + datadash + radion). Discovered wallets
are ingested, backfilled, and ranked in the same run — there is no separate
manual-review gate. See `docs/27-WINNER-DISCOVERY-RUNBOOK.md` for the discovery
sources and their configuration.
