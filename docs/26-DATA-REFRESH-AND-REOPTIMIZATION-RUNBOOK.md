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
  (the push reads them). `rank_and_push.sh` selects its own interpreter so the documented
  one-line command is independent of an interactive shell's `PATH`: `PE_PYTHON` (an
  executable-path override) → `.venv-analysis/bin/python3` → `.venv/bin/python3`. It does
  not silently use the system `python3`. Install `scripts/requirements.txt` into one of those
  repository environments. Before taking the PID lock, creating a run directory, or refreshing
  the cache, the wrapper imports its required modules and exits with remediation instructions if
  the environment is incomplete. DuckDB remains optional under `engine=auto` (SQLite fallback)
  and is mandatory under `engine=duck`.

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

> **Gamma/CLOB UA + batching (issue #382 Phase-0 live probe, `scripts/probe_gamma_ua.py`, 2026-06-20).**
> Tier-1 matrix against live Gamma `/markets` and CLOB `/markets`:
> - **The 403 gate is the literal `Python-urllib/*` default User-Agent, not "missing browser UA".**
>   `&closed=true` returned 200 for a *headerless* request (a bare `reqwest::Client`, = the shipped
>   Rust clients), an empty UA, a product UA (`prediction-edge/1.0`), and a browser UA — and 403
>   **only** for `Python-urllib/3.11`. So `pe-bootstrap`'s CLOB closed walk and the paper-pnl
>   resolution poller (both UA-less) do **not** 403; resolution ingestion is fine. The proven
>   scripts' "browser UA required (else 403)" note is correct only because `urllib` auto-injects the
>   blocklisted `Python-urllib` UA — any non-bot UA (or none) works.
> - **Repeat-key batching works for BOTH variants.** `?condition_ids=A&condition_ids=B…&limit=500`
>   returned 50/50 (open, plain) and 100/100 (open) with clean demux-by-`conditionId` and no
>   cross-market leak; the `&closed=true` variant likewise (35/50, the 15 omitted are markets Gamma
>   no longer lists, not truncation). Comma-separated joining returns 0 — repeat-key is mandatory.
>   Observed cap ≥ 100; default `gamma_batch_size` stays 50 (`_GLOSSARY.md`).
> - This unblocks a shared batched Gamma client (~60× the per-ID ~20 req/s) across `pe-bootstrap`,
>   `pe-service`, and `pe-paper-pnl` — including the *open* passes, which the stale
>   `crates/bootstrap/src/gamma.rs:5-6` "batching fails silently" comment wrongly excludes.

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
# The single one-shot entry point. Zero args. Runs on the cache box.
bash scripts/rank_and_push.sh
```

In order it runs: **Step 0** data refresh — `winner-discovery
--defer-activation` (leaderboard + datadash ingest) → one transactionally
audited `activate-next` batch (`bootstrap_pipeline_activation_batch_wallets`) →
`backfill --defer-activation` (trades only) → `events` → `resolutions` →
**Step 0i** archive and delete every infra wallet's live wallet-keyed data
before the ranker snapshot (and write a durable, non-liftable infra exclusion).
`resolutions` also backfills missing schedule `end_date`s — the
`resolutions` subcommand runs the full `fetch_resolutions_and_schedules`, so no
separate `schedules` stage is needed (#383). **Stage 1** ranks the full trade
universe (`--universe-from-trades` —
have-data ⇒ in-universe; the ranker's own eligibility filters decide the cohort, so
there is no curated pre-gate); **Stage 2** rerank (adds `hit_rate`); **Stage 3** push
to Supabase and verify `latest_ranking` is populated; **Stage 4** purge proven-loser
and dead-weight wallets when armed; **Stage 5** run
`PRAGMA wal_checkpoint(TRUNCATE)` against the local cache so committed WAL pages are
checkpointed and the WAL file releases its disk footprint. The checkpoint is always
attempted last, including re-pushes and runs that skip purge. A busy/error result warns
without failing the already-complete Supabase publish; the next run retries it.

Production defaults are baked in (override via flags): `--universe-from-trades`,
`HALF_LIFE_DAYS=30` (30-day recency decay, #366/#370), relative 180-day window,
mid-price band 0.15–0.85, TTR 48h (`ranker_ttr_hours`), MinTRL 20 (`ranker_prod_min_trl`
— replaces the per-month activity gates, which production zeroes; run28 cutover
2026-07-03), `--scheduled-only` (no resolved-at look-ahead), `floor_tstat=2.0`,
`top_n=200`.

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
- `--skip-discovery` / `--skip-backfill` — skip Step-0 stages. Every backfill
  launched through this wrapper still defers global activation;
  `--skip-discovery` also skips `activate-next`, so it activates no new wallet.
- `--skip-rank` — reuse existing CSVs in `--out-dir`; just (re-)push.
- `--skip-purge` — skip wallet deletion; the final WAL checkpoint still runs.
- Pure re-push: `--skip-discovery --skip-backfill --skip-rank --out-dir <prior run>`.

### Continuous Forge supervisor

Production repetition is file-governed and runs the complete one-shot command
with exactly zero parameters each cycle. Before cutover, inventory and disable
every user/root crontab, systemd timer, or other supervisor that invokes
`rank_and_push.sh`; retain a backup for rollback. A scheduled one-shot must not
race this supervisor.

The Linux/Forge supervisor requires `flock`, `setsid`, an external `kill`, and
negative process-group signaling. It fails before taking its singleton lock or
launching a child if that preflight is unavailable.

```bash
cd /home/sean/git/prediction-markets

# Enable atomically, then launch one supervisor.
loop_flag_tmp="data/eval-results/.rank_and_push.loop.$$"
printf 'run\n' > "$loop_flag_tmp"
mv "$loop_flag_tmp" data/eval-results/rank_and_push.loop
nohup bash scripts/rank_and_push_loop.sh \
  > data/eval-results/rank-and-push-loop.log 2>&1 < /dev/null &
```

The flag accepts exactly `run` or `stop`. Missing means a clean stop; empty or
any other value is fatal. The supervisor checks only between completed cycles:

```bash
# Graceful: finish the current full cycle, then stop.
loop_flag_tmp="data/eval-results/.rank_and_push.loop.$$"
printf 'stop\n' > "$loop_flag_tmp"
mv "$loop_flag_tmp" data/eval-results/rank_and_push.loop

# Immediate: terminate the supervisor; it TERM-signals the entire current
# one-shot process group, waits, and escalates to KILL after its bounded grace.
kill -TERM "$(tr -cd '0-9' < data/eval-results/.rank_and_push_loop.lock)"
```

After an immediate stop, verify no loop, wrapper, bootstrap, or ranking Python
descendant remains; neither `.rank_and_push.lock` nor the cache mutation lock is
held; and SQLite opens/read-checks cleanly. A nonzero child exit stops the loop
instead of retry-spinning. Rollback writes `stop`, waits for termination, and
restores the backed-up prior scheduler only if one existed.

Each cycle logs `LOOP_CYCLE_START`, the child PID/process-group ID, the one-shot
`RANK_AND_PUSH_RUN_DIR=...` handoff, and `LOOP_CYCLE_END` with exit status. The
distinct supervisor lock prevents two loops from racing at a cycle boundary.

The final checkpoint remains inside that lock and starts only after every pipeline DB
writer has exited. It checkpoints committed data rather than deleting rows; its storage
effect is to truncate the separate `wallet_cache.db-wal` file.

`pe-service` on the VPS picks up the new `latest_ranking` on its next refresh
(score-update-only). MEMBERSHIP follows `watchlist_membership_mode` (`_GLOSSARY.md`):
`knockout` (legacy — the maintenance tick's eviction/backfill is the sole membership
path) or `full_rerank` (each batch transition wholesale-replaces the live
top-`active_watchlist_size` — the cutover production mode). `active_watchlist_size` is
Supabase-authoritative (default 100, valid `1..=200`) and is polled every 30 seconds. A
grow fetches the requested top-N and preloads all newly admitted wallets' prior-market
history and current positions before the atomic membership swap; a shrink uses the same
atomic swap. Invalid values, Supabase failures, or incomplete admission preparation keep
the last-known-good target and membership, then retry independently on the
capacity worker's next 30-second retry. Check
`status.json`: `watchlist_size` is actual membership and `watchlist_target_size` is the
last safely applied runtime cap. Installing this runtime-capacity support requires one normal
`pe-service` restart; subsequent valid `service_config` edits hot-swap without a restart.
Run28 fixed `k=25` and did not sweep watchlist width. The earlier top-50 paper experiment
adopted 2026-07-13 and the superseding 100-wallet default are operator-directed choices,
not run28-backed N choices.

---

## Output artifacts

`rank_and_push.sh` writes each run's ranked CSVs under an auto-timestamped
`data/eval-results/cron-<UTC>/` (override with `--out-dir`). Retire superseded
artifacts to `data/archive/`; never delete eval outputs — they are the audit trail
for what was published to `latest_ranking` and when.

Each full cycle also writes `activated_wallets.csv`. Its rows are derived from
the same run's durable SQLite activation batch; if CSV materialization fails,
rerun `activate-next` with that same logged batch ID and audit path to regenerate
the exact cohort without activating another batch.

### Supabase batch retention (#411)

The push appends one `ranking_batches` epoch (+ its `ranking_entries`) per run, so the
table grows unbounded. After a successful push, `push_ranking_to_supabase.py` prunes
`ranking_batches` to the newest `--keep-batches` rows (default **1080**, owned by
the `ranking_batches_retention` default in `docs/_GLOSSARY.md`; actual wall-clock
coverage depends on full-cycle duration under the continuous supervisor). CASCADE
removes their entries. `latest_ranking` reads only `max(batch_id)`, so pruning older
epochs never touches the live read path or the `wallet_live_stats_mv` matview — it is
storage hygiene, and it keeps enough epochs for the wholesale-swap-at-frequency-X replay.
The prune is best-effort: a failure logs a warning and does not fail the push (growth
stays bounded and self-heals on the next run). `--keep-batches 0` disables it for a
history-preserving research re-push.

---

## Adding new wallets

New-wallet discovery is **Step 0** of `rank_and_push.sh` (`pe-bootstrap
winner-discovery`: Polymarket leaderboard + datadash). The wrapper defers the
legacy immediate activation call sites and admits only its one controlled batch;
those wallets are backfilled and ranked in the same run — there is no separate
manual-review gate. See `docs/27-WINNER-DISCOVERY-RUNBOOK.md` for the discovery
sources and their configuration.
