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
- **SQLite scratch space (`SQLITE_TMPDIR`).** Bundled SQLite writes its working files —
  the `VACUUM` temp database and the external-sort spill of every `CREATE INDEX` on a
  large table — into the system temp directory, NOT beside the cache. On a host whose
  root filesystem is a different (smaller, or failure-prone) device than the cache
  volume, point `SQLITE_TMPDIR` at a writable directory on the CACHE volume, e.g.
  `SQLITE_TMPDIR=/mnt/storage/tmp` in `.env` (the wrapper exports its whole `.env` to
  every stage). Proven necessary on 2026-08-27: forge's root SD card remounted
  read-only mid-purge, and because the default temp dir lived there, the bulk purge's
  `VACUUM` and its index rebuild both failed while the deletes (which write only to the
  cache volume) succeeded — leaving 38.5 GiB of unreclaimed free pages, an absent
  `trades` index, and a subsequent cycle that silently paid a multi-hour rebuild.
  Symptom to recognize: `sqlite: unable to open database file` from a stage whose cache
  path is demonstrably writable.
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
# 1. Refresh stale active-wallet trades; configured resolution refreshes full-walk CLOB.
PE_BOOTSTRAP_CACHE_PATH=data/wallet_cache.db \
  ./target/release/pe-bootstrap backfill

# 2. Refresh condition→event + fee mappings (needed by the ranker's
#    distinct-events eligibility gate; backfill does not cover this).
PE_BOOTSTRAP_CACHE_PATH=data/wallet_cache.db \
  ./target/release/pe-bootstrap events

# 3. (Optional) Run the full CLOB resolution re-walk + Gamma auxiliaries explicitly.
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
>   resolution poller (both UA-less) do **not** 403; transport was healthy during the 2026-06-24
>   through 2026-08-21 cursor-wedge incident, while repeat resolution ingestion was not. The proven
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

**Exit codes** (a vocabulary — each `pe-bootstrap` subcommand emits a subset):
`0` = success, `1` = permanent failure, `2` = partial (durable soft-fail — safe
to re-run; it retries the failures), `75` = temporary failure (a bounded
retryable operation exhausted its in-process retries; the production loop
supervisor retries the cycle — currently emitted by `events` and `resolutions`).
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
> now enforces three same-bound freshness probes. With `--db data/wallet_cache.db`
> (always passed by `rank_and_push.sh`) it **aborts the push** (non-zero exit, no
> Supabase write) unless the cache's global `MAX(trades.timestamp_unix)`, global
> `MAX(market_resolutions.fetched_at_unix)`, and completed CLOB sweep marker are
> all fresh under `--max-cache-staleness-hours`. Completion requires a present
> `source_cursor['clob_closed']` row with `value=''`; a missing, non-empty, or
> stale row fails closed. It also:
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
> Because these checks read `wallet_cache.db`, **run Part 1 (backfill) immediately
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
`resolutions` performs a full CLOB closed-market re-walk, backfills missing
schedule `end_date`s, then audits and repairs every traded, scheduled past-end
market still missing a terminal row. The subcommand runs the full
`fetch_resolutions_and_schedules`, so no separate `schedules` stage is needed
(#383/#519). **Stage 1** ranks the full trade
universe (`--universe-from-trades` —
have-data ⇒ in-universe; the ranker's own eligibility filters decide the cohort, so
there is no curated pre-gate); **Stage 2** rerank in three sub-stages (#536): **2a**
emit the per-token reference fetch windows for the candidate positions; **2b**
`pe-bootstrap prices-history --targets-csv` fetches only the uncovered remainder of
minute reference prices into the isolated ranker price store (write-once + range
algebra ⇒ resumable; transient page failures are a partial and pass-2's
terminal-coverage gate then exits 75 so the supervisor retries — a partially fetched
cycle can never publish); **2c** pass-2 reprices every candidate position at the
latest reference sample at-or-before `entry+Δ` (adds `hit_rate`, writes the
per-position `oracle_outcomes.csv` and the versioned `oracle_manifest.json` whose
canonical hash the push stores as `ranking_batches.config_hash`); **Stage 3** record
the exact publication request, atomically publish it through the idempotent
`publish_ranking_batch` RPC, and verify that exact batch is `latest_ranking`; **Stage 4**
purge proven-loser
and dead-weight wallets when armed; **Stage 5** run
`PRAGMA wal_checkpoint(TRUNCATE)` against the local cache so committed WAL pages are
checkpointed and the WAL file releases its disk footprint. The checkpoint is always
attempted last, including re-pushes and runs that skip purge. A busy/error result warns
without failing the already-complete Supabase publish; the next run retries it.


**Oracle rollback (#536):** reverting the ranker code alone does NOT restore the
prior ranking — `latest_ranking` always serves the maximum `batch_id`. To roll back
externally: stop the loop, restore the prior revision, run the one-shot with a unique
`--notes "rollback-of=<batch-id>"` (the note is hashed into the content-addressed
publish key, guaranteeing a NEW batch even same-day with unchanged inputs — a
same-day revert without it can reproduce an old key and silently fail to advance the
epoch), then verify a strictly larger `batch_id` is `latest_ranking` and service
membership converged. The additive `ranker_price_*` tables are inert thereafter.


> **Purge I/O priority (#527).** Both purge entry points — Step 0i `purge-infra` and the
> Stage 4 ordinary `purge` — run the cache mutator under `ionice -c3` (idle block-I/O
> class): the 2026-08-23 bulk purge saturated the cache disk at normal priority and
> starved SSH until a power cycle. The dependency fails closed: when a run can reach a
> purge site and `ionice` is not on `PATH`, the wrapper exits 2 before the run lock,
> cycle directory, or any cache state; an `ionice` execution failure is fatal at the
> infra site and a post-publication warning at the ordinary site — a purge never falls
> back to normal priority. Trade-off: under competing I/O an idle-class purge can take
> longer or stall entirely, which is preferred over starving the control plane. Ordinary
> purge cannot delay an already-completed publication (it runs after the push), but
> Step 0i `purge-infra` remains a prerequisite for ranking and may lengthen or stall
> the cycle. Ordinary
> purge stays disarmed (`PE_BOOTSTRAP_PURGE_ENABLED=false`) until #527 Phase 2 witnesses
> one real bulk-mode run complete under idle priority with control-plane probes intact —
> the gate is an organically produced disabled report whose delete set reaches the
> canonical `purge_bulk_min_wallets`; wiring-only subthreshold runs do not qualify.
> Rollback: atomically write the loop flag to `stop` (natural run-down — a unit stop
> needs the owner's explicit authorization for the active process), keep ordinary purge
> false, and keep the priority wrapper; if the wrapper itself must be reverted, leave the
> loop stopped, because `purge-infra` is armed on every invocation.

Production defaults are baked in (override via flags): `--universe-from-trades`,
`HALF_LIFE_DAYS=30` (30-day recency decay, #366/#370), relative 180-day window,
mid-price band 0.15–0.85, TTR 48h (`ranker_ttr_hours`), MinTRL 20 (`ranker_prod_min_trl`
— replaces the per-month activity gates, which production zeroes; run28 cutover
2026-07-03), `--scheduled-only` (no resolved-at look-ahead), `floor_tstat=2.0`,
`top_n=200`, latency shift Δ=2s (#530: the measured websocket-path copy speed —
batch-68 sweep: survivors/active 290/12 at 2s vs 185/6 at the old 20s; tape ties
resolve deterministically via `(timestamp_unix, source_trade_id)`).

> **Δ deploy ordering (#530).** The ranker's Δ=2 assumes the service copies at
> websocket speed. Deploying a Δ-lowering ranker change follows SERVICE-FIRST
> order: pe-service ships with `polymarket_activity_ws_enabled=true` and its
> stale-fallback budget verified (docs/35), and only then does the forge
> checkout pull the new `LATENCY_SHIFT_SECS`. Rollback is the reverse (docs/35
> "#530 websocket rollback ordering"). The +1-week re-check compares the
> measured leader→fill p95 span artifact against 2s and raises Δ only if
> measurement demands it.

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

The supervisor's process lifecycle and logging are owned by the user-mode
systemd unit `deploy/systemd/pe-rank-loop.service` (issue #521): output goes to
the journal, the script converts a stop signal into a clean numeric exit 143
(declared by `SuccessExitStatus=143`), and `loginctl enable-linger` starts the
unit at boot — the flag file still gates whether a boot-started supervisor
cycles. At boot the unit first waits for the repository's storage mount
(`ExecStartPre` path wait, issue #528); until the path appears the unit is
`activating`, and if it never appears within the unit's `TimeoutStartSec` the
start fails visibly with no main process and no automatic restart — inspect
the mount, then `systemctl --user start pe-rank-loop`. Install per `deploy/systemd/README.md`. There is no unmanaged fallback
launch: a detached supervisor would be a second production lifecycle, so if
user systemd is unavailable, fix the unit before running the loop.

```bash
cd ~/prediction-markets

# Enable the flag atomically, then hand lifetime to systemd.
loop_flag_tmp="data/eval-results/.rank_and_push.loop.$$"
printf 'run\n' > "$loop_flag_tmp"
mv "$loop_flag_tmp" data/eval-results/rank_and_push.loop
systemctl --user enable --now pe-rank-loop
```

The flag accepts exactly `run` or `stop`. Missing means a clean stop; empty or
any other value is fatal. The supervisor checks only between completed cycles:

```bash
# Graceful: finish the current full cycle, then stop (unit ends inactive/exit 0).
loop_flag_tmp="data/eval-results/.rank_and_push.loop.$$"
printf 'stop\n' > "$loop_flag_tmp"
mv "$loop_flag_tmp" data/eval-results/rank_and_push.loop

# Immediate: systemd sends TERM; the supervisor TERM-signals the entire current
# one-shot process group, waits, escalates to KILL after its bounded grace, and
# exits 143. systemd's own KILL backstop fires at TimeoutStopSec=45.
systemctl --user stop pe-rank-loop
```

Status and logs: `systemctl --user is-active pe-rank-loop` and
`journalctl --user -u pe-rank-loop`. A non-75 child failure stops the
supervisor deliberately for diagnosis (`Restart=no`); restart with
`systemctl --user start pe-rank-loop` after resolving it.

After an immediate stop, verify no loop, wrapper, bootstrap, or ranking Python
descendant remains; neither `.rank_and_push.lock` nor the cache mutation lock is
held; and SQLite opens/read-checks cleanly.

Each zero-argument one-shot creates its run directory, then atomically writes
`data/eval-results/rank_and_push.cycle` after dependency preflight and run-lock
acquisition but before discovery, activation, or any other cache mutation. The
directory deterministically owns that cycle's activation batch ID. A direct
zero-argument invocation and the supervisor both honor an existing recovery
pointer rather than allocating a new run.

Exit 75 means a bounded transient operation exhausted its in-process retries or
the resolution audit was blocked (`blocked > 0 || clipped > 0`: a market whose
available venue truth could not be recorded — fetch failure, identity mismatch,
contradictory payload, unrecordable terminal state — or a repair-cap clip). The
loop waits 60 seconds while
checking the run flag once per second, then:

- If `rank_and_push.pending` exists, it takes precedence. Recovery replays the
  same content-addressed Supabase request and completes only that run's
  purge/checkpoint tail; it does not rediscover, activate, backfill, export, or
  rank.
- Otherwise, if `rank_and_push.cycle` exists, the loop invokes the normal
  zero-argument command. The one-shot reuses the pointed run directory and
  deterministic activation batch, so retrying discovery, activation, backfill,
  events, or ranking cannot admit another 20,000-wallet cohort.
- Exit 75 without either pointer is fatal. Any nonzero code other than 75 is a
  permanent failure and stops the loop with the applicable pointer retained for
  diagnosis and an operator-directed retry.

Markets the venue itself has not resolved (open lagged/extended/inactive, or
closed with winner flags not yet posted) do NOT block publication: they are
counted in the audit summary (`lagged`/`extended`/`inactive`/`open_unknown`/
`pending`), write nothing, and are retried by later ordinary cycles because
audit membership derives from the absence of a resolution row. A blocked audit
retries without a loop-level cap and logs each blocked condition ID with a
typed reason; clipped overflow is identified by count only. To stop a
persistent retry, write the normal `stop` flag; the operator escape — manually
inserting a terminal NULL-winner row — is permitted only after independently
verifying the market terminal, with the evidence and source recorded, and never
for a merely open or pending market.

Both pointers are regular, non-symlink, one-line repository-relative paths and
are validated beneath `data/eval-results/cron-<UTC>/`. Successful completion
compare-and-clears only a pointer that still names the run being completed. If
the pointer changed or became malformed, the wrapper warns and preserves it.
Rollback writes `stop`, waits until `systemctl --user is-active pe-rank-loop`
reports `inactive`, then runs `systemctl --user disable pe-rank-loop` if the
loop should not return at boot.

Inspect recovery state without changing it:

```bash
for pointer in \
  data/eval-results/rank_and_push.pending \
  data/eval-results/rank_and_push.cycle
do
  if [[ -L "$pointer" ]]; then
    printf '%s\n' "$pointer: unsafe symlink"
  elif [[ -f "$pointer" ]]; then
    printf '%s: ' "$pointer"
    sed -n '1,2p' "$pointer"
  else
    printf '%s\n' "$pointer: absent"
  fi
done
```

For a production cycle interrupted before this pointer contract was deployed,
do not simply start a new zero-argument run. First prove from the old run log
and SQLite audit that its `cron-<UTC>` directory maps to the already-committed
activation batch, prove no pipeline process or lock is live, and prove no
publication pointer conflicts. Only then atomically seed `rank_and_push.cycle`
with that exact repository-relative directory and start the current supervisor.
If any identity or liveness check is ambiguous, leave the loop stopped.

Each cycle logs `LOOP_CYCLE_START`, the child PID/process-group ID, the one-shot
`RANK_AND_PUSH_RUN_DIR=...` handoff, and `LOOP_CYCLE_END` with exit status.
Recovery additionally logs `LOOP_TEMPFAIL`, `LOOP_RESUME_START`, and
`LOOP_RESUME_END`, with `kind=publication-resume` or `kind=cycle-resume`.
The distinct supervisor lock prevents two loops from racing at a cycle boundary.

Before first deployment, apply `scripts/supabase_schema.sql` before updating the
Forge checkout. The additive `ranking_batches.publish_key` column and unique
index preserve historical batches whose key is null. The service-role-only
`publish_ranking_batch` RPC creates/reuses the keyed batch and inserts all entries
inside one PostgreSQL transaction, so a failed request exposes neither a partial
epoch nor a duplicate epoch. If code rollback is required, stop the loop and
restore the prior checkout; the additive schema can remain in place.

The final checkpoint remains inside that lock and starts only after every pipeline DB
writer has exited. It checkpoints committed data rather than deleting rows; its storage
effect is to truncate the separate `wallet_cache.db-wal` file.

`pe-service` on the VPS picks up the new `latest_ranking` on its next refresh
(score-update-only). MEMBERSHIP follows `watchlist_membership_mode` (`_GLOSSARY.md`):
`knockout` (legacy — the maintenance tick's eviction/backfill is the sole membership
path) or `full_rerank` (each batch transition wholesale-replaces the live
top-`active_watchlist_size` SURVIVORS — the cutover production mode; the rows are read
from `ranking_entries` pinned to the triggering `batch_id`, never from the moving view, so
the applied rows and the committed marker name one batch, #542). Every read is
gated on the ranker's `survives` verdict (#518), so the published batch is a bench and
`active_watchlist_size` caps the survivors admitted from it rather than selecting a raw
top-N; membership converges at the deploy restart itself, because boot validates the live
set from the same filtered read. Every post-boot addition on either path is prepared first
(#542/#544): its prior-market history is complete and its current positions pass the
five-step causal bracket before the orchestrator records the validation and the wallet is
published (log line `hot-watchlist admission state prepared`, then `full re-rank membership
swap applied` / `maintenance tick applied`); a preparation failure publishes no additions
and the tick retries. `active_watchlist_size` is
Supabase-authoritative (default 100, valid `1..=200`) and is polled every 30 seconds. A
grow fetches the requested top-N and validates all newly admitted wallets' prior-market
history and current positions before the atomic validation/membership swap; a shrink uses
the same atomic membership swap. Invalid values, Supabase failures, or incomplete admission preparation keep
the last-known-good target and membership, then retry independently on the
capacity worker's next 30-second retry. Check
`status.json`: `watchlist_size` is actual membership and `watchlist_target_size` is the
last safely applied runtime cap. The additive optional `live` block reports
`pending_dispatch_seeds`, `ready_dispatch_seeds`, `fetched_at_unix` (last successful accounts
poll; `null` before one succeeds), `stale` (`true` past `live_accounts_stale_after_secs` —
no new live work is staged while stale, #514), and per-account `account_id, is_primary, enabled,
requested_live_mode, effective_live_mode, armed` (#508). Installing this runtime-capacity support
requires one normal `pe-service` restart; subsequent valid `service_config` edits hot-swap without
a restart.
Run28 fixed `k=25` and did not sweep watchlist width. The earlier top-50 paper experiment
adopted 2026-07-13 and the superseding 100-wallet default are operator-directed choices,
not run28-backed N choices.

---

## Output artifacts

`rank_and_push.sh` writes each run's ranked CSVs and
`ranking_publish_request.json` under an auto-timestamped
`data/eval-results/cron-<UTC>/` (override with `--out-dir`). The request records
the exact batch provenance, filtered entries, retention value, and content hash
used for retry/audit. During any incomplete zero-argument production cycle,
`data/eval-results/rank_and_push.cycle` contains the repository-relative run
directory and preserves the cycle's deterministic activation identity. During
an incomplete production publication,
`data/eval-results/rank_and_push.pending` contains one repository-relative path
to that request and takes recovery precedence over the broader cycle pointer.
Both are compare-and-cleared only after publication verification and the same
run's purge/checkpoint tail. Retire superseded artifacts to
`data/archive/`; never delete eval outputs — they are the audit trail for what
was published to `latest_ranking` and when.

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
