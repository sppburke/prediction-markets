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
# For the #606 comparison, preserve the installed baseline first (protocol below).
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
  read-only during a historical operator-authorized purge. Because the default temp dir
  lived there, that bulk purge's `VACUUM` and index rebuild both failed while the deletes
  (which write only to the cache volume) succeeded — leaving 38.5 GiB of unreclaimed free
  pages, an absent `trades` index, and a subsequent cycle that silently paid a multi-hour
  rebuild.
  Symptom to recognize: `sqlite: unable to open database file` from a stage whose cache
  path is demonstrably writable.
- **Wallet-cache memory.** The operator knobs and defaults are in
  [Bootstrap defaults](_GLOSSARY.md#bootstrap-defaults-pe-bootstrap); the effective
  [mmap ceiling](https://www.sqlite.org/pragma.html#pragma_mmap_size) is capped by
  bundled SQLite's platform/build limit (or zero when mmap is unavailable), so use
  the writer's `wallet cache: connection tuning applied` log rather than assuming
  the requested size was granted.
- **Market resolutions (no RPC).** The Polymarket CLOB `/markets?closed=true`
  listing is the sole payout-resolution source (#369; key-free). Gamma supplies
  schedules, event mappings, liquidity, and mark-price inputs, never payouts.
  No Polygon RPC / Alchemy provider is required.
- **Supabase + Python.** `.env` must carry `SUPABASE_URL` + `SUPABASE_SECRET_KEY`
  (the push reads them). `rank_and_push.sh` selects its own interpreter so the documented
  one-line command is independent of an interactive shell's `PATH`: `PE_PYTHON` (an
  executable-path override) → `.venv-analysis/bin/python3` → `.venv/bin/python3`. It does
  not silently use the system `python3`. Install `scripts/requirements.txt` into one of those
  repository environments. Before taking the PID lock, creating a run directory, or refreshing
  the cache, the wrapper imports its required modules and exits with remediation instructions if
  the environment is incomplete. Schema one retains `engine=auto` with its SQLite fallback;
  schema two requires the verified Parquet/DuckDB path and rejects SQLite.

---

## Wallet-cache tuning measurement and rollback (#606)

Run this comparison on Forge through the existing
[systemd loop lifecycle](#continuous-forge-supervisor), using the defaults in
[Bootstrap defaults](_GLOSSARY.md#bootstrap-defaults-pe-bootstrap) for the candidate.
This changes read-side caching only; acquisition transactions, WAL and index maintenance
remain unchanged. No automatic memory sizing is performed.

1. Before building, preserve the currently installed `c1cdf82` binary as
   `target/release/pe-bootstrap.pre-606-c1cdf82` and retain the current `.env` for
   rollback. Use that binary and `.env` as the baseline. Record baseline pragmas as
   **SQLite defaults (no pragma set by the binary, `cache.rs:95–96` at `c1cdf82`)**;
   a fresh read-only Python connection's cache/mmap pragmas are not the writer's view.
2. Measure two 10-minute windows per binary, on the same cache generation and the
   same cycle kind (`cycle-resume`). Select the `backfill` stage child from
   `pgrep -x pe-bootstrap`, checking `/proc/<pid>/cmdline` for `backfill`. Retain
   the PID, binary identity, cache generation, cycle log and window timestamps;
   discard a window if that stage exits or its PID changes.
3. At each window's start/end, query `select max(rowid) from trades` through a
   read-only Python SQLite connection. Its delta is inserted rows. Record the
   `/proc/<pid>/io` `read_bytes` and `write_bytes` deltas. Every 30 seconds sample
   `/proc/<pid>/status` `VmRSS` and `/proc/meminfo` `MemAvailable`; retain the
   sampled RSS peak and available-memory minimum. Read bytes per 1,000 inserted
   rows is `1000 * read_bytes_delta / inserted_rows`; zero inserted rows makes
   the window inconclusive and requires a repeat. Do not report pages/s.
4. Build the candidate on Forge, stop the loop, verify its descendants have exited,
   and restart via systemd with the candidate binary and configured `.env`.
   Retain the new writer log's `requested_cache_kib`, `effective_cache_kib`,
   `requested_mmap_bytes` and `effective_mmap_bytes` for each candidate window;
   cache KiB values use SQLite's negative-KiB convention. Success requires lower
   read bytes per 1,000 inserted rows than baseline with `MemAvailable` never
   below **2 GiB**.
5. If `MemAvailable` falls below that floor or any `MemoryPressure`/OOM kernel
   line occurs during measurement, stop the loop through systemd, verify descendants
   have exited, restore the preserved binary and prior `.env`, and restart the
   loop through that same lifecycle. Keep the cache generation and recovery pointers.

## Part 1 — Backfill tradeable wallets (no new wallet discovery)

**Goal:** bring trade history, resolutions, and event mappings current for the
wallets **already** marked tradeable in the pile, *without* discovering or adding
any new wallets.

### What this does and does not do

`pe-bootstrap backfill` selects active, non-infrastructure wallets through
`active_tradeable_wallets`. A marked wallet is due regardless of its previous
fetch stamp; otherwise the existing staleness rule applies. The timeout and
concurrency defaults are unchanged (see [bootstrap defaults](_GLOSSARY.md#bootstrap-defaults-pe-bootstrap)).

Each wallet walk samples one frozen bound, `hi = now - ACTIVITY_SETTLE_LAG_SECS`.
Every request uses DESC order and an explicit upper bound no greater than `hi`.
Backward requests also carry `start=1` to avoid the venue's default history window.
The cache stores a durable closed interval
`[backward_floor_unix, forward_frontier_unix]`:

- Backward pages commit the rows strictly above their minimum second `m` and
  `floor=m+1` together, then acquire **all** of second `m` and commit its rows
  with `floor=m`. Cancellation between these transactions resumes at `m`.
  Short or empty terminal pages record floor 1. Empty or wholly unconvertible
  pieces still advance coverage; stored `MIN(timestamp_unix)` is not a cursor.
- Forward acquisition scans upward from the persisted frontier. Its first window
  is one second wide; later widths target `FORWARD_WINDOW_TARGET_ROWS` using
  **raw** response density, with at most eightfold growth, the canonical maximum
  width, and eightfold shrinkage on saturation. Each complete window commits its
  rows and frontier in one transaction. There is no known-ID early stop.
- Before acquisition, `begin_walk` upserts `backfill_partial=1`, seeding a NULL
  frontier from `min(entry_max, hi+1)-1` on both insert and update. Legacy cached
  maxima are therefore re-covered for same-second siblings. A wallet without a
  pile row is supported; its new row defaults inactive. No history means no seed;
  the first cold backward commit establishes frontier `hi`. Persisted coverage
  is retained even when the trade table is empty.
- Timeout, transport/JSON/conversion-boundary errors, saturation of a single
  second, or failed transactions leave earlier committed pieces intact. The
  wallet remains partial, its old stamp is unchanged, and logs report durable
  transaction progress. Finalization clears the marker, extends the frontier to
  at least `hi`, and optionally stamps success in one transaction. Failure to
  finalize is a failed wallet, including when stamping is disabled. Cold-empty
  completion is valid: marker zero, defined frontier, no trade rows.

Durability is **per completed backward piece or forward window transaction**, not
per HTTP response. One interrupted window loses its buffered work. At the
inclusive offset ceiling a window requires at most eleven page responses; a
saturated attempt commits nothing. If a budget cannot complete a dense first
second, that bounded attempt repeats next cycle. Progress across interruptions
requires at least one completed window per walk; there is no promise of eventual
completion. A durable in-flight-window checkpoint was deliberately excluded.

The interval certifies what covering requests acquired, not that every returned
row is retained. Schema one still rejects individual trades during conversion
(zero/negative size, quantity overflow, unknown side, invalid price/timestamp),
while malformed DTO fields reject an entire page. It also keys `trades` on
`source_trade_id=transactionHash` alone: counterparties in different wallets and
distinct trades sharing a hash collide under `INSERT OR IGNORE`. Schema-two
identity repair remains outside this change.

Marker zero additionally establishes that the backward phase exhausted available
history, so **requested coverage** extends from the beginning of history through
the frontier. During a cold partial walk, history below the floor remains
unacquired even though the frontier is already set. The coverage pair moves only
outward under the walker. Inherited history below a legacy seeded anchor is not
retrospectively re-queried or proven settled. Pre-#609 holes below that anchor and
rows first becoming visible after their crossing request are outside this
coverage guarantee. The settle lag applies to normalized integer timestamps;
at `t=hi`, the bucket `[t,t+1)` has been closed one second less than the lag.
The stricter bucket-end reading would require subtracting one additional second;
this implementation uses the approved timestamp discipline. The next crossing is
on the wallet's next **due walk**, governed by staleness, not the loop's success
wait. No immutable activity-bucket closure is promised by the venue.

Every wallet with zero stored trades receives the settled-head infra probe,
including a previously completed empty wallet. Classification precedes inserts;
probe rows are discarded and infra classification preserves the prior stamp,
even a non-NULL stamp from an earlier empty success. Two verdict changes are
accepted: the settled head can be sparser than the previously unbounded newest
head, and a timed-out walk that retained a sparse page is no longer cold on retry
and therefore does not re-probe a newly dense head. No persistent probe lifecycle
was added.

Schema-one production Python ranking and newly prepared database-backed
publications exclude **all** marked wallets before truncation, including inactive
wallets and retained-CSV `--skip-rank` publications. Wallets without a pile row
and unmarked legacy wallets remain eligible. Saved publication requests remain
immutable on resume. The cycle fingerprint includes the full marked set; the
`unchanged` guard reads current state and refuses only while active,
non-infrastructure marked wallets remain retryable. Activating an inactive marked
wallet deliberately makes it retryable while it stays excluded.

The single `run_ranking_stage` policy owner interprets schema-one exit 76 (empty
universe, summaries, eligible set, edge floor, candidates or publication; also
**global trade staleness**) as 75 while retryable partial wallets currently exist,
and as 1 otherwise. It reads the database after refresh, independently of the
frozen manifest, including resumed cycles. No request is persisted for these
refusals; the loop retains the logical-cycle pointer and retries backfill.
Other failures, including resolution/CLOB freshness and stored publication hash
mismatches, retain their existing exit codes. Schema two is unchanged.

Coverage reports count partial histories as incomplete even with prior stamps.
Both purge rules protect marked wallets, including a retained loser CSV; the
rule-B selector also guards independently. Retroactive infra classification
excludes them in preview and apply mode. Stamp seeding and raw trade readers are
unchanged. The exclusion guarantee is scoped to the production Python ranking
pipeline: offline watchlist/backtest consumers may incorporate retained partial
history. The production service continues to bootstrap from Supabase only.

Backfill also refreshes configured resolutions and schedules. It does not perform
new wallet discovery; it may activate existing pile wallets under the existing
activation policy. Event mappings are refreshed by the separate `events` command.

### #608/#609 rollout, acceptance and rollback on Forge

Stop the loop through the [systemd lifecycle](#continuous-forge-supervisor) and wait
for its cycle/descendants to exit. Preserve `target/release/pe-bootstrap` as
`pe-bootstrap.pre-608-<rev>`, record the revision, then build and restart with the
Python changes. Both public writable openers delegate to `open_with_tuning`, whose
schema-one migration installs the marker and two nullable coverage columns without
changing #606 tuning. `capture` runs before the first writable open and tolerates
missing columns; `winner-discovery` is the cycle's first writable opener.

Before deployment freeze the active zero-trade cohort on Forge and retain its
hash outside the repository:

```bash
sqlite3 -readonly data/wallet_cache.db 'SELECT a.wallet_hex FROM active_tradeable_wallets a LEFT JOIN trades t ON t.wallet_hex = a.wallet_hex WHERE t.wallet_hex IS NULL ORDER BY 1;' > ~/608-cohort.txt
sha256sum ~/608-cohort.txt
```

Record each wallet's disposition for the first three cycles: not due (unmarked,
fresh stamp, unchanged rows), partial/retryable (non-decreasing acquired rows and
frontier, still selected), completed empty (marker zero, stamp/frontier, zero
rows), completed nonempty (marker zero, stamp/frontier, positive rows), or infra
(discarded probe, preserved prior stamp, excluded from completion/retry assertions).
No cohort wallet's acquired row count may decrease. Every marked **active,
non-infrastructure** wallet must be re-selected. Universal completion is not the
gate: each wallet must match its disposition.

Audit one completed nonempty cohort wallet with the read-only operational tool:

```bash
python3 scripts/audit_wallet_history.py --db data/wallet_cache.db --wallet "$wallet" --cutoff "$cutoff"
```

The cutoff must be at or below the stored frontier and the marker must be zero,
checked from one consistent snapshot with the rows. The auditor walks full
activity with complete boundary-second pagination, mirrors DTO/conversion rules,
and reports per-UTC-day counts and the symmetric transaction-ID difference.
That mirroring is enforced, not asserted: the auditor and the Rust writer both
read `crates/bootstrap/tests/fixtures/dto_parity.jsonl`, so any input the two
parsers judge differently fails CI. Python's `json` and `Decimal` accept several
inputs serde rejects — duplicate recognized fields, `NaN`/`Infinity`, the `-0`
literal in an integer field, surrounding whitespace, Unicode digits and unpaired
surrogates — and serde accepts some the auditor must not reject, such as a
repeated field the DTO ignores. Extend the corpus when either parser changes.
It reports intra-wallet collisions **before** collapsing IDs, with all conflicting
normalized rows and the stored representative. A failed page or saturated second
refuses comparison. Acceptance requires an empty difference for that completed
wallet; collisions remain explicit evidence of schema-one loss, not a false clean
audit. `--base-url` supports a deterministic test endpoint.

**Binary-only rollback:** stop the loop, restore the preserved executable, restart.
The old writer never updates the new columns, so marked histories remain
quarantined and become retryable as soon as the corrected binary returns.
If the old writer appended above an established frontier with #609 gaps, the
corrected walker repairs them from that frontier. Exception: rollback after a
cold NULL-anchor `begin_walk` but before the first coverage commit leaves no
established frontier. If the old writer then builds gapped history, restoring the
corrected binary seeds from cached bounds and may inherit those holes; use the
re-walk lever or accept the documented legacy limitation. Never clear a marker
by hand from an old success stamp.

**Full rollback** including Python exposes retained partial histories to ranking.
Keep the filter or explicitly accept that visibility and use the
[published-batch recovery](#part-2--rank-and-publish-the-one-command) procedure if
necessary. Replay impact: none; this is the schema-one research cache and ranking
universe, with no live trading mutation.

**Operator re-walk lever**, with the loop stopped:

```sql
UPDATE wallets SET forward_frontier_unix = 0, backward_floor_unix = 1,
    backfill_partial = 1 WHERE wallet_hex = :w;
```

This deliberately resets coverage to the empty interval and is the sole exception
to outward-only bounds; it bypasses the walker guard. The marker schedules a walk
only for an active, non-infrastructure wallet. Do not seed from stored `MIN` or
`MIN-1`: that can skip same-second siblings or claim beyond the next frozen bound.
The backward phase issues no request and forward acquisition covers `[1,hi]`,
then later walks extend as time advances. A cached trade above `hi` neither blocks
correct finalization nor authorizes a request above it. Automated repair at scale
remains outside scope (#595 census).

### Commands

```bash
# 1. Refresh stale active-wallet trades; configured resolution refreshes full-walk CLOB.
PE_BOOTSTRAP_CACHE_PATH=data/wallet_cache.db \
  ./target/release/pe-bootstrap backfill

# 2. Refresh condition→event mappings (needed by the ranker's
#    distinct-events eligibility gate; backfill does not cover this).
PE_BOOTSTRAP_CACHE_PATH=data/wallet_cache.db \
  ./target/release/pe-bootstrap events

# 3. (Optional) Run the full CLOB resolution re-walk explicitly.
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
>   **only** for `Python-urllib/3.11`. So `pe-bootstrap`'s CLOB closed walk and the service CLOB
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
>   `pe-service` and bootstrap metadata paths — including the *open* passes, which the stale
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
relative to the markets your wallets traded, re-run step 3 (the CLOB resolution
walk — no RPC required, #369/#372).

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
The production wrapper performs no infrastructure or ordinary purge; exclusions gate
acquisition (discovery, activation, and backfill), and production rank/export applies
trade-recency, current-eligibility and schema-one completeness filters without deleting history
(`docs/37`).
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
`publish_ranking_batch` RPC, verify that exact batch is `latest_ranking`, and capture the
accepted cycle manifest used by the next unchanged-watermark check. No post-publication deletion,
reclamation, index rebuild, or checkpoint stage exists (#544).


**Oracle rollback (#536):** reverting the ranker code alone does NOT restore the
prior ranking — `latest_ranking` always serves the maximum `batch_id`. To roll back
externally: stop the loop, restore the prior revision, run the one-shot with a unique
`--notes "rollback-of=<batch-id>"` (the note is hashed into the content-addressed
publish key, guaranteeing a NEW batch even same-day with unchanged inputs — a
same-day revert without it can reproduce an old key and silently fail to advance the
epoch), then verify a strictly larger `batch_id` is `latest_ranking` and service
membership converged. The additive `ranker_price_*` tables are inert thereafter.


> **Purge-free publication (#544).** `rank_and_push.sh` never invokes `purge` or
> `purge-infra`; `--skip-purge` remains only as a backward-compatible no-op. Both direct
> commands obey `PE_BOOTSTRAP_PURGE_ENABLED=false` as report-only. Keep that value false.
> Any future delete requires separate operator authorization and is not part of a ranking
> cycle. A pre-existing `meta.reclamation_pending` obligation is recovered only through the
> recovery-only `pe-bootstrap recover-reclamation` path after the evidence gate below; that
> command cannot select or delete wallets.

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
- `--skip-purge` — backward-compatible no-op; automatic wallet deletion is retired.
- Pure re-push: `--skip-discovery --skip-backfill --skip-rank --out-dir <prior run>`.

### Forge lock and reclamation-evidence contract (#544)

The three production lock inodes are persistent and kernel-held:

1. `data/eval-results/.rank_and_push_loop.lock`;
2. `data/eval-results/.rank_and_push.lock`;
3. `data/wallet_cache.db.lock`.

Acquire them only in loop → one-shot run → cache order. Shell uses `flock`; Rust uses
`fs2` on the same inodes. Every owner opens without truncation, acquires exclusively and
nonblocking, then writes its decimal PID. Contention changes neither inode nor PID contents;
process exit releases ownership, including after a kill, without stale-file reclamation. Every
read-write `pe-bootstrap` entry acquires the cache lock before opening SQLite; true readers use
the read-only open path. Pre-#544 PID-file artifacts do not coordinate safely with this scheme:
before activation, stop and prove the absence of every legacy loop, one-shot, and bootstrap
process, then prohibit those artifacts from invocation.

Before cache activation, hold all three locks and capture the gate:

```bash
pe-bootstrap reclamation-evidence --db data/wallet_cache.db \
  > "$CACHE_ACTIVATION_RECLAMATION_EVIDENCE"
```

Inspect the newest `purge_status.jsonl` records, `meta.reclamation_pending`, freelist pages,
required trades-index inventory, and resolved `SQLITE_TMPDIR` path/device/free space. Activation
requires the marker absent, all required indexes present, and the path/device unchanged from the
preliminary capture. If and only if the marker is pending and the operator separately authorizes
recovery, run `pe-bootstrap recover-reclamation --db data/wallet_cache.db`, then capture and
inspect the evidence again. Missing index, changed path/device, insufficient space, or recovery
failure stops before checkpoint or rename.

### Version-two cache generation and fixed-path activation (#544)

Build and resume the side cache by its hash-bound manifest and explicit `--db` path. Version one
is sealed into `*_v1_sealed` audit tables; version-two consumers read only complete normalized
activity and CLOB-payout generations, and no API unions the generations.

```bash
pe-bootstrap cache-migrate-v2 \
  --db "$CACHE_V2_SIDE" --manifest "$CACHE_BUILD_MANIFEST"

pe-bootstrap cache-verify-frozen-v1 \
  --db "$CACHE_V2_SIDE" --frozen-payload "$FROZEN_PAYLOAD_REFERENCE"

pe-bootstrap cache-populate-activity-v2 \
  --db "$CACHE_V2_SIDE" --frozen-payload "$FROZEN_PAYLOAD_REFERENCE" \
  --fixed-end "$FIXED_END_UNIX" --generation 1

pe-bootstrap cache-populate-payout-v2 --db "$CACHE_V2_SIDE"

pe-bootstrap cache-finalize-v2 \
  --db "$CACHE_V2_SIDE" --stage-record "$CACHE_STAGE_RECORD"
```

Frozen verification precedes all activity I/O. Each completed wallet commits its aggregates and
receipt together; restart schedules only missing exact receipts, and finalization requires the
receipt set to equal the frozen universe before atomically installing the activity manifest and
deleting staging. Finalization also verifies payout coverage, builds the Rust ledger/classifier
projection, and records its count and digest.

Rank and cut over through the one publication path. This snapshots the current published batch,
exports and verifies the schema-two Parquet projection, computes the minute-price rerank and exact
diff, durably prepares the publication request, activates the side cache, then resumes that exact
request. The targeted price-store write is re-finalized before request preparation so the stage
hash covers the installed bytes:

```bash
PE_PYTHON="$PE_PYTHON" bash scripts/rank_and_push.sh \
  --db "$CACHE_V2_SIDE" --engine duck \
  --cache-stage-record "$CACHE_STAGE_RECORD" \
  --fixed-db "$FIXED_PHYSICAL" \
  --prior-cache-backup "$CACHE_PRIOR_BACKUP" \
  --skip-discovery --skip-backfill
```

`FIXED_PHYSICAL` is the regular file behind `data/wallet_cache.db` (`readlink -f`); activation and
restore refuse a symbolic link. The exact publication request carries the side path, fixed path,
generic prior-backup path, and stage hash inside its `publish_key`. If the process stops after preparation, the existing pending
pointer resumes idempotent activation before publication; no additional pointer is used. Before
either first activation or resumed activation, the publisher validates the complete request and
returns the sole activation tuple consumed by the wrapper. A content-hash mismatch therefore fails
before any cache mutation. Request and pointer replacement fsync both the new file and containing
directory. Activation accepts a verified schema-one or schema-two fixed cache and retains one
generic prior-main backup.

The wrapper remains the one-shot run-lock owner. For activation it passes the inherited run-lock
descriptor and, under the supervisor, the inherited loop-lock descriptor. `pe-bootstrap` verifies
each descriptor's inode, PID stamp, and live kernel contention before skipping only that lock; Rust
always acquires the cache lock. A direct `pe-bootstrap cache-activate` call without that verified
handoff continues to acquire loop → run → cache itself.

Before the bound corrected batch becomes current, restore that exact prior cache by its recorded
hash and schema. Preserve the displaced cache for audit:

```bash
pe-bootstrap cache-restore-prior \
  --fixed-db "$FIXED_PHYSICAL" \
  --backup "$CACHE_PRIOR_BACKUP" \
  --displaced-backup "$DISPLACED_CACHE_BACKUP" \
  --prior-sha256 "$PRIOR_CACHE_SHA256" \
  --prior-schema "$PRIOR_CACHE_SCHEMA" \
  --publication-request "$PUBLISH_REQUEST_FILE" \
  --pending-pointer data/eval-results/rank_and_push.pending
```

Run this only with `SUPABASE_URL` and `SUPABASE_SECRET_KEY` in the environment after stopping the
rank supervisor. Restore validates the pending pointer, the complete request `publish_key`, the
fixed/prior activation paths, and the installed corrected-cache hash while holding the Forge lock
stack. It then asks authoritative `ranking_batches.publish_key` whether that exact publication was
ever consumed. A consumed publication, a missing/malformed pointer or request, or unavailable
authority refuses restoration; recover by rolling forward through the existing pending-publication
path. There is no operator assertion flag. Schema one retains `auto | duck | sqlite`; schema two
requires the verified DuckDB snapshot and refuses SQLite.

### Fresh private-candidate cycles and the scheduled schema-two lane (#588)

The frozen-payload flow above seals one historical snapshot; it cannot collect a later
generation because the frozen reference is bound to one generation and end, the wallet
list is fixed to the sealed schema-one history, and activity insertion moves matching rows
between generations inside one database. Recurring classifier-two publication therefore
builds every cycle in a **private candidate** copied from an **immutable prior** of the fixed
cache and collects fresh complete activity for the union of current acquisition candidates
and every retained history, without a frozen reference:

```bash
# Under the cache lock: checkpoint + quick-check the fixed main, copy it to the
# immutable prior, copy the prior to the candidate (each copy hash-verified before its
# rename). A schema-one prior also gets its hash-bound build manifest for the initial seal.
# Every path names a file in an existing directory (the cycle directory beside the fixed
# cache): staging never creates directories.
pe-bootstrap cache-stage-v2 --db "$FIXED_PHYSICAL" --prior "$PRIOR" --side "$SIDE" \
  --manifest "$CACHE_BUILD_MANIFEST"
pe-bootstrap cache-migrate-v2 --db "$SIDE" --manifest "$CACHE_BUILD_MANIFEST"   # unsealed candidate only
pe-bootstrap winner-discovery --db "$SIDE" --defer-activation
pe-bootstrap activate-next --db "$SIDE" --batch-id "$BATCH" --audit-csv "$AUDIT"
pe-bootstrap cache-populate-activity-v2 --db "$SIDE" --fresh-generation "$N"
pe-bootstrap cache-populate-payout-v2 --db "$SIDE"         # unless generation T is complete
pe-bootstrap cache-finalize-v2 --db "$SIDE" --stage-record "$CACHE_STAGE_RECORD"
```

`--fresh-generation N` records one versioned collection identity in the candidate
(`fresh_collection_json`, `_GLOSSARY.md`): the requested generation, its fixed end
(`now − ACTIVITY_SETTLE_LAG_SECS` at start), the sorted wallet union and its digest, which
is the `reference_sha256` of every receipt and manifest of that generation. Starting a
generation atomically invalidates finalization and clears only the candidate's superseded
projection, activity rows, receipts and manifests; `N` must exceed every generation the
candidate knows, and an unfinished generation can only be resumed. A retry with the same
`N` keeps the recorded end and wallet list and fetches only wallets without a valid
receipt; a completed generation returns its manifest without any source call. Fresh reads
request each wallet's full history (`start=1` on the wire; an omitted `start` returns only
the venue's recent window). A read that exhausts the fetcher's transient retries or is
rate-limited by the venue exits `rank_and_push_tempfail_exit` (75) so the supervisor resumes the
collection. A wallet whose fetched history the aggregator refuses as causally ambiguous (one
fill whose rows carry different venue timestamps) is excluded from the generation instead of
failing the cycle: the command logs a warning naming the wallet and the reason, its receipt keeps
the page evidence with zero aggregates and a zero source-row count, the resume does not refetch
it, and finalization projects no ranker entry for it.
Finalization, activation and the installed-cache validator accept the fresh identity without
a frozen-payload row; caches finalized under the frozen flow keep their authentic legacy
identity, including caches that physically lack the new column.

The zero-argument `rank_and_push.sh` production cycle enters this lane automatically when
the installed cache is schema two, and for the one-time initial cutover when `.env` sets
`PE_RANK_SCHEMA_TWO_CUTOVER` (`_GLOSSARY.md`). The lane is frozen in the cycle's
`cycle_configuration.json` (`cache_lane`), so a resumed cycle keeps its lane even if the
opt-in changes and an outstanding legacy cycle completes under its original contract. In
the lane, Step 0 is the sequence above (legacy `backfill`, `events` and `resolutions` read
retired `trades`/`source_cursor` and do not run), followed by the existing cutover path:
Parquet export, pass one, targeted `prices-history`, second finalization, candidate
recapture into `candidate_cycle_manifest.json` (bound into pass two; `cycle_manifest.json`
keeps the cycle's initial installed-cache watermark), `--prepare-only`, `cache-activate` and
the exact `--resume-request`. Both targets are fixed by the immutable prior and reused on
retry: `N` is the prior's newest activity generation plus one (1 for a schema-one prior);
the payout target is the prior's active walk if one exists, else its newest coverage
generation plus one, and a walk already completed on the candidate is reused rather than
restarted. After every successful publication — the fresh path, the automatic pending
resume and explicit `--resume-pending` — the accepted watermark is captured from the
request's installed fixed path before the pointers clear, so the next unchanged same-day
invocation skips before staging.

**Physical layout.** The candidate lane uses the regular physical fixed file
(`readlink -f data/wallet_cache.db`) and derives per-cycle names beside it from the
durable cycle directory: `wallet_cache.<cron-UTC>.side.db`, `.prior.db`, and the
`.displaced.db` name used only by `cache-restore-prior`. The Rust lock owners derive the
cache lock and the loop/run lock directory from that physical path, so the physical
directory must carry two aliases to the repository inodes, checked before any mutation:

| Alias | Target |
|---|---|
| `<physical dir>/eval-results` | `<repo>/data/eval-results` |
| `<physical dir>/wallet_cache.db.lock` | `<repo>/data/wallet_cache.db.lock` |

Fixed, prior, candidate and displaced files must be independent regular files on one
filesystem; staging refuses the same path, a hard link or a symbolic link among them. A
completed prior is never rewritten and a candidate without its prior is refused. The
existing locked prior-hash comparison at activation refuses a fixed cache changed after the
prior was captured; resume with the recorded candidate and prior, or, only while no request
has been prepared, abandon the cycle as described under recovery states and start a new one.

**Initial cutover and acceptance.** Complete any outstanding schema-one cycle and its
publication first. Create the two aliases, check free space for two additional copies of
the fixed file on that filesystem, then set `PE_RANK_SCHEMA_TWO_CUTOVER=prepare` and run
one zero-argument cycle, either under the supervisor or by hand while it is paused with
`scripts/deploy/forge_pause.sh`. It stages, seals, collects the full union, walks payout,
finalizes, ranks and prepares the exact request; the publisher's unchanged freshness checks
decide acceptance. On acceptance the wrapper prints `RANK_AND_PUSH_PREPARED_ONLY=<request>`
and exits 2 with the pending pointer retained and the installed cache untouched; under the
supervisor that non-75 exit stops the loop deliberately. While the value stays `prepare`,
neither recovery entry (automatic zero-argument or `--resume-pending`) activates: both print
the same line and exit 2 with the pointer retained. Record the measurement from the cycle
artifacts: the wallet union (`wallet_count` in the activity manifest of the candidate's
`activity_coverage_manifests_v2`, also in `candidate_cycle_manifest.json`), the candidate
size, elapsed time from the cycle log, and the source times the publisher accepted. Then set
the value to `1` and either run the zero-argument command once by hand or, if the loop was
paused, `scripts/deploy/forge_pause.sh restore` (it restores the recorded run flag and unit
state; a plain unit start would exit on the `stop` flag the pause wrote): the
pending-publication recovery activates and publishes exactly that request. If the publisher
refuses (stale source times, incomplete coverage), the cycle stops with the installed cache
untouched and the candidate, prior and log preserved; do not relabel times, narrow
membership or relax freshness. Require the next real scheduled refresh and publication
(installed schema two selects the lane automatically) before closing the classifier-two
handoff.

**Recovery states.** Before a prepared request exists, abandon a cycle only after
`scripts/deploy/forge_pause.sh pause` reports the loop inactive with no cycle descendant and
no held lock; then confirm `rank_and_push.pending` is absent and the cycle directory holds no
`ranking_publish_request.json`, remove `rank_and_push.cycle`, and delete only that cycle's
candidate. The fixed cache was never modified and the prior may be kept as evidence. Once a
request is prepared, never delete it or start another cycle: the pending pointer resumes
activation and publication. After activation but before the publication is consumed,
`cache-restore-prior` with the cycle's prior and displaced names restores the exact prior
bytes; the restore renames the prior file onto the fixed path, so the prior name is consumed
and the supervisor must stay paused until the recovery is resolved. After consumption, roll
forward. Forge's boot card is failing (#637); the caches, repository and evaluation results
live on the SSDs, but confirm the host before any cutover and keep the prior until the
publication is confirmed.

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

After any successful child with neither recovery pointer, the supervisor waits 60 seconds through
the shared flag-aware wait helper before starting another cycle. A stop or missing flag exits
during that wait. A retained recovery pointer bypasses only this success wait; transient exit 75
keeps its separate 60-second flag-aware backoff.

After an immediate stop, verify no loop, wrapper, bootstrap, or ranking Python
descendant remains; the persistent `.rank_and_push_loop.lock`, `.rank_and_push.lock`, and
`wallet_cache.db.lock` inodes may remain, but no process may hold them; and SQLite
opens/read-checks cleanly. Do not unlink any lock inode or interpret old PID text as ownership.

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
  same content-addressed Supabase request and verifies publication; it does not rediscover,
  activate, backfill, export, rank, delete, reclaim, or checkpoint.
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

Before allocating a new production run, the wrapper captures the current daily source watermark,
pipeline versions, and configuration. If they exactly match a prior accepted cycle, it prints
`RANK_AND_PUSH_UNCHANGED_DAILY_WATERMARK=1` and exits successfully before discovery, refresh,
export, rank, or publication. An existing publication pointer takes precedence over the cycle
pointer, and either pointer takes precedence over this new-cycle check.

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
Both are compare-and-cleared only after exact publication verification and accepted-watermark
capture. Retire superseded artifacts to
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
