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

On Forge, `~/prediction-markets` links to `/mnt/t7/prediction-markets`.

## Prerequisites

```bash
# Build from the prepared release checkout, separate from the installed checkout.
# Finish before draining; install through the release procedure below.
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
  `SQLITE_TMPDIR=/mnt/t7/sqlite-tmp` in `.env` on Forge (the wrapper exports its whole `.env` to
  every stage). Proven necessary on 2026-08-27: forge's root SD card remounted
  read-only during a historical operator-authorized purge. Because the default temp dir
  lived there, that bulk purge's `VACUUM` and index rebuild both failed while the deletes
  (which write only to the cache volume) succeeded — leaving 38.5 GiB of unreclaimed free
  pages, an absent `trades` index, and a subsequent cycle that silently paid a multi-hour
  rebuild.
  Symptom to recognize: `sqlite: unable to open database file` from a stage whose cache
  path is demonstrably writable.
- **Immutable reads.** Use `immutable=1` only for stopped or fixed copies without
  uncheckpointed writes. A running candidate requires an ordinary read-only connection.
- **Wallet-cache memory.** The operator knobs and defaults are in
  [Bootstrap defaults](_GLOSSARY.md#bootstrap-defaults-pe-bootstrap) and apply to
  writable `WalletCache` opens and activity collector writers in legacy, fresh and
  bulk modes, including resume. The effective
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

## Forge release deployment and rollback

Use this one procedure to install or roll back any Forge binary/checkout release. It retains the
configured writer tuning and requires no service restart. Emergency stop is incident control, not
deployment.

One operator owns a resumable deployment record containing completed steps, the original
pre-drain run intent and enablement, target checkout/binary identities, the prior rollback
binary/checkout pair, and cycle/pending/request identities. Preserve other operators' pause
records and all user work. Resume an interrupted deployment from its recorded step.

**Classifier-6 / format-3 release boundary (#739).** Install the first release only with the
loop flag `stop`, the unit inactive, no cycle and no pending or durable unconsumed request.
Record checkout/tree and binary identities. Pre-release classifier-3 requests belong to the prior
release: finish them there before crossing the boundary. The shared authenticated version-1 loader
still reads them for retention, but new publication refuses requests without `classifier_version`
before RPC, and activation/restore refuse classifier-3 candidates. The
[catch-up procedure](#classifier-6-catch-up-and-coordinated-release-739) keeps the restored pause
intent stopped until its explicit owner-consent points.

1. Prepare the combined release while work continues. Finish verification and building before
   pausing; keep the release executable separate from the installed executable.
2. Record the operator's run intent and enablement before changing them, then atomically set
   `data/eval-results/rank_and_push.loop` to `stop` using the existing temporary-file/rename pattern.
3. Let the current attempt finish or fail. Successful publication, exit 75 and a zero-price
   failure are valid stopped-attempt boundaries for compatible releases. Recovery pointers need not
   be cleared except at the first classifier-6 boundary above.
4. Verify no loop, wrapper, bootstrap or ranking descendants remain, and that the loop, one-shot
   and relevant cache mutation locks are free, including the candidate's actual physical path.
   Keep lock inodes; old PID text does not prove ownership.
5. Preserve fixed/prior/candidate files, committed WAL, cycle and pending pointers, configuration,
   generations, receipts and artifacts. Replay a compatible prepared publication request unchanged;
   never rebuild it from the new clock, cache or code revision. A pre-release request stays with
   its prior release; the tested branch-to-main handoff retains its format-3 request.
6. Run `bash scripts/deploy/forge_pause.sh status` first. Invoke `pause` and `restore` only when
   no pause record exists, or when the record belongs to this deployment's interrupted procedure:
   its `recorded_at` must match the deployment record. If another operator's record exists or
   ownership is unknown, preserve the record and paused state and obtain an explicit handoff.
   The helper's `pause` reuses an existing record and `restore` consumes it. Then run
   `bash scripts/deploy/forge_pause.sh pause` and require `status` to report `paused_complete`.
   Record the helper's `recorded_at` in the deployment record. Preserve the prior binary once,
   atomically replace the executable, install the matching checkout, and verify both identities.
   Installation is the operator step between `pause` and `restore`; the helper has no `install`
   command. A partial binary/checkout pair stays paused. Do not overwrite the original rollback
   backup when resuming an interrupted installation.
7. Run `bash scripts/deploy/forge_pause.sh restore`, consuming only this deployment's record,
   then explicitly reinstate the recorded **pre-drain run intent** and enablement: pause observed
   `stop`/inactive after draining. Verify that a running loop resumes the retained prepared
   request first, otherwise the retained cycle. A recorded stopped intent stays stopped.

Verify installed checkout/binary identities, retained-cycle recovery, exact publication and
successor creation. Observe the first three ordinary publications and the existing
batch-application, bounded-membership and service-health checks while the loop runs. Record
same-revision stage durations, source ages and publication cadence; no total saving is projected.

Rollback reinstalls the recorded prior binary/checkout pair through these same seven steps,
retaining cache state, recovery evidence and published batches. The target pair must support any
unfinished bulk root and recorded parser/classifier contracts.
For #739, rollback to the prior pair (`aaf185d`) is available only before the first format-3
request is prepared: prove inactivity and no request, abandon the unprepared candidate through
[recovery](#recovery-and-damage-boundaries), and keep the installed generation-8 format-2 cache and
published batch 86. From the first format-3 preparation onward, keep a format-3-capable release and
fix forward. Never downgrade a format-3 cache in place. Supabase's additive objects remain installed;
`cache-restore-prior` is paused integrity recovery, not release rollback.

For #747, the prior binary rejects multi-window, shortened and resumed receipts. The first new
generation can write these shapes. Before preparing a candidate holding them, prove inactivity and
no request, abandon the unprepared candidate through [recovery](#recovery-and-damage-boundaries),
and reinstall the prior binary/checkout pair. After preparation, retain a release that reads them
and fix forward; never downgrade that cache in place.

### Large-wallet collection progress (#747, AC9)

Keep one resumable operator record across cycles for wallets logged with `acquisition budget
exhausted` or holding partial receipts, until their first successful finalize or a documented
existing unreadable-row/aggregation exclusion. Run this read-only query against each cycle’s cache,
replacing the tracked-wallet `VALUES` with the record’s wallet set. It reports receipt-proved source
rows, separately from inserted-row changes; empty proved windows advance the frontier too.

```sql
WITH tracked(wallet_hex) AS (VALUES ('0xTRACKED_WALLET')),
head(generation) AS (SELECT MAX(generation) FROM activity_coverage_manifests_v2),
receipts AS (
  SELECT r.wallet_hex, r.generation, r.fixed_end_unix AS frozen_end,
         json_extract(r.acquisition_json, '$.version') AS acquisition_version,
         json_extract(r.acquisition_json, '$.mode') AS mode,
         json_extract(r.acquisition_json, '$.start_exclusive') AS start,
         json_extract(r.acquisition_json, '$.fixed_end_unix') AS acquired_end,
         json_extract(r.acquisition_json, '$.aggregation_status') AS aggregation_status,
         json_extract(r.acquisition_json, '$.exclusion_reason') AS reason,
         json_extract(r.acquisition_json, '$.fetched_source_row_count') AS proved_source_rows,
         json_extract(r.acquisition_json, '$.disposition') AS disposition
  FROM activity_wallet_coverage_staging_v2 r JOIN tracked USING (wallet_hex)
)
SELECT wallet_hex, generation, frozen_end,
       CASE WHEN acquisition_version = 3 AND disposition = 'excluded'
                 AND reason = 'acquisition_failure'
            THEN CASE WHEN aggregation_status = 'complete' THEN acquired_end ELSE start END
            ELSE NULL END AS continuation_frontier,
       CASE WHEN acquisition_version = 3 AND disposition = 'excluded'
                 AND reason = 'acquisition_failure' AND aggregation_status = 'not_attempted'
                 AND mode = 'full' THEN 1 ELSE 0 END AS restarts_full,
       proved_source_rows,
       disposition = 'complete' AS head_receipt_complete
FROM receipts JOIN head USING (generation) ORDER BY wallet_hex;
```

Record frozen end, frontier and full restarts, proved source rows, and head completeness every cycle.
A complete receipt has no continuation; record its acquired end as the completion frontier.
`E − frontier` and sampled raw-row arrival estimates diagnose stalls only, never decide pass/fail.
Bind every observation to generation, stage PID and binary revision. Reuse the read-only
[memory sampling procedure](#wallet-cache-tuning-measurement-and-rollback-606) every 30 seconds for
`cache-populate-activity-v2` and `cache-finalize-v2`, including `VmHWM`; do not reuse its trade-row rules.
Every tracked wallet must advance by a proved window each cycle. Collection `MemAvailable` must be
at least 4,500,000,000 B; finalize’s last sampled `VmHWM` at most 10,552,823,808 B and minimum
`MemAvailable` at least 5,517,873,152 B. These are sampled extrema and the finalize lines are a
no-regression gate, not demonstrated margin. Record every failed observation, its cause and explicit
resolution by later wallet advance/completion or existing exclusion, or a later passing measurement
of the same memory stage. The first cycle must add no error class. Keep the issue open until no
wallet remains tracked, all failed observations are resolved, and three ordinary publications pass
the existing batch-application, bounded-membership and service-health checks.

## Wallet-cache tuning measurement and rollback (#606)

Run this comparison on Forge through the existing
[systemd loop lifecycle](#continuous-forge-supervisor), using the defaults in
[Bootstrap defaults](_GLOSSARY.md#bootstrap-defaults-pe-bootstrap) for the candidate.
The tuning also applies to activity collector writers, including resume. Per-wallet
commits, `synchronous=FULL`, WAL and index maintenance remain unchanged. No automatic
memory sizing is performed.

1. Record the installed binary/checkout identities and retain the current `.env` as
   the measurement baseline; the [release procedure](#forge-release-deployment-and-rollback)
   owns its rollback backup. The historical baseline used `c1cdf82`. Record baseline pragmas as
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
4. Install the candidate binary/checkout pair with the
   [release procedure](#forge-release-deployment-and-rollback), retaining the configured `.env`.
   Retain the new writer log's `requested_cache_kib`, `effective_cache_kib`,
   `requested_mmap_bytes` and `effective_mmap_bytes` for each candidate window;
   cache KiB values use SQLite's negative-KiB convention. Success requires lower
   read bytes per 1,000 inserted rows than baseline with `MemAvailable` never
   below **2 GiB**.
5. If `MemAvailable` falls below that floor or any `MemoryPressure`/OOM kernel
   line occurs during measurement, restore the recorded prior binary/checkout pair and `.env`
   through the [same release procedure](#forge-release-deployment-and-rollback).
   Keep the cache generation and recovery pointers.

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

Use the [release procedure](#forge-release-deployment-and-rollback) for installation
and rollback of the matching binary/Python checkout pair. Both public writable openers delegate
to `open_with_tuning`, whose
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

**Historical writer compatibility:** the release procedure restores a matching binary/checkout
pair; the following describes the old writer's behavior.
The old writer never updates the new columns, so marked histories remain
quarantined and become retryable as soon as the corrected binary returns.
If the old writer appended above an established frontier with #609 gaps, the
corrected walker repairs them from that frontier. Exception: rollback after a
cold NULL-anchor `begin_walk` but before the first coverage commit leaves no
established frontier. If the old writer then builds gapped history, restoring the
corrected binary seeds from cached bounds and may inherit those holes; use the
re-walk lever or accept the documented legacy limitation. Never clear a marker
by hand from an old success stamp.

**Historical Python compatibility:** an older checkout can expose retained partial histories to ranking.
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
> trade time — which seeds `pe-service`'s inactivity clock at admission and is the
> candidate-freshness filter (`ACTIVE_WINDOW_HOURS = 72`, #357). It is a push-time
> snapshot of the cache; backfilling immediately before the push is mandatory.
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
audited `activate-next` batch (`bootstrap_pipeline_activation_batch_wallets`;
`PE_BOOTSTRAP_ACTIVATION_BATCH_WALLETS=0` admits none; keep the value unchanged
while a cycle is in flight, because a resumed `activate-next` stops on a changed
count) →
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
emit the per-token reference fetch windows for the candidate positions (on schema two, the
in-horizon positions of every wallet with at least MinTRL of them; other wallets are ranked
non-surviving without price work, #588). Schema two judges repricing coverage over the copy scope:
positions outside the shifted horizon need no reference window and do not count, and a position
whose slippage-adjusted reference price is outside the band leaves the denominator, while a missing or stale sample
still counts against the wallet; **2b**
`pe-bootstrap prices-history --targets-csv` fetches only the uncovered remainder of
minute reference prices into the isolated ranker price store (write-once + range
algebra ⇒ resumable; transient page failures are a partial and pass-2's
terminal-coverage gate then exits 75 so the supervisor retries — a partially fetched
cycle can never publish); **2c** pass-2 reprices every candidate position at the
latest reference sample at-or-before `entry+Δ` (adds `hit_rate`, writes the
per-position `oracle_outcomes.csv` and the versioned `oracle_manifest.json` whose
canonical hash the push stores as `ranking_batches.config_hash`); **Stage 3** record
the exact publication request, atomically publish it through the idempotent
`publish_ranking_batch_v2` RPC (classifier 6; historical requests belong to their prior release),
verify that exact batch is `latest_ranking`, and write the
accepted cycle record. The legacy lane captures a full watermark for its unchanged-day check;
the fresh lane writes its configuration without scanning the installed cache. Completed-cycle
retention deletes eligible old cycle cache files; there is no SQLite reclamation, index rebuild,
or checkpoint stage (#544).


**Oracle rollback (#536):** reverting the ranker code alone does NOT restore the
prior ranking — `latest_ranking` always serves the maximum `batch_id`. To roll back
externally: restore the prior binary/checkout pair through the
[release procedure](#forge-release-deployment-and-rollback), leaving the loop stopped, run the
one-shot with a unique
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
> measured leader→fill span artifact and reports late copies; the owner keeps Δ at 2s.

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

The frozen-payload and marker-projection procedures in this section describe historical
**format 2**. Current cycles use the [format-3 candidate contract](#fresh-private-candidate-cycles-and-the-scheduled-schema-two-lane-588-648)
and its export-bound final-stage record. Cache schema two alone does not select the history format.

Build and resume the side cache by its hash-bound manifest and explicit `--db` path. Version one
is sealed into `*_v1_sealed` audit tables; version-two consumers read only complete normalized
activity and CLOB-payout generations, and no API unions the generations.

New `activity_groups_v2` tables retain the `source_trade_id` primary-key index for
deduplication and projection joins and `idx_activity_groups_v2_wallet_time` for
collection and ordered wallet reads; the unused condition index was removed from
schema creation. A 761-second sample from `/proc/<pid>/io` and staging receipts
while collection ran undisturbed on Forge measured 1,535 committed rows/s,
25,188 physical write bytes per roughly 1.1 KB logical row, 2,506 physical read
bytes/row and 38.7 MB/s writes, with a 222.6 GB candidate on a host with 15.9 GB
of memory. The model assumes one randomly placed index leaf per row: a WAL frame
carries a whole 4,096-byte page plus a 24-byte frame header and checkpoint writes
the page back, or about 8,216 bytes/row for the removed condition index, about a
third of the measured total. This saving is an estimate, not a measurement,
pending a before-and-after comparison of physical write bytes per committed row
on the same collection. The retained primary-key index still incurs its own write cost.

Existing condition indexes remain accepted and are never dropped automatically;
finalized cache bytes must remain unchanged because finalization with `--stage-record` hashes the physical file.
Backward reads remain compatible, but do not run an older `cache-migrate-v2`,
`cache-populate-activity-v2` or `cache-finalize-v2` against a finalized cache created
without the condition index: these commands recreate the index, rewrite the file
and invalidate the physical hash bound into its stage record and any prepared
publication. Keep the artifact unchanged and finish its bound publication with
the index-free binary. If an older writer is required, resolve any pending
publication through the recovery procedure below, then use a separate private
candidate and regenerate its stage record and publication request before activation.

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
receipt together; restart schedules only missing exact receipts. Collection completion validates the
complete receipt set and aggregate content and installs the bounded activity manifest in the same
transaction, retaining receipt rows (the storage marker is defined in `_GLOSSARY.md`). This requires
no payout or classification, so another activity generation can start immediately.
Completion, first finalization, activation and restore validate aggregates one wallet at a time with
unchanged aggregate/receipt digests; Rust and Parquet projection verification stream their ordered rows.
Finalization also verifies payout coverage, builds the Rust ledger/classifier
projection, and records its count and digest.
For frozen and fresh collections, resuming an unfinished collection (no activity manifest
installed yet) validates receipt identity, shape and count constraints without reading
aggregate content; invalid receipts fail before source I/O.
Because wallet writes are atomic, completed-wallet content corruption (such as a deleted
group with its receipt intact) is detected at collection completion instead of restart,
and full content validation remains mandatory at completion and first finalization.

Rank and cut over through the one publication path. This snapshots the current published batch,
exports and verifies the schema-two Parquet projection, computes the minute-price rerank and exact
diff, durably prepares the publication request, activates the side cache, then resumes that exact
request. The export copies the certified activity rows with eight DuckDB cursors, one key range
each, merged into one file, and certifies the exported projection against the finalized count and
digest; it does not re-read SQLite's projection or assert that SQLite stays unchanged afterwards. An
export failure stops a schema-two cutover run. The fresh lane's first finalization certifies,
checkpoints and syncs without hashing the file or writing a stage record. It re-finalizes with
`--stage-record` after the targeted price-store write and before request preparation, so the stage hash covers the installed
bytes; a pending request's replay relies on activation's bound-artifact checks.
Refinalization reuses the projection only when the finalized database's saved activity generation,
reference, aggregate and manifest digests, payout generation/coverage and evidence digest, and
classifier version are unchanged, and the existing projection's recomputed count and digest match
the recorded values; missing or changed proof refuses reuse. Refinalization holds the write lock
without writing and recomputes that digest over the committed projection from eight read-only
connections, one key range each, hashed in key order, so the bytes are the serial read's while its
random activity reads overlap (#675). A different recorded classifier
version instead runs full activity verification and rebuilds the projection with the current
classifier. The current classifier version and its entry rules are `ranker_classifier_version`
in `_GLOSSARY.md`. An activation accepts an installed cache of classifier version one to three. Each rebuilding finalization loads a wallet once, validates its entire aggregate
vector, then classifies that same vector; a classifier stopping point never truncates validation.
Manifest installation and projection replacement commit together with a cleared, unfinalized state;
the finalized state commits in a second transaction, after the digest is computed over the committed
projection by the same eight readers, under a re-taken write lock that refuses if any other
connection committed in between. A failure before the first commit changes nothing; one after it
leaves only an unfinalized projection, which the next finalization rebuilds. Content
validation finishes before any deferred projection error is returned; a missing manifest and its
archived identity are installed only after validation and projection succeed, so a projection error
that automatically rolls back SQLite's transaction cannot leave either committed independently.
Reuse skips that activity-generation verification and reclassification. The payout evidence digest
streams every evidence row in market-ID order, binding `market_id`, `end_date_unix`, `payout_status`,
and `payout_vector_json`, including markets currently excluded from the projection. It covers the
entire payout table because the rebuild's eligibility query has no generation filter. Projection
digest recomputation walks the projection and looks up its activity rows by source-trade key;
the join order prevents SQLite from choosing a full activity traversal. It retains payout coverage
verification, checkpointing and sidecar checks; `--stage-record` additionally retains the full-file
hash and stage-record write. Receipt or activity
corruption outside the projected values introduced after first finalization is detected by
activation's full candidate manifest/content validation, before replacing the fixed cache:

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
rollback-backup path and finalized candidate hash inside its `publish_key`. New two-file cycles also
bind the SHA-256 of the immutable `wallet_cache.<cycle>.side.stage.json` staging evidence;
legacy requests keep their existing prior-bound tuple. If the process stops after preparation, the existing pending
pointer resumes idempotent activation before publication; no additional pointer is used. Before
either first activation or resumed activation, the publisher validates the complete request and
returns the sole activation tuple consumed by the wrapper. A content-hash mismatch therefore fails
before any cache mutation. Request and pointer replacement fsync both the new file and containing
directory. For a new cycle, activation checkpoints the installed main `F` and requires its hash to
match staging's recorded `H0`, including any writes previously committed only to WAL. Drift refuses
before any rename. It validates the candidate `C` against the prepared request, then renames
`F → D`, fsyncs the directory, renames `C → F`, and fsyncs again. Both destinations must be vacant;
there is no copy fallback. `D` holds the exact old bytes. A crash in the gap leaves `F` absent and
`D + C` present. The next activation or pending-publication resume validates `D` against `H0` and
`C` against the prepared candidate hash before completing `C → F`. Unknown combinations refuse
without moving or deleting files. Payout and every ordinary writable CLI invocation, including named,
no-argument and positional-config `all`, refuse an absent database instead of opening it with SQLite
CREATE. First installation explicitly opts in with `pe-bootstrap --create-cache` (also accepted by
named `all` and with a positional TOML config path). The flag creates and initializes an absent cache,
opens an existing regular cache file normally, and refuses other existing path types, including
symlinks. The supervised wrapper never passes it, and its preceding probes use read-only opens.
`cache-stage-v2` still provisions verified candidate copies. Legacy activation/restoration retain
their explicit backup-copy behavior; those paths never initialize an empty installed cache.
Legacy cycles with an existing prior and no new staging evidence retain their existing activation
and restoration behavior, including the live cutover cycle. Never convert their evidence or request.

The wrapper remains the one-shot run-lock owner. For activation it passes the inherited run-lock
descriptor and, under the supervisor, the inherited loop-lock descriptor. `pe-bootstrap` verifies
each descriptor's inode, PID stamp, and live kernel contention before skipping only that lock; Rust
always acquires the cache lock. A direct `pe-bootstrap cache-activate` call without that verified
handoff continues to acquire loop → run → cache itself.

The wrapper also passes the cycle's final-stage record (`--final-stage-record`). The finalizer wrote
it after computing or verifying the candidate's projection digest over exactly the bytes it hashed, so
activation compares the candidate's stored projection summary with the record instead of recomputing
the digest; it refuses a record of another format, schema, path or hash. Candidate activity
content and receipts, payout coverage, projection count and classifier agreement still run.
For a schema-two outgoing cache, the wrapper passes the newest accepted candidate-lane request.
When that cycle's accepted activation installed exactly the outgoing fixed main at staging's H0,
activation skips its activity-manifest verification and the outgoing projection digest, checking
only the stored projection summary's form, count and classifier; the H0 hash still binds the bytes.
Otherwise both run in full, the digest recomputed from the same eight read-only readers while
activation holds its write lock without writing (#675). Without the final-stage record, activation also
recomputes the candidate's digest (#682).

Before the bound corrected batch becomes current, restore that exact prior cache by its recorded
hash and schema. For a new cycle set `CACHE_PRIOR_BACKUP` to its `.displaced.db` (`D`) and
`DISPLACED_CACHE_BACKUP` to its now-vacant `.side.db` (`C`). For a legacy cycle retain the original
prior and displaced arguments. A history-format-3 publication also needs its cycle's final-stage
record, `FINAL_STAGE_RECORD=<cycle out dir>/cache_stage_record.json` (written by the export-bound
re-finalize); restore refuses format 3 without it, including an interrupted restore, and does not
use it for a format-2 publication. Preserve the rejected cache for audit:

```bash
pe-bootstrap cache-restore-prior \
  --fixed-db "$FIXED_PHYSICAL" \
  --backup "$CACHE_PRIOR_BACKUP" \
  --displaced-backup "$DISPLACED_CACHE_BACKUP" \
  --prior-sha256 "$PRIOR_CACHE_SHA256" \
  --prior-schema "$PRIOR_CACHE_SCHEMA" \
  --publication-request "$PUBLISH_REQUEST_FILE" \
  --pending-pointer data/eval-results/rank_and_push.pending \
  --final-stage-record "$FINAL_STAGE_RECORD"
```

Run this only with `SUPABASE_URL` and `SUPABASE_SECRET_KEY` in the environment after stopping the
rank supervisor. Restore validates the pending pointer, the complete request `publish_key`, the
fixed/prior activation paths, and the installed corrected-cache hash while holding the Forge lock
stack. It then asks authoritative `ranking_batches.publish_key` whether that exact publication was
ever consumed. A consumed publication, a missing/malformed pointer or request, or unavailable
authority refuses restoration; recover by rolling forward through the existing pending-publication
path. There is no operator assertion flag. New-layout restore records the direction in
`wallet_cache.<cycle>.side.restore.json`, renames the rejected `F → C`, fsyncs the directory, then
renames `D → F` and fsyncs again. The same restore command completes an interrupted gap or confirms
an already completed restore; it copies no cache. Keep the request, pointer, staging evidence and
restore marker. The marker prevents a pending-publication resume from reinstalling the rejected
candidate. Stay paused until the recovery is resolved. Schema one retains `auto | duck | sqlite`; schema two
requires the verified DuckDB snapshot and refuses SQLite.

### Fresh private-candidate cycles and the scheduled schema-two lane (#588, #648)

Recurring schema-two publication builds each cycle in a **private candidate** copied from the
checkpointed fixed cache. Current cycles use [history format 3](_GLOSSARY.md#history-format-3-and-classifier-6-739):
collection identity 4, acquisition 3 and export manifest 3, without a cache `user_version` change.
The union includes acquisition candidates, every retained history and the prior head's exclusions;
no frozen reference narrows it. Classifier 6 never finalizes a format-2 head.

```bash
# Under the cache lock: checkpoint, copy to pending while hashing the fixed main, then
# decide quick-check reuse from H0, atomically record evidence, verify and adopt the copy.
# --prior reserves the legacy name; no prior file is created for a new cycle.
# Schema-one input also gets its H0-bound build manifest for the initial seal.
# Every path names a file in an existing directory (the cycle directory beside the fixed
# cache): staging never creates directories.
pe-bootstrap cache-stage-v2 --db "$FIXED_PHYSICAL" --prior "$PRIOR" --side "$SIDE" \
  --manifest "$CACHE_BUILD_MANIFEST"
pe-bootstrap cache-migrate-v2 --db "$SIDE" --manifest "$CACHE_BUILD_MANIFEST"   # unsealed candidate only
pe-bootstrap winner-discovery --db "$SIDE" --defer-activation
pe-bootstrap activate-next --db "$SIDE" --batch-id "$BATCH" --audit-csv "$AUDIT"
pe-bootstrap cache-populate-activity-v2 --db "$SIDE" --fresh-generation "$N"
pe-bootstrap cache-populate-payout-v2 --db "$SIDE"         # skip only when candidate-targets --after-collection returns 1 as field three
pe-bootstrap cache-finalize-v2 --db "$SIDE"                # certify, checkpoint and sync without a stage record
```

`--fresh-generation N` records the versioned acquisition identity specified in
[`_GLOSSARY.md`](_GLOSSARY.md#history-format-3-and-classifier-6-739).
The collector validates predecessor records and certificates and derives the wallet union before
sampling/freezing the settled end. New roots read full history; polled successors read
`(previous_end,new_end]` for wallets with usable predecessor history; identity-4 acquisition
failures resume from their receipt’s continuation frontier instead. A partial resumes incrementally;
a no-progress read keeps its mode and start. New wallets, other previous exclusions and explicit
repairs read full history. Generation numbers may have gaps; the frozen
identity binds the predecessor. Acquisition 3 keeps otherwise valid rows missing a condition id or
token for scoped classification/ignored-record accounting; acquisition 2 and public service readers
retain strict acceptance.

**Transition in admission.** The first format-3 admission performs the format-2 predecessor walk
once, verifies its receipts and creates certificates, tables and write-authorization triggers in
the same transaction as identity 4. Complete wallets, including empty histories, are certified;
excluded wallets with an authenticated earlier complete receipt are certified with unknown recency;
proofless excluded wallets must have no rows and stay uncertified. Both excluded classes next read
in full. Retained rows without usable proof are fatal. Interruption before commit reruns the walk;
after commit, resume returns the frozen identity. Later admissions check certificates/receipts
without history visits. No separate transition command exists.

Identity-4 successors defer quiet wallets using the unchanged
[quiet/due/deferred schedule](_GLOSSARY.md#complete-activity-generations-and-incremental-acquisition-648).
A deferred predecessor stays quiet; committed rows since certification make a wallet active;
otherwise the quiet decision uses certified newest activity, not an activity-index probe.
Due complete wallets read incrementally; due `dormant_deferred` wallets read full history. Roots,
new wallets, continuing acquisitions, other exclusions and repairs never defer. Top-ups use the same rule, resume keeps
the frozen list, and admission/completion report deferrals separately from failures. A pause
lengthens the predecessor interval; deferred wallets whose weekly instant fell inside it read in full.

Admission preserves activity rows, receipts, manifests and cumulative drops, clears the derived
projection/binding and invalidates finalization. Incremental collection strictly inserts only fetched
rows and their fetched-set receipt in one wallet transaction; no carry or re-stamping occurs.
The receipt-chain verifier owns effective history. An unchanged automatic full read writes only its
receipt, without a history read/write or per-aggregate identity probe. A differing full read checks
stored history before replacing it; a foreign-wallet identity is fatal. A full partial also checks
and replaces retained history; an incremental partial inserts its proved rows. Explicit repairs
always replace after logging stored/certified digests, keeping a proved partial or empty history
when no window was proved. Other exclusions and deferrals keep retained rows and never reset the chain. Historical manifests are
commitments, not queryable snapshots. Keep staging evidence and `H0`; resumed staging leaves progress
intact and never recaptures a changed installed baseline. Activation preserves old fixed bytes at `D`.

A retry retains bounds/lists and skips valid receipts. Startup and collection completion authenticate
receipt membership, acquisition and sealed commitments without reading history; finalize performs
the verified pass over every available wallet. A completed generation returns the same manifest
without source calls or a new clock. Authentic older identities resume without rewriting their bytes.
Format-3 consumers use receipt availability and head certificates, never generation-equality probes
of activity rows or the `activity_type` mirror.

A wallet whose acquisition cannot be parsed, identified, bounded or bucketed, or has an incremental
same-wallet identity collision, is excluded with its reason/evidence retained. Identity 4 probes one
unsplit window, then walks forward with the verified [ascending boundary lookup](15-SOURCES.md#forge-forward-activity-window-lookup-747).
Continuing acquisitions and full reads certified with at least 5,500 source rows skip the probe.
The lookup places bounds only; descending terminal pages prove every kept window. A saturated
one-second window remains fatal, and a looked-up window that is still full stops without keeping it. Missing condition/token
alone follows acquisition 3's acceptance above. Every non-deferral failed acquisition with retained
history verifies it in the exclusion transaction, in either read mode, except an explicit repair’s
replacement. A partial atomically commits proved rows and an excluded `acquisition_failure` receipt
with complete aggregation and an acquired end strictly between its start and the generation end;
zero-row partials are valid history parts. Excluded receipts keep zero outer counts, the empty digest,
SQL `fixed_end_unix` equal to the frozen end and no current certificate/projection/spool rows.
With `bootstrap_polymarket_wallet_timeout_secs` set, identity 4 checks one non-resetting deadline
before outer retries and further walk windows, never cancels a page fetch in flight, and keeps proved
windows on expiry. A walk always attempts its first window. The shared held-row bound comes from
[`ACTIVITY_WALK_HELD_ROWS`](_GLOSSARY.md#polymarket-public-source-pollingconfig).
Same-generation resume skips the committed receipt; the next generation continues acquisition
failures in their own mode from the receipt frontier. Aggregation failures and collisions read full. Equal-revision incremental collisions also exclude. Missing
proof, foreign-wallet collisions or unreceipted current rows fail the cache closed.

Incremental reads do not discover revisions wholly before the lower bound. Select repair wallets
**before starting a new generation**:

```bash
pe-bootstrap cache-populate-activity-v2 --db "$SIDE" --fresh-generation "$N" \
  --full-read-wallets "$WALLET_A,$WALLET_B"
```

Selection is the frozen `repair_wallets` subset in identity 4; it cannot change on resume.
Repairs reconcile revisions/deletions and logical history damage, preserve cumulative drops, and
cannot repair structural SQLite damage or damaged certificates/receipts/manifests/state. Follow
[the damage boundary](#recovery-and-damage-boundaries) before selecting a repair. The shared paced
fetcher, bounded reads/channel and single writer remain. Under a budget, a window whose page
exhausts its internal retries retries in place while the deadline allows, retaining earlier proved
windows; without a budget, transient exhaustion exits `rank_and_push_tempfail_exit`. Permanent
errors stop the cycle.

**Verified pass, spool and export.** Finalize visits every available wallet once (complete,
non-excluded head receipt, even with zero fetched rows), verifies its effective history, classifies
with `classify_scoped_historical_second`, and writes its new certificate and cumulative drops.
It reports ignored activity by type, drops by cause and neg-risk markets missing a group id.
`ranker_entries_v2` stays empty. The [projection spool contract](_GLOSSARY.md#history-format-3-and-classifier-6-739)
binds `<candidate>.projection-v3.jsonl` in `ranker_projection_inputs_json` with `oracle_version`,
`projection_spool` and `certificate_digest`. The payout binding includes `neg_risk_market_id` and
requires coverage `group_version=1`; older staged payout pages restart from page one.

The streamed spool is synced/renamed beside the candidate before finalize commits. A rebuild deletes
its uncommitted/temporary spool first; committed reuse leaves it intact. Readers verify its size
and SHA-256 before reading. Missing/altered committed spools fail closed and require guarded
pre-prepare abandonment/restaging. Export converts it to compact Parquet, keeps payout evidence,
recomputes projection count/digest from Parquet and writes manifest 3 last. A retry after export or
an interrupted reference fetch reruns export from the retained spool. Retirement deletes it;
retained requests keep it. Format 3 has no marker inserts, join digests or copied activity export.

After price writes, the wrapper re-finalizes with the actual cycle export manifest, including a
non-default Parquet directory:

```bash
pe-bootstrap cache-finalize-v2 --db "$SIDE" --stage-record "$CACHE_STAGE_RECORD" \
  --export-manifest "$EXPORT_MANIFEST"
```

Re-finalize validates input bindings and the state's count/digest/classifier/oracle against that
manifest, re-checks certificates, spool and the cycle's sealed manifests/receipts inside its
verification transaction, then checkpoints/hashes the candidate. It never regenerates predecessors
from advanced certificates or re-reads history/projection. The final-stage record binds the export
manifest's SHA-256 and summary. Until finalize, phase checks use identity 4's incoming
`certified_digest`; afterwards they use the finalized `certificate_digest`, once per stable proof
while data-version checks show no outside write. Publication reads freshness, stamps and scopes
only from certificates at this finalized head.

**Structural checks (#643 step 2).** Each uninterrupted recurring cycle runs at most two
`PRAGMA quick_check` scans (previously eight): the checkpointed fixed main under the
staging lock, then the finalized candidate immediately before activation. Initial
schema-one cutover runs three (previously ten), adding the first-migration input check
because standalone backups have no structural-check provenance; schema-two migration
resume and both finalizations run none. Missing-side activation recovery runs one per
attempt (previously two) on the matching-hash installed main, because a missing side
file does not prove activation already checked it; restore runs one (previously three)
on the immutable prior after hash/schema validation and before any displacement.
Staging skips its scan when the newest accepted candidate-lane cycle's own request
(`rank_cycle_manifest.py installed-request`, the cycle retention selects) installed exactly
the checkpointed fixed main: its `cache_activation` names this fixed path, a candidate
named `wallet_cache.<that cycle>.side.db` that has since been moved, and an expected hash
equal to the fixed main's, and `accepted_cycle_manifest.json` (written only after verified
publication, which follows a successful activation) sits beside it. That activation scanned
those bytes before installing them, so a cycle whose fixed main is its predecessor's
untouched candidate runs one scan, at activation, instead of two; any other fixed main is
scanned at staging as before. At 590 GB the measured plain-check baseline took about
10 h (17 MB/s of 4 KB reads on Forge, 2026-09-23). The activation candidate now starts
the walk-order prefetch described in [#694](https://github.com/sppburke/prediction-markets/issues/694).

Finalization certifies activity, payout and projection evidence, and with `--stage-record`
binds exact bytes, not every SQLite page. Damage outside those reads may now survive migration resume, the
post-seal step and either finalization, wasting private collection/ranking work before
activation refuses installation; fault localization is consequently later. Outgoing
and retained backups keep hash and schema validation. Format-2 outgoing validation retains its
accepted-H0 shortcut and otherwise runs in full. Format-3 outgoing validation uses H0 and the state's
recorded form: admission already checked records at those exact bytes; it never walks history,
recomputes a retired projection or needs a retired backup. New-layout activation and restoration move existing files;
legacy fallback/audit copies remain hash-verified. Backups may contain preexisting damage:
the prior's restore-time check decides whether it is eligible for restoration, and
post-rename hash equality carries that proof without another scan. All checkpoints,
sidecar rejection, candidate receipt/content digests, locks and publication gates remain in place;
hash equality proves byte identity, not health. `quick_check` itself does not check
UNIQUE constraints or index-to-table agreement; no routine full `integrity_check` is added.

Format-3 activation checks candidate hash equality with the export-bound final-stage record,
`quick_check`, the state's export summary and payout coverage, then outgoing H0. It carries
re-finalize's record checks to the installed bytes without a history re-read. Logical history damage
after a wallet's check is caught at its next available-cycle check or quiet weekly due read, a week
plus at most one cycle. These are the owner's accepted integrity gaps (#739 Decision 6), not proof
that unchecked history stayed intact. Rankings and publication inputs use only this cycle's checked rows.

With `RUST_LOG=info` (or `pe_bootstrap::cache_migration=info`) in the loop environment,
each completed check emits one JSON event to stderr, inherited by the loop journal
(`journalctl --user -u pe-rank-loop`), with `role`, `path`, `file_size_bytes`, integer
`elapsed_ms` and `success`. The activation candidate's event also carries
`prefetch` (`done`, `cancelled`, or `unavailable`); an unavailable event includes
the advice error. Its duration includes prefetch setup, the pragma, and the worker
join. Other roles measure only the pragma. A check returning an error still emits
an event with `success = false`; only a killed process emits none. Duration excludes
copying, hashing, collection and ranking. Staging stdout remains the single report in
`$OUT_DIR/cache_stage.json`; `$OUT_DIR/cache_build_manifest.json` and
`$OUT_DIR/cache_stage_record.json` keep their existing hash-bound contracts.
The plain-check timing is a measured baseline. [#694](https://github.com/sppburke/prediction-markets/issues/694)
records the local prefetch measurement; the first Forge activation-candidate check
will establish its host duration. Neither establishes a total freshness bound.

Install and roll back through the [release procedure](#forge-release-deployment-and-rollback).
Resume the same cycle with its fixed/prior/candidate paths, committed WAL, identities, receipts,
configuration and prepared request intact. A release that preserves parser/classifier versions
may install with a cycle retained at a stopped-attempt boundary. A classifier-version change
requires a clean publication boundary with `paused_complete`, no cycle pointer,
`cache_stage_record.json`, `ranking_publish_request.json` or pending pointer: a resumed cycle freezes
its pipeline versions. For classifier 6, follow the [first format-3 boundary](#forge-release-deployment-and-rollback)
and [catch-up handoff](#classifier-6-catch-up-and-coordinated-release-739); a retained format-3 request
transfers from the tested branch tree to its runtime-identical main release unchanged.

The zero-argument `rank_and_push.sh` production cycle enters this lane automatically when
the installed cache is schema two, and for the one-time initial cutover when `.env` sets
`PE_RANK_SCHEMA_TWO_CUTOVER` (`_GLOSSARY.md`). The lane is frozen in the cycle's
`cycle_configuration.json` (`cache_lane`), so a resumed cycle keeps its lane even if the
opt-in changes and an outstanding legacy cycle completes under its original contract. In
the lane, Step 0 is the sequence above (legacy `backfill`, `events` and `resolutions` read
retired `trades`/`source_cursor` and do not run), followed by the existing cutover path:
candidate capture into `candidate_cycle_manifest.json` alongside Parquet export, pass one and
target emission, then targeted `prices-history` and second finalization with `--stage-record`
and `--export-manifest`, which binds the price writes and manifest-3 export summary. Pass two binds that
capture. An initial schema-one cutover keeps its full initial `cycle_manifest.json` capture and
same-day gate; recurring cycles started on installed schema two write a two-field lane record
(`version`, `configuration`) instead. The new-cycle `pipeline-versions` output remains available
to candidate capture and the publisher. Then come
`--prepare-only`, `cache-activate` and the exact `--resume-request`. The candidate snapshot stamps
the UTC day when capture starts. The immutable staging baseline fixes the initial activity generation
and payout target for new cycles; existing legacy cycles read those values from their prior. The activity owner resumes the candidate's recorded head, including a manually started or
interrupted successor belonging to this cycle. `candidate-targets --include-bulk-root` appends
bulk eligibility as field four and `activity_complete` as field five; the default interface stays
three fields. Inside its read transaction, completion requires schema two, the selected generation
matching the validated head, and a completed manifest referencing that head's digest. Only that
completed selected format-3 head skips the collection invocation. Every format-2 head takes the
ordinary successor path (`initial + 1`) before finalization, even when fresh; this runs the transition.
Incomplete and unsealed bulk roots still collect under their frozen acquisition contract;
standalone collection authenticates receipts, with content checks retained for format 2. The wrapper always calls
`--after-collection`, including after a skip, so freshness equality and the single-top-up allowance
are unchanged. Format-3 re-finalize rejects record damage before preparation; activation binds those
checks to installed bytes without another history read. Format 2 retains its content checks.
If the completed initial head's age plus `candidate_top_up_reserve_hours` exceeds the
publisher's unchanged `max_cache_staleness_hours`, the wrapper admits one linked top-up. Its persisted
base link consumes that allowance across restarts; a stale top-up stops before ranking and never
starts another. Actual trade/payout source times still govern preparation after downstream work.
The payout target is the staging baseline's active walk if present, otherwise its newest completed walk plus one;
a walk on the candidate is reused only when the newest completed walk's `completed_at_unix` is at or
after the selected activity head's fixed end. Otherwise the wrapper invokes `cache-populate-payout-v2`,
which resumes an active walk or starts `MAX(completed generation)+1`. Completion records the installing
invocation's start, so preparation never meets a payout older than its trades.
After every successful fresh-lane publication — the
ordinary path, automatic pending resume or explicit `--resume-pending` — the accepted two-field
lane record is written before the pointers clear. Recurring schema-two cycles therefore start
again on the same UTC day; the legacy lane retains the full accepted capture and unchanged-day gate.

**Physical layout.** The candidate lane uses the regular physical fixed file
(`readlink -f data/wallet_cache.db`) and derives per-cycle names beside it from the
durable cycle directory: `wallet_cache.<cron-UTC>.side.db` (`C`) and `.displaced.db` (`D`).
The `.prior.db` (`P`) name remains for legacy cycles. Rust writes the sibling `.side.stage.json`
staging evidence atomically before adopting the verified pending copy; `cache_stage.json` in the run directory is command output. The Rust lock owners derive the
cache lock and the loop/run lock directory from that physical path, so the physical
directory must carry two aliases to the repository inodes, checked before any mutation:

| Alias | Target |
|---|---|
| `<physical dir>/eval-results` | `<repo>/data/eval-results` |
| `<physical dir>/wallet_cache.db.lock` | `<repo>/data/wallet_cache.db.lock` |

Fixed, candidate, and retained backup must be independent regular files on one filesystem.
The cycle's own evidence selects the layout: new immutable staging evidence means direct-copy
staging and rename preservation; an existing prior without that evidence means legacy behavior.
An existing candidate with neither evidence nor prior refuses. Installed drift after staging is
reported against recorded `H0`; that hash detects drift but cannot reconstruct the original bytes.

After verified publication and both pointer clearings, retention deletes the completed cycle's
`D` (or legacy `P` and `D`), its projection spool and eligible older cycle artifacts. The existing durable
`accepted_cycle_manifest.json`, written after publication verification as a full legacy capture or
fresh-lane lane record, and `ranking_publish_request.json`
remain the discoverable cleanup obligation, together with the request-bound `.side.stage.json` for
new-layout cycles. Before admitting another cycle or taking the legacy unchanged-watermark exit, the wrapper
finds the newest accepted candidate cycle and validates its request and staging binding, then resumes
retirement. Pointerless `--resume-pending` also completes this cleanup without activating or publishing
again. The evidence stays as audit history; no new receipt format is introduced. Every completed
retirement pass synchronizes the physical cache directory, even if an interrupted pass already unlinked
the last backup. It accepts only regular files named
`wallet_cache.cron-<YYYYMMDDTHHMMSSZ>.{prior,side,displaced}.db` and their `-wal`/`-shm` sidecars,
and the candidate's `.projection-v3.jsonl` spool, from that cycle or earlier, in the request's
physical cache directory. A retained request keeps its spool. Pending pointer, cycle pointer
or `.forge_pause.json` presence, including malformed/symlink records, prevents deletion. A pause
defers cleanup and new-cycle admission; removing it lets the next loop pass finish retirement.
The installed file, its inode aliases, symlinks, directories, `..` paths and other names remain
protected. Staging still refuses another candidate allocation while an exact-cycle prior/displaced
backup remains; never delete a backup to bypass an unresolved publication.

At **830 GB per cache**, two full caches use **about 1.66 TB**, leaving **about 240 GB on a 1.9 TB**
device. Staging, activation, rename-gap recovery and rollback retain at most those two full mains.
Keeping the previous backup into the next copy would require 2.49 TB and is refused. The 240 GB
remainder is shared by WAL, exports, sort scratch, growth, filesystem overhead and other usage;
this arithmetic does not measure their peak usage. Format-3 capacity also includes the spool;
recompute headroom and retention horizon from catch-up measurements before ordinary operation.

**Historical schema-one cutover and acceptance.** For classifier 6, use the
[one-time catch-up](#classifier-6-catch-up-and-coordinated-release-739), including its temporary
freshness policy. The following remains the initial schema-one procedure. Complete any outstanding schema-one cycle and its
publication first. Create the two aliases, retire eligible previous backups, and check free space
for one candidate plus the measured WAL/export/scratch budget on that filesystem, then set `PE_RANK_SCHEMA_TWO_CUTOVER=prepare` and run
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
the value to `1` and resume with the recorded run intent through the
[release procedure](#forge-release-deployment-and-rollback): the
pending-publication recovery activates and publishes exactly that request. If the publisher
refuses (stale source times, incomplete coverage), the cycle stops with the installed cache
untouched and the candidate, staging evidence, any legacy prior and log preserved; do not relabel times, narrow
membership or relax freshness. Require the next real scheduled refresh and publication
(installed schema two selects the lane automatically) before closing the schema-two
handoff.

#### Recovery and damage boundaries

Before a prepared request exists, abandon a cycle only after
`scripts/deploy/forge_pause.sh pause` reports the loop inactive with no cycle descendant and
no held lock; then confirm `rank_and_push.pending` is absent and the cycle directory holds no
`ranking_publish_request.json`, remove `rank_and_push.cycle`, and delete only that cycle's
candidate, its projection spool (including uncommitted/temporary spool) and staging metadata.
Preserve the failure evidence before deletion. Preserve any legacy prior until its own recovery is resolved;
an outstanding prior will block the next new-layout copy. Once a
request is prepared, never delete it or start another cycle: the pending pointer resumes
activation and publication. If the pointer is missing but this cycle has a durable
`ranking_publish_request.json`, both recovery entries validate that request and reconstruct its pointer
through the publisher before discovery or collection. Both remain held while cutover is `prepare`.
After activation but before the publication is consumed,
`cache-restore-prior` with backup `D`, rejected destination `C` and `--final-stage-record` restores
the old bytes by rename for a new cycle, including its interrupted gap. For format 3 the record must
match the candidate path/hash; both interrupted-restore branches use its export summary, not a marker
projection. Missing or mismatched evidence fails closed. A legacy cycle keeps its prior/displaced arguments
and existing semantics. The supervisor must stay paused until the recovery is resolved. After consumption, roll
forward. Confirm the host paths before cutover and keep the prior until publication is confirmed.

**Logical history damage.** A structurally readable database can be repaired for named wallets.
A failed history check reports expected/observed digests and stops the cycle. Pause, preserve evidence,
abandon the failed unprepared candidate under the guards above, stage from the installed cache and
admit a new generation with `--full-read-wallets` for the named wallets. Repair logs stored/certified
digests and replaces history, retaining proved partial windows or nothing if no window was proved, so repeated failed downloads do not
keep a damaged wallet blocking later cycles. The pass verifies replacement before preparation;
cumulative drops survive. This applies to history damage at rest in an otherwise readable installed
format-3 cache; outgoing validation needs neither history nor a retired backup.

**Record damage.** Certificates, receipts, manifests and migration state cannot be re-downloaded.
Re-finalize checks them before candidate hashing; damage there refuses preparation and is recovered
by abandonment/restaging from the untouched installed cache. Certificate digest failure names the
table, not wallets. Damage to those records at rest in the installed file stops admission and is
detected, not repaired: restaging and `--full-read-wallets` cannot fix it. After publication,
`cache-restore-prior` refuses and retention has deleted the displaced backup. Keep the loop paused,
preserve evidence and obtain an independently verified restore or separately authorized reseed.

**Structural damage.** `--full-read-wallets` never repairs SQLite structure. Abandon a structurally
damaged candidate and restage from an intact installed cache. A structurally damaged installed file
loses its accepted-hash shortcut when bytes change and staging's `quick_check` refuses it. Preserve
the file/evidence while paused for an independently verified restore or separately authorized reseed.
Neither manual SQL nor a new candidate bypasses this boundary. Missing/altered committed spools use
pre-prepare abandonment/restaging, never regeneration.

From the first format-3 preparation onward, keep a format-3-capable release and fix forward through
the exact request. Never downgrade the cache in place; see [release rollback](#forge-release-deployment-and-rollback).

### Classifier-6 catch-up and coordinated release (#739)

Rollout step 1 is the reviewed/tested combined release: current main reconciled, full gate and CI
green, cross-built Forge binaries and their dependency closure recorded with the tested git tree
and binary hashes, kept beside the installed release. One Forge operator owns the resumable record.
This docs procedure does not authorize Forge or Supabase mutation; the consent points below belong
to the owner. Record each completed step and resume it, rather than starting another cycle.

2. **Boundary.** Require flag `stop`, unit inactive, no cycle and no pending/unconsumed request.
   Resolve pre-release requests with their prior release and record installed/target identities.
3. Install the tested branch release through [release steps 1–7](#forge-release-deployment-and-rollback).
   Restoring the pause record must keep the recorded stop/inactive intent.
4. Record original `.env` values, then set this catch-up configuration before its cycle starts:

   | `.env` key | Catch-up value |
   |---|---|
   | `MAX_CACHE_STALENESS_HOURS` | [Owner's interim freshness bound](_GLOSSARY.md#catch-up-freshness) |
   | `PE_RANK_SCHEMA_TWO_CUTOVER` | `prepare` |
   | `PE_BOOTSTRAP_ACTIVATION_BATCH_WALLETS` | `0` |

   Keep existing `PE_BOOTSTRAP_CLOB_CONCURRENCY` unless a two-minute read-only Forge probe of
   reference pages/s shows request latency, rather than the shared rate gate, binding. If so, raise
   that existing knob before starting, preserving the [canonical prices-history gate and documented
   limit](_GLOSSARY.md#bootstrap-defaults-pe-bootstrap). Never change configuration during the cycle.
5. **Owner consent to the catch-up**, then run one detached `bash scripts/rank_and_push.sh` while
   the loop unit stays inactive; record its PID/process group and log. It runs staging → transition
   → collection → payout → finalize → export → passes/reference fetch → export-bound re-finalize →
   prepare. Acceptance is `RANK_AND_PUSH_PREPARED_ONLY=<request>` and exit 2 with request/pending
   pointer retained and installed bytes unchanged; other exit-2 failures are not acceptance.
   Before launching, cross-build from the tested tree with the bootstrap release recipe:
   `cargo build --release -p pe-bootstrap --features scenario --example classifier_v6_sample`,
   keeping the same Forge target/toolchain arguments and environment. Record the example executable's
   SHA-256 beside the tested tree. Right after finalize, while later stages write only prices, run
   `<release-example>/classifier_v6_sample <finalized-candidate-side-cache>` for #739 AC1's
   deterministic wallet sample through an ordinary read-only connection. It compares selected IDs,
   all fourteen spool fields and certificate drops, and explains classifier-5 differences by their
   explicit rules. Keep its stdout JSON and exit status in the cycle's record; on any nonzero exit,
   stop the recorded wrapper process group before preparation. Pre-prepare abandonment stays available.
   Reference pages warm through the ordinary resumable stage, not a special pre-head fetch.
   Record the full candidate-through-prepare acceptance on the tested tree;
   activation/publication/retention and cadence complete acceptance after merge.
6. Merge only after green CI with an expected-head guard, coordinating the runtime-change window on
   [#588](https://github.com/sppburke/prediction-markets/issues/588) and
   [#694](https://github.com/sppburke/prediction-markets/issues/694). Acceptance transfers only across
   identical Forge runtime inputs: bootstrap's dependency closure, `Cargo.lock`, toolchain, wrapper
   scripts, `supabase_schema.sql` and configuration. If main moved, rerun the full gate on the merged
   tree; a runtime-input change invalidates affected acceptance, which must rerun with the prepared
   request unchanged. Documentation-only differences take their applicable checks. Install the
   main release built from that merged tree, recording identities and retaining the exact request
   through release step 5; verify main CI. Keep cutover at `prepare`.
7. **Owner consent to Supabase**, then apply `scripts/supabase_schema.sql` idempotently. It adds
   `ranking_batches.classifier_version`, `ranking_entries.history_through_unix`,
   `ranking_scope_drops` and `publish_ranking_batch_v2`, with no removals. Verify objects read-only:
   nullable historical fields, scope/cause constraints and primary key, cascading batch reference,
   RLS/anon reads and service-role-only execution. The v2 function preserves the original RPC's
   validation/idempotency/count checks and commits batch, entries and drops atomically; the original
   three-argument RPC stays for older releases.
8. Set `PE_RANK_SCHEMA_TWO_CUTOVER=1` and resume the pending-publication path. It activates and
   publishes exactly the saved request through `publish_ranking_batch_v2`, without a new clock or
   rebuilt request. Verify its classifier, scoring limits, entry coverage ends and scope rows as
   well as exact latest-batch/application checks. pe-service release 2 starts on its coordinated
   restart after this batch is applied; its code and deploy belong to #588, not this Forge release.
9. Recompute both drives' capacity headroom, retention horizon and stage forecasts from measured
   classifier-6 spool bytes, positions and reference demand at the [shared floor](_GLOSSARY.md#ranking-horizon-floor).
   Set `PE_BOOTSTRAP_ACTIVATION_BATCH_WALLETS` to the owner's ordinary-cycle choice. **Explicit owner
   restart consent** is required before setting flag `run` and starting the loop unit. The first
   ordinary cycle keeps the [interim freshness override](_GLOSSARY.md#catch-up-freshness): deferred
   wallets due after the catch-up read in full and fetch their unwarmed reference pages. After it
   prepares, remove that override by a `.env` edit between cycles, before the second ordinary cycle
   starts. Observe three ordinary publications under the existing batch-application,
   bounded-membership and service-health checks.

For each cycle record stage durations, rows read/written and commitments, drops and ignored records,
peak RSS, spool/export/spill/WAL bytes and both drives' free space. Collection timing separates fetch
completion, writer completion, blocked-producer time and final drain. Since #747,
`producer_blocked_ms` is the summed time individual wallet futures wait to send to the writer,
including overlapping waits; it is not comparable with earlier cycles’ wall-clock blocked time.
Unfinished reads continue while other completions await a full queue. Report publication-to-publication
cadence; report the first ordinary cycle separately. Forecasts are not measurements. A cycle exceeding
the canonical freshness/cadence goal is reported to the owner the same day with options; closure needs
measured ordinary cadence meeting the owner's goal. Rollback before preparation uses the guarded
abandonment above; after preparation fix forward with a format-3-capable release.

### Fresh bulk root with deferred global uniqueness (#588)

The supervised `rank_and_push.sh` pipeline starts and resumes a qualifying bulk root itself,
using `cache-populate-activity-v2 --fresh-generation 1 --bulk-root --fixed-db …`, adding `--prior …`
for a legacy cycle. New roots read the immutable staging baseline without a prior. Admission is only
for a newly staged and migrated private candidate. Fixed, candidate and any legacy prior must be
distinct paths and inodes. The candidate must have no activity rows, receipts,
completed activity manifest, frozen-reference verification or finalized projection. Admission freezes
identity 4 in format 3, with no predecessor, the full wallet union, empty `repair_wallets` and the
empty certificate table's `certified_digest`. The same admission creates format-3 tables/triggers,
removes the empty named identity index and sets `PRAGMA user_version=-2`. This negative version is
reserved for the unfinished private layout; it is not an activity/parser/identity version or a
configuration key. Existing collections are never converted. Historical version-1 identities,
frozen-reference collections, ordinary collections without the flag, and every successor retain
immediate indexed uniqueness. New roots use acquisition-3 full reads without a predecessor and its
bootstrap-only relaxed row acceptance. An unfinished identity-2 root keeps its frozen acquisition-2
contract through resume/sealing; it is never reinterpreted as acquisition 3.

New ordinary schemas use `idx_activity_groups_v2_source_trade_id`, a full single-column `BINARY`
unique index, in place of the automatic primary-key index. Historical primary-key schemas remain
supported unchanged. Bulk admission changes neither column types, `NOT NULL`/`CHECK` constraints,
rowids, nor the wallet/time/ID index. Only the two identity collision probes are skipped in the
admitted bulk state. Before a wallet writes any row, its bounded aggregate batch must contain unique
IDs. Transaction/data-version checks, strict inserts, wallet-index reads, ordering, atomic receipts
and exclusions still run.

After the serial writer drains, `collect_activity_v2` builds the named unique index with **plain
`CREATE UNIQUE INDEX`**, verifies its definition through `index_list`/`index_xinfo`, validates all
receipt commitments (with the historical acquisition-2 content checks for an older root), records
the completed manifest and archived identity, and restores
`user_version=2` in one transaction. A successful checkpoint/truncate follows. The build therefore
precedes successor admission and the successor's settled-end clock; no top-up freshness budget is
spent building the root index. A new format-3 root finalizes through the ordinary verified pass,
certifying each available wallet's recency and drops from its complete full receipt. A sealed
identity-2 root must first admit an identity-4 successor, even when fresh, so classifier 6 never
finalizes format 2. It does not prove that later top-up, payout, ranking and preparation
fit the unchanged freshness limit.

Global duplicates, an existing index of the same name (even one with the right definition), index
I/O/build failure, content mismatch or manifest failure stop permanently with exit 1, retain committed
wallet rows/receipts and leave the generation incomplete behind the fence. No deduplication or
`IF NOT EXISTS` substitutes for the build. A stop before or during the build rolls back the sealing
transaction; rerun the **same bulk-root command** to validate/skip retained receipts and retry sealing.
Do not run migration again, manually set the schema version, delete WAL/SHM, transplant receipts,
or start a successor. A duplicate failure requires investigation; an unchanged retry fails again.
Once sealed, repeating the command reads the completed root idempotently. In binaries containing
the #588 fence and archive fix, `WalletCache` writable/read-only opens, purge archive attachments,
and guarded migration opens refuse unfinished roots with the recovery instruction. These cover
coverage, migrate, payout, finalize, activation, staging and ordinary/successor collection; only the
bulk collector opens the unfinished layout for mutation. The updated Python cache-opening boundaries
(ranking/export/publication, cycle snapshots, partial-wallet checks, token mapping and the research
diagnostics) also refuse it. The cycle target helper's explicit `--include-bulk-root` mode and the
wrapper's pre-staging version probe are read-only routing exceptions, not readiness certification.
Raw SQLite inspection remains an operator exception, never an alternate mutation owner.

**Required compatibility prerequisite.** Exclude all older binaries and script checkouts from these
cache paths before admission and throughout collection/sealing. In particular, the previous binary
at `d4ac7ef` rejects the reserved version through its normal writable `WalletCache` opener, but its
read-only coverage opener ignores it: missing legacy tables can produce zero gaps and report
`CLEAN`. Its existing-candidate `cache-stage-v2` path can succeed with `side_schema=-2`. The reserved
value cannot retroactively protect those entry points. While paused, verify the deployed binary's
reviewed build/hash and the scripts' revision, pin the supervisor and operator commands to them,
stop older processes, and disable old cron/unit/manual launch paths. Do not start or restore the
loop until this exclusion is established; do not roll back tooling while a bulk artifact exists.

**Final-build disk admission.** For the supplied estimate of 627 million physical aggregate rows,
budget approximately **50–60 GB finished index + 50–60 GB WAL + 100 GB sort scratch = 200–220 GB
additional free space before margin**, beyond the installed cache, collected candidate, exports
and other pipeline files. Legacy cutover also retains its existing prior; new cycles do not. These are decimal GB estimates from the independent
layout/I/O analysis, not measurements on Forge. Reusable database pages may reduce index allocation;
do not count them without measuring. Check database and scratch filesystems separately if different.
Keep additional operational margin for filesystem overhead, receipts and concurrent disk use.
The collector sets connection-local `temp_store=FILE` for the bulk path; select a sufficiently large
SSD directory with `SQLITE_TMPDIR` before process start. Do not use a RAM-backed `/tmp` for this sort.
Every bulk-root invocation, including a resume, first forces a small SQLite temporary-table spill on
a disposable connection before fetching any wallet. Failure is permanent and names SQLite's error,
the documented temporary-directory order and the sort-scratch requirement. Passing this probe proves
temporary-file creation and writes at admission; it does not reserve or prove the full disk budget.
SQLite documents the temporary-file directory selection and index metadata in
[temporary files](https://www.sqlite.org/tempfiles.html) and
[index_xinfo](https://www.sqlite.org/pragma.html#pragma_index_xinfo).
No automatic disk estimator, conversion driver or resource-tuning framework is introduced.
Capture available bytes, `dbstat` table/index sizes, free pages, WAL peak and build/checkpoint elapsed
time on Forge; the implementation tests do not establish throughput, duration or production capacity.

**Supervised sequence for a fresh Forge cycle.** Install and resume through the
[release procedure](#forge-release-deployment-and-rollback), establishing the compatibility and
disk prerequisites above. Preserve any interrupted cycle and its recovery artifacts. Keep the
persisted `.env` cutover setting at `prepare` until acceptance of the exact pending request;
installed schema-two caches use ordinary successors. Configure SSD scratch and verify final-build
headroom in the supervisor's environment before resuming the recorded run intent.

The zero-argument wrapper stages/migrates, discovers and activates, then asks
`candidate-targets --include-bulk-root` for the initial generation and durable eligibility. A new
root requires schema two, generation one, no existing collection identity, activity rows or receipts,
the exact named unique index with no primary-key layout, and the unfinalized private state with no
completed activity manifest, frozen verification or projection. A reserved-state root resumes only
with its identity-2 or identity-4 generation-one root, no predecessor, and that same private state; retained
wallet rows/receipts are allowed. Rust still owns admission and fails closed. Ordinary interrupted
roots and unfinished successors use ordinary collection. Completed selected format-3 heads
skip wrapper collection re-entry; the always-run `--after-collection` check still selects an allowed
stale-head top-up or refuses a stale completed top-up. A completed format-2 head first selects its
ordinary successor (`initial + 1`) regardless of freshness. After a transient exit 75 the existing
supervisor re-enters the same cycle, skips staging/migration/discovery/activation for a fenced
candidate, and resumes bulk collection from its receipts. After sealing, the one allowed top-up
never receives `--bulk-root`. Payout, finalization and preparation keep their existing gates.
Exit 2 with `RANK_AND_PUSH_PREPARED_ONLY=…` stops the loop before activation; permanent failures also
stop it for investigation. Keep `prepare` until operator acceptance of the exact pending request.

**Forge temporary storage (host observation, 2026-09-18).** `SQLITE_TMPDIR` must point to large
SSD-backed writable storage; the live setting is `/mnt/t7/sqlite-tmp`. `/mnt/storage/tmp`
is on the USB stick.
The default `/var/tmp` is read-only with the root filesystem, and writable `/tmp` is too small.
The supervisor unit `pe-rank-loop.service` sets no `Environment`, but the wrapper exports
its whole `.env`, including `SQLITE_TMPDIR`, to the collector. Export it for manual
invocations/resumes from the same retained configuration.

**Interrupted collection.** Use the retained cycle and exact paths through the
[release procedure](#forge-release-deployment-and-rollback). An unfinished bulk root resumes its
same command and receipts; never archive the cycle pointer, allocate a replacement candidate or
force a successor as part of deployment. A prepared request takes precedence over collection.

### Incremental top-up measurement and paused-cycle handoff (#648)

The carry/re-stamping measurements below describe historical format 2. For format 3, measure fetched
inserts, differing full replacements and failed-acquisition checks separately, plus the single
verified pass, spool and export; use the [#739 catch-up acceptance](#classifier-6-catch-up-and-coordinated-release-739).

The implementation fixtures prove acquisition/certification equivalence, crash atomicity and consumer
selection. They do **not** establish production throughput, disk budget or freshness acceptance.
The following measurement and deployment gates remain mandatory before calling the release publishable.
Use an authorized disposable candidate with representative wallet sizes, physical interleaving and
indexes. Preserve fixed/prior/cycle/request recovery artifacts throughout the measurement.

Capture table versus index bytes with `dbstat` before and after collection:

```sql
SELECT name, SUM(pgsize) AS bytes FROM dbstat GROUP BY name ORDER BY name;
```

Record the actual collector writer's startup log: `journal_mode`, `synchronous`,
`wal_autocheckpoint`, `page_size`, `cache_size`, `mmap_size`, SQLite version/source ID. This connection
retains its guarded opener and busy timeout, then applies the configured cache and mmap tuning
through `WalletCache::apply_connection_tuning`, including on resume. Use this writer log to verify
effective settings; a separate SQLite shell has its own connection-local settings. Per-wallet commits,
`synchronous=FULL` and automatic checkpoint settings remain unchanged.
Enable the `pe_bootstrap::cache_migration` debug log for wallet transaction elapsed time and carried
row counts (`activity predecessor verified`). Record all of:

- `/proc/<collector-pid>/io` read/write-byte deltas, device-counter deltas and elapsed time;
- WAL peak space and checkpoint progress/time across completed wallet transactions, including the
  largest wallet; SQL batching does not cap the wallet transaction's WAL;
- initial `1 → 2`, a same-width increment, and a SQLite integer-width boundary;
- validation, classification, hashing, export and ranking elapsed time separately.

A generation-only update preserves rowid and indexed values but dirties table pages; integer-width
changes can allocate additional space. If T is table-page bytes touched once, ideal carry writes
are approximately T to WAL plus T to the database. Add delta/index writes, replacements, repeated
page dirtying, splits, receipts, projections and filesystem amplification. Index-inclusive cache size
is not T. WAL file size is peak space, not cumulative writes. Do not claim a measured duration from
these sensitivity estimates.

Do not hold a long-lived candidate read transaction across wallet commits. WAL growth must stabilize
and checkpoint progress must continue; checkpoint starvation fails the gate. Reserve space for the
largest wallet, the candidate/prior and export, not just the automatic checkpoint threshold. Final
sealing still requires successful checkpoint/truncate and sidecar checks; never truncate WAL externally.
Require the complete production-sized top-up → preparation → activation path to fit the unchanged
freshness and measured disk budget. Preparation must accept actual trade/payout times with the full
union. Report any bottleneck and keep recovery evidence; never relabel clocks or relax gates.

For an interrupted handoff, install the combined release through the
[release procedure](#forge-release-deployment-and-rollback), preserving the authentic identities,
fixed/prior/candidate paths, payout target, cycle configuration and any prepared request. On resume,
a completed selected head skips re-entry, an unfinished head resumes, and a stale initial head may
receive exactly one linked top-up. A stale completed top-up stops before ranking. Preparation
still requires `RANK_AND_PUSH_PREPARED_ONLY=...`, exit 2, retained pending state and unchanged fixed
bytes while cutover is `prepare`; unrelated exit-2 failures are not acceptance. Preserve any exact
prepared request through acceptance and resume rather than recreating it.

### Continuous Forge supervisor

Classifier 6 / format 3 changes no loop unit, flag or lock lifecycle. Its
[catch-up procedure](#classifier-6-catch-up-and-coordinated-release-739) owns the stopped installation,
detached one-shot acceptance, exact pending-request handoff and explicit owner restart consent.
The [interim `.env` freshness bound](_GLOSSARY.md#catch-up-freshness) covers catch-up and the first
ordinary cycle only; remove it between cycles before the second ordinary cycle. Normal cycles use
the canonical freshness checks, the owner's frozen activation batch and the one
[`MIN_TTR_SECS` scoring value](_GLOSSARY.md#ranking-horizon-floor), with batch limits taken from the
validated oracle manifest. Scope/coverage publication is atomic; the supervisor adds no special stage.

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
any other value is fatal. The supervisor checks it before each attempt and every second
while waiting (retry backoff or idle):

```bash
# Graceful: finish the current attempt, then stop (unit ends inactive/exit 0).
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
  events, or ranking cannot admit another activation batch.
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
Deployment and rollback use the [release procedure](#forge-release-deployment-and-rollback),
which preserves the original run intent and enablement before draining.

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

For classifier 6, apply `scripts/supabase_schema.sql` with owner consent at
[catch-up step 7](#classifier-6-catch-up-and-coordinated-release-739), before resuming publication.
It adds the nullable classifier/coverage fields, `ranking_scope_drops` and the service-role-only
`publish_ranking_batch_v2`; verify the new objects read-only. All five batch limits, scope drops and
coverage ends belong to the exact saved request and commit atomically with its entries. The original
three-argument function stays for older releases; the additions remain on rollback.

For the historical first publication-RPC deployment, the additive `ranking_batches.publish_key` column and unique
index preserve historical batches whose key is null. The service-role-only
`publish_ranking_batch` RPC creates/reuses the keyed batch and inserts all entries
inside one PostgreSQL transaction, so a failed request exposes neither a partial
epoch nor a duplicate epoch. If code rollback is required, restore the recorded prior binary/checkout
pair through the
[release procedure](#forge-release-deployment-and-rollback); the additive schema can remain in place.

Before allocating a new legacy schema-one production run, the wrapper captures the current
daily source watermark, pipeline versions, and configuration. If they exactly match a prior
full accepted capture, it prints `RANK_AND_PUSH_UNCHANGED_DAILY_WATERMARK=1` and exits
successfully before discovery, refresh, export, rank, or publication. An initial schema-one
cutover keeps this gate. Installed schema-two runs write a two-field lane record and start a
new cycle even on the same UTC day. An existing publication pointer takes precedence over the
cycle pointer, and either pointer takes precedence over this new-cycle check.

`pe-service` on the VPS picks up the new `latest_ranking` on its next refresh
(score-update-only). MEMBERSHIP follows `watchlist_membership_mode` (`_GLOSSARY.md`):
`knockout` (legacy — the maintenance tick's eviction/backfill is the sole membership
path) or `full_rerank` (each batch transition wholesale-replaces the live
top-`active_watchlist_size` SURVIVORS — the cutover production mode; the rows are read
from `ranking_entries` pinned to the triggering `batch_id`, never from the moving view, so
the applied rows and the committed marker name one batch, #542). Every read is
gated on the ranker's `survives` verdict (#518), so the published batch is a bench and
`active_watchlist_size` caps the survivors admitted from it rather than selecting a raw
top-N. A financial Start boots from the replayed Start-bound structural membership;
a newer published batch is applied on a maintenance tick, subject to live admission filters.
Every post-boot addition on either path is prepared first
(#542/#544): its prior-market history is complete and its current positions pass the
five-step causal bracket before the orchestrator records the validation and the wallet is
published. A typed wallet failure defers that wallet and replans from the remaining ranked
survivors; each published structural change is one atomic record with proofs for its additions.
Shared failures keep full-rerank and capacity unchanged for their next retry, while knockout
may publish decided evictions without a failed backfill. `active_watchlist_size` is
Supabase-authoritative (default 100, valid `1..=200`) and is polled every 30 seconds. A
grow fetches the requested top-N and validates all newly admitted wallets' prior-market
history and current positions before the atomic validation/membership swap; a shrink uses
the same atomic membership swap. Typed wallet deferrals allow a nonempty partial capacity result
to apply the requested cap. A zero-member result remains pending with no membership record;
invalid values, Supabase failures, and other shared errors keep the last-known-good target and
membership. Pending or failed requests retry independently on the capacity worker's next
30-second retry. Check
`status.json`: `watchlist_size` is actual membership and `watchlist_target_size` is the
last safely applied runtime cap. The additive optional `live` block reports
`pending_dispatch_seeds`, `ready_dispatch_seeds`, `fetched_at_unix` (last successful accounts
poll; `null` before one succeeds), `stale` (`true` past `live_accounts_stale_after_secs` —
no new live work is staged while stale, #514), `control_available` and `control_age_secs`,
and per-account `account_id, is_primary, enabled` (historical only), `requested_live_mode,
effective_live_mode, armed, credential_binding_ready`. `armed` means both observed modes are
`live_tiny`; control availability and credential readiness are separate order observations. Installing this runtime-capacity support
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
Both are compare-and-cleared only after exact publication verification and an accepted
record (full legacy capture or fresh-lane lane record). Retire superseded artifacts to
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
