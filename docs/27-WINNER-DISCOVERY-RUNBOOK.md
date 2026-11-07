# 27 — Winner-Discovery Runbook

Automates the ingest of new candidate wallets from datadash.xyz cohorts and ranking-scope
maker and taker participants. Leaderboard HTTP acquisition is retired (#842). The
`pe-bootstrap winner-discovery` subcommand upserts each discovered wallet into the
local `wallet_cache.db`. A standalone invocation immediately applies the legacy
activation rules; the full rank-and-push wrapper explicitly defers that behavior
and admits its transactionally audited controlled batch; participant holds can require additional
operator-named completion batches on the retained side cache. Wallets then flow into the
ranking pipeline (Step 0 of `scripts/rank_and_push.sh`) and reach the live set only
via Supabase `latest_ranking` — admitted by the maintenance tick (knockout mode) or
the next batch swap (`full_rerank`, the cutover production mode) — never directly. This is the
only sanctioned path for adding new wallets; see `AGENTS.md` "Do not add wallets" rule.

The ranking uses [classifier 6 and its scoped drops](_GLOSSARY.md#history-format-3-and-classifier-6-739)
and the shared [ranking/copy horizon floor](_GLOSSARY.md#ranking-horizon-floor). Discovery is
acquisition, not first-entry classification; pe-service release 2's alignment is coordinated on
[#588](https://github.com/sppburke/prediction-markets/issues/588).

## Required environment

Set `PE_BOOTSTRAP_LEADERBOARD_TOP_N=0`. The retained positive default now refuses
acquisition before building an HTTP client. Other settings and defaults remain in the glossary.

```
PE_BOOTSTRAP_LEADERBOARD_BASE_URL        # override leaderboard host (testing)
PE_BOOTSTRAP_LEADERBOARD_TOP_N           # must be 0; a positive depth refuses retired acquisition
PE_BOOTSTRAP_MARKET_RECURRENCES          # participant markets' Gamma recurrences, TOML array (canonical default in _GLOSSARY.md)
PE_BOOTSTRAP_DATADASH_API_URL            # default https://api.datadash.xyz (on by default; "" disables)
PE_BOOTSTRAP_PARTICIPANT_LOOKBACK_SECS    # overlap from persisted coverage; 0 disables with no HTTP or coverage change
PE_BOOTSTRAP_CACHE_PATH                  # default wallet_cache.db
```

## Invocation

`winner-discovery` is **Step 0** of `scripts/rank_and_push.sh` (run automatically
before each rank). To run it standalone:

```bash
# Build the bootstrap binary first if not already built:
cargo build --release -p pe-bootstrap

# Standalone discovery preserves legacy immediate activation:
PE_BOOTSTRAP_CACHE_PATH=data/wallet_cache.db \
  ./target/release/pe-bootstrap winner-discovery \
    --participant-audit "data/participant_audit.$$.$(date -u +%Y%m%dT%H%M%S%NZ).json"
```

Do not add `--defer-activation` to an ad-hoc command unless the same operation
also runs `activate-next` with a stable batch ID. `rank_and_push.sh` owns that
pairing: both discovery and backfill defer the global rule, and one
`activate-next` transaction selects at most
`bootstrap_pipeline_activation_batch_wallets`, records the exact batch in
SQLite, and exports `activated_wallets.csv` into the run directory. Empty or
partially depleted candidate piles warn. In the private-candidate lane, any participant still
inactive or tombstoned holds before collection, except an ever-infra one, which is excluded and named
(#826); the batch cap cannot silently omit it.

The [one-time classifier-6 catch-up](26-DATA-REFRESH-AND-REOPTIMIZATION-RUNBOOK.md#classifier-6-catch-up-and-coordinated-release-739)
records an activation batch of zero. Before the first ordinary cycle, the owner selects the
ordinary batch size; a recorded cycle keeps that value through retries.

There is no separate eval/export stage or candidates selection JSON: `winner-discovery` fetches
the leaderboard, datadash and market participant slices and upserts new wallets (recording the
source bit) straight into the cache. In standalone mode it activates eligible
ones immediately. `rank_and_push.sh` instead admits only the controlled batch, then
backfills their trade history and ranks them alongside the rest of the universe — the
ranker's own eligibility filters decide which discovered wallets make the published
cohort.

## Collection beside staging (C9)

After export, Stage 2 hashes beside pass one using the readiness-marker protocol in
[the refresh runbook](26-DATA-REFRESH-AND-REOPTIMIZATION-RUNBOOK.md#fresh-private-candidate-cycles-and-the-scheduled-schema-two-lane-588-648).

Plain discovery still collects and applies leaderboard → datadash → participants on one cache,
committing each source before the next. In the schema-two wrapper lane, an attempt whose candidate
and prior are both absent starts collection immediately before `cache-stage`:

```bash
pe-bootstrap winner-discovery --defer-activation --collect-to "$SPOOL" \
  --participant-audit "$OUT_DIR/participant_audit.$$.$(date -u +%Y%m%dT%H%M%S%NZ).json" \
  --coverage-from "$FIXED_DB" --not-before "$((RANK_WINDOW_END_UNIX + FILL_WINDOW_SECS))" \
  --end-date-min "$FLOOR" --recurrences "$RECURRENCES" [bootstrap.toml]
PE_BOOTSTRAP_CACHE_PATH="$SIDE" pe-bootstrap winner-discovery --defer-activation \
  --apply-from "$SPOOL" --end-date-min "$FLOOR" --recurrences "$RECURRENCES" [bootstrap.toml]
```

Collector flags are required together; apply forbids them. Both modes require
`--defer-activation`, and no other subcommand accepts these flags. Collection uses the same
fetchers, limits and retries. It writes its attempt's participant audit, spool and atomic-write
temporary files; it takes no cache lock or writable opener. It refuses an existing spool, any
spool/temp alias of the fixed cache, sidecars or lock, and a fixed schema other than two before
requests. Only the reached, enabled participant arm waits for `--not-before`, samples its clock
and reads raw coverage immutably (ignoring WAL). Decode/validation failures are recorded source
outcomes; local coverage-read and audit errors exit nonzero without a spool. Exit zero means a
complete spool was written.

Apply validates the version, settings, source prefix, coverage flag and wallet/count/coverage
records and roles before taking the candidate lock. Under that lock it compares raw coverage
through a read-only connection, closes it, then opens writably. Refusal before the writable opener
exits [77](_GLOSSARY.md#discovery-spool). Binding success replays the ordinary
commits and completion/warning lines; an earlier cache error wins over a later recorded source
failure. Participant upserts still commit before coverage. Tombstones are evaluated at apply time,
and `discovered_at_unix` is the insert time at apply.

After staging/migration, the activation-batch probe still decides whether discovery runs. A recorded
batch stops/reaps the collector and skips discovery. Otherwise the wrapper joins it and applies its
spool. Collector nonzero or apply 77 runs ordinary discovery; every other apply status uses ordinary
stage status mapping (including participant 75). A staging failure stops/reaps collection and keeps
staging's status. Existing candidates, prior-backed attempts and bulk-root resumes retain their
ordinary flow. Each eligible attempt chooses a new spool; completed spools and abandoned temporary
files remain evidence and are never used by another attempt. Each collecting invocation, including
ordinary fallback and serial retries, also gets a fresh participant-audit path; apply gets none.

Evidence includes `[winner-discovery] collecting beside cache-stage: <command>`, the stage start/end,
`[winner-discovery] joining the collector started beside cache-stage`,
`[winner-discovery] collector exited <status>`, and the ordinary apply/fallback start/end lines.
Collector and apply log `winner-discovery: spool` with path and SHA-256. Collection writes participant
chunk/page evidence to the audit and logs one summary with its path, SHA-256 and byte length;
datadash soft failure and source completion lines occur at apply. On the first installed eligible
cycle, measure overlap, join wait and critical-path saving from these journal lines; this rollout
measurement is not a test claim. Earlier collection can advance coverage less far than serial
discovery; the next cycle's canonical overlap prevents a gap.

## Retired leaderboard acquisition (#842)

A positive leaderboard depth returns `BootstrapError::Leaderboard` before HTTP client
construction. Plain discovery exits 1 with the retirement and `PE_BOOTSTRAP_LEADERBOARD_TOP_N=0`
fix. C9 collection exits 0 and writes a version-1 spool with a leaderboard
`Terminal { exit_code: 1, message }`; application returns that recorded failure after the
existing binding and writable-opener checks. Depth zero retains the `Disabled` slot and
zero leaderboard report counts. An old spool containing collected leaderboard data refuses
with exit 77. Settings, source order and the production-config spool bytes are unchanged.

`SRC_LEADERBOARD` remains a persisted source bit with its existing activation and tombstone
semantics. Historical rows stay valid. Participant scope and interval guarantees are unchanged;
this retirement introduces no new wallet-coverage claim. The retired Radion bit stays reserved.

## Market participants (#800, #821)

The third source reads public Data API `/v2/trades` for both maker and taker wallets.
The wrapper passes the ranking `--recurrences` and `--end-date-min` market floor to discovery.
Discovery retains covered series by id and adds every listed series of those recurrences;
ranking's `market-scope --series` then reads that explicit coverage set by id. A listing
omission cannot drop a covered series; a missing, renamed or changed-recurrence by-id reply
fails temporarily without changing coverage. `winner-discovery --series` is rejected.
Every in-scope market is read over the whole discovery interval, regardless of its end date or
open status. Trades after a market's end,
and an old market trading between runs then closing, remain discoverable. Future markets can be
listed; an empty trades reply adds no wallet. Standalone discovery without `--end-date-min` uses
the read's start as its floor. Cursor requests repeat the same condition, role and size filters.
Timestamps must never increase; early stop requires a strict decrease before crossing the
read's lower bound, otherwise the chain reaches its terminal page. The size filter retains
every positive fill. Bounds, window guard, concurrency and request spacing are canonical in
[`participant_lookback_secs`](_GLOSSARY.md#bootstrap-defaults-pe-bootstrap).

Valid `proxy_wallet` addresses are canonicalized to lowercase and deduplicated before tagging with
`SRC_MARKET_PARTICIPANT`. A row without a valid wallet makes the chunk incomplete; its page hash
is retained in the participant audit, and no participant from that run is upserted. Preferred
activation eligibility is the same as datadash, including already-known inactive wallets. Under the
owner's 2026-10-31 decisions, participant upserts lift `proven_loser` tombstones, while infra marks
are permanent (#826): `infra` tombstones stay blocked, a live `is_infra` flag is kept, and the four
exchange contracts stay blocked (`docs/37` §6). Each skipped infra-tombstoned participant is logged
by wallet and counted. Normal history acquisition, eligibility, ranking and sizing still apply.

Completed coverage lives in the dedicated `meta` key
[`participant_discovery_coverage`](_GLOSSARY.md#bootstrap-defaults-pe-bootstrap), including on
schema-two caches without `source_cursor`. It records normalized recurrences, an explicit
series set, the market floor, and one half-open interval. With matching recurrences and an
explicit set, covered series start at `covered_through − participant_lookback_secs` and additions
read back to `covered_from`, so the interval holds for every named series. Without a record,
with other recurrences or with legacy `series: null`, discovery lists the set, starts at
`now − participant_lookback_secs` and replaces coverage; the publication hold stays until an
import repairs any missing interval. Thus stopped loops retain the gap to read. Only an
absent key means no record; query, parse and validation errors stop discovery. Matching-scope
success extends coverage and keeps the later market floor. Under the cache mutation
lock, participant upserts commit before coverage, so interruption between commits repeats an
idempotent upsert without advancing coverage past an uncommitted wallet. Disabled or failed
collection leaves coverage unchanged.

Collecting discovery requires `--participant-audit PATH` (or `--participant-audit=PATH`) when
lookback is positive. Lookback zero allows omission; a supplied path writes a `disabled` audit.
Apply forbids the flag. Missing, empty or duplicate values and use by other subcommands exit 1.
Before any source request, preflight refuses an existing target or symlink and aliases of the
protected cache (foreground cache or collector `coverage-from`), its spelled and physical SQLite
sidecars, mutation lock, collector spool or either atomic-write temporary file. Refusal exits 1,
not spool-refusal 77. Paths name files in existing directories.

At the participant emission boundary, collection atomically writes and syncs one JSON document:
`{"version":1,"record":<summary>,"chunks":[...],"pages":[...]}`. The summary retains source id
`polymarket-data-api-trades`, schema/parser versions, frozen window, wallet count, status, the
Gamma scope's own source ID, version, schema/parser versions, digest, count and series, and page
counts. Chunks retain lower bound, newest/oldest timestamps, page count and stop reason
(`terminal` or `threshold`). Pages retain URL, raw sha256, rows, observed/received/source times
and request/returned cursors, in Gamma sweep then trades order; ordinals are per stream.
The counts match the retained arrays. Each trades URL lists its condition ids; the scope digest
covers the list. One `winner_discovery: participants` journal event keeps the summary `record`
and adds `audit_path`, `audit_sha256` and `audit_bytes` from a file readback. No chunk/page journal
events are emitted. Disabled collection without a path keeps only its disabled summary.

Failed/incomplete attempts that reach this boundary also write their audit before returning
`TransientSource`. A prior-coverage read error or any earlier failure/interruption has no audit
persistence guarantee. A failed Gamma sweep transfers none of that sweep's pages; buffered trades
stop at the first error. These retention boundaries are unchanged.

Audit serialization, write, sync or digest-readback failure is a local fatal error: foreground
exits 1 before participant upserts, activation or coverage; collector exits nonzero before any
spool, so the wrapper falls back with a fresh audit path. Earlier foreground source commits
(datadash) and cache-opener migrations remain. A source failure still writes a recorded collector
outcome and spool, exits 0 in collection and returns 75 on apply. Completed audits and abandoned
temporary files remain cycle evidence; later attempts never reuse their paths.

Statuses:

- `ok`: collection completed; counts are included in both CLI discovery summaries.
- `disabled`: lookback is zero; no Gamma or trades HTTP or coverage change.
- `failed:<reason>` or `incomplete:<reason>`: a source or validation failure, including an
  invalid wallet, timestamp increase, repeated cursor, empty continuation or malformed envelope;
  no participant wallet is upserted, and `winner-discovery` exits with temporary failure.

Enabled collection failure is `TransientSource` (see `rank_and_push_tempfail_exit` in the
glossary). The wrapper retries before recording an activation batch. Cache upsert and activation
errors remain fatal. Short 429 answers are waited out within the glossary's reconciliation retry
budget; exhaustion is `failed:trades`. The Gamma sweep keeps its page retries. Once an activation
batch is recorded, wrapper retries skip discovery; that record never exempts the candidate's
participant/coverage holds. Resolve a retained cycle's short coverage with the exact frozen scope
and floor, then activate its new participants as described in `docs/26`.

### Full-window bootstrap and coverage repair

The first publication waits for the accepted full-window read (owner, 2026-10-31 08:31 CT).
Use an independently accepted read-only sweep covering every ranking-scope market over the
whole required interval, including the print tail; market close times do not excuse any interval.
A scope change or a long gap whose old-data queries keep timing out uses the same sweep/import
repair. Import the accepted union on the retained side cache, not the installed cache:

```bash
pe-bootstrap participants-import --db <side> --wallets <accepted-union> \
  --recurrences <cycle-recurrences> --series <covered-id:slug,...> --end-date-min <cycle-floor> \
  --covered-from <unix> --covered-through <unix>
```

`--series` is required and must name the complete accepted set. For matching recurrences,
an import must include every already-covered `id:slug`; a shrinking set is refused before
any wallet or coverage write. A superset is accepted only with coverage for the whole set.
Every nonempty line must parse as a wallet; one bad line refuses the whole file before any write.
The import canonicalizes/deduplicates addresses,
logs the file SHA-256 and count, and requires `0 ≤ from < through ≤ now`. It uses the same
participant upsert and exclusion rules under `CacheMutationLock`; it does not activate wallets.

An absent, other-recurrence or legacy null-set record is replaced, and so is the record when the
import names a strict superset of its series: that interval alone must cover the required window
for the whole set. An interval for the same recurrences and the same series set must overlap or
touch the record: the merged interval keeps the earliest start, latest end and later floor. A disjoint
interval is refused unchanged, naming the missing bridge. An earlier-floor import repairs a
floor hold only when its interval contains the record's entire interval; otherwise it is refused
unchanged, naming the coverage needed. All inputs and the resulting record are validated before
either write. Upserts commit before coverage; an interrupted or repeated import is rerunnable.
Then ordinary discovery reads only the recent tail from the imported coverage.

Disabling discovery with `PE_BOOTSTRAP_PARTICIPANT_LOOKBACK_SECS=0` leaves coverage and previously
admitted wallets intact. It cannot waive coverage or admission holds; a disabled run supplies no
new proof. Every cycle's report names infra participants (skipped or excluded, #826) and, until the
parser fix ships, unrepresentable-row exclusions. Build the latter list from the archived accepted union's
infra-tombstoned members plus all later discovery skip logs kept with the reports; a resumed
cycle can skip discovery, so one invocation's log is insufficient.

## Operational notes

- **No direct auto-deploy to live.** Discovery writes only local cache and audit/spool files.
  A discovered wallet reaches the live set only after the ranking pipeline
  publishes it to Supabase `latest_ranking` and the service admits it (maintenance-tick
  backfill in knockout mode; the next batch swap in `full_rerank`) — never directly
  from discovery.
- **Idempotent.** Re-running participant discovery is safe:
  the upsert is `ON CONFLICT(wallet_hex) DO UPDATE` and only OR-merges source bits, so a
  repeated snapshot changes nothing once its bits are present.
- **Logical-cycle retries reuse activation.** A zero-argument production cycle
  writes `data/eval-results/rank_and_push.cycle` before discovery or activation.
  If a transient pre-publication stage fails, the next zero-argument retry
  reuses that run directory, its deterministic activation batch ID, and the
  already-committed cohort; it skips discovery once the activation batch is recorded
  and does not automatically admit another batch. Participant holds use separately named
  completion batches on the retained side cache. The original batch size
  (`PE_BOOTSTRAP_ACTIVATION_BATCH_WALLETS`; default
  `bootstrap_pipeline_activation_batch_wallets`; `0` admits none) must not
  change between the attempt and its retry: the committed batch reloads only
  under the count it recorded, and a changed count stops `activate-next`.
  The [classifier-6 catch-up](26-DATA-REFRESH-AND-REOPTIMIZATION-RUNBOOK.md#classifier-6-catch-up-and-coordinated-release-739)
  and first ordinary cycle are distinct batches; change the owner's count only between them.
- **Bounded inside the wrapper.** `--skip-discovery` skips both discovery and
  controlled activation. The schema-two candidate lane owns collection; legacy refresh refuses.
- **Infrastructure exclusions do not require deletion.** Ordinary publication is
  purge-free. A cold-probe flag (`is_infra = 1`, no tombstone) and a historical
  `reason='infra'` tombstone are both enforced at acquisition time: discovery skips an
  infra-tombstoned wallet, and participant rediscovery keeps live flags while lifting
  `proven_loser` tombstones; activation requires `is_infra = 0` and no tombstone, and backfill selects
  through `active_tradeable_wallets`, so an excluded wallet accrues no trades. Fresh collection
  gives every ever-infra wallet (marks plus `deploy/forge/ever-infra-wallets.txt`) an excluded
  receipt with no read, pass 1 drops it from its candidates, and the completion rules never hold
  or wait for it (#826, `docs/37` §6). Direct `purge-infra` is report-only while
  `PE_BOOTSTRAP_PURGE_ENABLED=false`. Infra marks are permanent: no command clears one
  (`clear-infra-exclusion` and the reviewed-clear list are retired).
- **`CacheMutationLock`** is a persistent-inode, kernel-held lock acquired before
  every read-write wallet-cache open. Production coordination follows the sole
  loop → one-shot run → cache order; true read-only probes use
  `WalletCache::open_read_only`.
- **Exit codes** follow the shared `pe-bootstrap` vocabulary (each command emits
  a subset): 0 = success, 1 = permanent failure, 2 = partial failure,
  75 = temporary failure (loop-supervised retry; includes failed or incomplete enabled participant
  discovery, `events` and `resolutions`); 77 = discovery spool refused before a writable open.

## Related docs

- `docs/26-DATA-REFRESH-AND-REOPTIMIZATION-RUNBOOK.md` — full data-refresh and
  re-optimisation pipeline (includes watchlist export and VPS deploy).
- `docs/_GLOSSARY.md` — `bootstrap_leaderboard_*` and `bootstrap_datadash_*`
  defaults, `SRC_LEADERBOARD` / `SRC_DATADASH` / `SRC_MARKET_PARTICIPANT` bit definitions.
- `docs/19-WINNER-FOLLOW-STRATEGY.md` — wallet selection, order eligibility, and
  risk rules. The owner's requested account mode is the only live on/off switch;
  discovery and promotion evidence do not change it.
