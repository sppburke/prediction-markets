# 27 — Winner-Discovery Runbook

Automates the ingest of new candidate wallets from the Polymarket leaderboard
(plus the datadash.xyz cohorts and ranking-scope maker and taker participants). The
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

No mandatory environment variables. The `winner-discovery` subcommand does not
read `discovery_enabled` (that flag gates only the legacy `discovery` subcommand).

Optional overrides (all have safe defaults — see `_GLOSSARY.md` "Bootstrap
defaults"):

```
PE_BOOTSTRAP_LEADERBOARD_BASE_URL        # override leaderboard host (testing)
PE_BOOTSTRAP_LEADERBOARD_TOP_N           # list depth; paged (canonical default in _GLOSSARY.md); 0 disables the source
PE_BOOTSTRAP_MARKET_RECURRENCES          # participant markets' Gamma recurrences, TOML array (canonical default in _GLOSSARY.md)
PE_BOOTSTRAP_DATADASH_API_URL            # default https://api.datadash.xyz (on by default; "" disables)
PE_BOOTSTRAP_PARTICIPANT_LOOKBACK_SECS    # overlap from persisted coverage; 0 disables with no HTTP or coverage change
PE_BOOTSTRAP_CACHE_PATH                  # default wallet_cache.db
```

Each leaderboard slice is read to the configured list depth in fixed-size pages; see
[`bootstrap_leaderboard_top_n`](_GLOSSARY.md#bootstrap-defaults-pe-bootstrap) for depth, page size
and maximum offset. A short page ends the list; a failed page preserves earlier pages and moves
to the next list. Dedupe and source-bit upserts remain unchanged.

## Invocation

`winner-discovery` is **Step 0** of `scripts/rank_and_push.sh` (run automatically
before each rank). To run it standalone:

```bash
# Build the bootstrap binary first if not already built:
cargo build --release -p pe-bootstrap

# Standalone discovery preserves legacy immediate activation:
PE_BOOTSTRAP_CACHE_PATH=data/wallet_cache.db \
  ./target/release/pe-bootstrap winner-discovery
```

Do not add `--defer-activation` to an ad-hoc command unless the same operation
also runs `activate-next` with a stable batch ID. `rank_and_push.sh` owns that
pairing: both discovery and backfill defer the global rule, and one
`activate-next` transaction selects at most
`bootstrap_pipeline_activation_batch_wallets`, records the exact batch in
SQLite, and exports `activated_wallets.csv` into the run directory. Empty or
partially depleted candidate piles warn. In the private-candidate lane, any participant still
inactive, flagged or tombstoned holds before collection; the batch cap cannot silently omit it.

The [one-time classifier-6 catch-up](26-DATA-REFRESH-AND-REOPTIMIZATION-RUNBOOK.md#classifier-6-catch-up-and-coordinated-release-739)
records an activation batch of zero. Before the first ordinary cycle, the owner selects the
ordinary batch size; a recorded cycle keeps that value through retries.

There is no separate eval/export stage or candidates JSON: `winner-discovery` fetches
the leaderboard, datadash and market participant slices and upserts new wallets (recording the
source bit) straight into the cache. In standalone mode it activates eligible
ones immediately. `rank_and_push.sh` instead admits only the controlled batch, then
backfills their trade history and ranks them alongside the rest of the universe — the
ranker's own eligibility filters decide which discovered wallets make the published
cohort.

## Leaderboard slices

Eight API slices are always fetched per category — `{profit, volume}` ×
`{day, week, monthly, allTime}` (`SORT_WINDOW_SLICES` in
`crates/bootstrap/src/leaderboard_discovery.rs`):

| Sort | Window |
|---|---|
| `profit` | `day` |
| `profit` | `week` |
| `profit` | `monthly` |
| `profit` | `allTime` |
| `volume` | `day` |
| `volume` | `week` |
| `volume` | `monthly` |
| `volume` | `allTime` |

All slices are deduplicated on `proxyWallet` before upsert.  A wallet already
in the pile keeps its row (`ON CONFLICT(wallet_hex) DO UPDATE`); only its
`source_bits` column is OR-merged to record the new source, so a repeated
snapshot changes nothing once its bit is present.

The `SRC_LEADERBOARD` bit (16) bypasses the 100-trade activation gate in
`pe_bootstrap::pile::apply_activation_rules`, matching the curation-list
behaviour of the `SRC_GAP502` (64), `SRC_DATADASH` (128) and
`SRC_MARKET_PARTICIPANT` (256) bits.

> **Retired source — Radion.** A third source, the Radion `traders/analysis`
> trader-ranking API (issue #373), was removed once Radion deprecated that
> endpoint upstream (the REST API is now market-data only, no ranked-trader
> route). Its `radion_*` config knobs, `PE_BOOTSTRAP_RADION_*` env keys, and the
> `SRC_RADION` (bit 32) source bit were retired; bit 32 is now a reserved gap in
> `source_bits` (not reused), and existing radion-tagged rows stay inert. Discovery
> now runs leaderboard, datadash and market participant sources.

## Market participants (#800, #821)

The third source reads public Data API `/trades?takerOnly=false` for both maker and taker wallets.
The wrapper passes the same `--recurrences`, optional `--series id:slug,...` pin and `--end-date-min`
market floor to discovery as to ranking's `market-scope`: every in-scope market is read over the
whole discovery interval, regardless of its end date or open status. Trades after a market's end,
and an old market trading between runs then closing, remain discoverable. Future markets can be
listed; an empty trades reply adds no wallet. Standalone discovery without `--end-date-min` uses
the read's start as its floor. Bounds and request spacing are canonical in
[`participant_lookback_secs`](_GLOSSARY.md#bootstrap-defaults-pe-bootstrap).

Valid `proxyWallet` addresses are canonicalized to lowercase and deduplicated before tagging with
`SRC_MARKET_PARTICIPANT`. A row without a valid wallet makes the chunk incomplete; its page hash
is logged, and no participant from that run is upserted. Preferred activation eligibility is the
same as datadash, including already-known inactive wallets. Under the owner's 2026-10-31 decision,
participant upserts lift `proven_loser` tombstones and clear inherited live infra flags, while
`infra` tombstones and the four exchange contracts stay blocked (`docs/37` §6–7). Each skipped
infra-tombstoned participant is logged by wallet and counted. Normal history acquisition,
eligibility, ranking and sizing still apply.

Completed coverage lives in the dedicated `meta` key
[`participant_discovery_coverage`](_GLOSSARY.md#bootstrap-defaults-pe-bootstrap), including on
schema-two caches without `source_cursor`. It records normalized recurrences and the optional
series pin, the market floor, and one half-open interval. With matching scope a run starts at
`covered_through − participant_lookback_secs`; without a record or with another scope it starts at
`now − participant_lookback_secs`. Thus stopped loops retain the gap to read. Only an absent key
means no record; query, parse and validation errors stop discovery. Same-scope success extends
coverage and keeps the later market floor; another scope replaces it. Under the cache mutation
lock, participant upserts commit before coverage, so interruption between commits repeats an
idempotent upsert without advancing coverage past an uncommitted wallet. Disabled or failed
collection leaves coverage unchanged.

Each run logs one summary `record` under `winner_discovery: participants`: source id
`polymarket-data-api-trades`, schema/parser versions, frozen window, wallet count, status, the
Gamma scope's own source ID, version, schema/parser versions, digest, count and series, and page
counts. Every page of a completed Gamma sweep and every `/trades` page fetched then gets its own
`page` line under `winner_discovery: participant page` (URL, raw sha256, rows, and observed,
received and source times). The Gamma sweep is all-or-nothing, like `market-scope`: failure logs
`failed:gamma`, records no Gamma pages and writes no participant wallet. Each line stays under
journald's `LineMax`; a trades URL lists its condition ids and the scope digest covers the list.
Statuses:

- `ok`: collection completed; counts are included in both CLI discovery summaries.
- `disabled`: lookback is zero; no Gamma or trades HTTP or coverage change.
- `failed:<reason>`: collection failed (for example `gamma`, `trades` or `trades-json`);
  no participant wallet is upserted, and `winner-discovery` exits with temporary failure.
- `incomplete`: saturation at the smallest window; `incomplete:proxy-wallet`: an invalid/missing wallet;
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
  --recurrences <cycle-recurrences> --series <cycle-pin> --end-date-min <cycle-floor> \
  --covered-from <unix> --covered-through <unix>
```

Omit `--series` only for an unpinned scope. Every nonempty line must parse as a wallet; one bad
line refuses the whole file before any write. The import canonicalizes/deduplicates addresses,
logs the file SHA-256 and count, and requires `0 ≤ from < through ≤ now`. It uses the same
participant upsert and exclusion rules under `CacheMutationLock`; it does not activate wallets.

An absent or other-scope record is replaced. A same-scope interval must overlap or touch the
record: the merged interval keeps the earliest start, latest end and later floor. A disjoint
interval is refused unchanged, naming the missing bridge. An earlier-floor import repairs a
floor hold only when its interval contains the record's entire interval; otherwise it is refused
unchanged, naming the coverage needed. All inputs and the resulting record are validated before
either write. Upserts commit before coverage; an interrupted or repeated import is rerunnable.
Then ordinary discovery reads only the recent tail from the imported coverage.

Disabling discovery with `PE_BOOTSTRAP_PARTICIPANT_LOOKBACK_SECS=0` leaves coverage and previously
admitted wallets intact. It cannot waive coverage or admission holds; a disabled run supplies no
new proof. Every cycle's report names unrepresentable-row exclusions and kept `infra` participants
until their separate fixes ship. Build the latter list from the archived accepted union's
infra-tombstoned members plus all later discovery skip logs kept with the reports; a resumed
cycle can skip discovery, so one invocation's log is insufficient.

## Operational notes

- **No direct auto-deploy to live.** Discovery writes only to the local SQLite
  cache. A discovered wallet reaches the live set only after the ranking pipeline
  publishes it to Supabase `latest_ranking` and the service admits it (maintenance-tick
  backfill in knockout mode; the next batch swap in `full_rerank`) — never directly
  from discovery.
- **Idempotent.** Re-running with the same leaderboard snapshot is safe:
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
  controlled activation. Backfill launched by the wrapper always defers global
  activation, so no wrapper override can silently activate an unbounded cohort.
- **Infrastructure exclusions do not require deletion.** Ordinary publication is
  purge-free. A cold-probe flag (`is_infra = 1`, no tombstone) and a historical
  `reason='infra'` tombstone are both enforced at acquisition time: discovery skips an
  infra-tombstoned wallet, while participant rediscovery clears inherited live flags and lifts
  `proven_loser` tombstones; activation requires `is_infra = 0` and no tombstone, and backfill selects
  through `active_tradeable_wallets`, so an excluded wallet accrues no trades.
  Ranking/export/publisher filters do not consult either (`docs/37`); the private-candidate
  wrapper separately holds on participant flags/tombstones. Direct `purge-infra` is
  report-only while `PE_BOOTSTRAP_PURGE_ENABLED=false`. Outside the participant exception,
  reclassification requires
  the explicit operator-only `clear-infra-exclusion --wallet <hex> --confirm` command; it
  removes the infra tombstone and clears a live flag, and does not recreate or activate the
  wallet. With `--purged-at <unix>` it removes only the tombstone stamped at that second and no
  flag; the wrapper runs that form for each line of `deploy/forge/reviewed-infra-clears.txt`
  on each fresh cycle's private copy (`docs/37` §7). A cleared active wallet with no fetch stamp (the cold-probe shape) becomes due for
  backfill at once and is probed again on its newest page; a wallet flagged after ingestion
  resumes incremental backfill once its stamp is stale, without a new probe.
- **`CacheMutationLock`** is a persistent-inode, kernel-held lock acquired before
  every read-write wallet-cache open. Production coordination follows the sole
  loop → one-shot run → cache order; true read-only probes use
  `WalletCache::open_read_only`.
- **Exit codes** follow the shared `pe-bootstrap` vocabulary (each command emits
  a subset): 0 = success, 1 = permanent failure, 2 = partial failure,
  75 = temporary failure (loop-supervised retry; includes failed or incomplete enabled participant
  discovery, `events` and `resolutions`).

## Related docs

- `docs/26-DATA-REFRESH-AND-REOPTIMIZATION-RUNBOOK.md` — full data-refresh and
  re-optimisation pipeline (includes watchlist export and VPS deploy).
- `docs/_GLOSSARY.md` — `bootstrap_leaderboard_*` and `bootstrap_datadash_*`
  defaults, `SRC_LEADERBOARD` / `SRC_DATADASH` / `SRC_MARKET_PARTICIPANT` bit definitions.
- `docs/19-WINNER-FOLLOW-STRATEGY.md` — wallet selection, order eligibility, and
  risk rules. The owner's requested account mode is the only live on/off switch;
  discovery and promotion evidence do not change it.
