# 27 — Winner-Discovery Runbook

Automates the ingest of new candidate wallets from the Polymarket leaderboard
(plus the datadash.xyz cohorts). The
`pe-bootstrap winner-discovery` subcommand upserts each discovered wallet into the
local `wallet_cache.db`. A standalone invocation immediately applies the legacy
activation rules; the full rank-and-push wrapper explicitly defers that behavior
and admits only its one transactionally audited controlled batch. Wallets then flow into the
ranking pipeline (Step 0 of `scripts/rank_and_push.sh`) and reach the live set only
via Supabase `latest_ranking` — admitted by the maintenance tick (knockout mode) or
the next batch swap (`full_rerank`, the cutover production mode) — never directly. This is the
only sanctioned path for adding new wallets; see `AGENTS.md` "Do not add wallets" rule.

## Required environment

No mandatory environment variables. The `winner-discovery` subcommand does not
read `discovery_enabled` (that flag gates only the legacy `discovery` subcommand).

Optional overrides (all have safe defaults — see `_GLOSSARY.md` "Bootstrap
defaults"):

```
PE_BOOTSTRAP_LEADERBOARD_BASE_URL        # override leaderboard host (testing)
PE_BOOTSTRAP_LEADERBOARD_TOP_N           # default 50 (API hard-caps at 50)
PE_BOOTSTRAP_DATADASH_API_URL            # default https://api.datadash.xyz (on by default; "" disables)
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
  ./target/release/pe-bootstrap winner-discovery
```

Do not add `--defer-activation` to an ad-hoc command unless the same operation
also runs `activate-next` with a stable batch ID. `rank_and_push.sh` owns that
pairing: both discovery and backfill defer the global rule, and one
`activate-next` transaction selects at most
`bootstrap_pipeline_activation_batch_wallets`, records the exact batch in
SQLite, and exports `activated_wallets.csv` into the run directory. Empty or
partially depleted candidate piles warn without failing the cycle.

There is no separate eval/export stage or candidates JSON: `winner-discovery` fetches
the leaderboard + datadash slices and upserts new wallets (recording the
source bit) straight into the cache. In standalone mode it activates eligible
ones immediately. `rank_and_push.sh` instead admits only the controlled batch, then
backfills their trade history and ranks them alongside the rest of the universe — the
ranker's own eligibility filters decide which discovered wallets make the published
cohort.

## Leaderboard slices

Four API slices are always fetched:

| Sort | Window |
|---|---|
| `profit` | `monthly` |
| `profit` | `allTime` |
| `volume` | `monthly` |
| `volume` | `allTime` |

All slices are deduplicated on `proxyWallet` before upsert.  A wallet already
in the pile is a no-op (idempotent `INSERT OR IGNORE` on `wallet_hex`); only
its `source_bits` column is OR-merged to record the new source.

The `SRC_LEADERBOARD` bit (16) bypasses the 100-trade activation gate in
`pe_bootstrap::pile::apply_activation_rules`, matching the curation-list
behaviour of the `SRC_502_GAP` (64) and `SRC_DATADASH` (128) bits.

> **Retired source — Radion.** A third source, the Radion `traders/analysis`
> trader-ranking API (issue #373), was removed once Radion deprecated that
> endpoint upstream (the REST API is now market-data only, no ranked-trader
> route). Its `radion_*` config knobs, `PE_BOOTSTRAP_RADION_*` env keys, and the
> `SRC_RADION` (bit 32) source bit were retired; bit 32 is now a reserved gap in
> `source_bits` (not reused), and existing radion-tagged rows stay inert. Discovery
> now runs the leaderboard + datadash sources only.

## Operational notes

- **No direct auto-deploy to live.** Discovery writes only to the local SQLite
  cache. A discovered wallet reaches the live set only after the ranking pipeline
  publishes it to Supabase `latest_ranking` and the service admits it (maintenance-tick
  backfill in knockout mode; the next batch swap in `full_rerank`) — never directly
  from discovery.
- **Idempotent.** Re-running with the same leaderboard snapshot is safe:
  `INSERT OR IGNORE` on `wallet_hex` is a no-op for already-known wallets.
- **Bounded inside the wrapper.** `--skip-discovery` skips both discovery and
  controlled activation. Backfill launched by the wrapper always defers global
  activation, so no wrapper override can silently activate an unbounded cohort.
- **Infrastructure exclusions survive deletion.** `purge-infra` archives and
  deletes live infra wallet data, then ordinary discovery refuses to lift its
  durable `reason='infra'` tombstone. Exceptional reclassification requires the
  explicit operator-only `clear-infra-exclusion --wallet <hex> --confirm` command;
  it clears only that exclusion and does not recreate or activate the wallet.
- **`CacheMutationLock`** is held only during the DB-write window inside
  `winner-discovery` and released before `backfill` starts, so no lock
  conflict with parallel `pe-bootstrap` invocations.
- **Exit codes** follow the rest of the `pe-bootstrap` convention: 0 = success,
  1 = fatal error, 2 = partial failure.

## Related docs

- `docs/26-DATA-REFRESH-AND-REOPTIMIZATION-RUNBOOK.md` — full data-refresh and
  re-optimisation pipeline (includes watchlist export and VPS deploy).
- `docs/_GLOSSARY.md` — `bootstrap_leaderboard_*` and `bootstrap_datadash_*`
  defaults, `SRC_LEADERBOARD` / `SRC_DATADASH` bit definitions.
- `docs/19-WINNER-FOLLOW-STRATEGY.md` — promotion ladder and eligibility gates
  that govern whether a newly-activated wallet reaches live execution.
