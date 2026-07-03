# 27 — Winner-Discovery Runbook

Automates the ingest of new candidate wallets from the Polymarket leaderboard
(plus the datadash.xyz cohorts and the Radion trader-analysis API). The
`pe-bootstrap winner-discovery` subcommand upserts each discovered wallet into the
local `wallet_cache.db` and activates the eligible ones; they then flow into the
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
PE_BOOTSTRAP_RADION_API_URL              # default https://api.radion.app (on by default)
PE_BOOTSTRAP_RADION_API_KEY              # required to activate Radion (skips silently if unset)
PE_BOOTSTRAP_RADION_MAX_REQUESTS_PER_RUN # default 8 (Free-tier budget; resumable sweep)
PE_BOOTSTRAP_DATADASH_API_URL            # default https://api.datadash.xyz (on by default; "" disables)
PE_BOOTSTRAP_CACHE_PATH                  # default wallet_cache.db
```

## Invocation

`winner-discovery` is **Step 0** of `scripts/rank_and_push.sh` (run automatically
before each rank). To run it standalone:

```bash
# Build the bootstrap binary first if not already built:
cargo build --release -p pe-bootstrap

# Discover + activate new wallets into the cache:
PE_BOOTSTRAP_CACHE_PATH=data/wallet_cache.db \
  ./target/release/pe-bootstrap winner-discovery
```

There is no separate eval/export stage or candidates JSON: `winner-discovery` fetches
the leaderboard + datadash + radion slices and upserts new wallets (recording the
source bit, activating eligible ones) straight into the cache. `rank_and_push.sh` then
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
behaviour of existing `SRC_RADION` (32) and `SRC_502_GAP` (64) bits.

## Radion source (live, issue #373)

The Radion trader-analysis API is the third discovery source, **on by default**
(`radion_api_url` defaults to `https://api.radion.app`) but **inert until a key
is set** — the API mandates an `X-API-Key` header, so the branch is silently
skipped when `radion_api_key` is unset/empty. With a key, `winner-discovery`
walks `GET /v1/polymarket/traders/analysis` (cursor-paginated, ordered by
`traderScore`, best first), upserts each `traderId` (on Polymarket the wallet
address) with `SRC_RADION`, and activates it immediately (the bit-32 gate bypass).

The sweep is **resumable** to respect the Free-tier quota (50/hr, 300/mo, max 10
wallets/request): the `nextCursor` is persisted in the `source_cursor` kv table
under the `radion_traders_analysis` key, so each run resumes deeper into the
ranking (up to `radion_max_requests_per_run`, default 8) and resets to the top
when the sweep exhausts (`nextCursor=null`). A `429`/error persists the cursor
reached and bails — it never blocks on `Retry-After` (at the monthly wall that is
seconds-until-rollover, up to days). Radion failures **soft-fail** (warn + zero
counts) so an outage or quota wall never breaks the `discover → backfill → rank`
run.

The deployment `.env` carries the full Radion block explicitly (operator
preference — all knobs explicit, even when a code default exists):

```
PE_BOOTSTRAP_RADION_API_URL=https://api.radion.app
PE_BOOTSTRAP_RADION_API_KEY=rk_…            # the live Free key
PE_BOOTSTRAP_RADION_REQUEST_INTERVAL_MS=500
PE_BOOTSTRAP_RADION_MAX_REQUESTS_PER_RUN=8  # 8×30=240/mo, under the 300/mo Free cap
```

## Operational notes

- **No direct auto-deploy to live.** Discovery writes only to the local SQLite
  cache. A discovered wallet reaches the live set only after the ranking pipeline
  publishes it to Supabase `latest_ranking` and the service admits it (maintenance-tick
  backfill in knockout mode; the next batch swap in `full_rerank`) — never directly
  from discovery.
- **Idempotent.** Re-running with the same leaderboard snapshot is safe:
  `INSERT OR IGNORE` on `wallet_hex` is a no-op for already-known wallets.
- **`CacheMutationLock`** is held only during the DB-write window inside
  `winner-discovery` and released before `backfill` starts, so no lock
  conflict with parallel `pe-bootstrap` invocations.
- **Exit codes** follow the rest of the `pe-bootstrap` convention: 0 = success,
  1 = fatal error, 2 = partial failure.

## Related docs

- `docs/26-DATA-REFRESH-AND-REOPTIMIZATION-RUNBOOK.md` — full data-refresh and
  re-optimisation pipeline (includes watchlist export and VPS deploy).
- `docs/_GLOSSARY.md` — `bootstrap_leaderboard_*` and `bootstrap_radion_*`
  defaults, `SRC_LEADERBOARD` / `SRC_RADION` bit definitions.
- `docs/19-WINNER-FOLLOW-STRATEGY.md` — promotion ladder and eligibility gates
  that govern whether a newly-activated wallet reaches live execution.
