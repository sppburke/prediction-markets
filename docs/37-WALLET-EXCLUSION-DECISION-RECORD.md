# 37 — Wallet exclusion decision record: 24 breadth-based tombstones and the exclusion rules

**Status: BINDING DECISION RECORD (issue #589).** Numeric defaults live in
[`_GLOSSARY.md`](_GLOSSARY.md) (`infra_probe_span_secs`, `bootstrap_purge_*`,
`tombstone_override_sources`). This document records the wallet-by-wallet review of the 24
Polymarket wallets whose `infra` tombstones trace to the May 15, 2026 Dune CSV label
`breadth>=2000`, the producer/consumer map of exclusion state, the exclusion-policy decisions, and
the operator handoff. It authorizes no clearing, activation, ranking, deployment, or trading by
itself; every state change named here is a separately authorized operator action. The one code
change shipped with it (`clear-infra-exclusion` clearing a live flag, §7) is the consolidated
scope extension agreed for the #589 dev cycle; the issue's other production-mutation non-goals
stand.

Repository revision reviewed: `ab120d9997f216a27dfdb0199739df695a65c9ac`. Ranker-host
observations were taken read-only on 2026-09-10 between 21:11 and 21:52 UTC (schema one,
`PRAGMA user_version = 1`); the host checkout reported `8ea29a9` (`git rev-parse HEAD`, observed
21:05 UTC); the revision of the running binary was not verified. Committed evidence (scripts and
derived summaries) is in
[`data/archive/research-2026-09-exclusion-review/`](../data/archive/research-2026-09-exclusion-review/README.md),
referred to below as `evidence/`; raw captures are retained privately (§8).

## 1. Decisions

| Decision | Outcome |
|---|---|
| Cohort verdicts | 2 × **retain with evidence**, 18 × **recommend reclassification for requalification**, 4 × **unresolved** (never ingested). All are qualified review recommendations (§2); none certifies infrastructure identity, and none admits a wallet anywhere. |
| Follower eligibility | 24 × **not established**. Missing inputs: current ingested history (none of the 24 has a trade row in the live cache), the ranker's follower-price references and fee model, and current resolver evidence. Eligibility is decided only by the unchanged ranking, publication, and service-admission gates after a wallet is back in research. |
| Breadth label (`breadth>=2000`) | **Retired as a producer** (the Dune CSV importer was removed in #335). Legacy labels are **reviewable per wallet**; **no bulk clearing** (§5, §6). |
| Density probe (newest cold page) | **Retain** as a bounded-workload guard. A probe flag is a workload/copyability exclusion, not an identity verdict, and is reversible. |
| Retroactive sweep (`classify-infra`, oldest 500 trades) | **Retain as-is, operator-only.** Do not extend it to an any-window rule (§5, §6). |
| Verified system addresses | **Retain permanently** (the four exchange contracts; all four are `infra` tombstones). |
| Proven-loser rule | **Retain**; requalification stays evidence-dependent (leaderboard rediscovery lifts the tombstone; a rerank decides). No cooldown number is introduced. |
| Reversal path | `clear-infra-exclusion` now removes the infra tombstone **and** a live `wallets.is_infra` flag in one transaction. It still does not recreate or activate a wallet. |
| Rank/export filtering promise | The documented promise was **not implemented** on any path; the canonical docs now describe the acquisition-gating mechanism. The cross-path filter is deferred to #592. Measured on the ranker host: the last two completed cycles' pass-1 and pass-2 ranking outputs contain zero flagged and zero tombstoned wallets, no pending publication request exists, and every flagged wallet checked holds zero trade rows (§5). |

## 2. Cohort verdicts

Addresses are the identity; profile names are display hints from the retained review CSV.
"Archived trades" are the rows the July 20, 2026 archive-before-delete kept for the wallet
(`wallet_cache.purge-archive.db`); those rows are legacy history ending 2026-05-17 to 2026-05-19
(the exact ingestion path and time of those rows is unverified), so they are **not** a complete
history to the purge or to today. "Min 500-trade window" is the smallest `t[i+499] − t[i]` over every consecutive
500-trade window of the archived rows, with the count of windows under `infra_probe_span_secs`
in parentheses. Page hours are the retained September 10 spans of the official
500-trade activity pages (bounded by the tombstone timestamp, and current). "Sampled N / t-proxy"
and "both-outcome share" come from the retained Dune first-BUY screen
(`evidence/tombstone-audit-summary.json`); they describe sampled qualifying conditions, not
production qualification. Full derived rows: `evidence/cohort_summary.csv`.

| Wallet | Profile | Archived trades | Archived span (UTC) | Min 500-trade window, s (dense windows) | At-purge / current page, h | Sampled N / t-proxy | Both-outcome share | Exclusion verdict |
|---|---|---:|---|---:|---:|---:|---:|---|
| `0x1b912b17581b3d544431d13d5a060155a80127dc` | Diceman11 | 1762 | 2023-03-07→2026-05-18 | 1548416 (0) | 1184.82 / 293.46 | 53 / 5.92 | 0.00 | **recommend reclassification for requalification** |
| `0xfd8e46519d0a8f9c35e5010ef4e7f56f7583aea4` | oleksiyz | 31990 | 2026-01-27→2026-05-18 | 40068 (0) | 125.63 / 46.20 | 885 / 5.58 | 0.00 | **recommend reclassification for requalification** |
| `0x72e1597864456eda62878413cf3e60c332e4a45d` | ArbTraderRookie | 5550 | 2025-11-11→2026-05-18 | 242860 (0) | 52.37 / 56.11 | 34 / 4.64 | 0.21 | **recommend reclassification for requalification** |
| `0x0bf96a1c55e6f47ea84335bc3fc08a89653efd90` | alboto | 25603 | 2026-02-17→2026-05-18 | 18316 (0) | 25.92 / 18.75 | 27 / 4.18 | 0.00 | **recommend reclassification for requalification** |
| `0x5a3354a0a35d00d2a1dbe6cea0e37e9c30a2ca0d` | — | 3148 | 2026-01-22→2026-05-18 | 5218 (0) | 2383.66 / 2127.28 | 176 / 3.09 | 0.00 | **recommend reclassification for requalification** |
| `0xdf6539d1fadb951a02a999d31a72e0cd7fd9c36d` | nndk | 0 | — | — | 1.14 / 10.63 | 22 / 3.05 | 0.86 | **unresolved** (never ingested) |
| `0xb7ab821f037a4c8deb9b23b8001a4be985d116d0` | funfacts | 5943 | 2026-04-14→2026-05-18 | 127663 (0) | 20.57 / 23.90 | 258 / 2.79 | 0.25 | **recommend reclassification for requalification** |
| `0x057621bea5fc03a53af38530c95b17a510716232` | gnuisnot | 2711 | 2026-01-23→2026-05-19 | 595142 (0) | 272.15 / 861.46 | 30 / 2.79 | 0.00 | **recommend reclassification for requalification** |
| `0xc561b14904d769eda31628082c005362ba60dc22` | — | 0 | — | — | 2641.49 / 3837.68 | 22 / 2.60 | 0.00 | **unresolved** (never ingested) |
| `0x9fbfe50bf171adaa347a5cb2b789b4a6e12ef003` | prob1 | 35023 | 2025-10-13→2026-05-18 | 39110 (0) | 29.13 / 16.01 | 464 / 2.54 | 0.00 | **recommend reclassification for requalification** |
| `0x31646b754f77b973910e7376b7015cb81fe83c65` | Mr.Z. | 4597 | 2024-07-14→2026-05-18 | 478425 (0) | 104.46 / 232.86 | 214 / 2.43 | 0.00 | **recommend reclassification for requalification** |
| `0x38d812aff0b79f3bf5da2a477f780bcc163eea7c` | thanksforplayin | 0 | — | — | 49.10 / 22.93 | 141 / 2.41 | 0.30 | **unresolved** (never ingested) |
| `0x9c76cdb43fb46454da005fbc82047a64a18ec926` | Bagwell306 | 23022 | 2025-12-18→2026-05-18 | 5034 (0) | 87.87 / 225.24 | 92 / 2.40 | 0.01 | **recommend reclassification for requalification** |
| `0xdc5bd11896bfb0fb335dad88d99a7e9a6bc3f102` | ztao | 3035 | 2025-04-18→2026-05-19 | 3423853 (0) | 934.60 / 345.23 | 20 / 2.36 | 0.00 | **recommend reclassification for requalification** |
| `0x1f19c48aee80ec95396d91f0d21ac249b8a7f57a` | godspeed11 | 113883 | 2026-03-30→2026-05-18 | 696 (30127) | 1.27 / 13.98 | 29 / 2.32 | 0.93 | **retain with evidence** |
| `0x06dc51826bc524d9a83770e7de9dd7e005b04524` | — | 69142 | 2026-03-24→2026-05-18 | 16 (1565) | 4.60 / 5.70 | 396 / 2.31 | 0.89 | **retain with evidence** |
| `0xdfe29d6ef2a44606cf58967e1fd5854d9e594902` | — | 31827 | 2026-02-01→2026-05-17 | 7786 (0) | 48.29 / 16.87 | 23 / 2.22 | 0.04 | **recommend reclassification for requalification** |
| `0xaf17116ae2b1476032785a67bd5b7c8c05905c20` | huskyvs | 5581 | 2025-11-05→2026-05-18 | 372274 (0) | 118.61 / 57.86 | 114 / 2.19 | 0.04 | **recommend reclassification for requalification** |
| `0xc33d6aa3eb972639f31e46a6a02201cece380d40` | penguinec | 13835 | 2026-01-06→2026-05-18 | 52937 (0) | 68.25 / 121.47 | 63 / 2.13 | 0.68 | **recommend reclassification for requalification** |
| `0xa8ae2fb989de545c42c376d61ec887868ca57f3e` | ExBeZuZep | 6956 | 2025-07-15→2026-05-18 | 185645 (0) | 1078.79 / 311.93 | 36 / 2.11 | 0.00 | **recommend reclassification for requalification** |
| `0x4e9e342ff236323b43f79c0da642a82bd12f0c30` | — | 6542 | 2026-02-25→2026-05-18 | 239424 (0) | 103.00 / 172.17 | 20 / 2.10 | 0.00 | **recommend reclassification for requalification** |
| `0xdb5ad26b68d77ae966d29e7180147272ab7a3965` | — | 0 | — | — | 26.29 / 20.37 | 195 / 2.10 | 0.84 | **unresolved** (never ingested) |
| `0x8f1dfd0868d056f11f84e0233e1b89527c262fb6` | wapol-E | 22290 | 2026-03-31→2026-05-18 | 23046 (0) | 58.29 / 365.68 | 22 / 2.06 | 0.05 | **recommend reclassification for requalification** |
| `0xc7d02944a76b9f83b199e9090ecc92c82d241f8a` | — | 7424 | 2026-04-23→2026-05-18 | 45850 (0) | 2.47 / 32.41 | 224 / 2.02 | 0.40 | **recommend reclassification for requalification** |

Verdict rules applied:

- **Retain with evidence.** The canonical density rule (500 trades within
  `infra_probe_span_secs`) fires on archived windows. godspeed11 has 30,127 such windows (minimum 696 s);
  `0x06dc…4524` has 1,565 (minimum 16 s). Both buy both outcomes on more than 89% of sampled
  conditions. The acquisition exclusion is retained as a workload/copyability exclusion under the
  canonical rule pending any future review; this record asserts no "infrastructure identity".
  Neither the cold probe (newest page: 13.98 h and 5.70 h) nor the retroactive sweep (oldest 500:
  6.31 h and 139.68 h) would flag either today, which is a sensitivity limit of both producers (§5).
- **Recommend reclassification for requalification.** The only trigger is the breadth label;
  every archived window (1,762 to 35,023 trades per wallet, spans shown above) is above the
  threshold; every sampled page exceeds one hour. Activity after each wallet's archived span is
  known only from sampled pages. Requalification returns the wallet to research only; it confers
  nothing else.
- **Unresolved.** The four never-fetched wallets (`nndk`, `0xc561…dc22`, `thanksforplayin`,
  `0xdb5a…3965`) have no repository history (archive `trades_archived = 0`,
  `last_polymarket_fetch_at IS NULL`). Fifteen sampled pages each (two retained plus 13 recaptured
  monthly anchors, `evidence/recapture-summary.json`; anchors before a wallet's first activity
  return an empty list) never fire (minimum 1.138 h, nndk's tombstone-bounded page; recapture
  minimum 1.233 h, nndk at the 2026-08-11 anchor). Sampled pages
  cannot clear a wallet: on this cohort they missed both retained wallets' dense intervals. Exact
  next check: bounded re-ingestion under the cold probe followed by the window test in
  `evidence/forge_ro3.py`. nndk's 260,473 CSV trades with first public activity in March 2026 and
  an 0.86 both-outcome share are consistent with, but do not prove, an arbitrage pattern.

Original-rule compliance: all 24 satisfied the importer's rule as written (a row in the
filename-classified infra CSV ⇒ `is_infra = 1`; the `reason` column was never read; commit
`8d2d0c6` `crates/bootstrap/src/migrate.rs`). Present justification is the verdict column.

Non-cohort matches from the same screen (context only, no verdict change): Odaily-Draven
(`0x330f…91b1`, t-stat −2.42613, effective N 376.23, June 19 ranking) and haveseendate
(`0x5215…b02f`, −2.18071, 1,489.64, August 21 ranking) satisfy the proven-loser rule
(`bootstrap_purge_loser_tstat_max`, `bootstrap_purge_loser_neff_min`) on retained rows; they stay
excluded until a full current rerank says otherwise. `0x88ec…ec61` (archive row `is_infra = 1`,
`is_active = 1`, zero trades, never fetched, tombstoned 2026-08-20; pages 1.77 h and 1.48 h) has no
recoverable trigger evidence and stays out.

## 3. Provenance and lineage (class level)

- The original CSV (`data/dune_csvs/dune-infra-wallets-2026-05-15.csv.imported`, SHA-256
  `843f8065…f6e4186`, an untracked local artifact) has 14,422 rows: 14,418 `breadth>=2000` and
  four `known:*EXCHANGE*` contracts. Exact address intersection with the live tombstone table
  (`evidence/forge_ro6.py`, 21:48 UTC): all 14,422 are `infra` tombstones, none carries another
  reason, and 478 `infra` tombstones are outside the CSV (`evidence/aggregates.json`).
- Archive `infra` manifest rows by exact CSV membership: CSV members 14,422 — 8,115 with archived
  trades (101,986,315 rows) and 6,307 with none; non-members 478 — 136 with archived trades
  (3,478,401 rows) and 342 with none. The importer's infra branch set `is_infra` and no source
  bit, so `source_bits` does not identify CSV membership; only the address intersection does.
- All 24 cohort archive rows carry `is_infra = 1`, `is_active = 0`, `discovered_at_unix =
  1778877276` (2026-05-15, the import). They were never activated (activation requires
  `is_infra = 0`), so pipeline activation and backfill never fetched them; 20 nevertheless hold
  archived rows dated up to 2026-05-19 (ingestion path unverified), four hold none.
- Archive `purge_manifest` says `purged_at_unix = 1784530028` (2026-07-20) for all 24; the live
  tombstone says `1784747039` (2026-07-22) for 16 and 2026-07-20 for eight. Tracked code at
  `76db535` (the July revision) cannot produce that split from a same-archive rerun (`run_infra_purge`
  selects `is_infra = 1` rows, which no longer existed; the manifest row is replaced on
  re-archive). **UNKNOWN:** the executing binary, arguments, and archive path of the July 20 and
  July 22 runs. This affects no verdict: both timestamps post-date the import and pre-date any
  rediscovery, and the archived rows carry the import-time state.
- Live `wallets` rows with `is_infra = 1`: 53 at 21:11 UTC (all 53 without a fetch stamp and with
  `trade_count = 0` at 21:16), 54 at 21:31 UTC (all 54 checked: zero trade rows,
  `evidence/forge_ro5.py`), 56 at 21:52 UTC (count only; the two newest were not rechecked). The
  running loop wrote the new cold-probe flags during the review. Zero tombstoned wallets have a
  `wallets` row.

## 4. Producer/consumer map of exclusion state (revision `ab120d9`)

`W` = `wallets` (`is_infra`, `is_active`, `source_bits`), `P` = `purged_wallets`, `V` =
`active_tradeable_wallets` (`is_active = 1 AND is_infra = 0`). "Wrapper" = reachable from a
zero-argument `scripts/rank_and_push.sh` cycle; "operator" = explicit command only.

| Stage | Owner | Reads / writes | Reach | Disposition |
|---|---|---|---|---|
| Discovery upsert | `cache.rs:3295-3345` `upsert_wallets_bulk` | Reads P; skips tombstoned wallets unless a `tombstone_override_sources` bit lifts a non-`infra` tombstone (`DELETE FROM purged_wallets`); inserts `is_infra` as supplied by the source (`false` for leaderboard/datadash); on conflict `MAX(is_infra, …)` (sticky). | wrapper (`rank_and_push.sh:615`) | Matches docs. |
| Cold probe | `infra_probe.rs:66-92`, `polymarket.rs:288-308` | Conclusive when the raw first page has ≥500 rows; fires when the parsed span is `< threshold`; writes `is_infra = 1` (no P row), discards the page, no fetch stamp; logs `span_secs` at `info`. | wrapper (`:624` backfill) | Matches docs after correction. The request does not pin `sortDirection`; "newest page" is the intended, not proven, window. |
| Retroactive sweep | `cache.rs:3963-4013` `classify_infra_retroactive` | Oldest 500 trades per wallet (`ORDER BY timestamp_unix ASC`, `rn <= 500`, `cnt = 500`); writes `is_infra = 1` in bulk; keeps trades. | operator (`classify-infra`) | Retain as-is (§6). |
| Flag write | `cache.rs:3927-3945` `mark_infra`, `mark_infra_bulk` | `UPDATE wallets SET is_infra = 1`; sticky; leaves `is_active` untouched (hence `is_infra = 1 AND is_active = 1` rows). | via probe/sweep | Reversal added (this record). |
| Purge, rules A/B | `purge.rs:75-170`, `cache.rs:3436-3560` | Archive-before-delete (mirror rows `INSERT OR IGNORE`, `cache.rs:3646`); tombstone `INSERT OR REPLACE` (`:3537`); report-only unless `PE_BOOTSTRAP_PURGE_ENABLED=true` (`config.rs:650`, `purge.rs:87`). | operator | Matches docs. |
| Infra purge | `purge.rs:173-235` | Selects `is_infra = 1` rows regardless of `is_active`; same arming and archive rules. | operator | Matches docs. |
| Clearing | `cache.rs:3367` `clear_infra_exclusion`; `main.rs:1001-1035` | Before this record: `DELETE FROM purged_wallets WHERE … reason = 'infra'` only. Now also `UPDATE wallets SET is_infra = 0` in the same transaction. Never recreates or activates. Locked (`CacheMutationLock`). | operator (`--confirm`) | Fixed here. |
| Activation | `cache.rs:4060-4072` legacy, `:4080-4200` `activate_next_batch` | Direct `wallets` query: `is_active = 0 AND is_infra = 0 AND NOT EXISTS (P)`; batch cap `bootstrap_pipeline_activation_batch_wallets`; durable audit tables. | wrapper (`:616`) | Matches docs. |
| Backfill selection | `cache.rs:4030` `select_backfill_due` | `FROM active_tradeable_wallets … ORDER BY last_polymarket_fetch_at ASC NULLS FIRST`. | wrapper (`:624`) | Matches docs. The effective exclusion for every flagged wallet lives here. |
| Coverage | `cache.rs:2517` | Reads V. | wrapper | Matches docs. |
| Parquet export | `scripts/export_trades_parquet.py:34,77` | `COPY (SELECT * FROM src.trades)`; `wallets` is not exported. | wrapper (Step 0a) | **Does not filter.** Docs corrected; code deferred to #592. |
| Pass-1 universe | `scripts/rank_72hr_buyandhold.py:199-222` | `--universe-from-trades`: `SELECT DISTINCT wallet_hex FROM trades` (schema one) or the `ranker_entries_v2` join (schema two); `--universe <file>`: any address. | wrapper (`:540`) | **Does not filter.** Same disposition. |
| Pass-2 rerank | `scripts/latency_shift_rerank.py` | Consumes pass-1 output; no wallet-flag check. | wrapper | Same disposition. |
| Publication | `scripts/push_ranking_to_supabase.py:120-135,146-215` | Trade-recency map from `trades`/`activity_groups_v2`; global staleness abort; keeps rows by recency only. Pending-request replay (`:463`, `:859`) re-submits saved entries unchanged. | wrapper | **Does not filter.** Same disposition. |
| Schema-two frozen cohort / projection | `crates/bootstrap/src/cache_migration.rs:1321,1508-1568` | Recency-derived cohort; projection checks entry evidence, not `is_infra`/P. | wrapper (`CUTOVER_MODE=1`) | Same disposition. |
| Service admission | `crates/service/src/watchlist_maintenance.rs:179`, `watchlist_admission.rs:335`, `supabase_reader.rs:130` | Published survivors plus service-owned fences, history, and position validation. Bootstrap exclusion state is not consulted (by design: a separate fence set). | service | Matches architecture; the docs now assign wallet fences here. |
| Tests | `crates/bootstrap/tests/scenario_purge.rs:527-535`, `scenario_mutator_cli.rs:326`, `scenario_infra_probe.rs`, `pile.rs:438`; `scripts/test_rank_and_push.py:138,1136` | Clear-then-rediscover, CLI confirmation/reason, probe boundaries, sticky flag; the Python tests assert purge is absent, not that ranking filters. | — | Flag-clearing case added to the CLI scenario; filtering tests belong to #592. |

Where the effective exclusion lives today: historically tombstoned wallets are absent from
ranking because their trades were deleted and discovery re-admits nothing with an `infra`
tombstone; flagged wallets are absent because activation and backfill never fetch them. Ranking,
export, and publication rely on those upstream facts, not on their own filter.

## 5. Isolated comparison: candidate selection and processing cost

| Question | Measurement (read-only) | Source |
|---|---|---|
| Would today's producers flag any of the 24? | Cold probe on the current newest page: 0/24 (all > 1 h). Retroactive sweep on archived oldest 500: 0/20 (minimum 6.31 h). | `evidence/tombstone-review.csv`, `evidence/archive-density.json` |
| Would an any-window rule flag any of the 24? | 2/20 (the retained pair); 18/20 have zero dense windows within their archived spans; 4 untestable. | `evidence/cohort_summary.csv` |
| Do infra tombstones outside the CSV (the probe-era shape) hold dense histories? | Of the 478, 136 have archived trades; 132 of those have at least one dense window and 4 have none (`0xbf67…` 613 trades, `0xd1c5…` 2,611, `0x0681…` 5,519, `0x8b5a…` 13,568). The four flags' triggers are not recoverable. | `evidence/controls.json` |
| Do the largest CSV members hold dense histories? | The five largest by archived trades (148,064 to 1,055,353 rows) all have dense windows (minimum 24 to 714 s). | `evidence/controls.json` |
| Verified system control | All four `known:*EXCHANGE*` addresses are `infra` tombstones. | `evidence/aggregates.json` |
| Cost of re-admitting the 24 | 419,864 archived trades = 0.25% of the active universe (279,478 wallets, 167,703,257 trades). | `evidence/aggregates.json` |
| Cost of bulk-clearing the breadth class | The 8,115 CSV members with archived trades hold 101,986,315 rows (61% of the active universe's count); rejected. | `evidence/aggregates.json` |
| Effect of an any-window retroactive rule on the live universe | Random 300 of the 44,460 active wallets with ≥500 trades (seed 589): any-window flags 9 (3.0%) holding 18.0% of the sampled trades; the oldest-500 rule flags 0; the newest-500 rule flags 0. | `evidence/aggregates.json` |
| Do excluded wallets reach ranking outputs today? | Last two completed cycles (`cron-20260909T002836Z`, `cron-20260910T002554Z`): pass-1 outputs (105,325 and 107,364 rows) and pass-2 outputs (1,064 and 1,056 rows) contain 0 flagged and 0 tombstoned wallets; no pending publication request at 21:52 UTC. The activation batches of those two cycles and of the in-progress `cron-20260910T210538Z` cycle contain 13, 7, and 3 wallets that were flagged by the probe after activation, as the mechanism predicts. | `evidence/aggregates.json` |

Unmeasured: the follower-return effect of any rule change (no ranker run; inputs not
established), the fetch-workload saved by a probe flag (the discarded page is not recorded), and
any excluded-wallet overlap in artifacts older than the two cycles checked.

## 6. Policy matrix

| Class | Current producer | Evidence standard | Duration / review trigger | Rediscovery that lifts | Bounded work | Reversal | Idempotency and audit | Decision |
|---|---|---|---|---|---|---|---|---|
| Verified system addresses | None today (the four came from the CSV `known:*` rows; importer removed #335) | Contract identity (exchange/neg-risk adapters) | Permanent | None (`infra` tombstones are never lifted by discovery) | n/a | Operator command; not expected | Tombstone row; this record | **Retain.** |
| Explicit operator exclusions | None today; the shape is an `infra` tombstone or `is_infra` flag written under the cache lock | Written evidence recorded with the action | Until reviewed | None | n/a | `clear-infra-exclusion` | Tombstone/flag row plus the operator's audit file (precedent: `data/eval-results/manual-*` batches) | **Retain the capability; add a repository command only when a second case arises.** |
| Historical losers (`proven_loser`) | Rule A of `pe-bootstrap purge` (report-only unless armed; not in the wrapper) | Ranking-CSV row with `eligible = true AND tstat_net <= bootstrap_purge_loser_tstat_max AND mean_net < 0 AND n_eff >= bootstrap_purge_loser_neff_min`, intersected with active non-infra wallets (`purge.rs:116,427`) | Until leaderboard rediscovery; a current rerank then decides | `tombstone_override_sources` (leaderboard) | Activation batch cap | Automatic lift by leaderboard; otherwise a rerank | Tombstone row `INSERT OR REPLACE`; ranking CSV named in the purge report | **Retain.** No cooldown number is introduced; the two counterexamples show recent positive screens on different price evidence, which is not a rerank. |
| Breadth label (`breadth>=2000`) | None (retired #335) | A market-count heuristic; not a density or identity criterion | Legacy labels are reviewable per wallet on evidence | None | Per-wallet review only | `clear-infra-exclusion` per wallet after a recorded verdict | This record; the CSV and archive are the provenance | **Retire the producer (already done); revise legacy handling to per-wallet review; no bulk clearing.** |
| Density probe (cold, newest page) | Wrapper backfill | `infra_probe_span_secs` on a full 500-row page | Until an operator clears it; the next fetch re-probes | None | The page is discarded; the wallet leaves the backfill queue | `clear-infra-exclusion` (clears the flag; an active wallet with no fetch stamp becomes due at once and is probed again; a wallet flagged after ingestion resumes incremental backfill when stale, without a new probe) | Flag only; `span_secs` in the backfill log | **Retain** as a workload guard. A flag is not an identity verdict. |
| Retroactive sweep (oldest 500) | Operator `classify-infra` | Same threshold on cached trades | Operator-run | None | Full `trades` scan | `clear-infra-exclusion` | Flag only | **Retain as-is.** In the sample, an any-window variant would flag about 3% of active wallets with ≥500 trades, holding about 18% of their trades; their ingestion cost is already incurred and the future refresh work such flags would save was not measured. |
| Unknown evidence (`0x88ec…`, the 4 non-dense non-CSV tombstones, and the 56 live probe flags with zero trades) | Cold probe (flag-only shape) | None recoverable beyond the log line | Unresolved until a bounded re-ingestion review | None | Per-wallet | `clear-infra-exclusion` after review | Flag/tombstone row only | **Unresolved; retain.** No schema change: the log line plus this review method suffice until an operator needs durable per-flag evidence. |

Leaderboard-only override versus evidence-based requalification: keep the leaderboard-only
automatic lift for non-`infra` tombstones (bounded by the activation batch) and use this record's
method plus the operator command for `infra` exclusions. No automatic reevaluation lifecycle is
added; a cleared wallet returns to research through ordinary discovery or an audited activation
batch and is decided by the unchanged ranking, publication, and service-admission gates.

## 7. Handoff

Shipped with this record: `clear-infra-exclusion` clears both exclusion shapes (the consolidated
scope extension for #589); the canonical docs describe the acquisition-gating mechanism; the
evidence directory.

Operator actions the record supports (each is separately authorized, idempotent, and resumable):

1. For each of the 18 reclassification wallets: `pe-bootstrap clear-infra-exclusion --wallet <hex>
   --confirm` on the ranker host between loop cycles (the command takes `CacheMutationLock`;
   order per `27-WINNER-DISCOVERY-RUNBOOK.md`). Exit 1 with "no infra tombstone or live is_infra
   flag" means already cleared. The host must run a binary built from a revision that includes
   this change for the flag path; the tombstone path is unchanged.
2. Return to research either through ordinary rediscovery (leaderboard, datadash, 502-gap, trades)
   or through an audited activation batch (`wallet_activation_batches`, batch id
   `review-589-<date>`). Clearing a tombstone-only wallet leaves no `wallets` row, and
   `activate-next` only selects rows that already exist, so a row must first be created by
   rediscovery or by an explicit operator insertion under the cache lock that also records the
   exact cohort in `wallet_activation_batches` / `wallet_activation_batch_wallets` (no repository
   command takes a named cohort; the September 10 five-candidate insertion on the ranker host,
   audited under its `data/eval-results/manual-dune-20260910-five-candidates` directory, is the
   operational precedent). The first backfill of a row with no fetch stamp runs the cold probe on
   the newest page; a re-flag is evidence, not an error.
3. For the four unresolved wallets the same path is the density check; run it only if the operator
   wants the answer (bounded to four wallets). The two retained wallets need nothing.
4. Rollback: revert the PR. A cleared never-fetched wallet that proves dense is re-flagged by its
   first fetch;
   one that is not dense but unwanted needs an explicit operator exclusion (row under the lock).

Deferred to #592 (multi-owner behavior change; measured impact today nil, §5): apply one
infra/tombstone predicate at Parquet export, both pass-1 universe sources and engines, publication
preparation (including pending-request replay), and the schema-two frozen cohort/projection, with
tests that exercise the real selection and publication functions against retained flagged trades.
Until then, a wallet flagged after ingestion (`classify-infra`) keeps its history in ranking inputs.

Regression scenarios for any future exclusion change: a verified system address stays blocked
through every rediscovery source; a heuristic candidate can be cleared and re-enters research
without bypassing qualification; a poor performer is reconsidered only on a new rerank; a retry
with no new evidence stays bounded (one fetch, one probe); clearing leaves no unexplained effective
exclusion (tombstone and flag both gone; `is_active` unchanged); a re-flag after clearing is
recorded in the backfill log.

## 8. Evidence index and no-mutation record

All ranker-host reads used `sqlite3.connect("file:…?mode=ro", uri=True)` through the committed
scripts; no write connection was opened. Public recapture used 52 `GET
https://data-api.polymarket.com/activity?user=<w>&type=TRADE&limit=500&offset=0&end=<unix>`
requests (identity and `end` bound verified on every page). No Dune execution was run. No wallet,
tombstone, ranking, configuration, deployment, or trading state was changed by this review; the
only observed background changes were three new cold-probe flags (53 → 56) written by the running
loop.

Committed (`evidence/`): the read-only scripts (`forge_ro*.py`, `recapture.py`), the derivation
script (`derive_summaries.py`) and its outputs (`cohort_summary.csv`, `aggregates.json`,
`controls.json`), `recapture-summary.json` (per-page metadata and SHA-256 of public pages), and
the retained September 10 bundle's summary files (`tombstone-review.csv` — the historical
per-wallet input, superseded by §2 — `archive-density.json`, `tombstone-audit-summary.json`,
`manifest.json` with the SHA-256 of every original raw page, and
`03_tombstoned_history_audit.sql`, Dune execution `01M26ASVHBDNGRJ9M7FCY42GA6`).

Retained privately (git-ignored data root on the development box,
`data/research-2026-09-exclusion-review/`, with its own `README.md` and SHA-256 manifest): the raw
ranker-host captures (`raw-forge/*.out.json`, which copy cache rows), the 52 recaptured raw pages,
`tombstone-provenance.json`, the plan and review artifacts. The original `/tmp/pe-dune-research/`
bundle expired during this cycle; its raw pages are not retained, only their hashes and the
summary files above. Recaptured observations (2026-09-10, `recapture.py`) are distinct from the
original September 10 pages.

| Script | Content | Observed (UTC) |
|---|---|---|
| `forge_ro.py` | Schemas, tombstone/flag counts, cohort rows on live and archive | 21:11:29 |
| `forge_ro2.py` | Archive manifest by reason and purge run, active-universe volume, flagged rows | 21:16:32 |
| `forge_ro3.py` | Sliding 500-trade window test for the 27 wallets and eight controls | 21:19:00 |
| `forge_ro4.py` | Random-sample any-window estimate on the active universe | 21:28:20 |
| `forge_ro5.py` | Trade rows held by flagged wallets (zero) | 21:31:29 |
| `forge_ro6.py` | Exact CSV address intersection, class costs, largest CSV members, non-CSV controls | 21:48:44 |
| `forge_ro7.py` | Flagged/tombstoned overlap in recent cycle outputs; pending marker | 21:52:14 |
| `recapture.py` | 13 anchored public pages for each of the four never-fetched wallets | 2026-09-10 |

Sources checked: `AGENTS.md`, `_EVIDENCE-FIRST.md`, `_BASELINE.md`, `_GLOSSARY.md`, `19-`,
`16-`, `26-`, `27-`, `15-SOURCES.md` (user-activity contract re-verified 2026-09-10);
`crates/bootstrap/src/{cache,infra_probe,polymarket,purge,pile,main,migrate,cache_migration}.rs`;
`scripts/{rank_and_push.sh,export_trades_parquet.py,rank_72hr_buyandhold.py,latency_shift_rerank.py,push_ranking_to_supabase.py,test_rank_and_push.py}`;
`crates/service/src/{watchlist_maintenance,watchlist_admission,supabase_reader}.rs`; git history
at `8d2d0c6`, `51608fd` (#336), `76db535` (#502), `eaf755a` (#504), `6aaec86` (#548).

Limitations: archived cohort rows end by 2026-05-19, so later density is known only from sampled
pages; the July 20/22 purge-run lineage is UNKNOWN (§3); the running binary revision on the
ranker host was not verified; no ranker run was performed; the cohort is a positive-screen
selection, so its false-exclusion rate does not estimate the population's.
