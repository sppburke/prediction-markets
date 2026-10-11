# 32 — Ranker adjudication decision record and retired forward bench

**Status: BINDING DECISION RECORD; FORWARD-BENCH PROTOCOL RETIRED.** Numeric defaults and the
paper measurement contract live in [`_GLOSSARY.md`](_GLOSSARY.md). This document preserves the
ranker adjudication and the adopted one-observed-system decision; it is not an operative
qualification protocol.

## Decision record

- run25 was the closed confirmatory adjudication. No challenger was certified. run24 was
  de-certified because rectangular cleaning made the evaluation panel depend on the challenger
  grid and because its top set was effectively unresolved.
- run26 and the proposed run27/full-grid rerun were cancelled. The older cache cannot support the
  intended early periods, and reopening the selection surface would not repair its
  era-asymmetric purge bias.
- CLV diagnostics from that cache are not certification evidence. Historical cache results are
  descriptive bench-composition evidence only.
- The certification program is terminated: no new exploratory certification grids, rerolls, or
  single-config argmax freezes are authorized by this record. Frozen-matrix diagnostics may still
  be used for research when labeled descriptive.
- run28 was an explicit operator exception for descriptive analysis. Its evidence did not itself
  authorize production.

## Adopted one-system decision

The 2026-07-03 operator decision moved the paper system to the run28 default ranker shape and made
the absolute forward result of that single observed system authoritative. The incumbent-control
bench ceased to exist at the reset. That decision superseded the former paired challenger/control
composition, paired gate, bootstrap/resampling protocol, interim look, extension, and second
window.

Issue #545 preserves that one-system boundary while replacing the old paper economics with one
exact, replayable financial era:

- the active ranker publishes one corrected schema-two batch;
- paper decisions, fees, signed quantities, financial Finals, marks, membership changes, and the
  qualification seal are recorded as replay evidence;
- the network-free verifier evaluates only the sealed observed-paper prefix; and
- a `Pass` report records paper-measurement evidence; it does not control ordinary live mode.

Backtests remain valuable for walk-forward research, regression, and implementation parity. They
are not a comparator, control arm, or promotion sample for the sealed qualification era.

## Retired-history note

The original version of this document pre-registered a three-arm forward bench, paired weekly P&L,
fixed-horizon empirical-Bernstein bounds, multiplicity correction, regime guards, one possible
extension, and a forced incumbent-default decision. Those details are retained in Git history for
audit but are deliberately absent here so repository searches cannot mistake them for current
operational gates. Amendments A0–A8 and the complete run24–run28 discussion remain recoverable from
the #417 decision comments and earlier revisions of this file.

## What will not happen

No simulator/control promotion comparison, resampling gate, interim qualification look, extension,
second window, or automatic result-based wait follows the sealed paper report. No production ranker
change is justified solely by the retired cache evidence.


## Rules before #757

**Historical, non-operative snapshot.** These values describe the issue's exact base commit
`475de88` (`475de8839496d5f25930fa0ee9aaabbc544aa68f`), not current defaults. Never update this
snapshot to track deployed policy; current contracts remain in [`_GLOSSARY.md`](_GLOSSARY.md)
and [`19-WINNER-FOLLOW-STRATEGY.md`](19-WINNER-FOLLOW-STRATEGY.md). Every `file:line` below refers
to that base, including documentation citations. Read its exact code with
`git show 475de88:<path>` (for example `git show 475de88:scripts/latency_shift_rerank.py`).
Code defaults, wrapper overrides, supplied operational captures and unknown deployed values are
identified separately; seeds are not proof of deployed state. This snapshot starts from the
reviewed `rules_before_757_draft.md`; all cited locations resolve at the base and samples across
the pipeline were checked against the exact code.

### 1. Discovery

- Leaderboard categories — OVERALL, POLITICS, SPORTS, CRYPTO, CULTURE, MENTIONS, WEATHER, ECONOMICS, TECH, FINANCE — `crates/source-polymarket-public/src/endpoint.rs:86`; default selection: `crates/bootstrap/src/config.rs:606`.
- Leaderboard slices — `{profit,volume} × {day,week,monthly,allTime}`, encoded `{PNL,VOL} × {DAY,WEEK,MONTH,ALL}`; 80 default requests — `crates/bootstrap/src/leaderboard_discovery.rs:22`; `crates/source-polymarket-public/src/endpoint.rs:35`; `crates/source-polymarket-public/src/endpoint.rs:55`.
- Leaderboard pagination — requested limit 50; offset omitted; one request per slice, no pagination; default request interval 500 ms — `crates/bootstrap/src/config.rs:34`; `crates/bootstrap/src/config.rs:35`; `crates/source-polymarket-public/src/endpoint.rs:216`; `crates/bootstrap/src/leaderboard_discovery.rs:124`.
- Datadash cohort admission — enabled by default; all listed cohorts except exact ID `07NQHFRAGB6HV`, exact title `Polymarket Twitter/X Linked Traders`, or advertised `numWallets > 10,000`; distinct PnL>$100k cohort remains eligible — `crates/bootstrap/src/config.rs:37`; `crates/bootstrap/src/config.rs:619`; `crates/bootstrap/src/datadash_discovery.rs:258`; `crates/bootstrap/src/datadash_discovery.rs:322`.
- Datadash pagination/rate — limit 1,000, initial offset 0, advance 1,000 until short page; default interval 500 ms, request timeout 15 s — `crates/bootstrap/src/datadash_discovery.rs:52`; `crates/bootstrap/src/datadash_discovery.rs:55`; `crates/bootstrap/src/datadash_discovery.rs:342`; `crates/bootstrap/src/config.rs:38`.
- Discovery failure policy — leaderboard slice errors skip; zero valid addresses across all slices fails; datadash cohort-page errors skip remainder, datadash-specific source errors soft-fail, shared cache errors propagate — `crates/bootstrap/src/leaderboard_discovery.rs:137`; `crates/bootstrap/src/leaderboard_discovery.rs:149`; `crates/bootstrap/src/datadash_discovery.rs:363`; `crates/bootstrap/src/winner_discovery.rs:65`.
- Address/upsert rule — lowercase, require `0x` prefix and length 42, sort/deduplicate, OR source bits; existing infra flag is sticky via MAX — `crates/bootstrap/src/leaderboard_discovery.rs:131`; `crates/bootstrap/src/leaderboard_discovery.rs:157`; `crates/bootstrap/src/datadash_discovery.rs:351`; `crates/bootstrap/src/cache.rs:3493`.
- Other sources/provenance — winner-discovery invokes only leaderboard and datadash; bits: wallet-set JSON=1, trades=2, leaderboard=16, 502-gap=64, datadash=128; retired gaps 4/8/32; explicit migration imports wallet-set/trade wallets — `crates/bootstrap/src/winner_discovery.rs:47`; `crates/bootstrap/src/winner_discovery.rs:57`; `crates/bootstrap/src/pile.rs:50`; `crates/bootstrap/src/migrate.rs:42`.
- Discovery tombstone lifting — incoming leaderboard bit 16 alone may lift a non-`infra` tombstone; datadash/502-gap/trades/wallet-set cannot; `infra` never lifts through discovery — `crates/bootstrap/src/pile.rs:66`; `crates/bootstrap/src/cache.rs:3503`.

### 2. Admission/activation

- Production activation ownership — discovery uses `--defer-activation`, followed by one audited `activate-next`; standalone immediate activation remains available — `scripts/rank_and_push.sh:870`; `crates/bootstrap/src/pile.rs:88`.
- Curated/preferred predicate — `COALESCE(trade_count,0) >= 100 OR COALESCE(dune_closed_markets,0) >= 100 OR source_bits & (16/64/128) != 0` — `crates/bootstrap/src/pile.rs:33`; `crates/bootstrap/src/cache.rs:4461`.
- Candidate ordering — preferred predicate first; then Dune win-rate DESC NULLS LAST, closed markets DESC NULLS LAST, trade_count DESC, wallet_hex ASC — `crates/bootstrap/src/cache.rs:4461`.
- Activation preconditions — inactive, non-infra, no tombstone; the preferred predicate controls priority rather than excluding ordinary candidates; selected wallets atomically become active — `crates/bootstrap/src/cache.rs:4458`; `crates/bootstrap/src/cache.rs:4494`.
- Batch bound/retry — default 20,000; `PE_BOOTSTRAP_ACTIVATION_BATCH_WALLETS` wins, 0 admits none; repeated batch ID reloads exact cohort only with identical requested count; Forge sets 5,000 in both `.env` files (captured 2026-10-10 02:42:37 UTC, `archive/evidence/discovery_1010/forge_env.txt`); other unobserved overrides and per-cycle requested/selected counts not captured — `crates/bootstrap/src/pile.rs:37`; `crates/bootstrap/src/config.rs:301`; `crates/bootstrap/src/config.rs:48`; `crates/bootstrap/src/cache.rs:4419`; `docs/26-DATA-REFRESH-AND-REOPTIMIZATION-RUNBOOK.md:1337`.

### 3. Exclusions

- Cold density probe, retained legacy backfill path — wallet with no stored trades: raw page ≥500 and parsed newest−oldest `< 3,600 s` flags infra; `PE_BOOTSTRAP_INFRA_SPAN_SECS` overrides; discard probe trades and preserve fetch stamp; fresh schema-two collector uses its separate semantic-window reader — `crates/bootstrap/src/infra_probe.rs:20`; `crates/bootstrap/src/infra_probe.rs:50`; `crates/bootstrap/src/infra_probe.rs:72`; `crates/bootstrap/src/polymarket.rs:351`; `crates/bootstrap/src/polymarket.rs:280`; `crates/bootstrap/src/cache_migration.rs:2185`.
- Retroactive density sweep — operator-only; oldest 500 stored trades, strict span `< threshold`; exclude partial histories; mark `is_infra=1` unless dry-run — `crates/bootstrap/src/cache.rs:4268`; `crates/bootstrap/src/cache.rs:4277`; `crates/bootstrap/src/cache.rs:4311`; `docs/37-WALLET-EXCLUSION-DECISION-RECORD.md:30`.
- Automatic purge — absent from production wrapper; direct `purge`/`purge-infra` default report-only (`purge_enabled=false`), require explicit arming and no dry-run — `docs/26-DATA-REFRESH-AND-REOPTIMIZATION-RUNBOOK.md:616`; `crates/bootstrap/src/config.rs:695`; `crates/bootstrap/src/purge.rs:87`; `crates/bootstrap/src/purge.rs:190`.
- Retained proven-loser purge rule — active tradeable, non-partial, CSV-eligible; `tstat_net <= -2.0 AND mean_net < 0 AND n_eff >= 20`; delete and tombstone — `crates/bootstrap/src/config.rs:42`; `crates/bootstrap/src/purge.rs:116`; `crates/bootstrap/src/purge.rs:437`; `crates/bootstrap/src/cache.rs:574`.
- Retained dead-weight purge rule — active tradeable, complete, not CSV-eligible, fetch stamp ≥now−86,400 s, newest trade `< now−1,209,600 s`; zero-trade wallets do not match; no tombstone — `crates/bootstrap/src/purge.rs:125`; `crates/bootstrap/src/pile.rs:48`; `crates/bootstrap/src/config.rs:41`; `crates/bootstrap/src/cache.rs:3592`; `crates/bootstrap/src/cache.rs:3608`.
- Exclusion persistence/clearance — tombstone reasons `proven_loser`/`infra`; `dead_weight` has none; explicit clearance deletes only infra tombstone and clears live infra flag without recreating/activating; ranking/publication do not independently filter these flags — `crates/bootstrap/src/cache.rs:564`; `crates/bootstrap/src/cache.rs:574`; `crates/bootstrap/src/cache.rs:3559`; `docs/37-WALLET-EXCLUSION-DECISION-RECORD.md:168`.

### 4. Collection

- Production lane/universe — installed schema two selects private-candidate fresh activity collection; union includes active non-infra wallets, retained histories, required full reads and prior exclusions — `scripts/rank_and_push.sh:77`; `docs/_GLOSSARY.md:809`; `crates/bootstrap/src/cache.rs:442`; `crates/bootstrap/src/cache_migration.rs:1677`; `crates/bootstrap/src/cache_migration.rs:1691`.
- Collection window/end — full reads start exclusively at 0; ordinary incremental reads start at predecessor fixed end or continuation frontier; new fixed inclusive end=`now_utc−120 s`, frozen on resume — `crates/bootstrap/src/cache_migration/incremental.rs:631`; `crates/bootstrap/src/cache_migration.rs:623`; `crates/bootstrap/src/main.rs:485`; `crates/bootstrap/src/polymarket.rs:38`.
- Activity requests — TRADE/SPLIT/MERGE/REDEEM/CONVERSION, descending pages of 500, offsets 0 through 5,000; forward boundary lookup uses ascending limit 1/offset 4,999 — `crates/source-polymarket-public/src/endpoint.rs:5`; `crates/source-polymarket-public/src/endpoint.rs:262`; `crates/source-polymarket-public/src/endpoint.rs:270`; `crates/source-polymarket-public/src/reconciliation.rs:31`.
- Quiet-wallet deferral — predecessor newest verified activity absent or strictly `< predecessor_end−2,592,000 s` makes completed history quiet; prior deferral remains quiet; full/continuing reads take precedence — `crates/bootstrap/src/cache_migration.rs:148`; `crates/bootstrap/src/cache_migration.rs:1641`; `crates/bootstrap/src/cache_migration.rs:1721`; `docs/_GLOSSARY.md:1399`.
- Weekly repoll — period 604,800 s; phase=wallet low 48 bits modulo period; due iff latest phased instant ≤predecessor end is `> predecessor start`; due deferred predecessor becomes full-read — `crates/bootstrap/src/cache_migration.rs:150`; `crates/bootstrap/src/cache_migration.rs:1392`; `crates/bootstrap/src/cache_migration.rs:1660`.
- Availability/failure rule — only complete, non-excluded head receipts supply current projection/certificate inputs; partial windows retain progress but excluded wallets supply no current ranking; digest/chain failures fail the cycle — `docs/_GLOSSARY.md:1405`; `docs/26-DATA-REFRESH-AND-REOPTIMIZATION-RUNBOOK.md:977`; `docs/_GLOSSARY.md:1505`.
- Activity resource/retry bounds — 32 concurrent wallet reads and queue slots, shared 50 ms/20 req/s gate, 2,000,000 held walk rows; wallet budget default 300 s checked between windows/retries, without cancelling in-flight pages; request timeout 30 s, 16 transient retries, 200 ms exponential backoff capped 30 s; separate 10 short-429 retries accepting Retry-After ≤1 s — `crates/bootstrap/src/cache_migration.rs:86`; `crates/bootstrap/src/cache_migration.rs:2163`; `crates/bootstrap/src/config.rs:24`; `crates/bootstrap/src/main.rs:26`; `crates/source-polymarket-public/src/fetcher.rs:18`; `crates/source-polymarket-public/src/fetcher.rs:24`; `crates/source-polymarket-public/src/fetcher.rs:134`; `crates/source-polymarket-public/src/fetcher.rs:416`; `docs/26-DATA-REFRESH-AND-REOPTIMIZATION-RUNBOOK.md:981`.
- Reference collection bounds — default CLOB concurrency 8, dedicated price gate 10 ms/100 req/s, targeted fidelity 1 minute overriding generic 60-minute default, logical pages ≤80,000 s; full terminal activity second saturation remains fatal — `crates/bootstrap/src/config.rs:26`; `crates/source-polymarket-public/src/clob_prices_history.rs:39`; `crates/source-polymarket-public/src/clob_prices_history.rs:44`; `crates/bootstrap/src/prices_history.rs:189`; `crates/bootstrap/src/prices_history.rs:195`; `crates/bootstrap/src/prices_history.rs:389`; `crates/source-polymarket-public/src/reconciliation.rs:297`.

### 5. Pass 1

- Universe/engine — wrapper `--universe-from-trades`; schema two actually enumerates certified projection wallets and requires verified DuckDB export, with no SQLite fallback — `scripts/rank_and_push.sh:664`; `scripts/rank_72hr_buyandhold.py:234`; `scripts/ranker_duck.py:194`.
- Ranking window — entry timestamps `[win_end−180 days, win_end)`, default end UTC midnight when pass 1 parses arguments; wrapper leaves dates empty — `scripts/ranker_decay.py:38`; `scripts/rank_72hr_buyandhold.py:173`; `scripts/rank_and_push.sh:102`; `scripts/ranker_duck.py:666`.
- Shared `as_of` — wrapper resolves once before refresh: explicit value, otherwise explicit WIN_END, otherwise that invocation’s UTC date/midnight; passes to both scoring calls; independently resolved pass-1 window end can differ across midnight; schema-two pass 2 requires explicit anchor — `scripts/rank_and_push.sh:684`; `scripts/rank_and_push.sh:993`; `scripts/rank_and_push.sh:1041`; `scripts/latency_shift_rerank.py:556`.
- Schema-two retained first buys — positive admitted BUY Entry, no previous consumed BUY for market, zero pre-position long/short on outcome, no action-order dependence, eligible token-verified resolved payout and end marker; same-second homogeneous BUY pieces retain minimum source ID once; mixed first-entry outcomes remain ambiguous — `crates/copy-signal-engine/src/classifier.rs:80`; `crates/position-ledger/src/lib.rs:1712`; `crates/position-ledger/src/lib.rs:1729`; `crates/position-ledger/src/lib.rs:1742`; `crates/position-ledger/src/lib.rs:1777`; `crates/bootstrap/src/cache_migration/projection_v3.rs:271`.
- Payout/structural rule — token-derived outcome, payout vector `[1,0]`, `[0,1]` or `[0.5,0.5]`; invalid projected structure fails; schema two applies no leader-price, band, horizon or pass-1-statistical extraction gate — `scripts/ranker_duck.py:601`; `scripts/ranker_duck.py:604`; `scripts/ranker_duck.py:638`; `scripts/rank_72hr_buyandhold.py:567`.
- Classifier-6 scope exclusions — `conversion`, `unknown_type`: neg-risk group/event when resolved, otherwise market; `order_dependent`, `overflow`, `underflow`, `unknown_condition`, `unmapped`: resolved market; unresolvable problems are ignored/countable rather than wallet-wide exclusions — `docs/_GLOSSARY.md:1461`; `docs/_GLOSSARY.md:1474`; `crates/position-ledger/src/lib.rs:1265`; `crates/position-ledger/src/lib.rs:1335`.
- Problem-second/history rule — score nothing in a problem second; apply other valid scopes, consume effective BUY markets, retain earlier scored entries; drops are cumulative from their starting second and survive replacement/repair; BUY→SELL→BUY cannot reopen eligibility — `crates/position-ledger/src/lib.rs:1310`; `crates/position-ledger/src/lib.rs:1379`; `docs/_GLOSSARY.md:1488`; `docs/_GLOSSARY.md:1501`.
- Raw-only rule — zero-share/combo/non-position effects remain raw-only; positive token-less TRADE is ignored; zero-share REDEEM with condition is no-op, without condition needs unknown anchor — `docs/_GLOSSARY.md:1478`; `crates/position-ledger/src/lib.rs:1253`.
- Pass-1 returns — slip default 1 cent; `eff=min(leader_price+0.01,0.999)`, gross=`(payoff−price)/price`, net=`(payoff−eff)/eff`; CLOB end marker supplies `resolved_at` — `scripts/rank_72hr_buyandhold.py:150`; `scripts/rank_72hr_buyandhold.py:360`; `scripts/ranker_duck.py:664`.
- Shared decay/statistics — wrapper half-life 30 days wins, matching script/glossary default; `w=exp(−ln2·max(0,as_of−entry_ts)/(30·86400))`; μ=Σwx/W, variance=`W·Σw(x−μ)²/(W²−Σw²)`, Kish N=`W²/Σw²`, t=`μ/SD·√N`; equal weights use ddof=1; SD≤1e−9 or insufficient effective sample makes t NaN — `scripts/rank_and_push.sh:110`; `scripts/ranker_decay.py:37`; `scripts/ranker_decay.py:46`; `scripts/ranker_decay.py:61`; `scripts/ranker_decay.py:108`; `scripts/ranker_decay.py:117`; `docs/_GLOSSARY.md:813`.
- Eligibility/pass-2 edge floor — reported eligibility: raw n≥20, n>1, defined t, monthly gates 0/0; wrapper overrides script defaults n=0/monthly 20/3; legacy candidates additionally require t≥2 and mean>0; schema two bypasses this candidate floor and greedy group selection entirely — `scripts/rank_and_push.sh:122`; `scripts/rank_and_push.sh:995`; `scripts/rank_72hr_buyandhold.py:141`; `scripts/rank_72hr_buyandhold.py:616`; `scripts/rank_72hr_buyandhold.py:647`; `scripts/latency_shift_rerank.py:355`.

### 6. Pass 2

- Targeted-price plumbing — `pe-bootstrap prices-history --targets-csv` was the ranker store's sole writer; pass two was its reader. Wrapper emitted targets, waited for capture, fetched uncovered pages and wrote the export-bound final stage record before scoring; installed points/pages had separate coverage identity from generic CLV/mark history — `crates/bootstrap/src/main.rs:119`; `crates/bootstrap/src/main.rs:1025`; `scripts/rank_and_push.sh:999`; `scripts/rank_and_push.sh:1009`; `scripts/rank_and_push.sh:1021`; `docs/_GLOSSARY.md:1299`.

- Schema-two candidate bound — every position wallet remains in output; evaluate only wallets with >1 and ≥20 in-horizon positions; neither pass-1 eligibility nor its t/mean gates select them — `scripts/latency_shift_rerank.py:155`; `scripts/latency_shift_rerank.py:387`.
- Oracle identity/fetch windows — `clob-minute-reference`, oracle version 6, fidelity 1 minute/parser 1; merged inclusive windows `[int(entry+Δ−120)−1,int(entry+Δ)+1]`, padded again at fetch envelope — `scripts/latency_shift_rerank.py:68`; `scripts/latency_shift_rerank.py:340`; `crates/bootstrap/src/prices_history.rs:405`.
- Reference lookup/validity — latest stored sample `t <= entry+Δ`; require age≤120 s, `sample_t < resolved_at` and `0 < sample_price < 1`; resolved_at is the stored CLOB end marker; otherwise typed no-reprice disposition — `scripts/latency_shift_rerank.py:437`; `scripts/latency_shift_rerank.py:443`; `scripts/ranker_duck.py:664`.
- Latency/horizon — wrapper Δ=2 s, floor=10 s, ceiling=172,800 s: `10 <= ttr_secs−2 < 172800`; wrapper overrides standalone floor 60/max 259,200 and pass-1 TTR default 72 h; wrapper TTR_HOURS=48 — `scripts/rank_and_push.sh:114`; `scripts/rank_and_push.sh:130`; `scripts/rank_and_push.sh:263`; `scripts/rank_and_push.sh:978`; `scripts/rank_and_push.sh:1044`; `scripts/latency_shift_rerank.py:115`; `scripts/latency_shift_rerank.py:150`; `scripts/rank_72hr_buyandhold.py:132`.
- Price band — apply to slipped effective reference price, inclusive lower/exclusive upper: `0.15 <= eff < 0.85`; wrapper arguments win over matching script defaults — `scripts/rank_and_push.sh:116`; `scripts/rank_and_push.sh:1045`; `scripts/latency_shift_rerank.py:119`; `scripts/latency_shift_rerank.py:462`.
- Pass-2 net — slip default 1 cent; `eff=min(reference_price+0.01,0.999)`; net=`(payoff−eff)/eff`; same entry-time decay and weighted statistics as pass 1 — `scripts/latency_shift_rerank.py:96`; `scripts/latency_shift_rerank.py:461`; `scripts/latency_shift_rerank.py:510`.
- Coverage — `fill_rate=n_filled/(n_total−price_band_drops)`, zero when denominator zero; n_total counts in-horizon positions; missing/unmapped/stale/invalid/post-end references remain in denominator; minimum 0.5, script default used by wrapper — `scripts/latency_shift_rerank.py:106`; `scripts/latency_shift_rerank.py:493`; `scripts/latency_shift_rerank.py:508`.
- Survival/month gates — raw repriced n≥20 and >1, fill_rate≥0.5, active months≥0, repriced n/month≥0, defined weighted t≥2, weighted mean>0; wrapper zeroes standalone monthly defaults 3/20 and overrides min_trl=0 — `scripts/rank_and_push.sh:122`; `scripts/rank_and_push.sh:1042`; `scripts/latency_shift_rerank.py:109`; `scripts/latency_shift_rerank.py:512`.
- Statistics/order — hit_rate=unweighted mean payout; serialize mean to 6 decimals, t/coverage/Kish N/hit_rate to 4; order survivors first, serialized t DESC (missing=−9), scored rows first, first market/outcome/CSV ordinal — `scripts/latency_shift_rerank.py:511`; `scripts/latency_shift_rerank.py:516`; `scripts/latency_shift_rerank.py:527`.
- Terminal coverage gate — every needed mapped in-horizon reference window must be covered by committed fidelity-1 pages; any uncovered pair exits 75 before scoring/publication; complete empty reference pages permit ordinary no-reprice outcomes — `scripts/latency_shift_rerank.py:619`; `scripts/latency_shift_rerank.py:629`; `scripts/rank_and_push.sh:1015`.

### 7. Publication

- Published rows/order — activity filter first, then survivors DESC/t DESC (missing=−9), stable ties, top 200 including non-survivors when needed; reject missing survives verdict or empty result — `scripts/rank_and_push.sh:132`; `scripts/rank_and_push.sh:1082`; `scripts/push_ranking_to_supabase.py:568`; `scripts/push_ranking_to_supabase.py:638`; `scripts/push_ranking_to_supabase.py:649`; `scripts/push_ranking_to_supabase.py:657`.
- Activity/completeness filter — default 72 h; retain newest verified TRADE≥publication_now−259,200 s and exclude partial/unavailable wallets; schema-two timestamps derive from finalized-head certificates — `scripts/push_ranking_to_supabase.py:174`; `scripts/push_ranking_to_supabase.py:221`; `scripts/push_ranking_to_supabase.py:241`; `scripts/push_ranking_to_supabase.py:585`.
- Global freshness — newest TRADE, payout/resolution fetch and terminal CLOB sweep must all exist and age≤same bound; script default 24 h; documented interim Forge `.env` override is 96 h and wins when sourced/passed; actual deployed value UNKNOWN — `scripts/push_ranking_to_supabase.py:176`; `scripts/push_ranking_to_supabase.py:183`; `scripts/push_ranking_to_supabase.py:198`; `scripts/push_ranking_to_supabase.py:588`; `scripts/rank_and_push.sh:262`; `scripts/rank_and_push.sh:1078`; `docs/_GLOSSARY.md:803`.
- Batch/request contract — ranks renumbered 1..N; n_trades=n_filled; real last_trade stamp, classifier 6, coverage ends and cumulative scope drops; five scoring limits from bound oracle manifest; durable content-addressed request precedes cache activation and atomic/idempotent v2 RPC — `scripts/push_ranking_to_supabase.py:356`; `scripts/push_ranking_to_supabase.py:693`; `scripts/push_ranking_to_supabase.py:773`; `scripts/rank_and_push.sh:1167`; `scripts/supabase_schema.sql:203`; `scripts/supabase_schema.sql:214`.
- Publication retries — initial attempt+5 transient retries; transport/timeout or HTTP 408/425/429/5xx only; backoff 1 s doubling, cap 30 s, bounded numeric Retry-After; exhaustion exits 75 for exact-request recovery — `scripts/push_ranking_to_supabase.py:32`; `scripts/push_ranking_to_supabase.py:276`; `scripts/push_ranking_to_supabase.py:300`; `scripts/push_ranking_to_supabase.py:35`.
- Read-back/retention — require latest exact batch, ranks 1..N, survives verdicts and config_hash matching request; retain newest 1,080 batches, best-effort prune (0 disables); latest view selects max(batch_id) — `scripts/push_ranking_to_supabase.py:796`; `scripts/push_ranking_to_supabase.py:817`; `scripts/push_ranking_to_supabase.py:829`; `scripts/push_ranking_to_supabase.py:577`; `scripts/push_ranking_to_supabase.py:845`; `scripts/supabase_schema.sql:277`.

### 8. Service settings consuming rankings or sizing copies

- Copy-entry authority/hold — continuation 7 requires each wallet's first qualifying BUY Entry in receipt order for frames, or first-ever attributable BUY with complete-read authority; homogeneous same-second same-outcome pieces choose the minimum source ID. No same-market cross-leader paper hold in continuation 7; earlier continuations retain the outcome-keyed hold. SELL/add/trim/exit/flip/re-entry do not copy; SELL is ignored and copies hold to resolution — `docs/_GLOSSARY.md:473`; `docs/_GLOSSARY.md:532`; `docs/_GLOSSARY.md:627`; `crates/service/src/orchestrator.rs:4235`.
- Gate E exception (#752) — before staging a grouped neg-risk YES (outcome 0), no other market in that group may have an unsettled YES BUY fill, from any leader. Same-market YES and all NO legs are unaffected; accepted staging retains the check. Recorded raw CLOB group mapping owns the gate; unmapped older holdings do not block siblings until mapped; gate store errors stop intake — `crates/service/src/orchestrator.rs:3843`; `docs/19-WINNER-FOLLOW-STRATEGY.md:417`.
- Copy book/admission — current CLOB state/token/tick/share-minimum/fee and freshness evidence mandatory; paper permits positive/absent matching delay, ordinary live requires zero delay. Continuation 7 checks accepted ask ticks against inclusive floor, exclusive ceiling and impact cap once; no later VWAP-band refusal or decision mid gate. Dollar paper can sign available in-band capacity up to its conservative budget; ordinary live is complete-request. Kelly/Contract are exact-or-decline; there is no one-contract fallback — `docs/_GLOSSARY.md:632`; `docs/_GLOSSARY.md:764`; `docs/19-WINNER-FOLLOW-STRATEGY.md:418`; `docs/19-WINNER-FOLLOW-STRATEGY.md:448`.
- Concentration posture — production `concentration_caps=None` leaves leader/market/family/total caps unenforced by owner decision; backtest/tests use canonical 300/200/800/2,500 bps — `crates/risk-engine/src/snapshot.rs:21`; `crates/risk-engine/src/snapshot.rs:41`; `docs/19-WINNER-FOLLOW-STRATEGY.md:371`.


- Configuration authority — complete validated Supabase snapshot wins at production boot; hot keys poll every 30 s; SQL seeds use ON CONFLICT DO NOTHING and cannot establish current deployed values — `crates/service/src/main.rs:644`; `crates/service/src/main.rs:662`; `crates/service/src/runtime_config.rs:267`; `crates/service/src/config_poller.rs:32`; `scripts/supabase_schema.sql:390`.
- active_watchlist_size — default/seed 100, valid 1..=200, no TOML/env surface; rank-ordered `survives=is.true` only, cap rather than guaranteed membership count — `crates/service/src/runtime_config.rs:96`; `scripts/supabase_schema.sql:376`; `crates/service/src/supabase_reader.rs:124`; `docs/_GLOSSARY.md:1139`.
- Ranking application — refresh default/smoke 300 s; smoke production mode `full_rerank`, code default `knockout`; full rerank uses batch-pinned survivors, with history/current-position validation before atomic membership publication — `crates/service/src/config.rs:340`; `crates/service/src/config.rs:348`; `smoke-test/service.toml:30`; `smoke-test/service.toml:39`; `crates/service/src/supabase_reader.rs:140`; `docs/_GLOSSARY.md:1139`.
- max_resolution_horizon_secs — code/seed/smoke/glossary 172,800; gate compares parsed CLOB `end_date_iso−gate_now`, rejects strictly `> max`, so equality passes; 0 disables upper bound, missing end fails when horizon gate enabled — `crates/service/src/config.rs:328`; `scripts/supabase_schema.sql:381`; `smoke-test/service.toml:28`; `docs/_GLOSSARY.md:468`; `crates/source-polymarket-public/src/live_admission.rs:222`; `crates/service/src/orchestrator.rs:3798`; `crates/service/src/orchestrator.rs:4830`.
- min_resolution_horizon_secs — code/seed/smoke/glossary 60; reject strictly `< min`, equality passes, 0 disables; glossary records future release-2 alignment to ranker floor 10, deployed alignment UNKNOWN — `crates/service/src/config.rs:332`; `scripts/supabase_schema.sql:380`; `smoke-test/service.toml:31`; `docs/_GLOSSARY.md:469`; `crates/service/src/orchestrator.rs:4839`.
- max_fill_price — code/seed/smoke/glossary 0.85, exclusive accepted-price ceiling; current ladder stops at ask≥ceiling; 0 disables band edge — `crates/service/src/config.rs:336`; `scripts/supabase_schema.sql:378`; `smoke-test/service.toml:32`; `docs/_GLOSSARY.md:761`; `crates/service/src/orchestrator.rs:2862`; `crates/venue-polymarket/src/ladder.rs:308`.
- min_fill_price — code/seed/smoke/glossary 0.15, inclusive floor; current continuation-7 accepted asks must meet it; earlier continuations use VWAP floor; 0 disables band edge — `crates/service/src/config.rs:344`; `scripts/supabase_schema.sql:379`; `smoke-test/service.toml:29`; `docs/_GLOSSARY.md:762`; `crates/venue-polymarket/src/ladder.rs:305`.
- sizing_mode — strategy enum default Kelly; reviewed shared production/smoke/SQL seed Dollar=$25, parked contracts seed=1; fractional Kelly defaults paper/shadow 0.10, live/promoted 0.25, hard max 0.50, override absent — `crates/strategy-winner-follow/src/config.rs:52`; `crates/strategy-winner-follow/src/config.rs:108`; `smoke-test/service.toml:46`; `scripts/supabase_schema.sql:387`; `docs/_GLOSSARY.md:1717`; `crates/strategy-winner-follow/src/evaluate.rs:309`; `crates/kelly-sizer/src/lib.rs:22`.
- Live sizing precedence — account `live_sizing_*` wins when mode non-null; null falls back to shared sizing; dollar/contracts require positive valid value, otherwise decline; actual account settings UNKNOWN — `crates/service/src/live_fanout.rs:6969`; `scripts/supabase_schema.sql:388`.
- Per-trade cap — reviewed production `unlimited` resolves 10,000 bps/full bankroll; enum/SQL seed `mode_default` resolves LiveTiny=25 bps, Promoted=100 bps; post-activation accepts explicit bps:1..10000; actual hot value UNKNOWN — `docs/_GLOSSARY.md:1715`; `scripts/supabase_schema.sql:385`; `crates/strategy-winner-follow/src/config.rs:32`; `crates/service/src/runtime_config.rs:570`.
- Copy economics/size limits — default/seed slippage rate 0.01; mandatory price-impact cap default/seed 100 bps, valid 1..10000; planner bounds principal+fee reserve against collateral/caps; production concentration caps deliberately unenforced — `crates/strategy-winner-follow/src/config.rs:99`; `scripts/supabase_schema.sql:382`; `scripts/supabase_schema.sql:386`; `docs/_GLOSSARY.md:1718`; `docs/19-WINNER-FOLLOW-STRATEGY.md:366`.
- Day stop — intraday PnL≤−200 bps/−2%; numerator=current equity−preceding UTC-midnight equity (starting bankroll before first mark), denominator=fixed starting bankroll — `crates/risk-engine/src/lib.rs:29`; `crates/risk-engine/src/engine.rs:31`; `crates/risk-engine/src/inputs.rs:140`.
- Week stop — rolling realized-close PnL≤−600 bps/−6%; closes in `(now−7 days,now]`, denominator=fixed starting bankroll — `crates/risk-engine/src/lib.rs:30`; `crates/risk-engine/src/engine.rs:36`; `crates/risk-engine/src/inputs.rs:119`; `crates/risk-engine/src/inputs.rs:152`.
- Absolute kill — absolute PnL≤−1,000 bps/−10%; `(equity−starting bankroll)/starting bankroll`; strategy kill requires manual review; drawdown stops armed from owner-local evidence — `crates/risk-engine/src/lib.rs:28`; `crates/risk-engine/src/engine.rs:25`; `crates/risk-engine/src/inputs.rs:146`; `docs/19-WINNER-FOLLOW-STRATEGY.md:369`.
- Copy latency — budget default 120 s, valid 1..3600; websocket-primary REST/WS and Prepared gates reject strictly age>budget; ranker Δ remains 2 s; current copy-latency kill switch retired, historical semantic-1 replay retains it — `crates/service/src/config.rs:396`; `crates/service/src/config.rs:590`; `crates/service/src/trade_poller.rs:3121`; `crates/service/src/bucket_commit.rs:88`; `crates/service/src/risk_inputs.rs:514`; `docs/_GLOSSARY.md:49`.

### 9. Cycle cadence

- Cycle start — run flag=`run`, no concurrent loop/run lock holder; launch complete zero-argument wrapper; missing/stop flag ends loop — `scripts/rank_and_push_loop.sh:121`; `scripts/rank_and_push_loop.sh:152`; `scripts/rank_and_push.sh:374`; `docs/26-DATA-REFRESH-AND-REOPTIMIZATION-RUNBOOK.md:1546`.
- Successful repetition — after completed child and cleared recovery pointers, wait 60 s then next cycle; installed schema two admits new same-day cycles; retained recovery pointer bypasses success wait — `scripts/rank_and_push_loop.sh:36`; `scripts/rank_and_push_loop.sh:166`; `docs/26-DATA-REFRESH-AND-REOPTIMIZATION-RUNBOOK.md:1600`; `docs/26-DATA-REFRESH-AND-REOPTIMIZATION-RUNBOOK.md:1122`.
- Publication timing — after discovery/activation, activity, payout, finalization/export, pass 1, targeted reference fetch and pass 2; prepare exact request, activate bound cache, publish/read back; cadence depends on stage duration, not a fixed publish clock — `scripts/rank_and_push.sh:77`; `scripts/rank_and_push.sh:989`; `scripts/rank_and_push.sh:1018`; `scripts/rank_and_push.sh:1036`; `scripts/rank_and_push.sh:1167`; `docs/26-DATA-REFRESH-AND-REOPTIMIZATION-RUNBOOK.md:1350`.
- Recovery cadence — exit 75 waits 60 s, resumes pending publication before retained cycle, reuses activation cohort/request; other nonzero exits stop supervisor — `scripts/rank_and_push_loop.sh:35`; `scripts/rank_and_push_loop.sh:137`; `scripts/rank_and_push_loop.sh:176`; `docs/26-DATA-REFRESH-AND-REOPTIMIZATION-RUNBOOK.md:1617`.

**Historical evidence limits at drafting**

- PARTLY RESOLVED — Forge `.env` activation override is 5,000 (captured 2026-10-10 02:42:37 UTC); other environment overrides and recorded per-cycle batch counts not captured; checked configuration precedence, wrapper sourcing and ordinary-cycle runbook — `crates/bootstrap/src/config.rs:48`; `scripts/rank_and_push.sh:262`; `docs/26-DATA-REFRESH-AND-REOPTIMIZATION-RUNBOOK.md:1337`.
- UNKNOWN — Supabase hot values at drafting, service horizon-floor alignment and live-account sizing; checked boot authority, seeds, smoke config and account override parser; need deployed configuration snapshot/applied event evidence and account rows — `crates/service/src/main.rs:644`; `scripts/supabase_schema.sql:375`; `smoke-test/service.toml:28`; `crates/service/src/live_fanout.rs:6969`.