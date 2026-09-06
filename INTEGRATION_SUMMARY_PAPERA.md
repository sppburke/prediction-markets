# Issue #545 PAPERA integration checkpoint

Started `2026-09-05 23:30:43 UTC`; checkpoint written before the mandatory 40-minute
interruption boundary. This is intentionally a partial checkpoint: completed items are marked DONE
and every remaining item has a concrete blocker. `PROGRESS_PAPERA.md` is the durable item-by-item
trail.

## Checklist

- **C3 / RC12 — DONE:** all five controls are unconditional in
  `crates/service/src/orchestrator_control.rs:58`; owned exhaustive scenario/helper matches compile,
  and the active authority methods are required in `crates/service/src/supabase_state.rs:221`.
- **C1 — NOT DONE:** the Prepared → authority → local → Final serializer and sole composer call are
  present at `crates/service/src/orchestrator.rs:505,2773`, but Kelly still composes the preliminary
  Dollar plan. The LEAF summary's caller-supplied allocator S4 is absent from this checkout.
- **C2 — DONE:** every active fill redrives the verified oldest unmatched Prepared before successor
  admission at `crates/service/src/orchestrator.rs:517`.
- **RC1 — DONE:** both RPCs require non-null stored/supplied Start identity and seeding rejects an
  orphan `last_prepared_seq` at `scripts/supabase_paper_state_schema.sql:91,174,358`; unchanged-table
  pre-Start coverage is in `crates/service/tests/pg_parity.rs:1762`.
- **RC2 — DONE:** boot retains the complete Start, makes its bankroll the local/API/risk baseline,
  and checks config, local cash, and authoritative cash before seeding/mutation at
  `crates/service/src/main.rs:225-487`.
- **RC3 — DONE:** existing fill and resolution retries reconstruct and compare the original
  predecessor in SQLite and PostgreSQL at `crates/paper-state/src/lib.rs:3184,3279` and
  `scripts/supabase_paper_state_schema.sql:192,377`; changed null/numeric tests are at
  `crates/paper-state/src/lib.rs:6656`.
- **RC4 — DONE:** binary outcome validation exists before Prepared, at SQLite, and at SQL; SQL also
  rejects null, over-scale, and out-of-range exact amounts before mutation at
  `crates/service/src/orchestrator.rs:507,2743`, `crates/paper-state/src/lib.rs:3170`, and
  `scripts/supabase_paper_state_schema.sql:251`.
- **RC8 — DONE:** condition/schema/source/hash/payout validation and settlement-time derivation occur
  before the resolution Prepared append at `crates/service/src/orchestrator.rs:445`; recovery repeats
  it in `crates/service/src/supabase_state.rs:1310`.
- **RC7 — DONE:** active and rehearsal resolution polls isolate errors per condition at
  `crates/service/src/main.rs:1846,1982`.
- **RC10 — DONE:** both private 50 ms clients were replaced by one boot-owned 20-second client,
  canonical 200 ms CLOB gate, and no private retries at `crates/service/src/main.rs:297,1801,1920`.
- **RC9 — DONE:** active recovery precedes bankroll/sizing reads and fresh fills read current
  `financial_snapshot.cash` at `crates/service/src/main.rs:511-558` and
  `crates/service/src/orchestrator.rs:2349`.
- **RC5 — DONE:** `/paper/pnl` requires one strict price per open position and returns typed 503
  unavailability rather than zero at `crates/service/src/paper_api.rs:68-96,193`.
- **RC11 — DONE:** fills use the exact lifetime reader and status uses lifetime `fills_count` at
  `crates/paper-state/src/lib.rs:2951` and `crates/service/src/paper_api.rs:119,168`.
- **RC6 / C9 — NOT DONE:** `PaperPositionRow`/`FillRecord` remain integer legacy shapes beside
  `FinancialPositionRow`/`FinancialFillRecord` at `crates/paper-state/src/lib.rs:478-575`.
- **RC13 / C7 / C11 — NOT DONE:** dispatcher/executor files are deleted and canonical CLOB cadence
  is live at `crates/service/src/config.rs:127`, but `financial_log_paths: Option<_>` remains an era
  switch at `crates/service/src/orchestrator.rs:343`, and public legacy fill DTO/write paths remain.
- **D1 — DONE:** coherent snapshot/Start/last-Prepared validation, exact equity, seven-day closes,
  preceding midnight, source-log latency, and preservation of proposal snapshot fields are composed
  at `crates/service/src/risk_inputs.rs:353`; the caller derives real exposures at
  `crates/service/src/orchestrator.rs:620`.
- **D2 — DONE:** the sole service mid cache is source-log backed at
  `crates/service/src/main.rs:900` (live receives a cloned source-backed cache too).
- **D3 — DONE:** active economics resolves V3 observation evidence from the source log at
  `crates/service/src/orchestrator.rs:2696`.
- **D4 — DONE:** missed/discovered midnight boundaries, pending draining, durable boundary append,
  restart scan, and ready handoff are in `crates/service/src/trade_poller.rs:62-209,641-690`.
- **D5 — DONE:** historical attempts use `fetch_page_observed`, append transport-successful bytes,
  and classify those same bytes at `crates/service/src/risk_inputs.rs:72-112`; the orchestrator mark
  is synchronized at `crates/service/src/orchestrator.rs:766-899`.
- **D6 — NOT DONE:** partition and audit helpers exist at
  `crates/service/src/config_poller.rs:45` and `crates/service/src/risk_inputs.rs:291`, but boot/poll
  never turn a valid match into the synchronized matching release control.
- **D7 / RD1 — DONE:** `BinaryPayout` structurally owns exactly two conserved outcomes and the one
  aggregate-floor credit function at `crates/risk-engine/src/inputs.rs:44-91`; paper-pnl's duplicate
  aggregate module is deleted and service/live/qualifier call S2.
- **D8 — NOT DONE:** active paper/live fabricated builders are gone, but the legacy pre-Start zero
  builder remains at `crates/service/src/orchestrator.rs:3545`.
- **D9 — DONE:** `RiskHaltChanged` is synchronized before the in-memory active set changes at
  `crates/service/src/orchestrator.rs:1026-1050`.
- **RD2 — DONE:** there is no count-based financial-sequence validator; risk uses scanner-derived
  Start/latest-completed/unmatched state at `crates/service/src/risk_inputs.rs:353-374`.
- **RD3 — NOT DONE:** copied receipt timestamps are gone and source envelopes own latency, but the
  nested `BucketDecisionContext.page_occurrences/observed_source_receipts` copies remain at
  `crates/service/src/bucket_commit.rs:47`.
- **RD4 — DONE:** page evidence joins as a multiplicity-preserving `(URL, hash)` FIFO multiset while
  returning acquisition-order receipts at `crates/service/src/trade_poller.rs:1090-1130`.
- **RD5 — DONE:** exact zero preceding equity is accepted and tested at
  `crates/service/src/risk_inputs.rs:446,823`.
- **F2 — NOT DONE:** `PublishMembership` carries full `WatchlistEntry` replacements and synchronizes
  before its own publication at `crates/service/src/orchestrator.rs:1011`, but maintenance/capacity
  still publish directly at `crates/service/src/watchlist_maintenance.rs:422,444` and
  `crates/service/src/watchlist_capacity.rs:341`; boot replay is also absent.
- **F3 / F9 — NOT DONE:** `SealCheck` still returns the temporary fail-closed error at
  `crates/service/src/orchestrator.rs:1060`; boot/poll semantic drift and automatic 30-day/90-close
  seal production are not wired.
- **C4 — DONE:** `PublishMembership` carries exact replacement entries at
  `crates/service/src/orchestrator_control.rs:68`.
- **C5 — NOT DONE:** `DailyBoundary` appends its canonical mark before acknowledgement, but
  `SealCheck` does not append `QualificationSealed`.
- **C6 / B3 — DONE:** `ConfigEra` is derived once from the verified Start and passed to initial load
  and the poller at `crates/service/src/main.rs:315,1357`.
- **B5 — DONE (paper consumer):** budget inversion is delegated to `plan_sized_buy` at
  `crates/service/src/orchestrator.rs:1675`.
- **B6 — DONE (verified only):** the driver applies `migrate_service_config_545.sql` after Start at
  `scripts/paper_reset/activate_financial_era.sh:252-254`.
- **B8 — DONE for current economics:** Financial15 excludes retired keys; Legacy17 names remain only
  for the explicitly required pre-Start compatibility parser at
  `crates/service/src/runtime_config.rs:338-429`.
- **RB2-paper — NOT DONE:** paper has a sole local `plan_sized_buy` call, but the harvested API is
  still `BuySizing::Kelly { contracts }`, not the stated caller-supplied allocation closure. The
  adaptation site is `Orchestrator::plan_impact_gate` at `crates/service/src/orchestrator.rs:1520`.
- **RB8-paper — NOT DONE:** active economics avoids MarketEndCache and the named dispatcher/executor
  files are deleted; public legacy fill surfaces and the pre-Start fabricated risk builder remain.
- **C8 — NOT DONE:** `golden_stream_v1` and the requested runtime/crash/conflict/concurrency matrices
  are absent.
- **C10 — DONE:** paper handlers emit decimal strings; `site` has no consumer of these HTTP routes,
  and its shared `Numeric`/`formatQty` accepts fractional strings at `site/lib/types.ts:45` and
  `site/lib/format.ts:93`.
- **C12 — NOT DONE (environment):** six-test PostgreSQL parity cannot run because `PE_TEST_PG_URL`
  is unset. The new pre-seed no-mutation case is present at `crates/service/tests/pg_parity.rs:1762`.
- **RC14 — NOT DONE:** changed-predecessor coverage is present, but the exact HTTP contract,
  concurrent financial snapshot, terminal decision receipt, and full resolution identity matrices
  are absent.
- **RB7-paper — NOT DONE:** no merged cross-path planner/signer/paper/live quantity scenario or full
  active admission append-once matrix exists.

## Deleted duplicate or temporary paths

- Deleted retired `crates/service/tests/scenario_dispatch.rs` after it continued asserting the old
  dispatcher/pre-Start paper behavior.
- Confirmed `crates/execution-core/src/dispatcher.rs`,
  `crates/strategy-winner-follow/src/paper.rs`, and `crates/paper-pnl/src/aggregate.rs` are absent.
- Deleted both private resolution HTTP clients/retry policies; both pollers share the injected CLOB
  evidence owner.
- Removed the duplicate post-bankroll active recovery call; recovery now precedes every active cash
  consumer.
- Kept the checkpoint's risk-engine S2 implementation as the named owner; no duplicate payout
  implementation was added.

## Retired-surface sweep

The required sweep is **NOT empty**, so retirement is not complete. Exact command:

```text
rg -n -w 'polymarket_fee_rate|paper_fill_haircut_bps|paper_fill_slippage_bps|fill_mode|plan_budget_buy|estimated_ladder_spend|zeroed_risk_snapshot|PaperExecutor|FillSource|PaperExecutionError|prefix_vwap|modeled_kelly_price|financial_log_paths|gamma_resolution_poll_interval_secs' crates scripts .env.example --glob '!**/target/**'
```

Material hits: `plan_budget_buy` in venue-polymarket; `modeled_kelly_price` in backtest;
schema-one `estimated_ladder_spend`; explicit Legacy17 parser/migration names; historical migration
544 names; and PAPER's `financial_log_paths`. The first two directly contradict the supplied LEAF
handoff but are outside PAPERA ownership. The exact output was nonempty (exit 0).

## Verification

All Cargo commands used `CARGO_TARGET_DIR=/home/sean/git/pm-545/target`.

1. `cargo build --workspace --all-targets` — **PASS** after consumer reconciliation. Final tail:
   `Finished dev profile [unoptimized + debuginfo] target(s) in 14.37s`.
2. `cargo fmt --all`; `cargo fmt --all --check` — **PASS**, no output.
3. `cargo clippy --workspace --all-targets --all-features -- -D warnings` — **FAIL outside PAPER
   ownership**: LIVE-owned `live_fanout.rs:825,833` has two `needless_borrow` errors and its test at
   `:5393` uses `panic!` without a test lint allowance. PAPERA did not edit the excluded live file.
4. `cargo nextest run --workspace --all-features --no-fail-fast` — **FAIL**:
   `1874 tests run: 1775 passed, 99 failed, 0 skipped`.
   Loopback-denied groups include all named bootstrap resolution-rewalk/soft-fail fixtures; service
   HTTP fixtures in live_fanout, live_venue_adapter, organic_canary, supabase_reader,
   supabase_state, watchlist_capacity, watchlist_maintenance, activity/anchor/bucket/position
   scenarios; source fetcher/CLOB-history/fetch/reconciliation HTTP fixtures; and venue redemption
   and V2 fixtures. Each fails at local listener creation with `PermissionDenied` as documented by
   the task. Non-environment failures still requiring owners: source reconciliation
   `saturated_positions_terminal_offset_is_typed_incomplete`, plus stale PAPER expectations in
   execution-gate/runtime-config scenarios. The two dispatcher failures were subsequently removed
   with the retired scenario file. Four continuation-v3 PAPER fixture failures were subsequently
   fixed and pass together (4/4) by giving the shared bucket context receipt-bearing page evidence.
5. `cargo test --doc --workspace --all-features` — **PASS**; every crate reports zero failed.
6. `cargo metadata --locked --format-version 1 > /dev/null` — **PASS**, no output.
7. `bash -n scripts/deploy/*.sh scripts/paper_reset/*.sh`;
   `python3 scripts/check_sql_unfiltered_dml.py`;
   `bash scripts/paper_reset/test_activate_financial_era.sh` — **PASS**. Tail:
   `PASS: financial-era driver preserves read-only prepare, restores every pre-Start seam once, forces roll-forward after Start, shares process proofs, and uses catalog-derived restore equality`.
8. `git diff --check` — **PASS**, no output.
9. Retired-name sweep — **FAIL**, nonempty output summarized above.

Focused checks passed before the final gate: the exact changed-predecessor paper-state test (1/1),
all risk-engine tests (46/46), service risk/paper-log tests (14/14), and the four formerly failing
continuation-v3 bucket scenarios (4/4).

## Lane-summary discrepancies

- The supplied LEAF summary says `plan_budget_buy` and `modeled_kelly_price` were deleted and S4
  takes a caller-supplied Kelly allocator. T1 in this checkout shows all three claims are false:
  `crates/venue-polymarket/src/ladder.rs:83,309` and `crates/backtest/src/simulation.rs:242`.
- The lane-C checkpoint left `SealCheck` as an explicit temporary error and membership producers on
  direct publication, despite the checklist describing those consumers as ready for integration.
- The checkpoint described exact local shapes, but T1 retains parallel integer and Financial*
  families. Fractional restart safety is therefore not complete.
- Concurrent integration changed the checkout during verification; newly required source receipts
  and authority trait methods were reconciled in PAPERA-owned scenarios before the final successful
  workspace build.

`Cargo.lock` was not changed.

Shortcuts / hacks taken: none
