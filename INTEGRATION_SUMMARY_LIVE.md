# Issue #545 LIVE integration summary

Started `2026-09-05 23:22:01 UTC`; stopped within the interruption-insurance budget. The prompt
named `/home/sean/git/pm-545`, but the assigned writable checkout is
`/home/sean/git/pm-545-int3` on `int/545-live`. The requested target directory is read-only in this
sandbox, so executable Cargo checks used `/home/sean/git/pm-545-int3/target` after recording the
exact failure.

## Assigned checklist

- **S1 — DONE.** `EconomicPrepared::compose` is the sole fee/reserve/all-in composer at
  `crates/execution-core/src/economic.rs:153`; the private live composer is absent. Focused economic
  tests passed 2/2.
- **B1 — DONE.** Ladder validation accepts improved expected quantity, floors aggregate expected
  spend to collateral precision, and requires only spend <= principal at
  `crates/execution-core/src/live_executor.rs:972`. The improved-quantity/signed-principal fixture is
  in the same file at `:1674`.
- **B2-live — DONE.** Admission and book readers receive `SourceLogHandle`; `/book` requires its
  receipt and the live path consumes derived quantity/spend/VWAP at
  `crates/service/src/live_fanout.rs:1051-1073,1581-1585`. The four-response append-once scenario is
  `crates/service/src/live_venue_adapter.rs:1307-1372`.
- **RE1 — DONE.** All three executor account gates use
  `economic.worst_case_all_in_debit()` and verify the stored balance audit at
  `crates/execution-core/src/live_executor.rs:406-490,964-969`; principal-plus-reserve boundary tests
  start at `:1720`.
- **RB2-live — DONE against the harvested lane-B API.** The path calls `plan_sized_buy`, supplies
  admitted tick/minimum and every monetary cap, and maps `CapExceeded` to the typed terminal reason
  at `crates/service/src/live_fanout.rs:1165-1194,1686-1699`. Both strategy evaluations are isolated
  behind the single named adapter `evaluate_live_candidate_at_price` at `:1374` for the primary's
  LEAF API reconciliation.
- **E1 — DONE.** Main passes the configured Polygon URL, the existing client, and source-log handle
  at `crates/service/src/main.rs:1051-1076`; fanout owns one `PolygonReceiptRpc` and the arming probe
  checks chain 137 plus finalized-head support at `crates/service/src/live_fanout.rs:190-198,3397`.
- **E2 — DONE.** Recovery gathers relevant targets into one `collect_order_finality` batch and
  applies Pending/Conflict/Finalized effects only after synchronized journal append at
  `crates/service/src/live_fanout.rs:827-922`.
- **RE2 — DONE.** One exact Prepared-to-Posted/Reconciled/Finalized matcher binds full identity,
  order hash, prepared sequence, account envelope, and allowed source/outcome pairs at
  `crates/service/src/live_fanout.rs:1802-1845`; reducer and recovery use it at
  `:1920-1961,2626-2673` before state changes.
- **RE3 — DONE.** Custody replay calls `reconstruct_redemption_attempts` over the causal prefix and
  requires exact `ConfirmedAwaitingBalance`; identical duplicate custody converges and changed facts
  conflict at `crates/service/src/live_fanout.rs:2091-2130`. The corrected scenario first proves the
  no-attempt rejection, then supplies transaction/confirmed receipt at `:6059-6156`.
- **RE4 — DONE.** Raw receipt logs enforce unique `(transaction_hash, log_index)` with identical raw
  hashes before any filter at `crates/venue-polymarket/src/receipt.rs:157-179`. The filtered-order
  collision fixture passes at `crates/venue-polymarket/tests/receipt.rs:207`.
- **E3 — DONE.** Fanout carries the source-log handle; complete-position and resolution responses
  append before use and retain real receipts at `crates/service/src/live_fanout.rs:4415-4467,3814-3847`.
- **E4 — DONE.** The sequential account pass uses `fetch_complete_positions`, verified admission
  mappings, both partitions/negative-risk evidence, and compares complete inventory before Baseline,
  marks, and posts at `crates/service/src/live_fanout.rs:4490-4542,3417-3590,1260-1267`.
  `fetch_redeemable_positions*` is absent.
- **RE5/E5 — DONE.** Baseline/Daily facts are appended in the sequential pass; replay requires
  increasing UTC-midnight cutoffs and reuses `historical_mark_price` for every sample's
  `[cutoff-120, cutoff]` proof at `crates/service/src/live_fanout.rs:3417-3590,2132-2163`. Strict live
  risk uses current mids, reducer equity/marks/realized closes, and durable `OrderPosted` prior-hour
  latency windows at `:1394-1554`. No zeroed risk builder remains.
- **E6 — DONE.** One per-condition CLOB discovery is source-appended, admission token mapping is
  verified, and `ResolutionFinalized` is appended at
  `crates/service/src/live_fanout.rs:3749-3906`; confirmed custody append is at `:4260-4335`.
- **RE6 — DONE.** Verified ordinary insertion checks reverse outcome reuse across both resolved and
  unresolved mappings at `crates/source-polymarket-public/src/reconciliation.rs:536-553`; the
  duplicate-stamped unresolved test passes at `:1071`.
- **RE8 — DONE.** Pending/conflict evidence contains only chain/head, the order's receipts, and its
  relevant heights at `crates/service/src/live_fanout.rs:481-526`. Both service-local hash loops were
  replaced with exported `live_journal::http_attempt_hashes` at
  `crates/execution-core/src/live_journal.rs:972` and live callers `:705,719,2835`.
- **RE9 — DONE.** POST accepts only `transactionHashes`, authenticated trades only
  `transaction_hash`, and receipt preparation requires exact `BUY` at
  `crates/service/src/live_venue_adapter.rs:483-489,578-585` and
  `crates/venue-polymarket/src/receipt.rs:236`.
- **B7 — DONE.** `LadderPlanAudit::expected_spend` retains aggregate negative-infinity collateral
  flooring at `crates/execution-core/src/live_journal.rs:184-201` alongside the final journal work.
- **RB8-live — DONE.** Current live economics contain no senderless direct-trade channel, raw
  admission identity hashes, economic `MarketEndCache`, fabricated snapshot, prefix rewalk, or
  redeemable-only reader. Removed hashes/spend remain solely in the explicit private schema-one
  decoder at `crates/execution-core/src/live_journal.rs:786-870`, as required for readability.
- **RB7-live — DONE for LIVE-owned fixtures.** Four-response append-once, legacy OrderPrepared,
  byte-identical canary (LEAF-owned), multi-level planner/signer/finality agreement, exact reserve
  equality, frozen aggregate receipts, and live latency windows are present at
  `crates/service/src/live_venue_adapter.rs:1307`,
  `crates/execution-core/src/live_journal.rs:1056`, and
  `crates/service/src/live_fanout.rs:4699,5470,5584`.
- **RE10 — DONE (execution of service fixtures is gate-blocked).** Added principal-plus-reserve account edges, filtered raw-log collision,
  requested-transaction/nonzero-high-word checks, cash-drift rejection, aggregate half-payout and
  losing-resolution reducer cases, confirmed custody convergence, and an old-bigint migration scenario at
  `crates/execution-core/src/live_executor.rs:1720`,
  `crates/venue-polymarket/tests/receipt.rs:207-258`,
  `crates/service/src/live_fanout.rs:5959,6059`, and
  `scripts/test_multi_account_schema.sh:103-116`. Aggregate half-payout/losing-resolution live
  reducer coverage is in `crates/service/src/live_fanout.rs:6030`; schema-one matched
  OrderReconciled and all three redemption transitions extend the legacy fixture at
  `crates/execution-core/src/live_journal.rs:1227-1348` and pass focused replay. The service test
  target cannot compile until PAPER-owned exhaustive matches and OrderBook
  fixtures are reconciled.

## Deleted duplicate/temporary paths

- Deleted finality's cross-order `all_evidence` collector.
- Deleted both service-local HTTP-attempt hash implementations.
- Deleted POST `transaction_hashes`, authenticated `transactionHash`, and case-insensitive BUY
  aliases.
- The checkpoint had already deleted `build_economic_prepared`, `zeroed_risk_snapshot`, and
  `fetch_redeemable_positions*`; they were verified absent rather than recreated.

## Retired-surface evidence

The LIVE ownership sweep exited with empty output:

```text
rg -n -w 'polymarket_fee_rate|paper_fill_haircut_bps|paper_fill_slippage_bps|fill_mode|plan_budget_buy|estimated_ladder_spend|zeroed_risk_snapshot|PaperExecutor|FillSource|PaperExecutionError|prefix_vwap|modeled_kelly_price|MarketEndCache|fetch_redeemable_positions|build_economic_prepared' crates/service/src/live_fanout.rs crates/service/src/live_venue_adapter.rs crates/service/src/live_projections.rs crates/service/src/clob_book.rs crates/execution-core/src/economic.rs crates/execution-core/src/live_executor.rs crates/execution-core/src/redemption_machine.rs crates/source-polymarket-public/src/reconciliation.rs crates/source-polymarket-public/src/live_admission.rs crates/venue-polymarket/src/receipt.rs
(no output)
```

The required repository-wide sweep is **not empty**: the harvested tree still has non-owned
`plan_budget_buy` in `venue-polymarket`, `modeled_kelly_price` in backtest, one snapshot-worker
comment, intended Legacy17/migration spellings, and `estimated_ladder_spend` only in the private
schema-one decoder. This contradicts the supplied LEAF summary and cannot be corrected within LIVE
ownership.

## Verification

1. Requested target: `CARGO_TARGET_DIR=/home/sean/git/pm-545/target cargo build --workspace
   --all-targets` — environment blocked: `failed to open .../.cargo-lock: Read-only file system`.
   Writable-target rerun — blocked by non-owned service test compilation. Tail:
   `OrderBook missing source_receipt`; non-exhaustive `OrchestratorControl` matches in
   `watchlist_maintenance.rs` and `watchlist_capacity.rs`; `could not compile pe-service (lib test)`.
2. `cargo fmt --all` ran successfully. `cargo fmt --all --check` initially passed; after restoring
   formatter-only changes to non-owned PAPER files, the final check reports only those pre-existing
   formatting diffs in `main.rs`, `orchestrator.rs`, `risk_inputs.rs`, and `trade_poller.rs`.
3. Workspace clippy is blocked first by non-owned
   `strategy-winner-follow/src/lib.rs:6-7` `doc_lazy_continuation`. Ownership-focused
   `source-polymarket-public` + `venue-polymarket` clippy passed; `execution-core` passed; and
   `cargo clippy -p pe-service --lib --no-deps -- -D warnings` passed:
   `Finished dev profile ... in 34.98s`; final execution-core no-deps clippy also passed in 15.18s.
4. Workspace nextest could not build tests. Tail includes missing `OrderBook.source_receipt` in
   `scenario_{paper_state,clob_book,snapshot_capture,dispatch,activity_ws,runtime_config}`, stale
   `RuntimeConfig.polymarket_fee_rate`, missing `market_end_cache`, and PAPER exhaustive matches;
   no tests ran, so there are no loopback-only failures to classify. Focused network-free tests:
   economic 2/2 PASS; executor additions 3/3 PASS; receipt 6/6 PASS; reconciliation inserter 2/2 PASS.
5. `cargo test --doc --workspace --all-features` — PASS; every crate reports zero failed.
6. `cargo metadata --locked --format-version 1 > /dev/null` — PASS; `Cargo.lock` unchanged.
7. `bash -n scripts/deploy/*.sh scripts/paper_reset/*.sh`; SQL DML guard; financial-era harness —
   PASS. Tail: `PASS: financial-era driver preserves read-only prepare ... catalog-derived restore equality`.
8. `git diff --check` — PASS, no output.
9. Retired sweeps — LIVE sweep empty; repository-wide sweep nonempty for the exact discrepancies
   listed above.

`scripts/test_multi_account_schema.sh` was syntax-checked but its new PostgreSQL bigint migration
scenario was not executed because no disposable PostgreSQL URL was supplied.

## Lane-summary discrepancies

- The supplied LEAF summary says `plan_budget_buy` and `modeled_kelly_price` were deleted and the
  strategy API lost cap arguments. T1 in this checkout shows all three remain. LIVE calls only
  `plan_sized_buy` and isolates the old strategy API in one named helper for reconciliation.
- The merged PAPER/service fixtures are not at the claimed cross-lane compile state: several still
  construct `OrderBook` without `source_receipt`, retain a removed runtime field, omit five control
  variants, and the binary has a missing `market_end_cache` binding.
- The legacy journal decoder intentionally retains retired wrapper fields; no current-schema record
  uses them.

Shortcuts / hacks taken: none.
