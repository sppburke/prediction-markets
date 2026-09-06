# PAPERB integration summary — issue #545

Work ran in the writable checkout `/home/sean/git/pm-545-int4` with
`CARGO_TARGET_DIR=/home/sean/git/pm-545-int4/target`; the requested `/home/sean/git/pm-545`
checkout was outside this worker's writable root. Start: `2026-09-05 23:30:42 UTC`.

## Checklist

- RF8 — DONE: an empty source prefix returns an empty map; a nonempty prefix requires its exact
  sequence/hash (`crates/service/src/qualification.rs:999`).
- RF10 — DONE: the shared scanner rejects every second physical Start, including an identical
  payload; `paper_era` preserves the first defensively (`crates/service/src/paper_recovery.rs:423`,
  test at `:958`).
- RF1 — DONE: `scan_paper_log` is the sole Prepared/Final sequencing reducer; qualification retains
  only validated payloads needed for replay and rejects an unmatched Prepared at the sealed bound
  (`crates/service/src/qualification.rs:354,622`).
- RF2 — DONE: qualification reconstructs `LiveAdmissionArtifact` and `LadderPlan`, recomposes through
  `EconomicPrepared::compose`, compares core hashes, and independently enforces cash, budget, band,
  chase, and impact caps (`crates/service/src/qualification.rs:1040`).
- RF6 — DONE: resolution verification requires the receipt inside the sealed source prefix, reuses
  the existing CLOB envelope/source/schema/condition/payout validator, derives settlement time from
  the envelope, and calls `aggregate_resolution_credit` with `BinaryPayout` directly
  (`crates/service/src/qualification.rs:476`).
- RF4 / RF5 — DONE: decision and fill Final are a bijection by exact receipt; both sides rebuild the
  strategy idempotency key and compare wallet, source trade, bucket, market, outcome, and side. Final
  economics are already compared to the Prepared canonical result (`crates/service/src/qualification.rs:1490`).
- RF7 — DONE: mark replay binds the financial prefix, canonical boundary, source tail, midnight,
  receipt interval and uniqueness; raw historical response points are decoded exactly and selected
  through D's `historical_mark_price` owner (`crates/service/src/qualification.rs:1235,1387`).
- RF9 — DONE: replay ends at the seal's exact financial prefix; the seal must be its immediate chained
  successor and its cutoff must lie between Start and seal receipt times (`crates/service/src/qualification.rs:907`).
- RF3 — NOT DONE: D1 `build_paper_risk_snapshot` exists, but `EconomicPrepared` does not retain the
  complete current-position price receipt set or the risk-evaluation clock. Exact historical
  reconstruction cannot be distinguished from choosing nearby source frames; guessing would violate
  replay equality.
- RF17 — DONE: descriptive era drawdown starts with `QualificationStarted.starting_bankroll`
  (`crates/service/src/qualification.rs:650`).
- RF18 — DONE: a valid `InsufficientEvidence` seal returns a deterministic report with Start/Seal
  receipts, prefixes, reason and semantic identities before pass-gate calculations
  (`crates/service/src/qualification.rs:793`).
- RF19 — DONE: the private p95 and redundant `CompletedFill.final_sequence` were deleted; the shared
  risk p95 and `final_receipt.sequence` are used (`crates/service/src/qualification.rs:678,1490`).
- F1 — DONE: Start invokes lane C's one-transaction schema-v3 reset API for the five financial
  tables, bankroll, Start identity and last-Prepared metadata (`crates/service/src/qualification.rs:1824`,
  `crates/paper-state/src/lib.rs:3011`).
- F6 — NOT DONE: lane E did not export an all-account read-only open-order inventory. Its `replay_all`
  and schema-one `OrderPrepared` identity decoder remain private in execution-core; adding another
  legacy decoder here would violate the one-reader rule.
- RF14 — DONE: stop and inert proof now precede backup/census and the sole authoritative preparation
  (`scripts/paper_reset/activate_financial_era.sh:215-248`).
- RF11 — DONE: Start detection has complete / proven-absent / error-unknown outcomes; only proven
  absence authorizes rollback (`scripts/paper_reset/activate_financial_era.sh:81,193`).
- RF12 — DONE for the forward path: every entry scans physical Start before reset, unknown is fatal,
  no-Start guarded entry requires an inert service, archive/Start/service-start intents are durable,
  and physical Start forces roll-forward without repeating archive (`scripts/paper_reset/activate_financial_era.sh:193,257-290`).
- RF13 — NOT DONE: rollback still needs separate archive-restored, local-restored and old-service-started
  receipts plus stop/inert proof before every retryable restore.
- RF15 — NOT DONE: the driver does not yet invoke `--verify-staged-identity`, derive effective staged
  configuration identities, or share the Legacy17 callable/grant inventory from rehearsal preflight.
- RF16 — NOT DONE: rollback records but does not consume `remote_census` and guarded log identities;
  financial restore proves counts, not catalog-ordered bidirectional `EXCEPT ALL` equality.
- F7 — DONE: after physical Start, the driver applies `supabase_paper_state_schema.sql` (new v2
  signatures, numeric positions and `seed_financial_start`) before `migrate_service_config_545.sql`
  and before artifact adoption (`scripts/paper_reset/activate_financial_era.sh:281-286`).
- F8 — NOT DONE: `verified` checks installed hashes and fresh readiness only; it does not yet assert
  Start/reset/source/replay/ranking/membership/accounts-off equality.
- RF20 / F13 — NOT DONE: the network-free harness now covers stop-before-prepare, unknown Start,
  remote archive, physical Start, rollback refusal, and crash after service start. The remaining
  line-by-line rollback/adoption seams and PostgreSQL fractional archive/restore/parity matrix need a
  PostgreSQL-capable final gate (`scripts/paper_reset/test_activate_financial_era.sh:223-309`).

## Deleted duplicate and temporary paths

- Deleted qualification's second Prepared/Final state machine (`PreparedFact`, completed set,
  local authority cursor and Prepared lookup); the scanner owns sequencing.
- Deleted the local resolution-credit wrapper; S2 owns payout validation/arithmetic.
- Deleted private `nearest_rank_p95` and duplicate fill sequence storage.
- Deleted acceptance of identical duplicate physical Starts.
- Replaced prepare-before-stop, two-outcome Start detection, and unguarded repeated archive/Start
  behavior with the durable driver flow above.

## Retired-surface sweep

PAPERB-owned scope is empty (exit 1, no output):

```text
rg -n -w 'polymarket_fee_rate|paper_fill_haircut_bps|paper_fill_slippage_bps|fill_mode|plan_budget_buy|estimated_ladder_spend|zeroed_risk_snapshot|PaperExecutor|FillSource|PaperExecutionError|prefix_vwap|modeled_kelly_price' crates/service/src/qualification.rs crates/service/src/paper_recovery.rs scripts/paper_reset
(no output)
```

The required repository sweep is NOT empty. It finds `plan_budget_buy` in
`venue-polymarket/src/{ladder,lib}.rs`, `modeled_kelly_price` in
`backtest/src/simulation.rs`, legacy runtime names in `service/src/runtime_config.rs`, a stale
`snapshot_worker.rs` comment, migration retirement references, and the intentionally private legacy
`estimated_ladder_spend` decoder. These files are outside PAPERB ownership. Full output is in
`/tmp/paperb-retired.log` for this worker lifetime.

## Verification

Commands ran in the requested order after the edits.

1. `cargo build --workspace --all-targets` — FAIL, three non-owned stale test consumers:

   ```text
   watchlist_maintenance.rs:1217: non-exhaustive OrchestratorControl match
   snapshot_worker.rs:282: missing OrderBook.source_receipt
   watchlist_capacity.rs:463: non-exhaustive OrchestratorControl match
   error: could not compile pe-service (lib test) due to 3 previous errors
   ```

   `cargo check -p pe-service --lib` separately PASSed: `Finished dev profile ... in 3.18s`.

2. `cargo fmt --all`; `cargo fmt --all --check` — PASS, no output.

3. `cargo clippy --workspace --all-targets --all-features -- -D warnings` — FAIL before PAPERB
   compilation on two non-owned `clippy::doc_lazy_continuation` errors in
   `strategy-winner-follow/src/lib.rs:6-7`.

4. `cargo nextest run --workspace --all-features --no-fail-fast` — could not start tests because the
   same three service lib-test compile errors from step 1 stopped `cargo test --no-run` (exit 101).
   No loopback-test result was reached.

5. `cargo test --doc --workspace --all-features` — PASS. Tail:

   ```text
   Doc-tests pe_venue_polymarket
   running 0 tests
   test result: ok. 0 passed; 0 failed; 0 ignored
   ```

6. `cargo metadata --locked --format-version 1 > /dev/null` — PASS, no output.

7. Shell/SQL/driver checks — PASS:

   ```text
   bash_syntax_status=0
   sql_guard_status=0
   PASS: financial-era driver preserves read-only prepare, restores every pre-Start seam once,
   forces roll-forward after Start, shares process proofs, and uses catalog-derived restore equality
   financial_harness_status=0
   ```

8. `git diff --check` — PASS, no output.

9. Retirement sweep — PAPERB scope empty; repository scope nonempty as listed above.

No `Cargo.lock` change was made. No PostgreSQL test was available or attempted by this worker.

## Lane-summary discrepancies

- The supplied LEAF summary says `plan_budget_buy` and `modeled_kelly_price` were deleted, but both
  are present in this integration checkout (T1 paths above).
- The expected lane-E read-only live-order inventory is absent; only private execution-core replay
  and account-scoped replay are present.
- D1 is present, contrary to the conditional warning in the assignment, but its exact offline price
  and clock preimages are not durable in `EconomicPrepared`.
- The merged tree still contains the non-owned stale exhaustive control matches and missing book
  receipt fixture that prevent the advertised whole-workspace compile.

Shortcuts / hacks taken: none
