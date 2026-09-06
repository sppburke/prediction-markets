# Issue #545 LEAF integration summary

## Assigned checklist

- **RB1 — DONE.** Compact fee parsing now preserves raw numeric lexemes with `RawValue`, parses
  exact `Decimal` values, accepts every representable rate in `[0,1)`, and accepts only `r/e/to` in
  `crates/venue-polymarket/src/fee.rs:40-125`. The typed SDK helper and long-name alias convergence
  were deleted. Compact minimum-order and tick values use the same exact parser in
  `crates/venue-polymarket/src/canary_market.rs:108-130`.
- **RB3 / S3 — DONE.** `evaluate_at_price` no longer accepts or applies book/notional caps and
  returns Contract `N` unchanged in `crates/strategy-winner-follow/src/evaluate.rs:170-234`; the
  contract is documented in `crates/strategy-winner-follow/src/config.rs:52-61` and proved by
  `crates/strategy-winner-follow/tests/scenario_flat_sizing.rs:467-494`. Every cap remains for the
  venue planner.
- **RB4 — DONE.** Kelly sizing has one deterministic aggregate-fee owner in
  `crates/venue-polymarket/src/ladder.rs:307-385`: candidate size from one-share cost, one aggregate
  fee/cost recomputation, one resize, then the smaller size. Backtest calls the common
  `plan_sized_buy` adapter at `crates/backtest/src/simulation.rs:263-301,1044-1132`.
  The review boundary is pinned at `crates/backtest/src/simulation.rs:1970-2001` (66 contracts).
  `modeled_kelly_price` was deleted.
- **RB5 — DONE.** `OpenPosition` and `PositionLifetime` retain exact aggregate `all_in_debit` at
  `crates/backtest/src/simulation.rs:126-160`; resolution, sell, and mark-to-market close from that
  aggregate at `:633`, `:1245`, `:1339`, `:1824`, and `:1839`. The non-divisible `1.026659` golden is
  at `:2003-2039`. Average fill price remains display/exposure data only.
- **RB6 / B8 — DONE in LEAF scope.** `_GLOSSARY.md` is the first canonical owner: it defines the
  exact Financial15 set at `docs/_GLOSSARY.md:421-440`, the venue fee/sized-plan authority at
  `:938-959`, and the modeled backtest-only fee at `:938`. The sizing chain and cap ownership are
  consistent in `docs/19-WINNER-FOLLOW-STRATEGY.md:289-329`. Retired names are absent from every
  LEAF-owned code/doc surface; exact sweep evidence is below.
- **RD6 — DONE.** The ordinary risk-block count is nine at
  `docs/19-WINNER-FOLLOW-STRATEGY.md:442`.
- **E7 — DONE.** Bootstrap re-exports the venue-owned Standard V2, NegRisk V2, and OrderFilled topic
  constants instead of defining duplicates at `crates/bootstrap/src/chain.rs:24`; its aggregate
  address/topic sets consume those exports at `:39-55`. The required dependency is one-way
  `bootstrap -> venue-polymarket`.
- **S4 — DONE.** `plan_sized_buy` is the sole public production planner at
  `crates/venue-polymarket/src/ladder.rs:122-230`; fixed Contract sizing is handled without clamping
  at `:175-205`, and Kelly uses the same owner at `:307-385`. `plan_budget_buy` was deleted.
  `plan_exact_shares` is crate-private at `:458` and its only non-test caller is the canary adapter
  at `crates/venue-polymarket/src/canary_market.rs:318`.
- **RB7 venue tests — DONE.** Exact principal-plus-reserve equality and one-atomic-under failure are
  proved at `crates/venue-polymarket/src/ladder.rs:848-874`; the Kelly aggregate-fee boundary at
  `:876-908`; unchanged Contract quantity with improved execution at `:910-940`; and the existing
  canary fixture's byte-identical `ExecutableLadder` JSON at
  `crates/venue-polymarket/src/canary_market.rs:390-423`.
- **Documentation consistency — DONE.** Paper/live risk and promotion wording is reconciled at
  `docs/14-COMPLIANCE-AND-RISK.md:29-38,73-75`; the CLOB resolution/ranker route at
  `docs/26-DATA-REFRESH-AND-REOPTIMIZATION-RUNBOOK.md:43-55,88-96,125-137,157-159`; and the
  Legacy17 -> Start/schema-v3 -> Financial15 order at
  `docs/35-PE-SERVICE-DEPLOY-RUNBOOK.md:250-262`. Lower backtest/site methodology docs were updated
  where they still described fee-free or reconstructed-cost behavior.

All assigned implementation items are complete. Workspace-wide PAPER/LIVE consumer work is outside
LEAF ownership and remains the reason the merged integration tree does not yet pass the workspace
gate; exact blockers are recorded below.

## Deleted duplicates and temporary paths

- Deleted long-name compact-fee aliases, equal-alias convergence, the six-decimal rate restriction,
  and the public typed helper that bypassed raw presence/null validation.
- Deleted the public `plan_budget_buy` planner and made `plan_exact_shares` canary-only.
- Deleted strategy-side cap arithmetic, `book_cap_contracts`, the missing-book fail-open path, and
  Contract clamping.
- Deleted backtest `modeled_kelly_price`, its one-share proxy ownership, and all close-time
  reconstruction of aggregate cost from an average.
- Deleted bootstrap's duplicate V2 exchange addresses and OrderFilled topic.

No duplicate frozen implementation was retained.

## Retired-surface sweep

This exact LEAF-ownership sweep exited 1 with empty output, as required:

```text
rg -n -w 'polymarket_fee_rate|paper_fill_haircut_bps|paper_fill_slippage_bps|fill_mode|plan_budget_buy|estimated_ladder_spend|zeroed_risk_snapshot|PaperExecutor|FillSource|PaperExecutionError|prefix_vwap|modeled_kelly_price' crates/venue-polymarket crates/backtest crates/strategy-winner-follow/src/evaluate.rs crates/strategy-winner-follow/src/config.rs crates/strategy-winner-follow/tests/scenario_flat_sizing.rs crates/bootstrap/src/chain.rs docs
(no output)
```

The same repository-wide command is not empty yet: it finds PAPER/LIVE-owned legacy strategy,
execution-core, service, and migration surfaces. LEAF was expressly forbidden to edit those paths.
In particular, `execution-core` still reads `estimated_ladder_spend`; this is also the first
workspace-build blocker below. The schema-one private live-journal decoder may retain its legacy
field by design.

## Verification

The requested `/home/sean/git/pm-545/target` is outside this worktree's writable root. The first
Cargo attempt failed before compilation with:

```text
failed to open: /home/sean/git/pm-545/target/debug/.cargo-lock
Read-only file system (os error 30)
```

All executable Cargo checks therefore used
`CARGO_TARGET_DIR=/home/sean/git/pm-545-int2/target`.

- `rustc --version` — PASS: `rustc 1.95.0 (59807616e 2026-04-14)`.
- `cargo build --workspace --all-targets` — expected integration BLOCKER outside ownership. Tail:

  ```text
  error[E0432]: unresolved import `crate::live_journal::LiveFeeEvidence`
  error[E0609]: no field `estimated_ladder_spend` on type `&LadderPlanAudit`
  error: could not compile `pe-execution-core` (lib) due to previous errors
  ```

  The additional execution-core test-fixture errors are stale removed fee/hash fields and
  `estimated_ladder_spend`. LEAF crates do not depend on service, and the task explicitly directs
  per-crate gates instead of fixing these non-owned consumers.
- `cargo build -p pe-venue-polymarket -p pe-strategy-winner-follow -p pe-backtest -p pe-bootstrap --all-targets --all-features`
  — PASS. Tail: `Finished dev profile [unoptimized + debuginfo] target(s) in 1m 16s`.
- `cargo fmt --all` then `cargo fmt --all --check` — PASS, no output.
- `cargo clippy -p pe-venue-polymarket -p pe-strategy-winner-follow -p pe-backtest -p pe-bootstrap --all-targets --all-features -- -D warnings`
  — PASS. Tail: `Finished dev profile [unoptimized + debuginfo] target(s) in 14.84s`.
- `cargo nextest run -p pe-venue-polymarket --all-features --no-fail-fast` — environment-blocked
  only: `56 tests run: 39 passed, 17 failed`. All 17 failures stop at existing local-fixture
  `TcpListener::bind` calls with `PermissionDenied`: the 13 V2 tests
  `negrisk_market_preparation_binds_negrisk_exchange_and_hash`,
  `standard_market_preparation_binds_standard_exchange_and_hash`,
  `prepares_v2_fok_and_posts_exact_bytes_once`,
  `authenticated_reconciliation_timeout_retains_exact_request_identity`,
  `server_error_posts_once_and_fails_closed`,
  `account_probe_reads_allowances_without_order_or_trade_pagination`,
  `balance_timeout_retains_complete_authenticated_query`,
  `collateral_path_signs_fractional_minimum_shares_and_zero_builder`,
  `cursor_timeout_retains_the_exact_next_cursor_query`,
  `venue_rejection_posts_once_and_fails_closed`,
  `pending_reconciliation_captures_exact_order_hash_lookup`,
  `caller_timeout_does_not_retry_the_post_seam`, and
  `version_mismatch_fails_before_any_post`; plus redemption tests
  `relayer_auth_failure_is_typed_and_never_retried`,
  `proxy_and_safe_transports_confirm_through_legacy_status_contract`,
  `relayer_submit_and_confirm_captures_raw_evidence`, and
  `timeout_after_submit_is_ambiguous_and_reconcilable`.
- Focused network-free venue economics/parser/canary tests — PASS:
  `Summary: 22 tests run: 22 passed, 34 skipped`.
- `cargo nextest run -p pe-strategy-winner-follow --all-features --no-fail-fast` — two
  non-owned stale-test blockers: `31 tests run: 29 passed, 2 failed`. Both
  `scenario_unlimited_cap_yields_more_contracts_than_bps_cap` and
  `scenario_clamp_bps_cap_limits_contracts` live in `tests/scenario_strategy.rs`, outside the
  explicit LEAF test ownership, and still require the prohibited strategy cap clamp. The owned
  `scenario_flat_sizing` suite passes: `13 tests run: 13 passed`.
- `cargo nextest run -p pe-backtest --all-features --no-fail-fast` — PASS:
  `134 tests run: 134 passed`.
- `cargo nextest run -p pe-bootstrap --all-features --no-fail-fast` — environment-blocked only:
  `409 tests run: 399 passed, 10 failed`. The failures are all existing loopback fixtures:
  eight `scenario_resolution_rewalk_audit` tests
  (`audit_observes_schedule_inserted_by_gamma_in_the_same_run`,
  `audit_mixed_non_blocking_classes_pass_without_writes`,
  `audit_rejects_per_market_identity_mismatch`,
  `rebuild_deletes_midwalk_cursor_and_repopulates_from_page_one`,
  `rebuild_failure_before_page_one_leaves_no_cursor_row`,
  `reset_failure_before_page_one_leaves_no_cursor_row_and_exits_one`,
  `unresolved_audit_exits_75_and_logs_counts`,
  `walk_record_missing_closed_keeps_schedule_and_tokens`) and two
  `scenario_resolutions_soft_fail` tests (`gamma_only_soft_fails_after_empty_clob_page`,
  `resolutions_hard_fail_on_fatal_clob_response`). Each fails at `TcpListener::bind` with
  `PermissionDenied`.
- `cargo test --doc -p pe-venue-polymarket -p pe-strategy-winner-follow -p pe-backtest -p pe-bootstrap --all-features`
  — PASS; all four crates report `0 passed; 0 failed` doc tests.
- `cargo metadata --locked --format-version 1 > /dev/null` — PASS, no output.
- `cargo tree -p pe-bootstrap | rg 'pe-(bootstrap|venue-polymarket|strategy-winner-follow|service)'`
  — PASS; shows only `pe-bootstrap` and `pe-venue-polymarket` among those names, proving the intended
  edge and no service dependency/cycle.
- `bash -n scripts/deploy/*.sh scripts/paper_reset/*.sh`; then
  `python3 scripts/check_sql_unfiltered_dml.py`; then
  `bash scripts/paper_reset/test_activate_financial_era.sh` — PASS. Tail:

  ```text
  PASS: financial-era driver preserves read-only prepare, restores every pre-Start seam once,
  forces roll-forward after Start, shares process proofs, and uses catalog-derived restore equality
  ```

- `git diff --check` — PASS, no output.

Workspace clippy, nextest, and workspace doc tests cannot start until the non-owned
`pe-execution-core` compile failures above are reconciled. The complete LEAF build/clippy/doc gates,
all network-free venue tests, the owned strategy scenario, and all backtest tests pass.

## Dependency and lockfile changes

- `crates/bootstrap/Cargo.toml` adds the required direct `pe-venue-polymarket` dependency so E7 can
  use the venue-owned V2 constants. This is the sole change outside `chain.rs` needed to compile E7.
- `crates/venue-polymarket/Cargo.toml` adds `pe-kelly-sizer` so the venue planner can own the one
  Kelly convergence chain, and enables `serde_json/raw_value` for exact wire lexemes.
- `Cargo.lock` changed only for those two direct dependency edges (`pe-bootstrap ->
  pe-venue-polymarket` and `pe-venue-polymarket -> pe-kelly-sizer`).

## Lane-summary discrepancies found

- Lane B reported compact parsing through the SDK DTO, long-name aliases, and a six-place rate
  restriction; those violate lexical exactness and were replaced by the raw-lexeme owner.
- Lane B reported `plan_sized_buy` but left `plan_budget_buy` and strategy cap/clamp semantics active;
  the duplicate planner and clamps are now removed in LEAF scope.
- Lane B's backtest summary said aggregate fees were debited, but the code still sized Kelly from a
  one-share proxy and reconstructed closes from average price. Both reviewer counterexamples were
  reproducible and are now pinned by tests.
- Bootstrap duplicated venue V2 constants despite lane E assigning venue as owner; bootstrap now
  consumes the venue constants through the allowed dependency direction.
- The merged non-owned execution-core and strategy scenario consumers have not yet caught up with
  the frozen interfaces, despite individual lane summaries describing isolated green gates.

Shortcuts / hacks taken: none
