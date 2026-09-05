# Issue #545 LEAF integration summary — round 2

## Outcome

The dependency-direction violation is removed. `pe-venue-polymarket` no longer depends on
`pe-kelly-sizer`, while the venue planner still owns the single deterministic one-share → one
aggregate-fee recomputation → one resize → smaller-allocation chain. Kelly policy is supplied by
the caller as a closure over the exact all-in `Price`.

The two stale strategy-side cap scenarios were deleted because both asserted only the retired
strategy clamp. The surviving strategy scenario at
`crates/strategy-winner-follow/tests/scenario_flat_sizing.rs:467-494` proves the new strategy
contract: fixed `N` passes through unchanged even with a configured bps cap. Venue cap rejection is
proved at `crates/venue-polymarket/src/ladder.rs:208-226,849-877` as `CapExceeded`.

## What changed

- `crates/venue-polymarket/Cargo.toml:11-22` removes the `pe-kelly-sizer` dependency.
  `Cargo.lock:3929-3948` correspondingly contains no `pe-kelly-sizer` entry in the
  `pe-venue-polymarket` dependency list.
- `crates/venue-polymarket/src/ladder.rs:85-100` defines caller-owned `KellyAllocator` and changes
  `BuySizing::Kelly` to carry only that allocator and the venue all-in slippage input. Probability,
  Kelly fraction, and bankroll no longer cross into the venue crate.
- `crates/venue-polymarket/src/ladder.rs:120-226` routes the allocator through the common planner and
  retains the single principal-plus-reserve cap owner.
- `crates/venue-polymarket/src/ladder.rs:321-404` retains the deterministic fee chain: plan one share,
  call the allocator with its exact all-in price, calculate the candidate's aggregate fee/cost once,
  call the allocator once more, and accept the smaller allocation.
- `crates/venue-polymarket/src/ladder.rs:880-923` keeps the 66-share aggregate-fee boundary golden,
  now with a deterministic caller allocator that validates both exact prices and exactly two calls.
  The non-divisible debit golden remains at `crates/backtest/src/simulation.rs:2021-2025`, and the
  principal-plus-reserve equality/one-atomic-under golden remains at
  `crates/venue-polymarket/src/ladder.rs:849-877`.
- `crates/venue-polymarket/src/lib.rs:26-29` exports the allocator type with the planner API.
- `crates/backtest/src/simulation.rs:304-319` adapts `pe_kelly_sizer::size_contracts` into the
  caller-owned `ShareAmount` allocator. The production backtest call is at `:1057-1075`; its actual
  66-share Kelly boundary is at `:1987-2018`.
- `crates/strategy-winner-follow/tests/scenario_strategy.rs` deletes
  `scenario_clamp_bps_cap_limits_contracts` and
  `scenario_unlimited_cap_yields_more_contracts_than_bps_cap`, plus their now-unused
  `p_moderate` helper. They had no assertion beyond the removed strategy-side cap behavior, and
  retaining rewritten duplicates would overlap the existing pass-through and venue-cap tests named
  above.

## Final `BuySizing::Kelly` signature

```rust
/// Caller-owned allocation from an exact all-in price per share.
pub type KellyAllocator<'a> =
    &'a dyn Fn(Price) -> Result<ShareAmount, LadderError>;

pub enum BuySizing<'a> {
    Dollar { budget: CollateralAmount },
    Contract { contracts: u64 },
    Kelly {
        allocate: KellyAllocator<'a>,
        slippage_rate: Decimal,
    },
}

pub fn plan_sized_buy(
    // ...
    sizing: BuySizing<'_>,
    // ...
) -> Result<SizedBuyPlan, LadderError>;
```

Example caller closure using `pe-kelly-sizer` (the implemented backtest adapter is
`crates/backtest/src/simulation.rs:304-319`):

```rust
let allocate = |all_in_price: Price| {
    let contracts = pe_kelly_sizer::size_contracts(&pe_kelly_sizer::KellyInput {
        p: probability,
        c: all_in_price,
        kelly_fraction,
        bankroll,
    })
    .map_err(|_| LadderError::KellySizing)?;
    ShareAmount::from_whole(contracts.0).map_err(|_| LadderError::KellySizing)
};

let sizing = BuySizing::Kelly {
    allocate: &allocate,
    slippage_rate,
};
```

`probability`, `kelly_fraction`, and `bankroll` are captured entirely on the caller side. The venue
planner supplies the exact all-in `Price` to this closure twice according to the bounded chain.

## Verification

All Cargo commands used
`CARGO_TARGET_DIR=/home/sean/git/pm-545-int2/target`.

- `cargo build -p pe-venue-polymarket -p pe-strategy-winner-follow -p pe-backtest -p pe-bootstrap --all-targets --all-features`
  — PASS. Tail:

  ```text
     Compiling pe-bootstrap v0.1.0 (/home/sean/git/pm-545-int2/crates/bootstrap)
     Compiling pe-backtest v0.1.0 (/home/sean/git/pm-545-int2/crates/backtest)
      Finished `dev` profile [unoptimized + debuginfo] target(s) in 39.17s
  ```

- `cargo fmt --all --check` — PASS, no output.

- `cargo clippy -p pe-venue-polymarket -p pe-strategy-winner-follow -p pe-backtest -p pe-bootstrap --all-targets --all-features -- -D warnings`
  — PASS. Tail:

  ```text
      Checking pe-strategy-winner-follow v0.1.0 (/home/sean/git/pm-545-int2/crates/strategy-winner-follow)
      Checking pe-bootstrap v0.1.0 (/home/sean/git/pm-545-int2/crates/bootstrap)
      Checking pe-backtest v0.1.0 (/home/sean/git/pm-545-int2/crates/backtest)
      Finished `dev` profile [unoptimized + debuginfo] target(s) in 11.71s
  ```

- `cargo nextest run -p pe-venue-polymarket -p pe-strategy-winner-follow -p pe-backtest --all-features --no-fail-fast`
  — 202 tests PASS; the remaining 17 are environment-only loopback blockers. Tail:

  ```text
  Summary [   1.763s] 219 tests run: 202 passed, 17 failed, 0 skipped
  error: test run failed
  ```

  All 17 failures stopped at the existing loopback `TcpListener::bind` helpers with
  `PermissionDenied` / `Operation not permitted`:

  - `redemption::tests::proxy_and_safe_transports_confirm_through_legacy_status_contract`
  - `redemption::tests::relayer_auth_failure_is_typed_and_never_retried`
  - `redemption::tests::relayer_submit_and_confirm_captures_raw_evidence`
  - `redemption::tests::timeout_after_submit_is_ambiguous_and_reconcilable`
  - `v2::tests::authenticated_reconciliation_timeout_retains_exact_request_identity`
  - `v2::tests::account_probe_reads_allowances_without_order_or_trade_pagination`
  - `v2::tests::balance_timeout_retains_complete_authenticated_query`
  - `v2::tests::collateral_path_signs_fractional_minimum_shares_and_zero_builder`
  - `v2::tests::caller_timeout_does_not_retry_the_post_seam`
  - `v2::tests::cursor_timeout_retains_the_exact_next_cursor_query`
  - `v2::tests::negrisk_market_preparation_binds_negrisk_exchange_and_hash`
  - `v2::tests::prepares_v2_fok_and_posts_exact_bytes_once`
  - `v2::tests::pending_reconciliation_captures_exact_order_hash_lookup`
  - `v2::tests::server_error_posts_once_and_fails_closed`
  - `v2::tests::standard_market_preparation_binds_standard_exchange_and_hash`
  - `v2::tests::venue_rejection_posts_once_and_fails_closed`
  - `v2::tests::version_mismatch_fails_before_any_post`

  The affected focused tests pass independently:

  ```text
  pe-venue-polymarket ladder::tests::kelly_resizes_once_from_the_exact_aggregate_fee
  Summary [   0.005s] 1 test run: 1 passed, 55 skipped

  pe-backtest simulation::tests::kelly_uses_aggregate_fee_to_reproduce_the_smaller_size
  Summary [   0.006s] 1 test run: 1 passed, 133 skipped

  pe-strategy-winner-follow::scenario_strategy
  Summary [   0.011s] 7 tests run: 7 passed, 0 skipped
  ```

- `cargo metadata --locked --format-version 1 >/dev/null` — PASS, no output.

- `cargo tree -p pe-venue-polymarket -e normal | rg 'pe-(kelly|strategy|execution|service)'` —
  PASS: no output; exit 1 is the expected `rg` no-match status.

- `git diff --check` — PASS, no output.

- Toolchain evidence: `rustc 1.95.0 (59807616e 2026-04-14)`.

## Impact

- Dependency direction: fixed; the venue crate depends only on lower-layer/domain and adapter
  dependencies, not Kelly, strategy, execution, or service crates.
- Replay/economics: unchanged bounded aggregate-fee chain; exact money/share types remain in use.
- Risk: strategy pass-through is unchanged from accepted round 1; venue `plan_sized_buy` remains the
  sole monetary-cap owner and returns `CapExceeded` rather than clamping fixed allocations.
- Deployment/config: no new configuration or migration.
- Commits/staging: none.

Shortcuts / hacks taken: none
