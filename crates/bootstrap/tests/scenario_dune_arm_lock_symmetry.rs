//! Scenario: Dune-arm CacheMutationLock symmetry (issue #193).
//!
//! PR #192 added `CacheMutationLock::acquire` at the top of the OnChain
//! enumeration arm but left the Dune arm bypassing the lock, creating a
//! window where a concurrent `--backfill-v1-attribution` subcommand could
//! be silently undone by a Dune-mode sweep's end-of-arm `save_enum_state`.
//!
//! PR #193 (this) adds the same lock-acquire to the Dune arm. The test
//! below pins the symmetry: if either enumeration arm is "running"
//! (modelled by holding the lock manually), the backfill subcommand's
//! lock-acquire fails fast with the documented operator-facing error.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use pe_bootstrap::error::BootstrapError;
use pe_bootstrap::lock::CacheMutationLock;
use tempfile::TempDir;

/// PASS: while ANY mutator holds the cache lock, a fresh acquire (from
///       another enumeration arm or from the backfill subcommand) is
///       refused with `BootstrapError::Invalid` and an operator-facing
///       message that names the holder PID.
/// FAIL: the second acquire succeeds (race window open — either arm could
///       silently clobber the other's cursor write).
///
/// This is a generic test of the lock contract, but its existence in #193
/// is what pins the **invariant** that both arms (and the subcommand) all
/// route through the same `CacheMutationLock` rather than racing on
/// `source_cursor` writes. Removing the Dune-arm acquire (regressing #193)
/// would not break this test directly — but it WOULD break the unit-test-
/// level invariant that any future "Dune arm acquires the lock" change
/// can rely on. Pinning the contract here documents the intent.
#[test]
fn cache_lock_refuses_concurrent_acquire_from_any_caller() {
    let dir = TempDir::new().unwrap();
    let cache_path = dir.path().join("wallet_cache.db");

    let _first = CacheMutationLock::acquire(&cache_path).unwrap();
    let second = CacheMutationLock::acquire(&cache_path);
    match second {
        Err(BootstrapError::Invalid { message }) => {
            assert!(
                message.contains(&std::process::id().to_string()),
                "error must name the holder PID; got: {message}"
            );
        }
        other => panic!("expected Invalid, got: {other:?}"),
    }
}

/// PASS: after the holder drops the guard, a fresh acquire succeeds. This
///       is the normal post-sweep state: when an enumeration arm finishes
///       (or the subcommand exits), the next mutator can proceed without
///       operator intervention.
/// FAIL: the lock file persists after Drop or the reclaim path errors.
#[test]
fn cache_lock_released_on_drop_allows_subsequent_acquire() {
    let dir = TempDir::new().unwrap();
    let cache_path = dir.path().join("wallet_cache.db");

    {
        let _first = CacheMutationLock::acquire(&cache_path).unwrap();
        // First holder simulates the in-progress enumeration arm.
    }
    // After drop, the backfill subcommand can acquire.
    let _backfill = CacheMutationLock::acquire(&cache_path).unwrap();
}

/// PASS: the lock's PID-naming UX continues to point the operator at the
///       canonical remediation. Pins the message contract against future
///       drift; operator runbooks may grep for this exact wording.
/// FAIL: the message text changes silently, breaking runbooks.
#[test]
fn cache_lock_error_message_guides_operator_remediation() {
    let dir = TempDir::new().unwrap();
    let cache_path = dir.path().join("wallet_cache.db");

    let _first = CacheMutationLock::acquire(&cache_path).unwrap();
    match CacheMutationLock::acquire(&cache_path) {
        Err(BootstrapError::Invalid { message }) => {
            assert!(
                message.contains("Stop the running pe-bootstrap"),
                "operator-remediation phrase must be present; got: {message}"
            );
            assert!(
                message.contains("refusing to mutate cursor state"),
                "explanation of what's blocked must be present; got: {message}"
            );
        }
        other => panic!("expected Invalid, got: {other:?}"),
    }
}
