//! Scenario: per-contract checkpoint for wallet enumeration.
//!
//! Tests cover the four observable behaviours of the checkpoint layer:
//!
//! 1. `scenario_fresh_start` — no file → default empty state, no contracts done.
//! 2. `scenario_partial_resume` — file with 1 of 4 contracts done → remaining
//!    3 contracts are not in `completed_contracts`, first IS skipped.
//! 3. `scenario_fully_cached` — file with all 4 contracts done → enumeration
//!    step is treated as complete; wallets are returned directly.
//! 4. `scenario_legacy_upgrade` — legacy bare-array file (PR #68 format) →
//!    `load_state` returns `None`, `load` returns the wallets, caller upgrades
//!    to checkpoint format and treats as fully enumerated.
//! 5. `scenario_dedup_across_contracts` — wallets appearing in multiple contracts
//!    are deduplicated in the final output.
//! 6. `scenario_checkpoint_atomic` — tmp file absent after a successful save.
//!
//! PASS criteria are stated per-scenario below.
//! No network calls; no live Etherscan requests.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use pe_bootstrap::wallet_set::{self, WalletSetState};
use pe_source_onchain_polygon::contracts::ALL_EXCHANGE_CONTRACTS;
use tempfile::TempDir;

// ─── helpers ─────────────────────────────────────────────────────────────────

fn all_contract_hexes() -> Vec<String> {
    ALL_EXCHANGE_CONTRACTS
        .iter()
        .map(|c| format!("0x{c:x}"))
        .collect()
}

fn write_legacy(dir: &TempDir, wallets: &[&str]) -> std::path::PathBuf {
    let path = dir.path().join("wallet_set.json");
    let json = serde_json::to_vec(wallets).unwrap();
    std::fs::write(&path, json).unwrap();
    path
}

fn write_state(dir: &TempDir, state: &WalletSetState) -> std::path::PathBuf {
    let path = dir.path().join("wallet_set.json");
    wallet_set::save_state(&path, state).unwrap();
    path
}

// ─── scenario 1: fresh start ─────────────────────────────────────────────────
//
// PASS: load_state returns None; WalletSetState::default() has zero completed
//       contracts and zero wallets.
// FAIL: any panic, or a non-empty default state.

#[test]
fn scenario_fresh_start() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("wallet_set.json");

    let result = wallet_set::load_state(&path).unwrap();
    assert!(result.is_none(), "no file → load_state must return None");

    let state = WalletSetState::default();
    assert!(state.completed_contracts.is_empty());
    assert!(state.wallets.is_empty());
    assert!(
        state.completed_contracts.len() < ALL_EXCHANGE_CONTRACTS.len(),
        "default state must not be treated as fully enumerated"
    );
}

// ─── scenario 2: partial resume ──────────────────────────────────────────────
//
// PASS: after writing a state with 1 contract done, load_state returns that
//       state; the first contract is present in completed_contracts; the
//       remaining 3 are absent.
// FAIL: any completed contract missing, or an absent contract falsely present.

#[test]
fn scenario_partial_resume() {
    let dir = TempDir::new().unwrap();
    let first_contract = format!("0x{:x}", ALL_EXCHANGE_CONTRACTS[0]);

    let partial = WalletSetState {
        completed_contracts: vec![first_contract.clone()],
        wallets: vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned()],
    };
    let path = write_state(&dir, &partial);

    let loaded = wallet_set::load_state(&path).unwrap().unwrap();
    assert_eq!(loaded.completed_contracts.len(), 1);
    assert!(
        loaded.completed_contracts.contains(&first_contract),
        "first contract must be present in checkpoint"
    );

    // The remaining contracts should NOT be in the checkpoint.
    let remaining: Vec<_> = ALL_EXCHANGE_CONTRACTS
        .iter()
        .skip(1)
        .map(|c| format!("0x{c:x}"))
        .filter(|h| !loaded.completed_contracts.contains(h))
        .collect();
    assert_eq!(
        remaining.len(),
        ALL_EXCHANGE_CONTRACTS.len() - 1,
        "3 contracts must remain un-enumerated"
    );
}

// ─── scenario 3: fully cached ────────────────────────────────────────────────
//
// PASS: state with all 4 contracts done → completed_contracts.len() equals
//       ALL_EXCHANGE_CONTRACTS.len(); wallets are returned as-is.
// FAIL: len check fails, or wallets differ from what was saved.

#[test]
fn scenario_fully_cached() {
    let dir = TempDir::new().unwrap();
    let full = WalletSetState {
        completed_contracts: all_contract_hexes(),
        wallets: vec![
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
        ],
    };
    let path = write_state(&dir, &full);

    let loaded = wallet_set::load_state(&path).unwrap().unwrap();
    assert_eq!(
        loaded.completed_contracts.len(),
        ALL_EXCHANGE_CONTRACTS.len(),
        "all 4 contracts must be present"
    );
    assert_eq!(loaded.wallets.len(), 2);

    // Simulate the lib.rs skip condition.
    let skip = loaded.completed_contracts.len() >= ALL_EXCHANGE_CONTRACTS.len();
    assert!(skip, "fully-cached state must trigger the skip condition");
}

// ─── scenario 4: legacy upgrade ──────────────────────────────────────────────
//
// PASS: a bare-array file (old format) → load_state returns None; load()
//       returns the wallets; upgraded state has all 4 contracts marked done.
// FAIL: load_state returns Some (it must not parse old format as new), or
//       upgraded state is missing contracts or wallets.

#[test]
fn scenario_legacy_upgrade() {
    let dir = TempDir::new().unwrap();
    let path = write_legacy(
        &dir,
        &[
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        ],
    );

    // load_state must NOT parse the old format.
    assert!(
        wallet_set::load_state(&path).unwrap().is_none(),
        "load_state must return None for legacy bare-array format"
    );

    // load() must successfully parse it.
    let legacy_wallets = wallet_set::load(&path).unwrap().unwrap();
    assert_eq!(legacy_wallets.len(), 2);

    // Simulate lib.rs upgrade: build new state, save, reload.
    let upgraded = WalletSetState {
        completed_contracts: all_contract_hexes(),
        wallets: legacy_wallets.iter().map(|w| w.to_string()).collect(),
    };
    wallet_set::save_state(&path, &upgraded).unwrap();

    let reloaded = wallet_set::load_state(&path).unwrap().unwrap();
    assert_eq!(
        reloaded.completed_contracts.len(),
        ALL_EXCHANGE_CONTRACTS.len(),
        "upgraded state must have all 4 contracts"
    );
    assert_eq!(reloaded.wallets.len(), 2, "wallets must be preserved");

    // After upgrade the skip condition must hold.
    assert!(reloaded.completed_contracts.len() >= ALL_EXCHANGE_CONTRACTS.len());
}

// ─── scenario 5: dedup across contracts ──────────────────────────────────────
//
// PASS: a state whose wallets list contains duplicates (same address from two
//       contracts) produces a deduplicated Vec after the lib.rs filter step.
// FAIL: duplicates survive into the final wallet list.

#[test]
fn scenario_dedup_across_contracts() {
    let dir = TempDir::new().unwrap();
    let dup_addr = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    // Simulate two contracts both emitting the same wallet.
    let state = WalletSetState {
        completed_contracts: all_contract_hexes(),
        wallets: vec![dup_addr.to_owned(), dup_addr.to_owned()],
    };
    let path = write_state(&dir, &state);

    let loaded = wallet_set::load_state(&path).unwrap().unwrap();

    // Replicate the dedup logic from lib.rs.
    let mut seen = std::collections::HashSet::new();
    let deduped: Vec<_> = loaded
        .wallets
        .iter()
        .filter(|h| seen.insert(h.as_str()))
        .collect();

    assert_eq!(
        deduped.len(),
        1,
        "duplicate wallet must be collapsed to one"
    );
}

// ─── scenario 6: atomic write ────────────────────────────────────────────────
//
// PASS: after save_state completes the final file exists and no tmp file lingers.
// FAIL: final file absent, or tmp file present after save.

#[test]
fn scenario_checkpoint_atomic() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("wallet_set.json");
    let state = WalletSetState {
        completed_contracts: vec!["0xabc".to_owned()],
        wallets: vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned()],
    };

    wallet_set::save_state(&path, &state).unwrap();

    assert!(path.exists(), "checkpoint file must exist after save");
    assert!(
        !path.with_extension("json.tmp").exists(),
        "tmp file must not linger after save"
    );

    // Round-trip sanity.
    let reloaded = wallet_set::load_state(&path).unwrap().unwrap();
    assert_eq!(reloaded.completed_contracts, state.completed_contracts);
}
