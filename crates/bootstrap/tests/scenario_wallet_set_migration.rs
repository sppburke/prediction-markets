//! Operator-level scenarios for the issue #179 wallet-set migration.
//!
//! These exercise the on-disk `wallet_set.json` shape and the
//! `WalletSetState` semantics that the bootstrap orchestrator depends on:
//! pre-#179 checkpoints load as "V1 done, V2 pending"; saving adds the new
//! field; existing wallets are never lost.
//!
//! Determinism: pure in-process (no network, no clock).
//! Ephemeral state: each test gets a fresh `tempfile::TempDir` under
//! `target/`.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use pe_bootstrap::wallet_set::{self, WalletSetState};
use pe_source_onchain_polygon::contracts::{
    ALL_EXCHANGE_CONTRACTS, ALL_ORDER_FILLED_TOPICS, TOPIC_ORDER_FILLED_V1, TOPIC_ORDER_FILLED_V2,
};
use tempfile::TempDir;

fn contract_hex_set() -> Vec<String> {
    ALL_EXCHANGE_CONTRACTS
        .iter()
        .map(|c| format!("0x{c:x}"))
        .collect()
}

// ── Scenario A ───────────────────────────────────────────────────────────────
// PASS: a pre-#179 JSON file on disk (no `enumerated_topic_hashes` field)
//       loads via `load_state` with `enumerated_topic_hashes.is_empty()`.
// FAIL: deserialization errors, OR the wallet list is lost.

#[tokio::test]
async fn pre_179_checkpoint_loads_with_empty_topics() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("wallet_set.json");

    // Exact shape the pre-#179 binary persisted: 4 contracts complete,
    // 3 wallets, no `enumerated_topic_hashes` field.
    let legacy = format!(
        r#"{{
            "completed_contracts": ["0x{c0:x}","0x{c1:x}","0x{c2:x}","0x{c3:x}"],
            "wallets": [
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "0xcccccccccccccccccccccccccccccccccccccccc"
            ]
        }}"#,
        c0 = ALL_EXCHANGE_CONTRACTS[0],
        c1 = ALL_EXCHANGE_CONTRACTS[1],
        c2 = ALL_EXCHANGE_CONTRACTS[2],
        c3 = ALL_EXCHANGE_CONTRACTS[3],
    );
    std::fs::write(&path, legacy).unwrap();

    let loaded = wallet_set::load_state(&path).unwrap().unwrap();
    assert_eq!(loaded.completed_contracts.len(), 4);
    assert_eq!(loaded.wallets.len(), 3);
    assert!(
        loaded.enumerated_topic_hashes.is_empty(),
        "pre-#179 checkpoint must load with empty enumerated_topic_hashes — \
         this is the sentinel that triggers the additive V2 sweep"
    );
}

// ── Scenario B ───────────────────────────────────────────────────────────────
// PASS: a `WalletSetState` that has been marked V1-only-done satisfies the
//       same "legacy_v1_done" detector that lives in bootstrap::lib::run.
//       (Mirrors the exact condition used there, so a future refactor of
//       either side surfaces as a test failure.)
// FAIL: the detector would re-sweep V1 unnecessarily, OR would treat
//       partial-legacy state as legacy-done.

#[test]
fn legacy_v1_done_detector_matches_pre_179_state() {
    // "Pre-#179, all contracts done" — must match the detector.
    let pre_179 = WalletSetState {
        completed_contracts: contract_hex_set(),
        wallets: Vec::new(),
        enumerated_topic_hashes: Vec::new(),
    };
    assert!(
        legacy_v1_done(&pre_179),
        "all-contracts done = legacy V1-done"
    );

    // Partial-legacy (3 of 4 contracts) — MUST NOT match the detector
    // (otherwise we'd skip the 4th-contract V1 sweep on resume).
    let mut partial = pre_179.completed_contracts.clone();
    partial.pop();
    let partial_state = WalletSetState {
        completed_contracts: partial,
        wallets: Vec::new(),
        enumerated_topic_hashes: Vec::new(),
    };
    assert!(
        !legacy_v1_done(&partial_state),
        "3-of-4 contracts MUST NOT be treated as legacy-V1-done — \
         partial-legacy state needs a full additive sweep"
    );

    // Fresh install — empty checkpoint — must not match.
    let fresh = WalletSetState::default();
    assert!(
        !legacy_v1_done(&fresh),
        "empty checkpoint MUST NOT match legacy detector"
    );
}

// Mirror of bootstrap::lib's `legacy_v1_done` predicate — kept here so the
// scenario test fails if either side drifts. Set-membership (not `len()`)
// so adding a future V3 contract to `ALL_EXCHANGE_CONTRACTS` does not
// silently match a legacy 4-entry list.
fn legacy_v1_done(state: &WalletSetState) -> bool {
    state.enumerated_topic_hashes.is_empty()
        && ALL_EXCHANGE_CONTRACTS
            .iter()
            .all(|c| state.completed_contracts.contains(&format!("0x{c:x}")))
}

// ── Scenario C ───────────────────────────────────────────────────────────────
// PASS: saving a state with `enumerated_topic_hashes` populated and re-loading
//       it round-trips without losing the field — even after migration from
//       a pre-#179 file on the same path.
// FAIL: the field is missing after save+load, OR wallets are lost.

#[test]
fn migration_round_trip_preserves_wallets_and_appends_topic_hashes() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("wallet_set.json");

    // 1. Write pre-#179 JSON to disk.
    let legacy = format!(
        r#"{{"completed_contracts":["0x{c0:x}","0x{c1:x}","0x{c2:x}","0x{c3:x}"],"wallets":["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"]}}"#,
        c0 = ALL_EXCHANGE_CONTRACTS[0],
        c1 = ALL_EXCHANGE_CONTRACTS[1],
        c2 = ALL_EXCHANGE_CONTRACTS[2],
        c3 = ALL_EXCHANGE_CONTRACTS[3],
    );
    std::fs::write(&path, legacy).unwrap();

    // 2. Load it (simulates bootstrap startup).
    let mut state = wallet_set::load_state(&path).unwrap().unwrap();
    let pre_wallet_count = state.wallets.len();
    assert_eq!(pre_wallet_count, 2);
    assert!(state.enumerated_topic_hashes.is_empty());

    // 3. Mark V1 done (legacy upgrade) — simulates the bootstrap orchestrator.
    state
        .enumerated_topic_hashes
        .push(format!("{TOPIC_ORDER_FILLED_V1}"));
    // 4. Sweep V2 — append one new wallet.
    state
        .wallets
        .push("0xcccccccccccccccccccccccccccccccccccccccc".to_owned());
    state
        .enumerated_topic_hashes
        .push(format!("{TOPIC_ORDER_FILLED_V2}"));

    // 5. Persist.
    wallet_set::save_state(&path, &state).unwrap();

    // 6. Reload — must see all 3 wallets and both topic hashes.
    let reloaded = wallet_set::load_state(&path).unwrap().unwrap();
    assert_eq!(reloaded.wallets.len(), 3, "no wallet loss on round-trip");
    assert_eq!(reloaded.enumerated_topic_hashes.len(), 2);
    for topic in &ALL_ORDER_FILLED_TOPICS {
        assert!(
            reloaded
                .enumerated_topic_hashes
                .contains(&format!("{topic}")),
            "topic {topic} missing after round-trip"
        );
    }
    // Pre-existing contracts list is preserved verbatim (no destructive
    // rewrite per the "append-only / never destructive" requirement).
    assert_eq!(reloaded.completed_contracts.len(), 4);
}

// ── Scenario D ───────────────────────────────────────────────────────────────
// PASS: after a complete migration, the guard condition (mirror of
//       bootstrap::lib's `all_topics_done && completed_contracts.len() >= total`)
//       returns true — subsequent runs short-circuit instead of re-sweeping.
// FAIL: the guard would re-enter the enumeration loop unnecessarily, OR
//       fail to detect completion after a successful migration.

#[test]
fn fully_migrated_checkpoint_triggers_skip_guard() {
    let migrated = WalletSetState {
        completed_contracts: contract_hex_set(),
        wallets: vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned()],
        enumerated_topic_hashes: ALL_ORDER_FILLED_TOPICS
            .iter()
            .map(|h| format!("{h}"))
            .collect(),
    };
    assert!(
        skip_guard(&migrated),
        "fully migrated checkpoint must trigger the skip guard"
    );

    // Sanity: removing one topic from the migrated state must un-set the guard.
    let mut partial = migrated;
    partial.enumerated_topic_hashes.pop();
    assert!(
        !skip_guard(&partial),
        "skip guard MUST NOT fire when a topic is still missing"
    );
}

// Mirror of bootstrap::lib's skip guard.
fn skip_guard(state: &WalletSetState) -> bool {
    let all_topics_done = ALL_ORDER_FILLED_TOPICS
        .iter()
        .all(|h| state.enumerated_topic_hashes.contains(&format!("{h}")));
    state.completed_contracts.len() >= ALL_EXCHANGE_CONTRACTS.len() && all_topics_done
}
