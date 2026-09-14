#![cfg(feature = "scenario")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

//! The activation seam: what `--financial-era=prepare` really produces, versus what
//! `scripts/paper_reset/activate_financial_era.sh` assumes it produces.
//!
//! No test has ever fed the real binary's prepare output to the real driver. The shell scenario
//! suite stubs the Rust side (`test_activate_financial_era.sh` shims `--financial-era=prepare` and
//! `--financial-era=start`), and the AC10 rehearsal exercises the Rust side without driving the
//! activation script. Every defect found in the `guarded → started → verified` audit (#628) lives
//! on that seam, which is why an entirely green suite sat on top of a path that cannot complete.
//!
//! These tests stand on the Rust side and pin what the real `MembershipProofBinding` is. They
//! assert its CONTENT, not merely its size and syntax: an oversized constant envelope carrying no
//! membership evidence would otherwise satisfy a size-and-shape check, which is the same vacuous
//! pass this file exists to make impossible.
//!
//! Document sizes are the measured live `gen/g557/paper_state.db` distribution (2026-09-14), used
//! as synthetic padding — no production data is copied:
//!
//! | column                                      | max         | mean      |
//! |---------------------------------------------|-------------|-----------|
//! | `position_validations.proof_json`           | 2,027,360 B | 341,056 B |
//! | `position_validations.activity_bounds_json` |   591,145 B |  46,880 B |
//! | `position_anchors.balances_json`            |   171,106 B |  17,368 B |
//!
//! For the then-live 26-wallet membership the reconstructed binding was 21,711,795 bytes.

use pe_core_types::WalletAddress;
use pe_paper_state::{AnchorInstallRecord, PaperStateDb, WalletHistoryStatusRecord};
use pe_service::qualification::scenario_membership_proofs_hash;

/// Linux caps one `argv` element at 32 pages, independent of the much larger `ARG_MAX`. The driver
/// passes the whole preparation as a single argument to `python3`
/// (`scripts/paper_reset/activate_financial_era.sh:945`), so this is the ceiling that matters.
const MAX_ARG_STRLEN: usize = 131_072;

/// Measured mean of `position_validations.proof_json`.
const MEAN_VALIDATION_PROOF_BYTES: usize = 341_056;
/// Measured mean of `position_validations.activity_bounds_json`.
const MEAN_ACTIVITY_BOUNDS_BYTES: usize = 46_880;

const NOW: i64 = 1_789_000_000;

fn wallet(index: u8) -> WalletAddress {
    WalletAddress::from_hex(&format!("0x{:040x}", u32::from(index))).unwrap()
}

/// A JSON document whose SERIALIZED length is exactly `bytes` (the padding is shorter by the
/// envelope overhead — the distinction matters when a size is quoted as evidence).
fn document(bytes: usize, tag: &str) -> String {
    let prefix = format!("{{\"tag\":\"{tag}\",\"pad\":\"");
    let suffix = "\"}";
    let pad = bytes.saturating_sub(prefix.len() + suffix.len());
    let doc = format!("{prefix}{}{suffix}", "a".repeat(pad));
    debug_assert_eq!(doc.len(), bytes.max(prefix.len() + suffix.len()));
    doc
}

/// Build a paper state whose members carry production-sized proof documents.
///
/// History is recorded first and the anchor install then writes the anchor, its validation and the
/// poll-cursor update in one transaction (`history_status: None` here, so this fixture does not
/// exercise the install's optional history write). Either way the cross-checks in
/// `MembershipProofManifest::capture` are satisfied by construction rather than by hand-maintained
/// agreement between separately written rows — which is how the shell fixtures drifted.
fn state_with_sized_members(
    count: u8,
    proof_bytes: usize,
) -> (tempfile::TempDir, PaperStateDb, Vec<WalletAddress>) {
    let dir = tempfile::tempdir().unwrap();
    let state = PaperStateDb::open(&dir.path().join("paper.db")).unwrap();
    let mut members = Vec::new();
    for index in 0..count {
        let member = wallet(index + 1);
        state
            .record_reconciled_history_status(&WalletHistoryStatusRecord {
                wallet: member,
                complete: true,
                proof_json: "{\"history\":true}".to_owned(),
                updated_at_unix: NOW,
            })
            .unwrap();
        state.set_cursor(&member, NOW).unwrap();
        // The document is stored on BOTH the anchor and its validation, and capture retains both
        // preimages, so each member contributes about twice this document to the binding.
        state
            .install_anchors(&[AnchorInstallRecord {
                history_status: None,
                wallet: member,
                balances: Vec::new(),
                activity_cutoff_unix: NOW,
                anchored_at_unix: NOW,
                ledger_hash_after: format!("ledger-{index}"),
                positions_proof_hash: format!("positions-{index}"),
                activity_bounds_json: document(MEAN_ACTIVITY_BOUNDS_BYTES, "bounds"),
                source_log_generation: "g557".to_owned(),
                proof_json: document(proof_bytes, &format!("proof-{index}")),
                recorded_at_unix: NOW,
            }])
            .unwrap();
        members.push(member);
    }
    (dir, state, members)
}

fn parse(binding: &str) -> serde_json::Value {
    serde_json::from_str(binding).expect("the binding is serialized JSON, not an opaque digest")
}

/// #626: the preparation cannot survive the driver's single-argument transport.
///
/// Three members at the measured mean clear the limit by an order of magnitude; the live 26-member
/// binding measured 21,711,795 bytes, about 165x. Deterministic at production scale.
#[test]
fn membership_binding_exceeds_the_driver_single_argv_limit() {
    let (_dir, state, members) = state_with_sized_members(3, MEAN_VALIDATION_PROOF_BYTES);

    let binding = scenario_membership_proofs_hash(&state, &members).unwrap();

    assert!(
        binding.len() > MAX_ARG_STRLEN,
        "binding is {} bytes, which would fit MAX_ARG_STRLEN ({MAX_ARG_STRLEN}); if production \
         documents really did shrink this far, re-measure before trusting the driver's transport",
        binding.len()
    );
}

/// #625: `activate_financial_era.sh:1081` requires this field to match `^[0-9a-f]{64}$` before it
/// will record `verified`. The real value is the serialized binding, so that check can never pass.
///
/// This also asserts the binding's CONTENT. Size and syntax alone would be satisfied by a constant
/// oversized envelope carrying no evidence at all.
#[test]
fn membership_binding_is_the_full_proof_envelope_not_the_bare_digest() {
    let (_dir, state, members) = state_with_sized_members(2, 4_096);

    let binding = scenario_membership_proofs_hash(&state, &members).unwrap();

    let is_bare_digest = binding.len() == 64
        && binding
            .chars()
            .all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c));
    assert!(
        !is_bare_digest,
        "binding is a bare 64-hex digest, so the driver's regex would pass; the seam this test \
         guards has closed and the driver check should be revisited"
    );

    let parsed = parse(&binding);
    let digest = parsed
        .get("proof_hash")
        .and_then(serde_json::Value::as_str)
        .expect("binding carries its digest under `proof_hash`");
    assert_eq!(
        digest.len(),
        64,
        "digest is not 64 hex characters: {digest}"
    );
    assert!(
        digest
            .chars()
            .all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c)),
        "digest is not lowercase hex: {digest}"
    );

    // The evidence itself must be present, in order, with each member's retained documents.
    let manifest = parsed
        .get("manifest")
        .expect("binding embeds the proof manifest");
    let carried: Vec<&str> = manifest
        .get("membership")
        .and_then(serde_json::Value::as_array)
        .expect("manifest carries the ordered membership")
        .iter()
        .map(|entry| entry.as_str().expect("wallet is a string"))
        .collect();
    let expected: Vec<String> = members.iter().map(|w| w.to_string()).collect();
    assert_eq!(
        carried, expected,
        "manifest membership differs or was reordered"
    );

    let proofs = manifest
        .get("proofs")
        .and_then(serde_json::Value::as_array)
        .expect("manifest carries one proof per member");
    assert_eq!(proofs.len(), members.len(), "one proof per member");
    for (index, proof) in proofs.iter().enumerate() {
        let anchor_doc = proof
            .get("anchor")
            .and_then(|a| a.get("proof_json"))
            .and_then(serde_json::Value::as_str)
            .expect("anchor proof document is retained");
        assert!(
            anchor_doc.contains(&format!("proof-{index}")),
            "member {index} retained the wrong anchor document: {}",
            &anchor_doc[..anchor_doc.len().min(64)]
        );
        assert!(
            proof.get("validation").is_some() && proof.get("history").is_some(),
            "member {index} is missing validation or history evidence"
        );
    }
}

/// The binding is deterministic for fixed contents and a fixed ordered membership — the property
/// that makes a pre-stop observation comparable with the post-stop one (#624) — and it CHANGES when
/// the captured evidence changes. Repeated equality alone would also hold for a constant.
#[test]
fn membership_binding_tracks_the_captured_evidence() {
    let (_dir, state, members) = state_with_sized_members(2, 4_096);

    let first = scenario_membership_proofs_hash(&state, &members).unwrap();
    let second = scenario_membership_proofs_hash(&state, &members).unwrap();
    assert_eq!(
        first, second,
        "capture must not read a clock or iterate a set"
    );

    // A different retained document must produce a different binding.
    let (_other_dir, other_state, other_members) = state_with_sized_members(2, 8_192);
    let different = scenario_membership_proofs_hash(&other_state, &other_members).unwrap();
    assert_ne!(
        first, different,
        "binding ignored a change in the captured proof documents, so it is not evidence-bound"
    );

    // Membership order is part of the identity.
    let reversed: Vec<WalletAddress> = members.iter().rev().copied().collect();
    let swapped = scenario_membership_proofs_hash(&state, &reversed).unwrap();
    assert_ne!(first, swapped, "binding ignored membership ordering");
}
