//! Scenario: deterministic exact wallet-second commits (#544).

#![cfg(feature = "scenario")]
#![allow(clippy::expect_used, clippy::unwrap_used)]
#![allow(clippy::too_many_arguments, clippy::type_complexity)]

use std::collections::HashMap;
use std::sync::Arc;

use pe_core_types::{
    MarketId, MarketOutcomeId, OutcomeId, ReceivedAt, ReconstructionQuality, ShareAmount, SourceId,
    SourceTimestamp, VenueMarketId, WalletAddress,
};
use pe_paper_state::{
    DecisionPendingState, NoCopyDisposition, PaperStateDb, WalletHistoryStatusRecord,
};
use pe_position_ledger::{LedgerEffect, PositionLedger, WalletFenceCause};
use pe_service::bucket_commit::{
    BucketCommitEngine, BucketDecisionContext, DecisionContinuationV2, IdentityOverride,
};
use pe_service::decision_replay::{
    AuthorityEvidence, DecisionClockEvidence, DecisionPostBoundaryEvidence,
    DecisionPostBoundaryEvidenceBody, TerminalDispositionEvidence, replay_decision_pending,
};
use pe_service::paper_recovery::{build_leader_ledger, replay_wallet_ledger};
use pe_service::position_seeder::{AnchorExpectation, AnchorInstall, AnchorProof, ledger_capture};
use pe_source_polymarket_public::{
    ActivityAggregate, ActivityParseContext, ActivityTransport, parse_activity_response,
};
use serde_json::{Value, json};
use time::OffsetDateTime;

const WALLET_HEX: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const MARKET_A: &str = "0xcondition-a";
const MARKET_B: &str = "0xcondition-b";
const MARKET_C: &str = "0xcondition-c";

fn wallet() -> WalletAddress {
    WalletAddress::from_hex(WALLET_HEX).unwrap()
}

fn timestamp(epoch: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(epoch).unwrap()
}

fn aggregate_rows(mut rows: Vec<Value>) -> ActivityAggregate {
    let epoch = rows[0]["timestamp"].as_i64().expect("fixture epoch");
    for row in &mut rows {
        row["proxyWallet"] = json!(WALLET_HEX);
    }
    let context = ActivityParseContext {
        source_id: SourceId("polymarket-data-api".to_owned()),
        observed_at: SourceTimestamp(timestamp(epoch + 10)),
        received_at: ReceivedAt(timestamp(epoch + 11)),
        transport: ActivityTransport::Rest,
    };
    let window =
        parse_activity_response(&serde_json::to_vec(&rows).unwrap(), wallet(), &context).unwrap();
    let mut aggregates = window.aggregates().unwrap();
    assert_eq!(aggregates.len(), 1);
    aggregates.remove(0)
}

fn aggregate(row: Value) -> ActivityAggregate {
    aggregate_rows(vec![row])
}

fn position_row(
    activity_type: &str,
    transaction_hash: &str,
    market: &str,
    outcome: u16,
    side: &str,
    size: &str,
    price: &str,
    epoch: i64,
) -> ActivityAggregate {
    aggregate(json!({
        "timestamp": epoch,
        "conditionId": market,
        "type": activity_type,
        "size": size,
        "usdcSize": "999999.000000",
        "transactionHash": transaction_hash,
        "price": price,
        "asset": format!("asset-{outcome}"),
        "side": side,
        "outcomeIndex": outcome,
        "outcome": if outcome == 0 { "Yes" } else { "No" },
        "isCombo": false,
    }))
}

fn pair_effect(
    activity_type: &str,
    transaction_hash: &str,
    market: &str,
    size: &str,
    epoch: i64,
) -> ActivityAggregate {
    aggregate(json!({
        "timestamp": epoch,
        "conditionId": market,
        "type": activity_type,
        "size": size,
        "usdcSize": "0.000000",
        "transactionHash": transaction_hash,
        "price": "1",
        "asset": "",
        "side": "",
        "outcomeIndex": 999,
        "outcome": "",
        "isCombo": false,
    }))
}

fn combo_effect(activity_type: &str, transaction_hash: &str, epoch: i64) -> ActivityAggregate {
    aggregate(json!({
        "timestamp": epoch,
        "conditionId": MARKET_A,
        "type": activity_type,
        "size": "1.000000",
        "usdcSize": "0.000000",
        "transactionHash": transaction_hash,
        "price": "1",
        "asset": "",
        "side": "",
        "outcomeIndex": 999,
        "outcome": "",
        "isCombo": true,
    }))
}

fn zero_basis() -> pe_service::bucket_commit::FrozenDecisionBasis {
    pe_service::bucket_commit::FrozenDecisionBasis {
        win_rate_p: pe_core_types::Probability::ZERO,
        bankroll: rust_decimal::Decimal::ZERO,
    }
}
fn context(epoch: i64, complete_history: bool) -> BucketDecisionContext {
    BucketDecisionContext {
        applied_configuration: pe_service::runtime_config::RuntimeConfig::from_service_config(
            &pe_service::config::ServiceConfig::default(),
        ),
        decision_inputs_json: "{\"source_window\":\"complete\"}".to_owned(),
        reconstruction_quality: ReconstructionQuality::new(100).unwrap(),
        signal_config: Default::default(),
        copy_eligible: true,
        bracket_commit: false,
        recorded_at_unix: epoch + 20,
        observation_provenance: HashMap::new(),
        no_copy_dispositions: HashMap::new(),
        identity_overrides: HashMap::new(),
        identity_unresolved: Default::default(),
        history_status: complete_history.then(|| WalletHistoryStatusRecord {
            wallet: wallet(),
            complete: true,
            proof_json: "{\"fixed_end_walk\":\"complete\"}".to_owned(),
            updated_at_unix: epoch + 20,
        }),
    }
}

fn state(engine: &BucketCommitEngine, market: &str, outcome: u16) -> ShareAmount {
    let key = MarketOutcomeId::new(
        MarketId(VenueMarketId(market.to_owned())),
        OutcomeId(outcome),
    );
    engine
        .ledger()
        .position(&wallet())
        .and_then(|snapshot| snapshot.positions.get(&key))
        .map(|position| position.long_contracts)
        .unwrap_or(ShareAmount::ZERO)
}

fn market_outcome(market: &str, outcome: u16) -> MarketOutcomeId {
    MarketOutcomeId::new(
        MarketId(VenueMarketId(market.to_owned())),
        OutcomeId(outcome),
    )
}

fn fresh() -> (tempfile::TempDir, Arc<PaperStateDb>, BucketCommitEngine) {
    let dir = tempfile::tempdir().unwrap();
    let paper = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
    let engine = BucketCommitEngine::load(Arc::clone(&paper), PositionLedger::new()).unwrap();
    (dir, paper, engine)
}

fn fresh_anchored() -> (tempfile::TempDir, Arc<PaperStateDb>, BucketCommitEngine) {
    let (dir, paper, mut engine) = fresh();
    paper.set_cursor(&wallet(), 0).unwrap();
    install_anchor(&mut engine, &paper, 0, Vec::new(), 0);
    (dir, paper, engine)
}

fn install_anchor(
    engine: &mut BucketCommitEngine,
    paper: &PaperStateDb,
    cutoff: i64,
    balances: Vec<(MarketId, OutcomeId, ShareAmount)>,
    recorded_at_unix: i64,
) {
    let captured = ledger_capture(engine.ledger(), paper, wallet()).unwrap();
    engine
        .install_anchors(&[AnchorInstall {
            wallet: wallet(),
            balances,
            cutoff,
            proof: AnchorProof {
                positions_proof_hash: "empty".to_owned(),
                activity_bounds_json: "[]".to_owned(),
                source_log_generation: "scenario".to_owned(),
                document: "{}".to_owned(),
                recorded_at_unix,
            },
            expected: AnchorExpectation {
                ledger_hash: captured.hash,
                cursor: captured.cursor,
                anchor_seq: captured.anchor_seq,
                coverage_generation: captured.coverage_generation,
            },
        }])
        .unwrap();
}

#[test]
fn pre_anchor_groups_are_covered_without_arithmetic_or_copy_side_effects() {
    let (_dir, paper, mut engine) = fresh();
    let redeem = position_row("REDEEM", "0xcovered1", MARKET_A, 0, "", "7", "0", 100);
    let sell = position_row("TRADE", "0xcovered2", MARKET_B, 0, "SELL", "4", "0.5", 100);
    let buy = position_row("TRADE", "0xcovered3", MARKET_A, 0, "BUY", "2", "0.5", 100);
    let result = engine
        .commit(vec![redeem, sell, buy], &context(100, true), zero_basis())
        .unwrap();

    assert!(
        result
            .dispositions
            .values()
            .all(|disposition| disposition == "anchor_covered")
    );
    assert_eq!(state(&engine, MARKET_A, 0), ShareAmount::ZERO);
    assert_eq!(state(&engine, MARKET_A, 1), ShareAmount::ZERO);
    assert!(result.pending.is_empty());
    assert!(paper.open_decision_pending().unwrap().is_empty());
    assert!(paper.pending_dispatch_seeds().unwrap().is_empty());
    assert!(!paper.is_wallet_fenced(&wallet()).unwrap());
    let history = paper.gate_history().unwrap();
    assert!(history[&wallet()].contains(&MarketId(VenueMarketId(MARKET_A.to_owned()))));
    assert!(
        !history[&wallet()].contains(&MarketId(VenueMarketId(MARKET_B.to_owned()))),
        "covered SELLs do not consume ever-traded history"
    );
    for group in paper.activity_groups_after(&wallet(), -1).unwrap() {
        LedgerEffect::from_document(&group.proof_json).unwrap();
    }
}

#[test]
fn covered_late_precedes_partial_and_late_equal_second_fences() {
    let (_dir, paper, mut engine) = fresh();
    paper.set_cursor(&wallet(), 0).unwrap();
    install_anchor(&mut engine, &paper, 100, Vec::new(), 101);

    let first = position_row("TRADE", "0xlate1", MARKET_A, 0, "BUY", "1", "0.5", 100);
    let first_id = first.group_id.key().clone();
    let result = engine
        .commit(vec![first.clone()], &context(100, true), zero_basis())
        .unwrap();
    assert_eq!(result.dispositions[&first_id.0], "anchor_covered_late");
    assert_eq!(
        paper
            .wallet_coverage(&wallet())
            .unwrap()
            .coverage_generation,
        1
    );
    assert!(paper.position_validation(&wallet()).unwrap().is_none());
    assert!(result.pending.is_empty());

    install_anchor(&mut engine, &paper, 100, Vec::new(), 102);
    let partial = position_row("TRADE", "0xlate2", MARKET_B, 0, "BUY", "2", "0.5", 100);
    let partial_id = partial.group_id.key().clone();
    let result = engine
        .commit(vec![first, partial], &context(100, true), zero_basis())
        .unwrap();
    assert_eq!(result.dispositions[&partial_id.0], "anchor_covered_late");
    assert_eq!(
        paper
            .wallet_coverage(&wallet())
            .unwrap()
            .coverage_generation,
        2
    );
    assert!(!paper.is_wallet_fenced(&wallet()).unwrap());

    install_anchor(&mut engine, &paper, 100, Vec::new(), 103);
    let late = position_row("TRADE", "0xlate3", MARKET_B, 1, "BUY", "3", "0.5", 100);
    let late_id = late.group_id.key().clone();
    let result = engine
        .commit(vec![late], &context(100, true), zero_basis())
        .unwrap();
    assert_eq!(result.dispositions[&late_id.0], "anchor_covered_late");
    assert_eq!(
        paper
            .wallet_coverage(&wallet())
            .unwrap()
            .coverage_generation,
        3
    );
    assert!(paper.position_validation(&wallet()).unwrap().is_none());
    assert!(paper.open_decision_pending().unwrap().is_empty());
    assert_eq!(state(&engine, MARKET_A, 0), ShareAmount::ZERO);
    assert_eq!(state(&engine, MARKET_B, 0), ShareAmount::ZERO);
    assert!(!paper.is_wallet_fenced(&wallet()).unwrap());

    let copy = position_row("TRADE", "0xlate-copy", MARKET_C, 0, "BUY", "1", "0.5", 101);
    let copy_id = copy.group_id.key().clone();
    let result = engine
        .commit(vec![copy], &context(101, true), zero_basis())
        .unwrap();
    assert_eq!(result.dispositions[&copy_id.0], "not_copy_eligible");
    assert!(result.pending.is_empty());
    assert!(paper.open_decision_pending().unwrap().is_empty());
}

#[test]
fn verified_identity_override_commits_v2_and_replays_with_v1() {
    let (dir, paper, mut engine) = fresh_anchored();
    let legacy = position_row("TRADE", "0xidentity-v1", MARKET_A, 0, "BUY", "2", "0.4", 90);
    engine
        .commit(vec![legacy], &context(90, true), zero_basis())
        .unwrap();

    let corrected = position_row("TRADE", "0xidentity-v2", MARKET_A, 0, "BUY", "3", "0.6", 91);
    let corrected_id = corrected.group_id.key().clone();
    let mut corrected_context = context(91, true);
    corrected_context.identity_overrides.insert(
        corrected_id.clone(),
        IdentityOverride {
            verified: market_outcome(MARKET_B, 1),
            evidence_hash: "gamma-page-hash".to_owned(),
        },
    );
    let result = engine
        .commit(vec![corrected.clone()], &corrected_context, zero_basis())
        .unwrap();

    assert_eq!(result.dispositions[&corrected_id.0], "decision_pending");
    assert_eq!(state(&engine, MARKET_A, 0).atomic(), 2_000_000);
    assert_eq!(state(&engine, MARKET_B, 1).atomic(), 3_000_000);
    let groups = paper.activity_groups_after(&wallet(), 0).unwrap();
    let legacy_document = groups
        .iter()
        .find(|group| group.source_trade_id != corrected_id)
        .unwrap();
    let corrected_document = groups
        .iter()
        .find(|group| group.source_trade_id == corrected_id)
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&legacy_document.proof_json).unwrap()["version"],
        1
    );
    let parsed = LedgerEffect::from_document(&corrected_document.proof_json).unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&corrected_document.proof_json).unwrap()["version"],
        2
    );
    let correction = parsed.correction().unwrap();
    assert_eq!(correction.stamped, market_outcome(MARKET_A, 0));
    assert_eq!(correction.verified, market_outcome(MARKET_B, 1));
    assert_eq!(correction.evidence_hash, "gamma-page-hash");

    let retry = engine
        .commit(vec![corrected], &corrected_context, zero_basis())
        .unwrap();
    assert!(retry.already_committed);
    assert_eq!(state(&engine, MARKET_B, 1).atomic(), 3_000_000);

    drop(engine);
    drop(paper);
    let restarted = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
    let replayed = build_leader_ledger(&restarted).unwrap();
    let restarted_engine = BucketCommitEngine::load(restarted, replayed).unwrap();
    assert_eq!(state(&restarted_engine, MARKET_A, 0).atomic(), 2_000_000);
    assert_eq!(state(&restarted_engine, MARKET_B, 1).atomic(), 3_000_000);
}

#[test]
fn unverified_identity_is_raw_only_reanchors_and_replays() {
    let (dir, paper, mut engine) = fresh_anchored();
    let coverage_before = paper.wallet_coverage(&wallet()).unwrap();
    let unverified = position_row(
        "TRADE",
        "0xidentity-unverified",
        MARKET_A,
        0,
        "BUY",
        "7",
        "0.5",
        92,
    );
    let source_trade_id = unverified.group_id.key().clone();
    let mut unresolved_context = context(92, true);
    unresolved_context
        .identity_unresolved
        .insert(source_trade_id.clone());
    unresolved_context.no_copy_dispositions.insert(
        source_trade_id.clone(),
        NoCopyDisposition {
            provenance: "rest_poll".to_owned(),
            age_secs: 0,
            reason: "identity_unresolved".to_owned(),
            recorded_at_unix: 112,
        },
    );

    let result = engine
        .commit(vec![unverified], &unresolved_context, zero_basis())
        .unwrap();
    assert_eq!(result.dispositions[&source_trade_id.0], "raw_only");
    assert!(result.pending.is_empty());
    assert_eq!(state(&engine, MARKET_A, 0), ShareAmount::ZERO);
    assert_eq!(
        paper.no_copy_disposition(&source_trade_id).unwrap(),
        Some(("rest_poll".to_owned(), 0, "identity_unresolved".to_owned()))
    );
    let coverage_after = paper.wallet_coverage(&wallet()).unwrap();
    assert!(coverage_after.reanchor_required);
    assert_eq!(
        coverage_after.coverage_generation,
        coverage_before.coverage_generation + 1
    );
    assert!(paper.open_decision_pending().unwrap().is_empty());
    assert!(
        paper
            .gate_history()
            .unwrap()
            .get(&wallet())
            .is_none_or(std::collections::HashSet::is_empty)
    );
    assert!(matches!(
        LedgerEffect::from_document(
            &paper.activity_groups_after(&wallet(), 0).unwrap()[0].proof_json
        ),
        Ok(LedgerEffect::RawOnly)
    ));

    drop(engine);
    drop(paper);
    let restarted = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
    let replayed = build_leader_ledger(&restarted).unwrap();
    let restarted_engine = BucketCommitEngine::load(restarted, replayed).unwrap();
    assert_eq!(state(&restarted_engine, MARKET_A, 0), ShareAmount::ZERO);
}

#[test]
fn bracket_unverified_covered_group_is_raw_only_without_reanchor_then_anchors() {
    let (_dir, paper, mut engine) = fresh_anchored();
    let before = paper.wallet_coverage(&wallet()).unwrap();
    let unverified = position_row(
        "TRADE",
        "0xbracket-identity-unverified",
        MARKET_A,
        0,
        "BUY",
        "7",
        "0.5",
        92,
    );
    let source_trade_id = unverified.group_id.key().clone();
    let mut bracket_context = context(92, true);
    bracket_context.bracket_commit = true;
    bracket_context
        .identity_unresolved
        .insert(source_trade_id.clone());
    bracket_context.no_copy_dispositions.insert(
        source_trade_id.clone(),
        NoCopyDisposition {
            provenance: "rest_poll".to_owned(),
            age_secs: 0,
            reason: "identity_unresolved".to_owned(),
            recorded_at_unix: 112,
        },
    );

    let result = engine
        .commit(vec![unverified], &bracket_context, zero_basis())
        .unwrap();
    assert_eq!(result.dispositions[&source_trade_id.0], "raw_only");
    assert!(result.pending.is_empty());
    assert_eq!(state(&engine, MARKET_A, 0), ShareAmount::ZERO);
    assert_eq!(paper.wallet_coverage(&wallet()).unwrap(), before);
    assert_eq!(
        paper.no_copy_disposition(&source_trade_id).unwrap(),
        Some(("rest_poll".to_owned(), 0, "identity_unresolved".to_owned()))
    );
    assert!(paper.open_decision_pending().unwrap().is_empty());
    assert!(
        paper
            .gate_history()
            .unwrap()
            .get(&wallet())
            .is_none_or(std::collections::HashSet::is_empty)
    );

    let captured = ledger_capture(engine.ledger(), &paper, wallet()).unwrap();
    engine
        .install_anchors(&[AnchorInstall {
            wallet: wallet(),
            balances: Vec::new(),
            cutoff: 92,
            proof: AnchorProof {
                positions_proof_hash: "bracket-unverified".to_owned(),
                activity_bounds_json: "[]".to_owned(),
                source_log_generation: "scenario".to_owned(),
                document: "{}".to_owned(),
                recorded_at_unix: 112,
            },
            expected: AnchorExpectation {
                ledger_hash: captured.hash,
                cursor: captured.cursor,
                anchor_seq: captured.anchor_seq,
                coverage_generation: captured.coverage_generation,
            },
        }])
        .unwrap();
    let anchored = paper.wallet_coverage(&wallet()).unwrap();
    assert_eq!(anchored.anchor_seq, Some(1));
    assert_eq!(anchored.coverage_generation, before.coverage_generation);
    assert!(!anchored.reanchor_required);
}

#[test]
fn unresolved_member_after_partial_durable_bucket_is_raw_only_without_fence() {
    let (_dir, paper, mut engine) = fresh_anchored();
    let committed = position_row(
        "TRADE",
        "0xpartial-committed",
        MARKET_A,
        0,
        "BUY",
        "1",
        "0.5",
        92,
    );
    let committed_id = committed.group_id.key().clone();
    engine
        .commit(vec![committed.clone()], &context(92, true), zero_basis())
        .unwrap();

    let unresolved = position_row(
        "TRADE",
        "0xpartial-unresolved",
        MARKET_B,
        0,
        "BUY",
        "2",
        "0.5",
        92,
    );
    let unresolved_id = unresolved.group_id.key().clone();
    let mut mixed_context = context(92, true);
    mixed_context
        .identity_unresolved
        .insert(unresolved_id.clone());
    mixed_context.no_copy_dispositions.insert(
        unresolved_id.clone(),
        NoCopyDisposition {
            provenance: "rest_poll".to_owned(),
            age_secs: 0,
            reason: "identity_unresolved".to_owned(),
            recorded_at_unix: 112,
        },
    );

    let result = engine
        .commit(vec![committed, unresolved], &mixed_context, zero_basis())
        .unwrap();

    assert_eq!(result.dispositions[&committed_id.0], "already_committed");
    assert_eq!(result.dispositions[&unresolved_id.0], "raw_only");
    assert_eq!(result.newly_fenced, None);
    assert!(!paper.is_wallet_fenced(&wallet()).unwrap());
    assert_eq!(
        paper.no_copy_disposition(&unresolved_id).unwrap(),
        Some(("rest_poll".to_owned(), 0, "identity_unresolved".to_owned()))
    );
    assert!(paper.wallet_coverage(&wallet()).unwrap().reanchor_required);
    assert_eq!(state(&engine, MARKET_A, 0).atomic(), 1_000_000);
    assert_eq!(state(&engine, MARKET_B, 0), ShareAmount::ZERO);
}

#[test]
fn covered_identity_resolution_uses_verified_history_and_keeps_unverified_raw_only() {
    let (_dir, paper, mut engine) = fresh();
    paper.set_cursor(&wallet(), 0).unwrap();
    install_anchor(&mut engine, &paper, 100, Vec::new(), 101);
    let corrected = position_row(
        "TRADE",
        "0xcovered-corrected",
        MARKET_A,
        0,
        "BUY",
        "1",
        "0.5",
        100,
    );
    let unverified = position_row(
        "TRADE",
        "0xcovered-unverified",
        MARKET_C,
        0,
        "BUY",
        "1",
        "0.5",
        100,
    );
    let corrected_id = corrected.group_id.key().clone();
    let unverified_id = unverified.group_id.key().clone();
    let mut resolved_context = context(100, true);
    resolved_context.identity_overrides.insert(
        corrected_id.clone(),
        IdentityOverride {
            verified: market_outcome(MARKET_B, 1),
            evidence_hash: "covered-gamma-page".to_owned(),
        },
    );
    resolved_context
        .identity_unresolved
        .insert(unverified_id.clone());
    resolved_context.bracket_commit = true;
    resolved_context.no_copy_dispositions.insert(
        unverified_id.clone(),
        NoCopyDisposition {
            provenance: "rest_poll".to_owned(),
            age_secs: 0,
            reason: "identity_unresolved".to_owned(),
            recorded_at_unix: 120,
        },
    );

    let result = engine
        .commit(vec![corrected, unverified], &resolved_context, zero_basis())
        .unwrap();
    assert_eq!(result.dispositions[&corrected_id.0], "anchor_covered_late");
    assert_eq!(result.dispositions[&unverified_id.0], "raw_only");
    assert!(paper.wallet_coverage(&wallet()).unwrap().reanchor_required);
    let history = paper.gate_history().unwrap();
    assert!(history[&wallet()].contains(&MarketId(VenueMarketId(MARKET_B.to_owned()))));
    assert!(!history[&wallet()].contains(&MarketId(VenueMarketId(MARKET_A.to_owned()))));
    assert!(!history[&wallet()].contains(&MarketId(VenueMarketId(MARKET_C.to_owned()))));
}

#[test]
fn mixed_corrected_unverified_bucket_rolls_back_every_surface_then_retries() {
    let (dir, paper, mut engine) = fresh_anchored();
    let corrected = position_row(
        "TRADE",
        "0xatomic-corrected",
        MARKET_A,
        0,
        "BUY",
        "3",
        "0.5",
        93,
    );
    let unverified = position_row(
        "TRADE",
        "0xatomic-unverified",
        MARKET_C,
        0,
        "BUY",
        "4",
        "0.5",
        93,
    );
    let corrected_id = corrected.group_id.key().clone();
    let unverified_id = unverified.group_id.key().clone();
    let mut mixed_context = context(93, true);
    mixed_context.identity_overrides.insert(
        corrected_id.clone(),
        IdentityOverride {
            verified: market_outcome(MARKET_B, 1),
            evidence_hash: "atomic-gamma-page".to_owned(),
        },
    );
    mixed_context
        .identity_unresolved
        .insert(unverified_id.clone());
    mixed_context.no_copy_dispositions.insert(
        unverified_id.clone(),
        NoCopyDisposition {
            provenance: "rest_poll".to_owned(),
            age_secs: 0,
            reason: "identity_unresolved".to_owned(),
            recorded_at_unix: 113,
        },
    );

    let database_path = dir.path().join("paper.db");
    let connection = rusqlite::Connection::open(&database_path).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER fail_identity_history BEFORE INSERT ON wallet_market_history_v2 \
             BEGIN SELECT RAISE(FAIL, 'injected identity bucket failure'); END;",
        )
        .unwrap();
    drop(connection);

    assert!(
        engine
            .commit(
                vec![corrected.clone(), unverified.clone()],
                &mixed_context,
                zero_basis(),
            )
            .is_err()
    );
    assert!(paper.activity_group_state(&corrected_id).unwrap().is_none());
    assert!(
        paper
            .activity_group_state(&unverified_id)
            .unwrap()
            .is_none()
    );
    assert!(paper.no_copy_disposition(&corrected_id).unwrap().is_none());
    assert!(paper.no_copy_disposition(&unverified_id).unwrap().is_none());
    assert!(paper.leader_positions().unwrap().is_empty());
    assert!(paper.open_decision_pending().unwrap().is_empty());
    assert!(
        paper
            .gate_history()
            .unwrap()
            .get(&wallet())
            .is_none_or(std::collections::HashSet::is_empty)
    );
    assert!(!paper.wallet_coverage(&wallet()).unwrap().reanchor_required);
    assert_eq!(state(&engine, MARKET_B, 1), ShareAmount::ZERO);
    let connection = rusqlite::Connection::open(&database_path).unwrap();
    let revision_count: i64 = connection
        .query_row("SELECT COUNT(*) FROM activity_group_revisions", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(revision_count, 0);
    connection
        .execute_batch("DROP TRIGGER fail_identity_history;")
        .unwrap();
    drop(connection);

    let result = engine
        .commit(vec![corrected, unverified], &mixed_context, zero_basis())
        .unwrap();
    assert_eq!(result.dispositions[&unverified_id.0], "raw_only");
    assert_eq!(state(&engine, MARKET_B, 1).atomic(), 3_000_000);
    assert_eq!(state(&engine, MARKET_C, 0), ShareAmount::ZERO);
    assert!(paper.activity_group_state(&corrected_id).unwrap().is_some());
    assert!(
        paper
            .activity_group_state(&unverified_id)
            .unwrap()
            .is_some()
    );
    assert!(paper.wallet_coverage(&wallet()).unwrap().reanchor_required);
}

#[test]
fn ordinary_batch_failure_restores_database_and_engine_then_retry_commits_all() {
    let (dir, paper, mut engine) = fresh_anchored();
    let first = position_row("TRADE", "0xbatch-first", MARKET_A, 0, "BUY", "3", "0.5", 93);
    let second = position_row(
        "TRADE",
        "0xbatch-second",
        MARKET_B,
        0,
        "BUY",
        "4",
        "0.5",
        94,
    );
    let first_id = first.group_id.key().clone();
    let second_id = second.group_id.key().clone();
    let mut first_context = context(93, true);
    first_context.copy_eligible = false;
    let mut second_context = context(94, true);
    second_context.copy_eligible = false;
    let database_path = dir.path().join("paper.db");
    let connection = rusqlite::Connection::open(&database_path).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER fail_second_batch_bucket BEFORE INSERT ON activity_groups
             WHEN NEW.source_epoch = 94
             BEGIN SELECT RAISE(FAIL, 'injected ordinary batch failure'); END;",
        )
        .unwrap();

    let result = engine.commit_batch(|engine| {
        engine.commit(vec![first.clone()], &first_context, zero_basis())?;
        engine.commit(vec![second.clone()], &second_context, zero_basis())?;
        Ok(())
    });
    assert!(result.is_err());
    assert!(paper.activity_group_state(&first_id).unwrap().is_none());
    assert!(paper.activity_group_state(&second_id).unwrap().is_none());
    assert!(paper.leader_positions().unwrap().is_empty());
    assert!(paper.open_decision_pending().unwrap().is_empty());
    assert!(paper.gate_history().unwrap().is_empty());
    assert!(!engine.history_complete(&wallet()));
    assert!(!engine.is_fenced(&wallet()));
    assert_eq!(state(&engine, MARKET_A, 0), ShareAmount::ZERO);
    assert_eq!(state(&engine, MARKET_B, 0), ShareAmount::ZERO);
    assert_eq!(paper.cursor(&wallet()).unwrap(), Some(0));
    assert!(paper.position_validation_current(&wallet()).unwrap());
    let revisions: i64 = connection
        .query_row("SELECT COUNT(*) FROM activity_group_revisions", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(revisions, 0);

    connection
        .execute_batch("DROP TRIGGER fail_second_batch_bucket;")
        .unwrap();
    drop(connection);
    let committed = engine
        .commit_batch(|engine| {
            let first = engine.commit(vec![first], &first_context, zero_basis())?;
            let second = engine.commit(vec![second], &second_context, zero_basis())?;
            Ok([first, second])
        })
        .unwrap();
    assert_eq!(committed[0].dispositions[&first_id.0], "not_copy_eligible");
    assert_eq!(committed[1].dispositions[&second_id.0], "not_copy_eligible");
    assert!(engine.history_complete(&wallet()));
    assert_eq!(state(&engine, MARKET_A, 0).atomic(), 3_000_000);
    assert_eq!(state(&engine, MARKET_B, 0).atomic(), 4_000_000);
    assert!(paper.activity_group_state(&first_id).unwrap().is_some());
    assert!(paper.activity_group_state(&second_id).unwrap().is_some());
}

#[test]
fn replay_is_identical_for_batched_and_single_bucket_commits() {
    let (_batch_dir, batch_paper, mut batch_engine) = fresh_anchored();
    let (_single_dir, single_paper, mut single_engine) = fresh_anchored();
    let first = position_row(
        "TRADE",
        "0xreplay-batch-first",
        MARKET_A,
        0,
        "BUY",
        "3",
        "0.5",
        93,
    );
    let second = position_row(
        "TRADE",
        "0xreplay-batch-second",
        MARKET_B,
        1,
        "BUY",
        "4",
        "0.5",
        94,
    );
    let mut first_context = context(93, true);
    first_context.copy_eligible = false;
    let mut second_context = context(94, true);
    second_context.copy_eligible = false;

    batch_engine
        .commit_batch(|engine| {
            engine.commit(vec![first.clone()], &first_context, zero_basis())?;
            engine.commit(vec![second.clone()], &second_context, zero_basis())?;
            Ok(())
        })
        .unwrap();
    single_engine
        .commit(vec![first], &first_context, zero_basis())
        .unwrap();
    single_engine
        .commit(vec![second], &second_context, zero_basis())
        .unwrap();

    let batch_replay = replay_wallet_ledger(&batch_paper, wallet()).unwrap();
    let single_replay = replay_wallet_ledger(&single_paper, wallet()).unwrap();
    let batch_capture = ledger_capture(&batch_replay, &batch_paper, wallet()).unwrap();
    let single_capture = ledger_capture(&single_replay, &single_paper, wallet()).unwrap();
    assert_eq!(batch_capture.hash, single_capture.hash);
}

#[test]
fn split_merge_redeem_preserve_exact_fractional_balances_atomically() {
    let (_dir, paper, mut engine) = fresh_anchored();
    engine
        .commit(
            vec![pair_effect(
                "SPLIT",
                "0x01",
                MARKET_A,
                "6500000.125000",
                100,
            )],
            &context(100, true),
            zero_basis(),
        )
        .unwrap();
    engine
        .commit(
            vec![pair_effect("MERGE", "0x02", MARKET_A, "500000.125000", 101)],
            &context(101, false),
            zero_basis(),
        )
        .unwrap();
    assert_eq!(state(&engine, MARKET_A, 0).atomic(), 6_000_000_000_000);
    assert_eq!(state(&engine, MARKET_A, 1).atomic(), 6_000_000_000_000);

    let redemptions = vec![
        position_row("REDEEM", "0x03", MARKET_A, 0, "", "0.000001", "0", 102),
        position_row("REDEEM", "0x04", MARKET_A, 1, "", "1.250000", "0", 102),
    ];
    engine
        .commit(redemptions, &context(102, false), zero_basis())
        .unwrap();
    assert_eq!(state(&engine, MARKET_A, 0).atomic(), 5_999_999_999_999);
    assert_eq!(state(&engine, MARKET_A, 1).atomic(), 5_999_998_750_000);
    assert_eq!(paper.fills_count().unwrap(), 0, "non-trades create no fill");
}

#[test]
fn trade_aggregate_uses_exact_size_weighted_price_and_not_usdc_audit() {
    let (_dir, paper, mut engine) = fresh_anchored();
    let trade = aggregate_rows(vec![
        json!({
            "timestamp": 150,
            "conditionId": MARKET_A,
            "type": "TRADE",
            "size": "1.000000",
            "usdcSize": "999999.000000",
            "transactionHash": "0xweighted",
            "price": "0.200000",
            "asset": "asset-0",
            "side": "BUY",
            "outcomeIndex": 0,
            "outcome": "Yes",
            "isCombo": false,
        }),
        json!({
            "timestamp": 150,
            "conditionId": MARKET_A,
            "type": "TRADE",
            "size": "3.000000",
            "usdcSize": "0.000001",
            "transactionHash": "0xweighted",
            "price": "0.600000",
            "asset": "asset-0",
            "side": "BUY",
            "outcomeIndex": 0,
            "outcome": "Yes",
            "isCombo": false,
        }),
    ]);
    engine
        .commit(vec![trade], &context(150, true), zero_basis())
        .unwrap();
    assert_eq!(state(&engine, MARKET_A, 0).atomic(), 4_000_000);
    let row = paper.open_decision_pending().unwrap().remove(0);
    let frozen = DecisionContinuationV2::from_durable(&row).unwrap();
    assert_eq!(frozen.share_amount.atomic(), 4_000_000);
    assert_eq!(frozen.price.0, rust_decimal::Decimal::new(5, 1));
    assert_eq!(
        frozen.applied_configuration_hash,
        frozen.applied_configuration.canonical_hash(),
        "the pending boundary stores the real canonical hash of its full hot snapshot"
    );
}

#[test]
fn tied_same_market_entries_are_symmetric_and_consume_history_once() {
    fn run(
        first_size: &str,
        second_size: &str,
        reverse: bool,
    ) -> (
        std::collections::BTreeMap<String, String>,
        Vec<pe_paper_state::LeaderPositionRow>,
        std::collections::HashMap<WalletAddress, std::collections::HashSet<MarketId>>,
    ) {
        let (_dir, paper, mut engine) = fresh_anchored();
        let first = position_row("TRADE", "0x11", MARKET_A, 0, "BUY", first_size, "0.21", 200);
        let second = position_row(
            "TRADE",
            "0x22",
            MARKET_A,
            0,
            "BUY",
            second_size,
            "0.79",
            200,
        );
        let groups = if reverse {
            vec![second, first]
        } else {
            vec![first, second]
        };
        let result = engine
            .commit(groups, &context(200, true), zero_basis())
            .unwrap();
        assert!(result.pending.is_empty());
        assert!(
            result
                .dispositions
                .values()
                .all(|value| value == "ambiguous_first_entry_same_second")
        );
        assert_eq!(paper.fills_count().unwrap(), 0);
        assert_eq!(paper.open_decision_pending().unwrap().len(), 0);
        assert!(paper.pending_dispatch_seeds().unwrap().is_empty());
        assert!(paper.unfinalized_ready_dispatch_seeds().unwrap().is_empty());
        (
            result.dispositions,
            paper.leader_positions().unwrap(),
            paper.gate_history().unwrap(),
        )
    }

    let left = run("1.125000", "2.875000", false);
    let right = run("2.875000", "1.125000", true);
    assert_eq!(left, right, "g2 order/economics cannot select a winner");

    let (dir, paper, mut engine) = fresh_anchored();
    let groups = vec![
        position_row("TRADE", "0x31", MARKET_A, 0, "BUY", "1", "0.4", 210),
        position_row("TRADE", "0x32", MARKET_A, 0, "BUY", "2", "0.6", 210),
    ];
    engine
        .commit(groups, &context(210, true), zero_basis())
        .unwrap();
    drop(engine);
    drop(paper);
    let restarted = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
    let ledger = build_leader_ledger(&restarted).unwrap();
    let mut restarted_engine = BucketCommitEngine::load(Arc::clone(&restarted), ledger).unwrap();
    let later = restarted_engine
        .commit(
            vec![position_row(
                "TRADE", "0x33", MARKET_A, 1, "BUY", "1", "0.5", 211,
            )],
            &context(211, false),
            zero_basis(),
        )
        .unwrap();
    assert_eq!(
        later.dispositions.values().next().map(String::as_str),
        Some("not_first_entry")
    );
}

#[test]
fn shuffled_opposite_side_and_split_merge_buckets_are_byte_identical() {
    fn opposite(reverse: bool) -> (Vec<pe_paper_state::LeaderPositionRow>, Value, Value) {
        let (_dir, paper, mut engine) = fresh_anchored();
        let mut no_copy = context(250, true);
        no_copy.copy_eligible = false;
        let buy = position_row("TRADE", "0x35", MARKET_A, 0, "BUY", "2", "0.4", 250);
        let sell = position_row("TRADE", "0x36", MARKET_A, 0, "SELL", "2", "0.6", 250);
        let groups = if reverse {
            vec![sell, buy]
        } else {
            vec![buy, sell]
        };
        let result = engine.commit(groups, &no_copy, zero_basis()).unwrap();
        assert_eq!(state(&engine, MARKET_A, 0), ShareAmount::ZERO);
        (
            paper.leader_positions().unwrap(),
            serde_json::to_value(result.dispositions).unwrap(),
            serde_json::to_value(paper.gate_history().unwrap()).unwrap(),
        )
    }

    assert_eq!(opposite(false), opposite(true));

    fn split_merge(reverse: bool) -> (Vec<pe_paper_state::LeaderPositionRow>, Value) {
        let (_dir, paper, mut engine) = fresh_anchored();
        engine
            .commit(
                vec![pair_effect("SPLIT", "0x37", MARKET_A, "2", 251)],
                &context(251, true),
                zero_basis(),
            )
            .unwrap();
        let split = pair_effect("SPLIT", "0x38", MARKET_A, "0.500000", 252);
        let merge = pair_effect("MERGE", "0x39", MARKET_A, "0.500000", 252);
        let groups = if reverse {
            vec![merge, split]
        } else {
            vec![split, merge]
        };
        let result = engine
            .commit(groups, &context(252, false), zero_basis())
            .unwrap();
        assert_eq!(state(&engine, MARKET_A, 0).atomic(), 2_000_000);
        assert_eq!(state(&engine, MARKET_A, 1).atomic(), 2_000_000);
        (
            paper.leader_positions().unwrap(),
            serde_json::to_value(result.dispositions).unwrap(),
        )
    }

    assert_eq!(split_merge(false), split_merge(true));
}

#[test]
fn different_markets_create_independent_pending_deliveries_and_restart_does_not_reapply() {
    let (dir, paper, mut engine) = fresh_anchored();
    let result = engine
        .commit(
            vec![
                position_row("TRADE", "0x41", MARKET_A, 0, "BUY", "1.25", "0.4", 300),
                position_row("TRADE", "0x42", MARKET_B, 0, "BUY", "2.75", "0.6", 300),
            ],
            &context(300, true),
            zero_basis(),
        )
        .unwrap();
    assert_eq!(result.pending.len(), 2);
    assert_eq!(paper.open_decision_pending().unwrap().len(), 2);
    let before = paper.leader_positions().unwrap();

    drop(engine);
    drop(paper);
    let restarted = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
    let ledger = build_leader_ledger(&restarted).unwrap();
    let _restarted_engine = BucketCommitEngine::load(Arc::clone(&restarted), ledger).unwrap();
    assert_eq!(restarted.leader_positions().unwrap(), before);
    for pending in restarted.open_decision_pending().unwrap() {
        let frozen = DecisionContinuationV2::from_durable(&pending).unwrap();
        let trade = frozen.incoming_trade().unwrap();
        assert_eq!(trade.source_trade_id, pending.source_trade_id);
        assert_eq!(trade.contracts, frozen.share_amount);
        let terminal = TerminalDispositionEvidence {
            disposition: "no_copy:test_terminal".to_owned(),
            reason: "test_terminal".to_owned(),
            fill: None,
            dispatch_id: None,
        };
        let evidence = DecisionPostBoundaryEvidence::from_body(DecisionPostBoundaryEvidenceBody {
            version: pe_service::decision_replay::POST_BOUNDARY_EVIDENCE_VERSION,
            owners: vec!["source_log".to_owned(), "paper_log".to_owned()],
            source_trade_id: pending.source_trade_id.clone(),
            applied_configuration_hash: frozen.applied_configuration_hash.clone(),
            market_end: None,
            market_price: None,
            book: None,
            clocks: vec![DecisionClockEvidence {
                purpose: "terminal_transition".to_owned(),
                unix_millis: 301_000,
            }],
            authority: AuthorityEvidence {
                kind: "not_read".to_owned(),
                outcome: "test_terminal".to_owned(),
                bankroll: None,
            },
            terminal,
        })
        .unwrap();
        restarted
            .close_decision_pending(
                &pending.source_trade_id,
                &serde_json::to_string(&evidence).unwrap(),
                "no_copy:test_terminal",
                301,
            )
            .unwrap();
    }
    assert!(restarted.open_decision_pending().unwrap().is_empty());
    let history = restarted.decision_pending_history().unwrap();
    assert_eq!(history.len(), 2);
    assert!(history.iter().all(|row| {
        row.state == DecisionPendingState::Terminal
            && row.terminal_disposition.as_deref() == Some("no_copy:test_terminal")
            && row.post_commit_inputs_json.contains("source_log")
            && replay_decision_pending(row).is_ok()
    }));
    assert_eq!(restarted.leader_positions().unwrap(), before);
}

#[test]
fn conversion_and_underflow_fence_without_partial_ledger_apply() {
    let (_dir, paper, mut engine) = fresh_anchored();
    let conversion = pair_effect("CONVERSION", "0x51", MARKET_A, "1", 400);
    let trade = position_row("TRADE", "0x52", MARKET_B, 0, "BUY", "7", "0.5", 400);
    let result = engine
        .commit(vec![trade, conversion], &context(400, true), zero_basis())
        .unwrap();
    assert_eq!(result.newly_fenced, Some(WalletFenceCause::Conversion));
    assert_eq!(state(&engine, MARKET_B, 0), ShareAmount::ZERO);
    assert!(paper.is_wallet_fenced(&wallet()).unwrap());

    let later = engine
        .commit(
            vec![position_row(
                "TRADE", "0x53", MARKET_B, 0, "BUY", "0.000001", "0.5", 401,
            )],
            &context(401, false),
            zero_basis(),
        )
        .unwrap();
    assert!(
        later
            .dispositions
            .values()
            .all(|value| value == "wallet_fenced_applied")
    );
    assert_eq!(state(&engine, MARKET_B, 0).atomic(), 1);

    let (_dir, paper, mut engine) = fresh_anchored();
    let result = engine
        .commit(
            vec![pair_effect("MERGE", "0x61", MARKET_A, "0.000001", 500)],
            &context(500, true),
            zero_basis(),
        )
        .unwrap();
    assert_eq!(result.newly_fenced, Some(WalletFenceCause::Underflow));
    assert_eq!(state(&engine, MARKET_A, 0), ShareAmount::ZERO);
    assert_eq!(state(&engine, MARKET_A, 1), ShareAmount::ZERO);
    assert!(paper.is_wallet_fenced(&wallet()).unwrap());

    let (_dir, paper, mut engine) = fresh_anchored();
    let result = engine
        .commit(
            vec![combo_effect("FUTURE_POSITION_EFFECT", "0x62", 501)],
            &context(501, true),
            zero_basis(),
        )
        .unwrap();
    assert_eq!(result.newly_fenced, Some(WalletFenceCause::UnknownEffect));
    assert!(paper.is_wallet_fenced(&wallet()).unwrap());

    let (_dir, paper, mut engine) = fresh_anchored();
    let result = engine
        .commit(
            vec![combo_effect("CONVERSION", "0x63", 502)],
            &context(502, true),
            zero_basis(),
        )
        .unwrap();
    assert_eq!(result.newly_fenced, Some(WalletFenceCause::Conversion));
    assert!(paper.is_wallet_fenced(&wallet()).unwrap());
}

#[test]
fn unexpressible_redeems_require_an_anchor_without_mutating_balances() {
    let (_dir, paper, mut engine) = fresh_anchored();
    engine
        .commit(
            vec![position_row(
                "TRADE", "0x70", MARKET_A, 0, "BUY", "5", "0.5", 559,
            )],
            &context(559, true),
            zero_basis(),
        )
        .unwrap();
    let sentinel = aggregate(json!({
        "timestamp": 560,
        "conditionId": MARKET_A,
        "type": "REDEEM",
        "size": "2",
        "usdcSize": "2",
        "transactionHash": "0x71",
        "price": "0",
        "asset": "",
        "side": "",
        "outcomeIndex": 999,
        "outcome": "",
    }));
    let sentinel_id = sentinel.group_id.key().clone();
    let sell = position_row("TRADE", "0x72", MARKET_A, 0, "SELL", "1", "0.5", 560);
    let result = engine
        .commit(vec![sentinel, sell], &context(560, true), zero_basis())
        .unwrap();
    assert_eq!(result.newly_fenced, None);
    assert_eq!(state(&engine, MARKET_A, 0).atomic(), 4_000_000);
    assert_eq!(
        result.dispositions[&sentinel_id.0],
        "reanchor_required_redemption"
    );
    assert!(paper.wallet_coverage(&wallet()).unwrap().reanchor_required);
    assert!(!paper.is_wallet_fenced(&wallet()).unwrap());

    let zero = aggregate(json!({
        "timestamp": 561,
        "conditionId": MARKET_A,
        "type": "REDEEM",
        "size": "0",
        "usdcSize": "0",
        "transactionHash": "0x73",
        "price": "0",
        "asset": "asset-0",
        "side": "",
        "outcomeIndex": 0,
        "outcome": "Yes",
    }));
    let zero_id = zero.group_id.key().clone();
    let result = engine
        .commit(vec![zero], &context(561, true), zero_basis())
        .unwrap();
    assert_eq!(result.newly_fenced, None);
    assert_eq!(state(&engine, MARKET_A, 0).atomic(), 4_000_000);
    assert_eq!(
        result.dispositions[&zero_id.0],
        "reanchor_required_redemption"
    );
    assert!(!paper.is_wallet_fenced(&wallet()).unwrap());
}

#[test]
fn equal_second_validity_is_order_independent_or_the_whole_bucket_fences() {
    let (_dir, paper, mut engine) = fresh_anchored();
    let split = pair_effect("SPLIT", "0x65", MARKET_A, "1", 550);
    let sell = position_row("TRADE", "0x66", MARKET_A, 0, "SELL", "2", "0.5", 550);
    let result = engine
        .commit(vec![sell, split], &context(550, true), zero_basis())
        .unwrap();
    assert_eq!(
        result.newly_fenced,
        Some(WalletFenceCause::OrderDependentEqualSecond)
    );
    assert_eq!(state(&engine, MARKET_A, 0), ShareAmount::ZERO);
    assert_eq!(state(&engine, MARKET_A, 1), ShareAmount::ZERO);
    assert!(paper.is_wallet_fenced(&wallet()).unwrap());

    let (_dir, paper, mut engine) = fresh_anchored();
    let mut no_copy = context(551, true);
    no_copy.copy_eligible = false;
    let disjoint = vec![
        pair_effect("SPLIT", "0x67", MARKET_A, "1.250000", 551),
        position_row("TRADE", "0x68", MARKET_B, 0, "BUY", "2.750000", "0.5", 551),
    ];
    let result = engine.commit(disjoint, &no_copy, zero_basis()).unwrap();
    assert_eq!(result.newly_fenced, None);
    assert_eq!(state(&engine, MARKET_A, 0).atomic(), 1_250_000);
    assert_eq!(state(&engine, MARKET_A, 1).atomic(), 1_250_000);
    assert_eq!(state(&engine, MARKET_B, 0).atomic(), 2_750_000);
    assert!(!paper.is_wallet_fenced(&wallet()).unwrap());
}

#[test]
fn changed_or_late_equal_second_groups_fence_without_reapplying_prior_state() {
    let (_dir, paper, mut engine) = fresh_anchored();
    let mut no_copy = context(600, true);
    no_copy.copy_eligible = false;
    let original = position_row("TRADE", "0x71", MARKET_A, 0, "BUY", "1.250000", "0.4", 600);
    let original_id = original.group_id.key().clone();
    engine
        .commit(vec![original], &no_copy, zero_basis())
        .unwrap();
    assert_eq!(state(&engine, MARKET_A, 0).atomic(), 1_250_000);

    let changed = position_row("TRADE", "0x71", MARKET_A, 0, "BUY", "9.000000", "0.4", 600);
    assert_eq!(changed.group_id.key(), &original_id);
    let result = engine
        .commit(vec![changed], &no_copy, zero_basis())
        .unwrap();
    assert_eq!(
        result.newly_fenced,
        Some(WalletFenceCause::RevisedAggregate)
    );
    assert_eq!(state(&engine, MARKET_A, 0).atomic(), 1_250_000);
    assert!(paper.is_wallet_fenced(&wallet()).unwrap());

    let (_dir, paper, mut engine) = fresh_anchored();
    let first = position_row("TRADE", "0x81", MARKET_A, 0, "BUY", "2", "0.4", 700);
    engine.commit(vec![first], &no_copy, zero_basis()).unwrap();
    let late = position_row("TRADE", "0x82", MARKET_B, 0, "BUY", "5", "0.6", 700);
    let result = engine.commit(vec![late], &no_copy, zero_basis()).unwrap();
    assert_eq!(
        result.newly_fenced,
        Some(WalletFenceCause::LateEqualSecondGroup)
    );
    assert_eq!(state(&engine, MARKET_A, 0).atomic(), 2_000_000);
    assert_eq!(state(&engine, MARKET_B, 0), ShareAmount::ZERO);
    assert!(paper.is_wallet_fenced(&wallet()).unwrap());
}

#[test]
fn legacy_history_import_is_once_only_and_remains_conservative() {
    let (_dir, paper, _engine) = fresh();
    let first = serde_json::to_vec(&json!({
        "wallets": [{"wallet": WALLET_HEX, "markets": [MARKET_A]}]
    }))
    .unwrap();
    let imported = paper.import_legacy_wallet_history(&first, 800).unwrap();
    assert!(!imported.already_imported);
    assert_eq!(imported.parsed_row_count, 1);
    assert!(!paper.wallet_history_complete(&wallet()).unwrap());
    assert!(
        paper.gate_history().unwrap()[&wallet()]
            .contains(&MarketId(VenueMarketId(MARKET_A.to_owned())))
    );

    // The durable import proof is checked before parsing or hashing later bytes.
    let second = paper
        .import_legacy_wallet_history(b"not-json-and-must-not-be-read", 900)
        .unwrap();
    assert!(second.already_imported);
    assert_eq!(second.source_hash, imported.source_hash);
    assert_eq!(second.imported_at_unix, 800);
    assert!(
        !paper.gate_history().unwrap()[&wallet()]
            .contains(&MarketId(VenueMarketId(MARKET_B.to_owned())))
    );
}
