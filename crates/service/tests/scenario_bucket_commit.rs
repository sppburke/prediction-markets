//! Scenario: deterministic exact wallet-second commits (#544).

#![cfg(feature = "scenario")]
#![allow(clippy::expect_used, clippy::unwrap_used)]
#![allow(clippy::too_many_arguments, clippy::type_complexity)]

mod support;

use std::collections::HashMap;
use std::sync::Arc;

use pe_core_types::{
    MarketId, MarketOutcomeId, OutcomeId, ReconstructionQuality, ShareAmount, VenueMarketId,
    WalletAddress,
};
use pe_paper_state::{
    DecisionPendingState, NoCopyDisposition, PaperStateDb, WalletHistoryStatusRecord,
};
use pe_position_ledger::{AppliedEffect, LedgerEffect, PositionLedger};
use pe_service::bucket_commit::{
    BucketCommitEngine, BucketDecisionContext, DecisionContinuationV3, IdentityOverride,
};
use pe_service::decision_replay::{
    AuthorityEvidence, DecisionClockEvidence, DecisionPostBoundaryEvidence,
    DecisionPostBoundaryEvidenceBody, TerminalDispositionEvidence, replay_decision_pending,
};
use pe_service::paper_recovery::{
    WalletLedgerReplayError, build_leader_ledger, replay_wallet_ledger,
};
use pe_service::position_seeder::{AnchorExpectation, AnchorInstall, AnchorProof, ledger_capture};
use pe_source_polymarket_public::ActivityAggregate;
use serde_json::{Value, json};

const WALLET_HEX: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const FENCED_WALLET_HEX: &str = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const MARKET_A: &str = "0xca";
const MARKET_B: &str = "0xcb";
const MARKET_C: &str = "0xcc";

fn wallet() -> WalletAddress {
    WalletAddress::from_hex(WALLET_HEX).unwrap()
}

/// Keep the source rows beside the parsed group until the complete bucket payload is assembled.
#[derive(Clone)]
struct FixtureAggregate {
    rows: Vec<Value>,
    aggregate: ActivityAggregate,
}

impl std::ops::Deref for FixtureAggregate {
    type Target = ActivityAggregate;
    fn deref(&self) -> &Self::Target {
        &self.aggregate
    }
}

fn aggregate_rows_for_wallet(wallet_hex: &str, mut rows: Vec<Value>) -> FixtureAggregate {
    let epoch = rows[0]["timestamp"].as_i64().expect("fixture epoch");
    for row in &mut rows {
        row["proxyWallet"] = json!(wallet_hex);
    }
    let read = support::producer_shaped_read_v1(
        WalletAddress::from_hex(wallet_hex).unwrap(),
        &serde_json::to_vec(&rows).unwrap(),
        epoch,
        epoch + 11,
        support::scenario_receipt(2),
    );
    assert_eq!(read.aggregates.len(), 1);
    FixtureAggregate {
        rows,
        aggregate: read.aggregates.into_iter().next().unwrap(),
    }
}

/// Assemble each commit's complete payload before asking the production engine to commit it.
trait CommitFixtureRead {
    fn commit_read(
        &mut self,
        groups: Vec<FixtureAggregate>,
        context: &BucketDecisionContext,
        basis: pe_service::bucket_commit::FrozenDecisionBasis,
    ) -> Result<
        pe_service::bucket_commit::BucketCommitResult,
        pe_service::bucket_commit::BucketCommitError,
    >;
}

impl CommitFixtureRead for BucketCommitEngine {
    fn commit_read(
        &mut self,
        groups: Vec<FixtureAggregate>,
        template: &BucketDecisionContext,
        basis: pe_service::bucket_commit::FrozenDecisionBasis,
    ) -> Result<
        pe_service::bucket_commit::BucketCommitResult,
        pe_service::bucket_commit::BucketCommitError,
    > {
        let wallet = groups[0].group_id.components().wallet;
        let fixed_end = groups
            .iter()
            .map(|group| group.source_time.0.unix_timestamp())
            .max()
            .unwrap();
        let rows: Vec<_> = groups.iter().flat_map(|group| group.rows.iter()).collect();
        let read = support::producer_shaped_read_v1(
            wallet,
            &serde_json::to_vec(&rows).unwrap(),
            fixed_end,
            template.recorded_at_unix,
            support::scenario_receipt(u64::try_from(fixed_end).unwrap()),
        );
        assert_eq!(read.aggregates.len(), groups.len());
        // Preserve caller order so permutation scenarios still exercise the engine's ordering.
        let aggregates: Vec<_> = groups
            .iter()
            .map(|group| {
                let parsed = read
                    .aggregates
                    .iter()
                    .find(|parsed| parsed.group_id == group.group_id)
                    .unwrap();
                assert_eq!(parsed, &group.aggregate);
                parsed.clone()
            })
            .collect();
        let mut context = template.clone();
        context.read_commitment = Some(
            pe_service::bucket_commit::ActivityReadCommitmentReceipt::LegacyV1(
                support::scenario_receipt(read.page.receipt.sequence.0 + 1),
            ),
        );
        context.decision_inputs_json = read.decision_inputs_json;
        if !template.decision_inputs_json.is_empty() {
            let template_inputs: Value =
                serde_json::from_str(&template.decision_inputs_json).unwrap();
            if let Some(observations) = template_inputs.get("ambiguous_observations") {
                let mut inputs: Value =
                    serde_json::from_str(&context.decision_inputs_json).unwrap();
                inputs["ambiguous_observations"] = observations.clone();
                context.decision_inputs_json = serde_json::to_string(&inputs).unwrap();
            }
        }
        context.page_occurrences = vec![read.page];
        self.commit(aggregates, &context, basis)
    }
}

fn aggregate_rows(rows: Vec<Value>) -> FixtureAggregate {
    aggregate_rows_for_wallet(WALLET_HEX, rows)
}

fn aggregate(row: Value) -> FixtureAggregate {
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
) -> FixtureAggregate {
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
) -> FixtureAggregate {
    pair_effect_for_wallet(
        WALLET_HEX,
        activity_type,
        transaction_hash,
        market,
        size,
        epoch,
    )
}

fn pair_effect_for_wallet(
    wallet_hex: &str,
    activity_type: &str,
    transaction_hash: &str,
    market: &str,
    size: &str,
    epoch: i64,
) -> FixtureAggregate {
    aggregate_rows_for_wallet(
        wallet_hex,
        vec![json!({
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
        })],
    )
}

fn combo_effect(activity_type: &str, transaction_hash: &str, epoch: i64) -> FixtureAggregate {
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
        verified_read: None,
        applied_configuration: pe_service::runtime_config::RuntimeConfig::from_service_config(
            &pe_service::config::ServiceConfig::default(),
        ),
        decision_inputs_json: String::new(),
        page_occurrences: Vec::new(),
        observed_source_receipts: HashMap::new(),
        reconstruction_quality: ReconstructionQuality::new(100).unwrap(),
        read_commitment: None,

        signal_config: Default::default(),
        copy_eligible: true,
        bracket_commit: false,
        recorded_at_unix: epoch + 20,
        observation_provenance: HashMap::new(),
        no_copy_dispositions: HashMap::new(),
        identity_overrides: HashMap::new(),
        scope_tokens: HashMap::new(),
        identity_unresolved: Default::default(),
        restamp_twins: Default::default(),
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
    for market in [MARKET_A, MARKET_B, MARKET_C] {
        paper.record_market_group(market, "").unwrap();
    }
    let engine = BucketCommitEngine::load(Arc::clone(&paper), PositionLedger::new()).unwrap();
    (dir, paper, engine)
}

fn fresh_anchored() -> (tempfile::TempDir, Arc<PaperStateDb>, BucketCommitEngine) {
    let (dir, paper, mut engine) = fresh();
    paper.set_cursor(&wallet(), 0).unwrap();
    install_anchor(&mut engine, &paper, 0, Vec::new(), 0);
    (dir, paper, engine)
}

#[test]
fn scoped_revision_retry_and_anchor_preserve_original_through_ambiguity() {
    let (dir, paper, mut engine) = fresh_anchored();
    let original = position_row(
        "TRADE",
        "0xdisposed-retry",
        MARKET_A,
        0,
        "BUY",
        "1",
        "0.5",
        100,
    );
    let revised = position_row(
        "TRADE",
        "0xdisposed-retry",
        MARKET_A,
        0,
        "BUY",
        "2",
        "0.5",
        100,
    );
    let id = original.group_id.key().clone();
    let ordinary = context(100, true);
    assert!(!ordinary.bracket_commit);
    engine
        .commit_read(vec![original], &ordinary, zero_basis())
        .unwrap();
    let original_state = paper.activity_group_state(&id).unwrap();
    engine
        .commit_read(vec![revised.clone()], &ordinary, zero_basis())
        .unwrap();
    assert!(paper.wallet_fence(&wallet()).unwrap().is_none());
    assert_eq!(
        paper.wallet_scope_drops(&wallet(), 100).unwrap()[0].cause,
        "revised_applied_aggregate"
    );
    let generation = paper
        .wallet_coverage(&wallet())
        .unwrap()
        .coverage_generation;
    let disposition = paper
        .activity_revision_state(&id, revised.semantic_revision.as_str())
        .unwrap();
    let pending_before_retry = paper.open_decision_pending().unwrap();
    let mut ambiguous_mapping = ordinary.clone();
    ambiguous_mapping.decision_inputs_json = json!({
        "ambiguous_observations": [{
            "group_id": id, "receipt": support::scenario_receipt(500), "source_epoch": 100,
            "candidates": [(id.clone(), 100)], "market_id": MARKET_A,
        }],
    })
    .to_string();
    {
        let (retry_context, already_committed) = (&ordinary, true);
        let retry = engine
            .commit_read(vec![revised.clone()], retry_context, zero_basis())
            .unwrap();
        assert_eq!(retry.already_committed, already_committed);
        assert!(!retry.retained_revision);
        assert!(retry.pending.is_empty());
        assert!(retry.newly_fenced.is_none());
        assert_eq!(
            paper
                .wallet_coverage(&wallet())
                .unwrap()
                .coverage_generation,
            generation
        );
        assert_eq!(paper.wallet_fence(&wallet()).unwrap(), None);
        assert_eq!(paper.activity_group_state(&id).unwrap(), original_state);
        assert_eq!(
            paper
                .activity_revision_state(
                    &id,
                    disposition.as_ref().unwrap().semantic_revision.as_str()
                )
                .unwrap(),
            disposition
        );
        assert_eq!(paper.open_decision_pending().unwrap(), pending_before_retry);
    }

    // Authoritative coverage includes the retained revision without clearing its drop.
    let captured = ledger_capture(engine.ledger(), &paper, wallet()).unwrap();
    engine
        .install_anchors(&[AnchorInstall {
            newest_activity_unix: None,
            fresh_history: Vec::new(),
            expected_fence: None,
            history_status: None,
            wallet: wallet(),
            balances: vec![(
                MarketId(VenueMarketId(MARKET_A.to_owned())),
                OutcomeId(0),
                ShareAmount::from_atomic(2_000_000),
            )],
            cutoff: 101,
            proof: AnchorProof {
                positions_proof_hash: "disposed-retry-recovery".to_owned(),
                activity_bounds_json: "[]".to_owned(),
                source_log_generation: "scenario".to_owned(),
                document: "{}".to_owned(),
                recorded_at_unix: 121,
            },
            expected: AnchorExpectation {
                ledger_hash: captured.hash,
                cursor: captured.cursor,
                anchor_seq: captured.anchor_seq,
                coverage_generation: captured.coverage_generation,
            },
        }])
        .unwrap();
    assert!(!engine.is_fenced(&wallet()));
    assert!(paper.wallet_fence(&wallet()).unwrap().is_none());
    assert_eq!(
        paper
            .wallet_coverage(&wallet())
            .unwrap()
            .activity_cutoff_unix,
        Some(101)
    );
    let pending = paper.decision_pending_history().unwrap();
    let coverage = paper.wallet_coverage(&wallet()).unwrap();
    assert_eq!(coverage.coverage_generation, generation);

    // The ambiguity carrier preserves the original and the already retained revision.
    let retry = engine
        .commit_read(vec![revised.clone()], &ordinary, zero_basis())
        .unwrap();
    assert!(retry.already_committed);
    assert!(retry.pending.is_empty());
    let quarantined = engine
        .commit_read(vec![revised.clone()], &ambiguous_mapping, zero_basis())
        .unwrap();
    assert_eq!(quarantined.newly_fenced, None);
    assert!(quarantined.already_committed);
    assert!(
        paper
            .activity_observation_retired(support::scenario_receipt(500))
            .unwrap()
    );
    assert!(!quarantined.retained_revision);
    assert!(quarantined.pending.is_empty());
    assert!(!engine.is_fenced(&wallet()));
    assert!(paper.wallet_fence(&wallet()).unwrap().is_none());
    assert_eq!(paper.activity_group_state(&id).unwrap(), original_state);
    assert_eq!(
        paper
            .activity_revision_state(&id, revised.semantic_revision.as_str())
            .unwrap(),
        disposition
    );
    assert_eq!(paper.wallet_coverage(&wallet()).unwrap(), coverage);
    assert_eq!(paper.decision_pending_history().unwrap(), pending);

    let history = paper.gate_history().unwrap();
    let history_status = paper.wallet_history_status(&wallet()).unwrap();
    let connection = rusqlite::Connection::open(dir.path().join("paper.db")).unwrap();
    let history_rows = || {
        connection
            .prepare("SELECT wallet_hex, market_id, first_epoch, source_trade_id, origin FROM wallet_market_history_v2 ORDER BY wallet_hex, market_id")
            .unwrap()
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                ))
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    };
    let history_before = history_rows();
    let anchors = paper.position_anchors(&wallet()).unwrap();
    for reload in [false, true] {
        if reload {
            engine =
                BucketCommitEngine::load(Arc::clone(&paper), build_leader_ledger(&paper).unwrap())
                    .unwrap();
        }
        for (retry_context, already_committed) in [(&ordinary, true), (&ambiguous_mapping, true)] {
            let retry = engine
                .commit_read(vec![revised.clone()], retry_context, zero_basis())
                .unwrap();
            assert_eq!(retry.already_committed, already_committed);
            assert!(!retry.retained_revision);
            assert!(retry.pending.is_empty());
            assert!(retry.newly_fenced.is_none());
            assert!(!engine.is_fenced(&wallet()));
            assert!(paper.wallet_fence(&wallet()).unwrap().is_none());
            assert_eq!(paper.activity_group_state(&id).unwrap(), original_state);
            assert_eq!(
                paper
                    .activity_revision_state(&id, revised.semantic_revision.as_str())
                    .unwrap(),
                disposition
            );
            assert_eq!(paper.wallet_coverage(&wallet()).unwrap(), coverage);
            assert_eq!(paper.gate_history().unwrap(), history);
            assert_eq!(history_rows(), history_before);
            assert_eq!(
                paper.wallet_history_status(&wallet()).unwrap(),
                history_status
            );
            assert_eq!(paper.position_anchors(&wallet()).unwrap(), anchors);
            assert_eq!(paper.decision_pending_history().unwrap(), pending);
            assert_eq!(paper.open_decision_pending().unwrap(), pending_before_retry);
        }
    }
}

fn install_anchor(
    engine: &mut BucketCommitEngine,
    paper: &PaperStateDb,
    cutoff: i64,
    balances: Vec<(MarketId, OutcomeId, ShareAmount)>,
    recorded_at_unix: i64,
) {
    install_anchor_for_wallet(engine, paper, wallet(), cutoff, balances, recorded_at_unix);
}

fn install_anchor_for_wallet(
    engine: &mut BucketCommitEngine,
    paper: &PaperStateDb,
    wallet: WalletAddress,
    cutoff: i64,
    balances: Vec<(MarketId, OutcomeId, ShareAmount)>,
    recorded_at_unix: i64,
) {
    let captured = ledger_capture(engine.ledger(), paper, wallet).unwrap();
    engine
        .install_anchors(&[AnchorInstall {
            newest_activity_unix: None,
            fresh_history: Vec::new(),
            expected_fence: None,
            history_status: None,
            wallet,
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
        .commit_read(vec![redeem, sell, buy], &context(100, true), zero_basis())
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
fn covered_late_drops_scopes_and_preserves_other_market_entries() {
    let (_dir, paper, mut engine) = fresh();
    paper.set_cursor(&wallet(), 0).unwrap();
    install_anchor(&mut engine, &paper, 100, Vec::new(), 101);

    let first = position_row("TRADE", "0xlate1", MARKET_A, 0, "BUY", "1", "0.5", 100);
    let first_id = first.group_id.key().clone();
    let result = engine
        .commit_read(vec![first.clone()], &context(100, true), zero_basis())
        .unwrap();
    assert_eq!(result.dispositions[&first_id.0], "anchor_covered_late");
    assert_eq!(
        paper
            .wallet_coverage(&wallet())
            .unwrap()
            .coverage_generation,
        0
    );
    assert!(paper.position_validation(&wallet()).unwrap().is_none());
    assert!(result.pending.is_empty());

    install_anchor(&mut engine, &paper, 100, Vec::new(), 102);
    let partial = position_row("TRADE", "0xlate2", MARKET_B, 0, "BUY", "2", "0.5", 100);
    let partial_id = partial.group_id.key().clone();
    let result = engine
        .commit_read(vec![first, partial], &context(100, true), zero_basis())
        .unwrap();
    assert_eq!(result.dispositions[&partial_id.0], "anchor_covered_late");
    assert_eq!(
        paper
            .wallet_coverage(&wallet())
            .unwrap()
            .coverage_generation,
        0
    );
    assert!(!paper.is_wallet_fenced(&wallet()).unwrap());

    install_anchor(&mut engine, &paper, 100, Vec::new(), 103);
    let late = position_row("TRADE", "0xlate3", MARKET_B, 1, "BUY", "3", "0.5", 100);
    let late_id = late.group_id.key().clone();
    let result = engine
        .commit_read(vec![late], &context(100, true), zero_basis())
        .unwrap();
    assert_eq!(result.dispositions[&late_id.0], "anchor_covered_late");
    assert_eq!(
        paper
            .wallet_coverage(&wallet())
            .unwrap()
            .coverage_generation,
        0
    );
    assert!(paper.position_validation(&wallet()).unwrap().is_none());
    assert!(paper.open_decision_pending().unwrap().is_empty());
    assert_eq!(state(&engine, MARKET_A, 0), ShareAmount::ZERO);
    assert_eq!(state(&engine, MARKET_B, 0), ShareAmount::ZERO);
    assert!(!paper.is_wallet_fenced(&wallet()).unwrap());

    let copy = position_row("TRADE", "0xlate-copy", MARKET_C, 0, "BUY", "1", "0.5", 101);
    let copy_id = copy.group_id.key().clone();
    let result = engine
        .commit_read(vec![copy], &context(101, true), zero_basis())
        .unwrap();
    assert_eq!(result.dispositions[&copy_id.0], "decision_pending");
    assert_eq!(result.pending, vec![copy_id]);
    assert_eq!(state(&engine, MARKET_C, 0).atomic(), 1_000_000);
    assert!(!paper.wallet_coverage(&wallet()).unwrap().reanchor_required);
}

#[test]
fn verified_identity_override_commits_v2_and_replays_with_v1() {
    let (dir, paper, mut engine) = fresh_anchored();
    let legacy = position_row("TRADE", "0xidentity-v1", MARKET_A, 0, "BUY", "2", "0.4", 90);
    engine
        .commit_read(vec![legacy], &context(90, true), zero_basis())
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
        .commit_read(vec![corrected.clone()], &corrected_context, zero_basis())
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
        .commit_read(vec![corrected], &corrected_context, zero_basis())
        .unwrap();
    assert!(retry.already_committed);
    assert_eq!(state(&engine, MARKET_B, 1).atomic(), 3_000_000);

    drop(engine);
    drop(paper);
    let restarted = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
    let anchor_replayed = replay_wallet_ledger(&restarted, wallet()).unwrap();
    assert_eq!(
        anchor_replayed.position(&wallet()).unwrap().positions[&market_outcome(MARKET_A, 0)]
            .long_contracts
            .atomic(),
        2_000_000
    );
    assert_eq!(
        anchor_replayed.position(&wallet()).unwrap().positions[&market_outcome(MARKET_B, 1)]
            .long_contracts
            .atomic(),
        3_000_000
    );
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
        .commit_read(vec![unverified], &unresolved_context, zero_basis())
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
        .commit_read(vec![unverified], &bracket_context, zero_basis())
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
            newest_activity_unix: None,
            fresh_history: Vec::new(),
            expected_fence: None,
            history_status: None,
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
fn unresolved_member_after_partial_durable_bucket_keeps_identity_reanchor() {
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
        .commit_read(vec![committed.clone()], &context(92, true), zero_basis())
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
        .commit_read(vec![committed, unresolved], &mixed_context, zero_basis())
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
        .commit_read(vec![corrected, unverified], &resolved_context, zero_basis())
        .unwrap();
    assert_eq!(result.dispositions[&corrected_id.0], "anchor_covered_late");
    assert_eq!(result.dispositions[&unverified_id.0], "raw_only");
    assert!(!paper.wallet_coverage(&wallet()).unwrap().reanchor_required);
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
            .commit_read(
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
        .commit_read(vec![corrected, unverified], &mixed_context, zero_basis())
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
    let fenced_wallet = WalletAddress::from_hex(FENCED_WALLET_HEX).unwrap();
    paper.set_cursor(&fenced_wallet, 0).unwrap();
    install_anchor_for_wallet(&mut engine, &paper, fenced_wallet, 0, Vec::new(), 91);
    rusqlite::Connection::open(dir.path().join("paper.db")).unwrap().execute(
        "INSERT INTO wallet_fences VALUES (?1, 'legacy', 'conversion_unknown_conditions', '{}', 92)", [fenced_wallet.to_string()]).unwrap();
    engine = BucketCommitEngine::load(Arc::clone(&paper), engine.ledger().clone()).unwrap();
    let pre_batch_fences = paper.wallet_fences().unwrap();
    assert_eq!(pre_batch_fences.len(), 1);
    assert_eq!(pre_batch_fences[0].wallet, fenced_wallet);
    assert!(engine.is_fenced(&fenced_wallet));
    assert!(!engine.is_fenced(&wallet()));
    let first = position_row("TRADE", "0xbatch-first", MARKET_A, 0, "BUY", "3", "0.5", 93);
    let fence_in_batch = pair_effect("CONVERSION", "0xbatch-fence", MARKET_C, "1", 94);
    let third = position_row("TRADE", "0xbatch-third", MARKET_B, 0, "BUY", "4", "0.5", 95);
    let first_id = first.group_id.key().clone();
    let fence_id = fence_in_batch.group_id.key().clone();
    let third_id = third.group_id.key().clone();
    let mut first_context = context(93, true);
    first_context.copy_eligible = false;
    let fence_context = context(94, true);
    let mut third_context = context(95, true);
    third_context.copy_eligible = false;
    let database_path = dir.path().join("paper.db");
    let connection = rusqlite::Connection::open(&database_path).unwrap();
    let pre_batch_revisions: i64 = connection
        .query_row("SELECT COUNT(*) FROM activity_group_revisions", [], |row| {
            row.get(0)
        })
        .unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER fail_third_batch_bucket BEFORE INSERT ON activity_groups
             WHEN NEW.source_epoch = 95
             BEGIN SELECT RAISE(FAIL, 'injected ordinary batch failure'); END;",
        )
        .unwrap();

    let result = engine.commit_batch(|engine| {
        engine.commit_read(vec![first.clone()], &first_context, zero_basis())?;
        engine.commit_read(vec![fence_in_batch.clone()], &fence_context, zero_basis())?;
        engine.commit_read(vec![third.clone()], &third_context, zero_basis())?;
        Ok(())
    });
    assert!(result.is_err());
    assert!(paper.activity_group_state(&first_id).unwrap().is_none());
    assert!(paper.activity_group_state(&fence_id).unwrap().is_none());
    assert!(paper.activity_group_state(&third_id).unwrap().is_none());
    assert!(paper.leader_positions().unwrap().is_empty());
    assert!(paper.open_decision_pending().unwrap().is_empty());
    assert!(paper.gate_history().unwrap().is_empty());
    assert!(!engine.history_complete(&wallet()));
    assert_eq!(paper.wallet_fences().unwrap(), pre_batch_fences);
    assert!(!paper.is_wallet_fenced(&wallet()).unwrap());
    assert!(engine.is_fenced(&fenced_wallet));
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
    assert_eq!(revisions, pre_batch_revisions);

    connection
        .execute_batch("DROP TRIGGER fail_third_batch_bucket;")
        .unwrap();
    drop(connection);
    let committed = engine
        .commit_batch(|engine| {
            let first = engine.commit_read(vec![first], &first_context, zero_basis())?;
            let fence = engine.commit_read(vec![fence_in_batch], &fence_context, zero_basis())?;
            let third = engine.commit_read(vec![third], &third_context, zero_basis())?;
            Ok([first, fence, third])
        })
        .unwrap();
    assert_eq!(committed[0].dispositions[&first_id.0], "not_copy_eligible");
    assert_eq!(committed[1].newly_fenced, None);
    assert_eq!(committed[1].dispositions[&fence_id.0], "scope_dropped");
    assert_eq!(committed[2].dispositions[&third_id.0], "not_copy_eligible");
    assert!(engine.history_complete(&wallet()));
    assert!(!paper.is_wallet_fenced(&wallet()).unwrap());
    assert!(!engine.is_fenced(&wallet()));
    assert_eq!(state(&engine, MARKET_A, 0).atomic(), 3_000_000);
    assert_eq!(state(&engine, MARKET_B, 0).atomic(), 4_000_000);
    assert!(paper.activity_group_state(&first_id).unwrap().is_some());
    assert!(paper.activity_group_state(&fence_id).unwrap().is_some());
    assert!(paper.activity_group_state(&third_id).unwrap().is_some());
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
    let third = position_row(
        "TRADE",
        "0xreplay-batch-third",
        MARKET_C,
        0,
        "SELL",
        "2",
        "0.5",
        95,
    );
    let mut first_context = context(93, true);
    first_context.copy_eligible = false;
    let mut second_context = context(94, true);
    second_context.copy_eligible = false;
    let mut third_context = context(95, true);
    third_context.copy_eligible = false;

    batch_engine
        .commit_batch(|engine| {
            engine.commit_read(vec![first.clone()], &first_context, zero_basis())?;
            engine.commit_read(vec![second.clone()], &second_context, zero_basis())?;
            engine.commit_read(vec![third.clone()], &third_context, zero_basis())?;
            Ok(())
        })
        .unwrap();
    single_engine
        .commit_read(vec![first], &first_context, zero_basis())
        .unwrap();
    single_engine
        .commit_read(vec![second], &second_context, zero_basis())
        .unwrap();
    single_engine
        .commit_read(vec![third], &third_context, zero_basis())
        .unwrap();

    let batch_replay = replay_wallet_ledger(&batch_paper, wallet()).unwrap();
    let single_replay = replay_wallet_ledger(&single_paper, wallet()).unwrap();
    for replay in [&batch_replay, &single_replay] {
        let balances = &replay.position(&wallet()).unwrap().positions;
        assert_eq!(balances.len(), 3);
        assert_eq!(
            balances[&market_outcome(MARKET_A, 0)].long_contracts,
            ShareAmount::from_atomic(3_000_000)
        );
        assert_eq!(
            balances[&market_outcome(MARKET_A, 0)].short_contracts,
            ShareAmount::ZERO
        );
        assert_eq!(
            balances[&market_outcome(MARKET_B, 1)].long_contracts,
            ShareAmount::from_atomic(4_000_000)
        );
        assert_eq!(
            balances[&market_outcome(MARKET_B, 1)].short_contracts,
            ShareAmount::ZERO
        );
        assert_eq!(
            balances[&market_outcome(MARKET_C, 0)].long_contracts,
            ShareAmount::ZERO
        );
        assert_eq!(
            balances[&market_outcome(MARKET_C, 0)].short_contracts,
            ShareAmount::from_atomic(2_000_000)
        );
    }
    let batch_capture = ledger_capture(&batch_replay, &batch_paper, wallet()).unwrap();
    let single_capture = ledger_capture(&single_replay, &single_paper, wallet()).unwrap();
    assert_eq!(batch_capture, single_capture);
}

#[test]
fn split_merge_redeem_preserve_exact_fractional_balances_atomically() {
    let (_dir, paper, mut engine) = fresh_anchored();
    engine
        .commit_read(
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
        .commit_read(
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
        .commit_read(redemptions, &context(102, false), zero_basis())
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
        .commit_read(vec![trade], &context(150, true), zero_basis())
        .unwrap();
    assert_eq!(state(&engine, MARKET_A, 0).atomic(), 4_000_000);
    let row = paper.open_decision_pending().unwrap().remove(0);
    let frozen = DecisionContinuationV3::from_durable(&row).unwrap();
    assert_eq!(frozen.facts.share_amount.atomic(), 4_000_000);
    assert_eq!(frozen.facts.price.0, rust_decimal::Decimal::new(5, 1));
    assert_eq!(
        frozen.facts.applied_configuration_hash,
        frozen.facts.applied_configuration.canonical_hash(),
        "the pending boundary stores the real canonical hash of its full hot snapshot"
    );
}

#[test]
fn tied_same_market_entries_are_symmetric_and_consume_history_once() {
    fn run(
        first_size: &str,
        second_size: &str,
        reverse: bool,
        extra: usize,
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
        let mut groups = vec![first, second];
        groups.extend((0..extra).map(|index| {
            position_row(
                "TRADE",
                &format!("0xextra-{index}"),
                MARKET_A,
                0,
                "BUY",
                "1",
                "0.5",
                200,
            )
        }));
        if reverse {
            groups.reverse();
        }
        let result = engine
            .commit_read(groups, &context(200, true), zero_basis())
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

    let left = run("1.125000", "2.875000", false, 0);
    let right = run("2.875000", "1.125000", true, 0);
    assert_eq!(left, right, "g2 order/economics cannot select a winner");

    assert_eq!(
        run("1.125000", "2.875000", false, 6),
        run("1.125000", "2.875000", true, 6)
    );

    let (dir, paper, mut engine) = fresh_anchored();
    let groups = (0..8)
        .rev()
        .map(|index| {
            position_row(
                "TRADE",
                &format!("0xrestart-{index}"),
                MARKET_A,
                0,
                "BUY",
                "1",
                "0.4",
                210,
            )
        })
        .collect();
    engine
        .commit_read(groups, &context(210, true), zero_basis())
        .unwrap();
    drop(engine);
    drop(paper);
    let restarted = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
    let ledger = build_leader_ledger(&restarted).unwrap();
    let mut restarted_engine = BucketCommitEngine::load(Arc::clone(&restarted), ledger).unwrap();
    let later = restarted_engine
        .commit_read(
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
        let result = engine.commit_read(groups, &no_copy, zero_basis()).unwrap();
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
            .commit_read(
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
            .commit_read(groups, &context(252, false), zero_basis())
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
        .commit_read(
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
        let frozen = DecisionContinuationV3::from_durable(&pending).unwrap();
        let trade = frozen.incoming_trade().unwrap();
        assert_eq!(trade.source_trade_id, pending.source_trade_id);
        assert_eq!(trade.contracts, frozen.facts.share_amount);
        let terminal = TerminalDispositionEvidence {
            disposition: "no_copy:test_terminal".to_owned(),
            reason: "test_terminal".to_owned(),
            fill: None,
            dispatch_id: None,
            dispatch_control_journal_seq: None,
            decline: None,
            final_receipt: None,
        };
        let evidence = DecisionPostBoundaryEvidence::from_body_for_continuation(
            DecisionPostBoundaryEvidenceBody {
                version: pe_service::decision_replay::POST_BOUNDARY_EVIDENCE_VERSION,
                owners: vec!["source_log".to_owned(), "paper_log".to_owned()],
                source_trade_id: pending.source_trade_id.clone(),
                applied_configuration_hash: frozen.facts.applied_configuration_hash.clone(),
                market_end: None,
                market_price: None,
                book: None,
                clocks: vec![DecisionClockEvidence {
                    purpose: "terminal_transition".to_owned(),
                    unix_millis: 301_000,
                    submillisecond_nanos: None,
                }],
                authority: AuthorityEvidence {
                    kind: "not_read".to_owned(),
                    outcome: "test_terminal".to_owned(),
                    bankroll: None,
                },
                terminal,
            },
            &frozen,
        )
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

/// RC14-DECISION-FINAL-RECEIPT
///
/// Preconditions: one admitted first-entry bucket has an open `decision_pending` continuation and
/// the paper serializer has synchronized its FinancialFinal receipt.
/// PASS: terminalization removes legacy copied fill evidence, retains the exact Final receipt, and
/// replay returns the same receipt after reopening SQLite.
/// FAIL: the receipt is absent/changed, legacy fill evidence survives, or replay rejects the row.
#[test]
fn terminal_decision_pending_retains_financial_final_receipt() {
    let (dir, paper, mut engine) = fresh_anchored();
    let result = engine
        .commit_read(
            vec![position_row(
                "TRADE", "0x43", MARKET_A, 0, "BUY", "1.25", "0.4", 302,
            )],
            &context(302, true),
            zero_basis(),
        )
        .unwrap();
    assert_eq!(result.pending.len(), 1);
    let pending = paper.open_decision_pending().unwrap().remove(0);
    let frozen = DecisionContinuationV3::from_durable(&pending).unwrap();
    let final_receipt = pe_event_log::AppendReceipt {
        sequence: pe_core_types::EventSeq(77),
        this_hash: blake3::hash(b"financial-final"),
    };
    let terminal = TerminalDispositionEvidence::final_fill(final_receipt);
    assert!(terminal.fill.is_none());
    let evidence = DecisionPostBoundaryEvidence::from_body_for_continuation(
        DecisionPostBoundaryEvidenceBody {
            version: pe_service::decision_replay::TERMINAL_EVIDENCE_VERSION,
            owners: vec!["source_log".to_owned(), "paper_log".to_owned()],
            source_trade_id: pending.source_trade_id.clone(),
            applied_configuration_hash: frozen.facts.applied_configuration_hash.clone(),
            market_end: None,
            market_price: None,
            book: None,
            clocks: vec![DecisionClockEvidence {
                purpose: "financial_final".to_owned(),
                unix_millis: 302_001,
                submillisecond_nanos: None,
            }],
            authority: AuthorityEvidence {
                kind: "commit_fill_v2".to_owned(),
                outcome: "applied".to_owned(),
                bankroll: Some("9".to_owned()),
            },
            terminal,
        },
        &frozen,
    )
    .unwrap();
    paper
        .close_decision_pending(
            &pending.source_trade_id,
            &serde_json::to_string(&evidence).unwrap(),
            "fill",
            303,
        )
        .unwrap();
    drop(engine);
    drop(paper);

    let reopened = PaperStateDb::open(&dir.path().join("paper.db")).unwrap();
    let row = reopened.decision_pending_history().unwrap().remove(0);
    assert_eq!(row.state, DecisionPendingState::Terminal);
    let replayed = replay_decision_pending(&row).unwrap();
    assert_eq!(
        replayed.post_boundary.body.terminal.final_receipt,
        Some(final_receipt)
    );
    assert!(replayed.post_boundary.body.terminal.fill.is_none());
}

#[test]
fn conversion_underflow_and_unknown_effect_drop_only_their_scopes() {
    let (_dir, paper, mut engine) = fresh_anchored();
    let conversion = pair_effect("CONVERSION", "0x51", MARKET_A, "1", 400);
    let trade = position_row("TRADE", "0x52", MARKET_B, 0, "BUY", "7", "0.5", 400);
    let result = engine
        .commit_read(vec![trade, conversion], &context(400, true), zero_basis())
        .unwrap();
    assert_eq!(result.newly_fenced, None);
    assert_eq!(state(&engine, MARKET_B, 0).atomic(), 7_000_000);
    assert!(!paper.is_wallet_fenced(&wallet()).unwrap());

    let later = engine
        .commit_read(
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
            .all(|value| value == "not_an_entry")
    );
    assert_eq!(state(&engine, MARKET_B, 0).atomic(), 7_000_001);

    let (_dir, paper, mut engine) = fresh_anchored();
    let result = engine
        .commit_read(
            vec![pair_effect("MERGE", "0x61", MARKET_A, "0.000001", 500)],
            &context(500, true),
            zero_basis(),
        )
        .unwrap();
    assert_eq!(result.newly_fenced, None);
    assert_eq!(state(&engine, MARKET_A, 0), ShareAmount::ZERO);
    assert_eq!(state(&engine, MARKET_A, 1), ShareAmount::ZERO);
    assert!(!paper.is_wallet_fenced(&wallet()).unwrap());

    let (_dir, paper, mut engine) = fresh_anchored();
    let result = engine
        .commit_read(
            vec![combo_effect("FUTURE_POSITION_EFFECT", "0x62", 501)],
            &context(501, true),
            zero_basis(),
        )
        .unwrap();
    assert_eq!(result.newly_fenced, None);
    assert!(!paper.is_wallet_fenced(&wallet()).unwrap());

    let (_dir, paper, mut engine) = fresh_anchored();
    let result = engine
        .commit_read(
            vec![combo_effect("CONVERSION", "0x63", 502)],
            &context(502, true),
            zero_basis(),
        )
        .unwrap();
    assert_eq!(result.newly_fenced, None);
    assert!(!paper.is_wallet_fenced(&wallet()).unwrap());
}

#[test]
fn known_unexpressible_redeems_preserve_balances_and_other_market_decisions() {
    for size in ["2", "0"] {
        let (_dir, paper, mut engine) = fresh_anchored();
        engine
            .commit_read(
                vec![position_row(
                    "TRADE", "0x70", MARKET_A, 0, "BUY", "5", "0.5", 559,
                )],
                &context(559, true),
                zero_basis(),
            )
            .unwrap();
        let history_before = paper.gate_history().unwrap();
        let sentinel = aggregate(json!({
            "timestamp": 560, "conditionId": MARKET_A, "type": "REDEEM", "size": size,
            "usdcSize": size, "transactionHash": "0x71", "price": "0", "asset": "",
            "side": "", "outcomeIndex": 999, "outcome": "",
        }));
        let sentinel_id = sentinel.group_id.key().clone();
        let result = engine
            .commit_read(vec![sentinel], &context(560, true), zero_basis())
            .unwrap();
        assert_eq!(result.newly_fenced, None);
        assert_eq!(state(&engine, MARKET_A, 0).atomic(), 5_000_000);
        assert_eq!(result.dispositions[&sentinel_id.0], "raw_only");
        assert!(!paper.wallet_coverage(&wallet()).unwrap().reanchor_required);
        assert_eq!(paper.gate_history().unwrap(), history_before);
        let entry = position_row("TRADE", "0x73", MARKET_B, 0, "BUY", "2", "0.5", 561);
        let entry_id = entry.group_id.key().clone();
        let result = engine
            .commit_read(vec![entry], &context(561, true), zero_basis())
            .unwrap();
        assert_eq!(result.dispositions[&entry_id.0], "decision_pending");
        assert_eq!(result.pending, vec![entry_id]);
        assert_eq!(state(&engine, MARKET_A, 0).atomic(), 5_000_000);
        assert!(!paper.is_wallet_fenced(&wallet()).unwrap());
        assert!(!paper.wallet_coverage(&wallet()).unwrap().reanchor_required);
        // Only a successful anchor replaces the stale resolved-condition balance.
        install_anchor(
            &mut engine,
            &paper,
            561,
            vec![(
                MarketId(VenueMarketId(MARKET_B.to_owned())),
                OutcomeId(0),
                ShareAmount::from_atomic(2_000_000),
            )],
            562,
        );
        assert_eq!(state(&engine, MARKET_A, 0), ShareAmount::ZERO);
        assert_eq!(state(&engine, MARKET_B, 0).atomic(), 2_000_000);
    }
}

#[test]
fn equal_second_validity_is_order_independent_or_drops_its_scope() {
    let (_dir, paper, mut engine) = fresh_anchored();
    let split = pair_effect("SPLIT", "0x65", MARKET_A, "1", 550);
    let sell = position_row("TRADE", "0x66", MARKET_A, 0, "SELL", "2", "0.5", 550);
    let result = engine
        .commit_read(vec![sell, split], &context(550, true), zero_basis())
        .unwrap();
    assert_eq!(result.newly_fenced, None);
    assert_eq!(
        paper.wallet_scope_drops(&wallet(), i64::MAX).unwrap()[0].cause,
        "order_dependent_equal_second"
    );
    assert_eq!(state(&engine, MARKET_A, 0), ShareAmount::ZERO);
    assert_eq!(state(&engine, MARKET_A, 1), ShareAmount::ZERO);
    assert!(!paper.is_wallet_fenced(&wallet()).unwrap());

    let (_dir, paper, mut engine) = fresh_anchored();
    let mut no_copy = context(551, true);
    no_copy.copy_eligible = false;
    let disjoint = vec![
        pair_effect("SPLIT", "0x67", MARKET_A, "1.250000", 551),
        position_row("TRADE", "0x68", MARKET_B, 0, "BUY", "2.750000", "0.5", 551),
    ];
    let result = engine
        .commit_read(disjoint, &no_copy, zero_basis())
        .unwrap();
    assert_eq!(result.newly_fenced, None);
    assert_eq!(state(&engine, MARKET_A, 0).atomic(), 1_250_000);
    assert_eq!(state(&engine, MARKET_A, 1).atomic(), 1_250_000);
    assert_eq!(state(&engine, MARKET_B, 0).atomic(), 2_750_000);
    assert!(!paper.is_wallet_fenced(&wallet()).unwrap());
}

#[test]
fn sibling_redeem_residual_commits_version_three_and_replay_checks_it() {
    let (dir, paper, mut engine) = fresh();
    paper.set_cursor(&wallet(), 0).unwrap();
    install_anchor(
        &mut engine,
        &paper,
        0,
        vec![
            (
                MarketId(VenueMarketId(MARKET_A.to_owned())),
                OutcomeId(0),
                ShareAmount::from_atomic(47_094_800),
            ),
            (
                MarketId(VenueMarketId(MARKET_B.to_owned())),
                OutcomeId(1),
                ShareAmount::from_atomic(33_322_200),
            ),
        ],
        1,
    );
    let exact = position_row(
        "REDEEM",
        "0xredeem-exact",
        MARKET_A,
        0,
        "",
        "47.094800",
        "0",
        570,
    );
    let clamped = position_row(
        "REDEEM",
        "0xredeem-clamped",
        MARKET_B,
        1,
        "",
        "33.322232",
        "0",
        570,
    );
    let result = engine
        .commit_read(vec![clamped, exact], &context(570, true), zero_basis())
        .unwrap();

    assert!(
        result
            .dispositions
            .values()
            .all(|disposition| disposition == "applied")
    );
    assert_eq!(state(&engine, MARKET_A, 0), ShareAmount::ZERO);
    assert_eq!(state(&engine, MARKET_B, 1), ShareAmount::ZERO);
    let groups = paper.activity_groups_after(&wallet(), 0).unwrap();
    let decoded = groups
        .iter()
        .map(|group| {
            (
                group.source_trade_id.clone(),
                AppliedEffect::from_document(&group.proof_json).unwrap(),
                serde_json::from_str::<Value>(&group.proof_json).unwrap(),
            )
        })
        .collect::<Vec<_>>();
    let exact_document = decoded
        .iter()
        .find(|(_, effect, _)| effect.clamped_residual.is_none())
        .unwrap();
    let clamped_document = decoded
        .iter()
        .find(|(_, effect, _)| effect.clamped_residual == Some(32))
        .unwrap();
    assert_eq!(exact_document.2["version"], 1);
    assert!(
        exact_document.2.get("clamped_residual_atomic").is_none(),
        "an exact redeem stays on the version-one schema"
    );
    assert_eq!(clamped_document.2["version"], 3);
    assert_eq!(clamped_document.2["clamped_residual_atomic"], 32);

    let replayed = replay_wallet_ledger(&paper, wallet()).unwrap();
    assert_eq!(
        replayed.position(&wallet()).unwrap().positions[&market_outcome(MARKET_A, 0)]
            .long_contracts,
        ShareAmount::ZERO
    );
    assert_eq!(
        replayed.position(&wallet()).unwrap().positions[&market_outcome(MARKET_B, 1)]
            .long_contracts,
        ShareAmount::ZERO
    );

    let mut mismatched = clamped_document.2.clone();
    mismatched["clamped_residual_atomic"] = json!(31);
    let connection = rusqlite::Connection::open(dir.path().join("paper.db")).unwrap();
    connection
        .execute(
            "UPDATE activity_groups SET proof_json = ?1 WHERE source_trade_id = ?2",
            rusqlite::params![
                serde_json::to_string(&mismatched).unwrap(),
                clamped_document.0.0
            ],
        )
        .unwrap();
    drop(connection);
    assert!(matches!(
        replay_wallet_ledger(&paper, wallet()),
        Err(WalletLedgerReplayError::ClampedResidualMismatch {
            source_epoch: 570,
            ..
        })
    ));
}

#[test]
fn replay_rejects_documents_that_attempt_to_stack_redeem_tolerance() {
    let (dir, paper, mut engine) = fresh();
    paper.set_cursor(&wallet(), 0).unwrap();
    install_anchor(
        &mut engine,
        &paper,
        0,
        vec![
            (
                MarketId(VenueMarketId(MARKET_A.to_owned())),
                OutcomeId(0),
                ShareAmount::from_atomic(100),
            ),
            (
                MarketId(VenueMarketId(MARKET_B.to_owned())),
                OutcomeId(0),
                ShareAmount::from_atomic(100),
            ),
        ],
        1,
    );
    engine
        .commit_read(
            vec![
                position_row(
                    "REDEEM",
                    "0xreplay-stack-a",
                    MARKET_A,
                    0,
                    "",
                    "0.000190",
                    "0",
                    575,
                ),
                position_row(
                    "REDEEM",
                    "0xreplay-stack-b",
                    MARKET_B,
                    0,
                    "",
                    "0.000190",
                    "0",
                    575,
                ),
            ],
            &context(575, true),
            zero_basis(),
        )
        .unwrap();
    let groups = paper.activity_groups_after(&wallet(), 0).unwrap();
    assert_eq!(groups.len(), 2);
    let documents = [
        AppliedEffect {
            effect: LedgerEffect::Redeem {
                market_id: MarketId(VenueMarketId(MARKET_A.to_owned())),
                outcome_id: OutcomeId(0),
                amount: ShareAmount::from_atomic(190),
            },
            clamped_residual: Some(90),
        }
        .to_document()
        .unwrap(),
        AppliedEffect {
            effect: LedgerEffect::Redeem {
                market_id: MarketId(VenueMarketId(MARKET_A.to_owned())),
                outcome_id: OutcomeId(0),
                amount: ShareAmount::from_atomic(20),
            },
            clamped_residual: Some(20),
        }
        .to_document()
        .unwrap(),
    ];
    let connection = rusqlite::Connection::open(dir.path().join("paper.db")).unwrap();
    for (group, document) in groups.iter().zip(documents) {
        connection
            .execute(
                "UPDATE activity_groups SET proof_json = ?1 WHERE source_trade_id = ?2",
                rusqlite::params![document, group.source_trade_id.0],
            )
            .unwrap();
    }
    drop(connection);

    assert!(matches!(
        replay_wallet_ledger(&paper, wallet()),
        Err(WalletLedgerReplayError::Ledger { .. })
    ));
}

#[test]
fn already_fenced_commit_serializes_only_clamped_redeems_as_version_three() {
    let (_dir, paper, mut engine) = fresh();
    paper.set_cursor(&wallet(), 0).unwrap();
    install_anchor(
        &mut engine,
        &paper,
        0,
        vec![
            (
                MarketId(VenueMarketId(MARKET_A.to_owned())),
                OutcomeId(0),
                ShareAmount::from_atomic(1_000),
            ),
            (
                MarketId(VenueMarketId(MARKET_B.to_owned())),
                OutcomeId(0),
                ShareAmount::from_atomic(1_000),
            ),
        ],
        1,
    );
    let connection = rusqlite::Connection::open(_dir.path().join("paper.db")).unwrap();
    connection.execute("INSERT INTO wallet_fences VALUES (?1, 'legacy', 'conversion_unknown_conditions', '{}', 580)", [wallet().to_string()]).unwrap();
    engine = BucketCommitEngine::load(Arc::clone(&paper), engine.ledger().clone()).unwrap();
    let clamped = position_row(
        "REDEEM",
        "0xfenced-clamped",
        MARKET_A,
        0,
        "",
        "0.001050",
        "0",
        581,
    );
    let exact = position_row(
        "REDEEM",
        "0xfenced-exact",
        MARKET_B,
        0,
        "",
        "0.001000",
        "0",
        581,
    );
    let result = engine
        .commit_read(vec![clamped, exact], &context(581, true), zero_basis())
        .unwrap();

    assert!(
        result
            .dispositions
            .values()
            .all(|disposition| disposition == "wallet_fenced_applied")
    );
    let groups = paper.activity_groups_after(&wallet(), 580).unwrap();
    let documents = groups
        .iter()
        .map(|group| {
            (
                AppliedEffect::from_document(&group.proof_json).unwrap(),
                serde_json::from_str::<Value>(&group.proof_json).unwrap(),
            )
        })
        .collect::<Vec<_>>();
    assert!(
        documents.iter().any(|(effect, value)| {
            effect.clamped_residual == Some(50) && value["version"] == 3
        })
    );
    assert!(documents.iter().any(|(effect, value)| {
        effect.clamped_residual.is_none()
            && value["version"] == 1
            && value.get("clamped_residual_atomic").is_none()
    }));
}

#[test]
fn homogeneous_five_buys_apply_and_allow_later_independent_entry() {
    let (_dir, paper, mut engine) = fresh_anchored();
    let five_connected = (0..5)
        .map(|ordinal| {
            position_row(
                "TRADE",
                &format!("0xwide-{ordinal}"),
                MARKET_A,
                0,
                "BUY",
                "0.000001",
                "0.5",
                592,
            )
        })
        .collect::<Vec<_>>();
    let result = engine
        .commit_read(five_connected, &context(592, true), zero_basis())
        .unwrap();
    assert_eq!(result.newly_fenced, None);
    assert_eq!(state(&engine, MARKET_A, 0).atomic(), 5);
    assert!(!paper.is_wallet_fenced(&wallet()).unwrap());
    assert!(result.pending.is_empty());
    assert_eq!(result.dispositions.len(), 5);
    assert!(
        result
            .dispositions
            .values()
            .all(|value| value == "ambiguous_first_entry_same_second")
    );
    assert!(
        paper.gate_history().unwrap()[&wallet()]
            .contains(&MarketId(VenueMarketId(MARKET_A.to_owned())))
    );
    let later = engine
        .commit_read(
            vec![position_row(
                "TRADE",
                "0xwide-later",
                MARKET_B,
                0,
                "BUY",
                "1",
                "0.5",
                593,
            )],
            &context(593, true),
            zero_basis(),
        )
        .unwrap();
    assert_eq!(later.pending.len(), 1);
    assert_eq!(later.newly_fenced, None);
    assert_eq!(
        state(&engine, MARKET_B, 0),
        ShareAmount::from_whole(1).unwrap()
    );
}

#[test]
fn equal_second_components_reject_stacking_cross_effects_and_undecidable_size() {
    let (_dir, paper, mut engine) = fresh();
    paper.set_cursor(&wallet(), 0).unwrap();
    install_anchor(
        &mut engine,
        &paper,
        0,
        vec![(
            MarketId(VenueMarketId(MARKET_A.to_owned())),
            OutcomeId(0),
            ShareAmount::from_atomic(100),
        )],
        1,
    );
    let stacking = vec![
        position_row("REDEEM", "0xstack-a", MARKET_A, 0, "", "0.000190", "0", 590),
        position_row("REDEEM", "0xstack-b", MARKET_A, 0, "", "0.000020", "0", 590),
    ];
    let result = engine
        .commit_read(stacking, &context(590, true), zero_basis())
        .unwrap();
    assert_eq!(result.newly_fenced, None);
    assert_eq!(
        paper.wallet_scope_drops(&wallet(), i64::MAX).unwrap()[0].cause,
        "order_dependent_equal_second"
    );
    assert_eq!(state(&engine, MARKET_A, 0).atomic(), 100);

    let (_dir, paper, mut engine) = fresh();
    paper.set_cursor(&wallet(), 0).unwrap();
    install_anchor(
        &mut engine,
        &paper,
        0,
        vec![(
            MarketId(VenueMarketId(MARKET_A.to_owned())),
            OutcomeId(0),
            ShareAmount::from_atomic(100),
        )],
        1,
    );
    let split_and_redeem = vec![
        pair_effect("SPLIT", "0xconnected-split", MARKET_A, "0.000050", 591),
        position_row(
            "REDEEM",
            "0xconnected-redeem",
            MARKET_A,
            0,
            "",
            "0.000150",
            "0",
            591,
        ),
    ];
    let result = engine
        .commit_read(split_and_redeem, &context(591, true), zero_basis())
        .unwrap();
    assert_eq!(result.newly_fenced, None);
    assert_eq!(
        paper.wallet_scope_drops(&wallet(), i64::MAX).unwrap()[0].cause,
        "order_dependent_equal_second"
    );
    assert_eq!(state(&engine, MARKET_A, 0).atomic(), 100);
    assert_eq!(state(&engine, MARKET_A, 1), ShareAmount::ZERO);

    let (_dir, paper, mut engine) = fresh_anchored();
    let mut mixed = (0..4)
        .map(|ordinal| {
            position_row(
                "TRADE",
                &format!("0xwide-mixed-{ordinal}"),
                MARKET_A,
                0,
                "BUY",
                "0.000001",
                "0.5",
                592,
            )
        })
        .collect::<Vec<_>>();
    mixed.push(pair_effect(
        "SPLIT",
        "0xwide-mixed-split",
        MARKET_A,
        "0.000001",
        592,
    ));
    let result = engine
        .commit_read(mixed, &context(592, true), zero_basis())
        .unwrap();
    assert_eq!(result.newly_fenced, None);
    assert_eq!(
        paper.wallet_scope_drops(&wallet(), i64::MAX).unwrap()[0].cause,
        "order_dependent_equal_second"
    );
    assert_eq!(state(&engine, MARKET_A, 0), ShareAmount::ZERO);
    assert!(!paper.is_wallet_fenced(&wallet()).unwrap());

    let (_dir, paper, mut engine) = fresh();
    paper.set_cursor(&wallet(), 0).unwrap();
    install_anchor(
        &mut engine,
        &paper,
        0,
        vec![(
            MarketId(VenueMarketId(MARKET_A.to_owned())),
            OutcomeId(0),
            ShareAmount::from_atomic(100),
        )],
        1,
    );
    let sell_then_redeem = vec![
        position_row(
            "TRADE",
            "0xsell-conflict",
            MARKET_A,
            0,
            "SELL",
            "0.000020",
            "0.5",
            593,
        ),
        position_row(
            "REDEEM",
            "0xredeem-conflict",
            MARKET_A,
            0,
            "",
            "0.000100",
            "0",
            593,
        ),
    ];
    let result = engine
        .commit_read(sell_then_redeem, &context(593, true), zero_basis())
        .unwrap();
    assert_eq!(result.newly_fenced, None);
    assert_eq!(
        paper.wallet_scope_drops(&wallet(), i64::MAX).unwrap()[0].cause,
        "order_dependent_equal_second"
    );
    assert_eq!(state(&engine, MARKET_A, 0).atomic(), 100);

    let (_dir, paper, mut engine) = fresh();
    paper.set_cursor(&wallet(), 0).unwrap();
    install_anchor(
        &mut engine,
        &paper,
        0,
        vec![
            (
                MarketId(VenueMarketId(MARKET_A.to_owned())),
                OutcomeId(0),
                ShareAmount::from_atomic(200),
            ),
            (
                MarketId(VenueMarketId(MARKET_A.to_owned())),
                OutcomeId(1),
                ShareAmount::from_atomic(100),
            ),
        ],
        1,
    );
    let merge_and_redeem = vec![
        pair_effect("MERGE", "0xstrict-merge", MARKET_A, "0.000150", 594),
        position_row(
            "REDEEM",
            "0xmerge-redeem",
            MARKET_A,
            0,
            "",
            "0.000050",
            "0",
            594,
        ),
    ];
    let result = engine
        .commit_read(merge_and_redeem, &context(594, true), zero_basis())
        .unwrap();
    assert_eq!(result.newly_fenced, None);
    assert_eq!(
        paper.wallet_scope_drops(&wallet(), i64::MAX).unwrap()[0].cause,
        "order_dependent_equal_second"
    );
    assert_eq!(state(&engine, MARKET_A, 0).atomic(), 200);
    assert_eq!(state(&engine, MARKET_A, 1).atomic(), 100);
}

#[test]
fn changed_and_all_unseen_late_groups_drop_scopes_without_reanchor() {
    let (_dir, paper, mut engine) = fresh_anchored();
    let mut no_copy = context(600, true);
    no_copy.copy_eligible = false;
    let original = position_row("TRADE", "0x71", MARKET_A, 0, "BUY", "1.250000", "0.4", 600);
    let original_id = original.group_id.key().clone();
    engine
        .commit_read(vec![original], &no_copy, zero_basis())
        .unwrap();
    assert_eq!(state(&engine, MARKET_A, 0).atomic(), 1_250_000);

    let changed = position_row("TRADE", "0x71", MARKET_A, 0, "BUY", "9.000000", "0.4", 600);
    assert_eq!(changed.group_id.key(), &original_id);
    let result = engine
        .commit_read(vec![changed], &no_copy, zero_basis())
        .unwrap();
    assert_eq!(result.newly_fenced, None);
    assert_eq!(state(&engine, MARKET_A, 0).atomic(), 1_250_000);
    assert!(!paper.is_wallet_fenced(&wallet()).unwrap());

    let (_dir, paper, mut engine) = fresh();
    paper.set_cursor(&wallet(), 0).unwrap();
    install_anchor(
        &mut engine,
        &paper,
        0,
        vec![
            (
                MarketId(VenueMarketId(MARKET_A.to_owned())),
                OutcomeId(0),
                ShareAmount::from_atomic(5_000_000),
            ),
            (
                MarketId(VenueMarketId(MARKET_B.to_owned())),
                OutcomeId(0),
                ShareAmount::from_atomic(5_000_000),
            ),
        ],
        1,
    );
    let first = position_row("REDEEM", "0x81", MARKET_A, 0, "", "5", "0", 700);
    engine
        .commit_read(vec![first], &no_copy, zero_basis())
        .unwrap();
    let late = position_row("REDEEM", "0x82", MARKET_B, 0, "", "5", "0", 700);
    let late_id = late.group_id.key().clone();
    let cursor_before = paper.cursor(&wallet()).unwrap();
    let result = engine
        .commit_read(vec![late], &no_copy, zero_basis())
        .unwrap();
    assert_eq!(result.newly_fenced, None);
    assert_eq!(result.dispositions[&late_id.0], "scope_dropped");
    assert_eq!(state(&engine, MARKET_A, 0), ShareAmount::ZERO);
    assert_eq!(state(&engine, MARKET_B, 0).atomic(), 5_000_000);
    assert!(!paper.is_wallet_fenced(&wallet()).unwrap());
    assert!(!paper.wallet_coverage(&wallet()).unwrap().reanchor_required);
    assert_eq!(paper.cursor(&wallet()).unwrap(), cursor_before);
    assert!(result.pending.is_empty());
    assert!(paper.open_decision_pending().unwrap().is_empty());
    let late_group = paper
        .activity_groups_after(&wallet(), 699)
        .unwrap()
        .into_iter()
        .find(|group| group.source_trade_id == late_id)
        .unwrap();
    assert!(matches!(
        LedgerEffect::from_document(&late_group.proof_json),
        Ok(LedgerEffect::Redeem { .. })
    ));

    let decrement = position_row("TRADE", "0x83", MARKET_B, 0, "SELL", "1", "0.5", 701);
    let decrement_id = decrement.group_id.key().clone();
    let activity_before = paper.activity(&wallet()).unwrap();
    let result = engine
        .commit_read(vec![decrement.clone()], &context(701, true), zero_basis())
        .unwrap();
    assert_eq!(result.newly_fenced, None);
    assert_eq!(result.dispositions[&decrement_id.0], "scope_dropped");
    assert!(result.pending.is_empty());
    assert_eq!(state(&engine, MARKET_B, 0).atomic(), 5_000_000);
    assert!(!paper.wallet_coverage(&wallet()).unwrap().reanchor_required);
    // Exact retries leave the committed cursor, activity clock and coverage unchanged.
    assert_eq!(paper.cursor(&wallet()).unwrap(), Some(701));
    assert!(paper.activity(&wallet()).unwrap() > activity_before);
    let coverage = paper.wallet_coverage(&wallet()).unwrap();
    let reread = engine
        .commit_read(vec![decrement], &context(701, true), zero_basis())
        .unwrap();
    assert!(reread.already_committed);
    assert_eq!(paper.cursor(&wallet()).unwrap(), Some(701));
    assert_eq!(paper.wallet_coverage(&wallet()).unwrap(), coverage);
    let decrement_group = paper
        .activity_groups_after(&wallet(), 700)
        .unwrap()
        .into_iter()
        .find(|group| group.source_trade_id == decrement_id)
        .unwrap();
    assert!(matches!(
        LedgerEffect::from_document(&decrement_group.proof_json),
        Ok(LedgerEffect::Trade { .. })
    ));

    let replayed = replay_wallet_ledger(&paper, wallet()).unwrap();
    assert_eq!(
        replayed.position(&wallet()).unwrap().positions[&market_outcome(MARKET_A, 0)]
            .long_contracts
            .atomic(),
        0
    );
    assert_eq!(
        replayed.position(&wallet()).unwrap().positions[&market_outcome(MARKET_B, 0)]
            .long_contracts
            .atomic(),
        5_000_000
    );

    install_anchor(&mut engine, &paper, 701, Vec::new(), 702);
    assert!(!paper.wallet_coverage(&wallet()).unwrap().reanchor_required);
    let mut after_anchor = context(702, true);
    after_anchor.copy_eligible = false;
    let applied = engine
        .commit_read(
            vec![position_row(
                "TRADE",
                "0xafter-late-anchor",
                MARKET_B,
                0,
                "BUY",
                "2",
                "0.5",
                702,
            )],
            &after_anchor,
            zero_basis(),
        )
        .unwrap();
    assert_eq!(applied.newly_fenced, None);
    assert_eq!(state(&engine, MARKET_B, 0).atomic(), 0);
}

#[test]
fn mixed_durable_and_unseen_equal_second_groups_drop_only_unseen_scope() {
    let (_dir, paper, mut engine) = fresh_anchored();
    let mut no_copy = context(710, true);
    no_copy.copy_eligible = false;
    let durable = position_row("TRADE", "0x83", MARKET_A, 0, "BUY", "2", "0.4", 710);
    let durable_id = durable.group_id.key().clone();
    engine
        .commit_read(vec![durable.clone()], &no_copy, zero_basis())
        .unwrap();
    let unseen = position_row("TRADE", "0x84", MARKET_B, 0, "BUY", "5", "0.6", 710);
    let unseen_id = unseen.group_id.key().clone();

    let result = engine
        .commit_read(vec![durable, unseen], &no_copy, zero_basis())
        .unwrap();

    assert_eq!(result.dispositions[&durable_id.0], "already_committed");
    assert_eq!(result.dispositions[&unseen_id.0], "scope_dropped");
    assert_eq!(result.newly_fenced, None);
    assert_eq!(state(&engine, MARKET_A, 0).atomic(), 2_000_000);
    assert_eq!(state(&engine, MARKET_B, 0), ShareAmount::ZERO);
    assert!(!paper.is_wallet_fenced(&wallet()).unwrap());
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

fn committed_source_read(
    dir: &tempfile::TempDir,
    groups: &[FixtureAggregate],
) -> (support::ProducerShapedRead, pe_event_log::AppendReceipt) {
    let rows: Vec<_> = groups.iter().flat_map(|group| group.rows.iter()).collect();
    let epoch = groups
        .iter()
        .map(|group| group.source_time.0.unix_timestamp())
        .max()
        .unwrap();
    let mut writer = pe_event_log::Writer::open(dir.path().join("source.log")).unwrap();
    support::append_committed_read_v1(
        &mut writer,
        wallet(),
        &serde_json::to_vec(&rows).unwrap(),
        epoch + 10,
        epoch + 11,
    )
}

fn commit_open_source_read(
    engine: &mut BucketCommitEngine,
    read: &support::ProducerShapedRead,
    receipt: pe_event_log::AppendReceipt,
    overrides: HashMap<pe_core_types::SourceTradeId, IdentityOverride>,
) {
    let epoch = read.aggregates[0].source_time.0.unix_timestamp();
    let mut context = context(epoch, true);
    context.decision_inputs_json = read.decision_inputs_json.clone();
    context.page_occurrences = vec![read.page.clone()];
    context.read_commitment =
        Some(pe_service::bucket_commit::ActivityReadCommitmentReceipt::LegacyV1(receipt));
    context.identity_overrides = overrides;
    let committed = engine
        .commit(read.aggregates.clone(), &context, zero_basis())
        .unwrap();
    assert_eq!(committed.pending.len(), read.aggregates.len());
}

fn online_census_copy(dir: &tempfile::TempDir) -> (std::path::PathBuf, std::path::PathBuf) {
    let state = dir.path().join("snapshot.db");
    let source = dir.path().join("snapshot-source.log");
    // SQLite's online backup captures committed WAL pages while the live owner stays open.
    let connection = rusqlite::Connection::open(dir.path().join("paper.db")).unwrap();
    connection
        .execute("VACUUM INTO ?1", [state.to_str().unwrap()])
        .unwrap();
    std::fs::copy(dir.path().join("source.log"), &source).unwrap();
    (state, source)
}

fn census_cli(state: &std::path::Path, source: &std::path::Path) -> std::process::Output {
    std::process::Command::new(env!("CARGO_BIN_EXE_pe-service"))
        .arg("--validate-open-continuations")
        .arg("--paper-state")
        .arg(state)
        .arg("--source-log")
        .arg(source)
        .output()
        .unwrap()
}

/// PASS: a live post-backup continuation is outside the offline census, but in the boot census.
#[test]
fn online_snapshot_does_not_cover_later_continuation() {
    let (dir, paper, mut engine) = fresh_anchored();
    let (read, receipt) = committed_source_read(
        &dir,
        &[position_row(
            "TRADE",
            "0xsnapshot-first",
            MARKET_A,
            0,
            "BUY",
            "2",
            "0.4",
            910,
        )],
    );
    commit_open_source_read(&mut engine, &read, receipt, HashMap::new());
    let captured = paper.open_decision_pending().unwrap();
    let (state, source) = online_census_copy(&dir);
    let (later, receipt) = committed_source_read(
        &dir,
        &[position_row(
            "TRADE",
            "0xsnapshot-later",
            MARKET_B,
            0,
            "BUY",
            "3",
            "0.6",
            911,
        )],
    );
    commit_open_source_read(&mut engine, &later, receipt, HashMap::new());
    let offline = PaperStateDb::open_read_only(&state).unwrap();
    let offline_index = pe_service::risk_inputs::SourceReceiptIndex::replay(&source).unwrap();
    assert_eq!(offline.open_decision_pending().unwrap(), captured);
    assert_eq!(
        pe_service::bucket_commit::validate_open_continuations(&offline, &offline_index).unwrap(),
        1
    );
    let current_index =
        pe_service::risk_inputs::SourceReceiptIndex::replay(&dir.path().join("source.log"))
            .unwrap();
    assert_eq!(
        pe_service::bucket_commit::validate_open_continuations(&paper, &current_index).unwrap(),
        2
    );
    assert!(
        paper
            .is_decision_pending_open(later.aggregates[0].group_id.key())
            .unwrap()
    );
    for (state, source, expected) in [
        (state, source, "open_rows=1 validated=1\n"),
        (
            dir.path().join("paper.db"),
            dir.path().join("source.log"),
            "open_rows=2 validated=2\n",
        ),
    ] {
        let output = census_cli(&state, &source);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(String::from_utf8(output.stdout).unwrap(), expected);
    }
}

/// PASS: newer invalid evidence fails before owner construction; all pending state stays unchanged.
#[tokio::test]
async fn post_snapshot_invalid_continuation_blocks_boot_resume() {
    let (dir, paper, engine) = fresh();
    drop(engine);
    let (state, source, id) = support::post_snapshot_invalid_continuation(
        &paper,
        &dir.path().join("paper.db"),
        &dir.path().join("source.log"),
        wallet(),
    );
    let pending = paper.open_decision_pending().unwrap();
    let positions = paper.leader_positions().unwrap();
    let history = paper.gate_history().unwrap();
    let cursor = paper.cursor(&wallet()).unwrap();
    let groups = paper.activity_groups_after(&wallet(), 0).unwrap();
    let offline = census_cli(&state, &source);
    assert!(
        offline.status.success(),
        "{}",
        String::from_utf8_lossy(&offline.stderr)
    );
    assert_eq!(
        String::from_utf8(offline.stdout).unwrap(),
        "open_rows=1 validated=1\n"
    );
    let hooks = support::continuation_hooks(920);
    let paper_path = dir.path().join("paper.log");
    drop(pe_event_log::Writer::open(&paper_path).unwrap());
    let index = pe_service::risk_inputs::SourceReceiptIndex::replay(&dir.path().join("source.log"))
        .unwrap();
    let mut resumes = 0;
    let result = async {
        let count = pe_service::bucket_commit::validate_open_continuations(&paper, &index)?;
        let (_control, receiver) = tokio::sync::mpsc::channel(4);
        let mut owner = support::continuation_orchestrator(
            paper.clone(),
            &paper_path,
            wallet(),
            receiver,
            hooks.clone(),
        );
        resumes += count;
        owner.resume_pending_before_producers().await.unwrap();
        Ok::<_, pe_service::bucket_commit::ContinuationValidationError>(count)
    }
    .await;
    assert_eq!(result.unwrap_err().source_trade_id.as_ref(), Some(&id));
    assert_eq!(resumes, 0);
    assert_eq!(paper.open_decision_pending().unwrap(), pending);
    assert_eq!(paper.leader_positions().unwrap(), positions);
    assert_eq!(paper.gate_history().unwrap(), history);
    assert_eq!(paper.cursor(&wallet()).unwrap(), cursor);
    assert_eq!(paper.activity_groups_after(&wallet(), 0).unwrap(), groups);
    support::assert_no_continuation_side_effects(
        &paper,
        &dir.path().join("paper.db"),
        &paper_path,
        &hooks,
    );
}

/// PASS: the actual offline command rejects a fixed torn frame and never repairs either input.
#[test]
fn offline_census_rejects_torn_source_copy_without_repair() {
    let (dir, _paper, mut engine) = fresh_anchored();
    let (read, receipt) = committed_source_read(
        &dir,
        &[position_row(
            "TRADE",
            "0xtorn-copy",
            MARKET_A,
            0,
            "BUY",
            "2",
            "0.4",
            910,
        )],
    );
    commit_open_source_read(&mut engine, &read, receipt, HashMap::new());
    let (state, source) = online_census_copy(&dir);
    // Keep the five-byte file header and one byte of the first frame length.
    std::fs::OpenOptions::new()
        .write(true)
        .open(&source)
        .unwrap()
        .set_len(6)
        .unwrap();
    assert!(matches!(
        pe_event_log::Reader::replay(&source),
        Err(pe_event_log::LogError::Truncated { .. })
    ));
    let before_state = std::fs::read(&state).unwrap();
    let before_source = std::fs::read(&source).unwrap();
    let output = census_cli(&state, &source);
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("build verified source receipt index for open-continuation census"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(std::fs::read(&state).unwrap(), before_state);
    assert_eq!(std::fs::read(&source).unwrap(), before_source);
}

/// PASS: the fact-altered row is named and validation stops before any resume, admission,
/// dispatch seed, financial prepare, or paper order. FAIL: either row resumes or the edit passes.
#[tokio::test]
async fn boot_validation_rejects_fact_altered_row_before_any_resume() {
    let (dir, paper, mut engine) = fresh_anchored();
    let (read, commitment) = committed_source_read(
        &dir,
        &[
            position_row("TRADE", "0xvalid-open", MARKET_A, 0, "BUY", "2", "0.4", 900),
            position_row(
                "TRADE",
                "0xaltered-open",
                MARKET_B,
                0,
                "BUY",
                "3",
                "0.6",
                900,
            ),
        ],
    );
    commit_open_source_read(&mut engine, &read, commitment, HashMap::new());
    let rows = paper.open_decision_pending().unwrap();
    assert_eq!(rows.len(), 2);
    let altered = &rows[1];
    let mut frozen: Value = serde_json::from_str(&altered.frozen_inputs_json).unwrap();
    frozen["price"] = json!("0.7");
    let connection = rusqlite::Connection::open(dir.path().join("paper.db")).unwrap();
    connection
        .execute(
            "UPDATE decision_pending SET frozen_inputs_json = ?1 WHERE source_trade_id = ?2",
            rusqlite::params![frozen.to_string(), altered.source_trade_id.0],
        )
        .unwrap();
    drop(connection);
    drop(engine);
    let hooks = support::continuation_hooks(920);
    let paper_path = dir.path().join("paper.log");
    drop(pe_event_log::Writer::open(&paper_path).unwrap());
    let index = pe_service::risk_inputs::SourceReceiptIndex::replay(&dir.path().join("source.log"))
        .unwrap();
    let mut resumes = 0;
    // Same ordering as service boot: validate the entire census before constructing the owner.
    let result = async {
        let count = pe_service::bucket_commit::validate_open_continuations(&paper, &index)?;
        let (_control, receiver) = tokio::sync::mpsc::channel(4);
        let mut orchestrator = support::continuation_orchestrator(
            Arc::clone(&paper),
            &paper_path,
            wallet(),
            receiver,
            Arc::clone(&hooks),
        );
        resumes += count;
        orchestrator
            .resume_pending_before_producers()
            .await
            .unwrap();
        Ok::<_, pe_service::bucket_commit::ContinuationValidationError>(count)
    }
    .await;
    let error = result.unwrap_err();
    assert_eq!(
        error.source_trade_id.as_ref(),
        Some(&altered.source_trade_id)
    );
    assert!(error.to_string().contains(&altered.source_trade_id.0));
    assert_eq!(resumes, 0);
    assert_eq!(paper.open_decision_pending().unwrap().len(), 2);
    support::assert_no_continuation_side_effects(
        &paper,
        &dir.path().join("paper.db"),
        &paper_path,
        &hooks,
    );
}

/// PASS: open continuation-5 rows under a semantic-1 Start resume before the semantic-2 seal;
/// a second restart changes no seal, terminal rows, bucket effects, history, or balances.
#[tokio::test]
async fn valid_pending_restart_resumes_once() {
    let (dir, paper, mut engine) = fresh_anchored();
    let source_path = dir.path().join("source.log");
    let paper_path = dir.path().join("paper.log");
    drop(pe_event_log::Writer::open(&source_path).unwrap());
    let mut paper_writer = pe_event_log::Writer::open(&paper_path).unwrap();
    drop(pe_execution_core::LiveJournal::open(dir.path().join("live_journal.log")).unwrap());
    let hot_hash = pe_service::runtime_config::RuntimeConfig::from_service_config(
        &pe_service::config::ServiceConfig::default(),
    )
    .canonical_hash();
    let at = time::OffsetDateTime::from_unix_timestamp(900).unwrap();
    let start = pe_service::paper_recovery::QualificationStarted {
        starting_bankroll: pe_core_types::CollateralAmount::from_decimal_exact(
            rust_decimal::Decimal::from(1_000),
        )
        .unwrap(),
        paper_prefix: pe_service::paper_recovery::TailBinding::from(
            &pe_event_log::Scanner::verify(&paper_path).unwrap(),
        ),
        source_prefix: pe_service::paper_recovery::TailBinding::from(
            &pe_event_log::Scanner::verify(&source_path).unwrap(),
        ),
        live_prefix: pe_service::paper_recovery::TailBinding {
            physical_tail: 0,
            last_sequence: None,
            last_hash: "00".repeat(32),
        },
        artifact_blake3: "scenario".to_owned(),
        static_config_hash: "scenario".to_owned(),
        hot_config_hash: hot_hash.clone(),
        generation: "scenario".to_owned(),
        activation_id: "scenario".to_owned(),
        ranking_batch_id: 1,
        membership: vec![wallet()],
        membership_proofs_hash: "scenario".to_owned(),
        schema_version: pe_service::paper_recovery::PAPER_LOG_SCHEMA_VERSION,
        parser_version: 1,
        financial_semantic_version: 1,
    };
    let start_receipt = paper_writer
        .append_synced(pe_event_log::EnvelopeIn {
            source_id: pe_core_types::SourceId("pe-service.qualification".to_owned()),
            schema_version: pe_service::paper_recovery::PAPER_LOG_SCHEMA_VERSION,
            parser_version: 1,
            observed_at: pe_core_types::SourceTimestamp(at),
            received_at: pe_core_types::ReceivedAt(at),
            content_type: pe_event_log::ContentType::Json,
            payload: serde_json::to_vec(
                &pe_service::paper_recovery::PaperLogRecord::QualificationStarted(Arc::new(start)),
            )
            .unwrap(),
        })
        .unwrap();
    drop(paper_writer);
    paper
        .reset_financial_era(
            start_receipt,
            pe_core_types::CollateralAmount::from_decimal_exact(rust_decimal::Decimal::from(1_000))
                .unwrap(),
            None,
        )
        .unwrap();
    let rows = [
        position_row("TRADE", "0xrestart-a", MARKET_A, 0, "BUY", "2", "0.4", 910),
        position_row("TRADE", "0xrestart-b", MARKET_B, 0, "BUY", "3", "0.6", 910),
    ];
    let payload = serde_json::to_vec(
        &rows
            .iter()
            .flat_map(|group| group.rows.iter())
            .collect::<Vec<_>>(),
    )
    .unwrap();
    let mut writer = pe_event_log::Writer::open(&source_path).unwrap();
    let (read, commitment) =
        support::append_committed_read_v2(&mut writer, wallet(), &payload, 920, 921);
    drop(writer);
    let mut context = context(910, true);
    context.decision_inputs_json = read.decision_inputs_json;
    context.page_occurrences = vec![read.page];
    context.read_commitment =
        Some(pe_service::bucket_commit::ActivityReadCommitmentReceipt::BindingsV2(commitment));
    let committed = engine
        .commit_with_freshness_policy(
            read.aggregates,
            &context,
            zero_basis(),
            Some(pe_service::bucket_commit::PaperFreshnessPolicy {
                activity_ws_enabled: true,
                copy_latency_budget_secs: 30,
            }),
        )
        .unwrap();
    assert_eq!(committed.pending.len(), 2);
    let positions = paper.leader_positions().unwrap();
    let history = paper.gate_history().unwrap();
    let groups = paper.activity_groups_after(&wallet(), 0).unwrap();
    let cursor = paper.cursor(&wallet()).unwrap();
    drop(engine);
    drop(paper);
    let state_path = dir.path().join("paper.db");
    // All rows in one complete activity read share a generation. Emulate the open
    // continuation-5 read left by the preceding binary at cutover.
    let legacy_rows = PaperStateDb::open(&state_path)
        .unwrap()
        .open_decision_pending()
        .unwrap();
    assert_eq!(legacy_rows.len(), 2);
    let connection = rusqlite::Connection::open(&state_path).unwrap();
    for legacy in legacy_rows {
        let mut historical: Value = serde_json::from_str(&legacy.frozen_inputs_json).unwrap();
        assert_eq!(historical["version"], json!(7));
        historical["version"] = json!(5);
        historical
            .as_object_mut()
            .unwrap()
            .remove("source_authority");
        assert_eq!(
            connection
                .execute(
                    "UPDATE decision_pending SET frozen_inputs_json = ?1 WHERE source_trade_id = ?2",
                    rusqlite::params![historical.to_string(), legacy.source_trade_id.0],
                )
                .unwrap(),
            1
        );
    }
    let mut terminal = Vec::new();
    for restart in 0..2 {
        let paper = Arc::new(PaperStateDb::open(&state_path).unwrap());
        let index =
            pe_service::risk_inputs::SourceReceiptIndex::replay(&dir.path().join("source.log"))
                .unwrap();
        assert_eq!(
            pe_service::bucket_commit::validate_open_continuations(&paper, &index).unwrap(),
            if restart == 0 { 2 } else { 0 }
        );
        let (_control, receiver) = tokio::sync::mpsc::channel(4);
        let mut orchestrator = support::continuation_orchestrator_with_authority(
            Arc::clone(&paper),
            &paper_path,
            wallet(),
            receiver,
            support::continuation_hooks(950),
            Some(pe_service::supabase_state::SupabaseStateClient::new(
                reqwest::Client::new(),
                "https://offline.invalid",
                "scenario-anon",
                "scenario-secret",
            )),
        )
        .with_source_receipt_index(index.clone());
        let (source_log, _source_rx) = pe_service::activity_ingest::SourceLogHandle::channel(1);
        let http = reqwest::Client::new();
        let admission = pe_service::live_venue_adapter::LiveAdmissionBuilder::new(
            http.clone(),
            "https://offline.invalid",
            "https://offline.invalid",
            source_log.clone(),
        );
        let marks = Arc::new(pe_service::mark_prices::HistoricalMarkAdapter::new(
            http,
            "https://offline.invalid",
            source_log,
        ));
        orchestrator
            .configure_financial_log_paths(
                paper_path.clone(),
                source_path.clone(),
                admission,
                marks,
                index,
            )
            .unwrap();
        orchestrator.seal_before_resume(&hot_hash, 2).await.unwrap();
        orchestrator
            .resume_pending_before_producers()
            .await
            .unwrap();
        orchestrator
            .resume_pending_before_producers()
            .await
            .unwrap();
        assert!(paper.open_decision_pending().unwrap().is_empty());
        let rows = paper.decision_pending_history().unwrap();
        assert_eq!(rows.len(), 2);
        for row in &rows {
            assert_eq!(row.state, DecisionPendingState::Terminal);
            let replayed = replay_decision_pending(row).unwrap();
            assert_eq!(
                replayed.post_boundary.body.terminal.reason,
                "stale_fallback_past_copy_budget"
            );
            assert_eq!(replayed.continuation.version(), 5);
            assert_eq!(replayed.post_boundary.financial_semantic_version, 1);
        }
        let seals = pe_service::paper_recovery::scan_paper_log(&paper_path)
            .unwrap()
            .into_iter()
            .filter_map(|frame| match &frame.frame {
                pe_service::paper_recovery::PaperLogFrame::Record(
                    pe_service::paper_recovery::PaperLogRecord::QualificationSealed(seal),
                ) => Some(seal.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(seals.len(), 1);
        assert!(matches!(
            &seals[0].reason,
            pe_service::paper_recovery::SealReason::InsufficientEvidence(reason)
                if reason.contains("financial semantic version changed")
        ));
        if restart == 0 {
            terminal = rows;
        } else {
            assert_eq!(rows, terminal);
        }
        assert_eq!(paper.leader_positions().unwrap(), positions);
        assert_eq!(paper.gate_history().unwrap(), history);
        assert_eq!(paper.activity_groups_after(&wallet(), 0).unwrap(), groups);
        assert_eq!(paper.cursor(&wallet()).unwrap(), cursor);
    }
}

/// PASS: recorded corrections and an uncorrected control validate at boot; changing either
/// frozen identity or stamped/verified agreement fails. Dynamic load preserves corrected facts.
/// FAIL: a valid correction is refused, a mutation passes, or fresh loading restores the raw identity.
#[tokio::test]
async fn corrected_continuation_identity_validates_across_consumers() {
    for mutation in [
        "control",
        "corrected",
        "market",
        "outcome",
        "stamped",
        "verified",
    ] {
        let (dir, paper, mut engine) = fresh_anchored();
        let (read, commitment) = committed_source_read(
            &dir,
            &[position_row(
                "TRADE",
                "0xcorrected-open",
                MARKET_A,
                0,
                "BUY",
                "2",
                "0.4",
                940,
            )],
        );
        let id = read.aggregates[0].group_id.key().clone();
        let overrides = if mutation == "control" {
            HashMap::new()
        } else {
            HashMap::from([(
                id.clone(),
                IdentityOverride {
                    verified: market_outcome(MARKET_B, 1),
                    evidence_hash: "recorded-gamma-proof".to_owned(),
                },
            )])
        };
        commit_open_source_read(&mut engine, &read, commitment, overrides);
        let row = paper.open_decision_pending().unwrap().remove(0);
        let connection = rusqlite::Connection::open(dir.path().join("paper.db")).unwrap();
        if matches!(mutation, "market" | "outcome") {
            let mut frozen: Value = serde_json::from_str(&row.frozen_inputs_json).unwrap();
            if mutation == "market" {
                frozen["market_id"] = json!(MARKET_C);
            } else {
                frozen["outcome_id"] = json!(0);
            }
            connection.execute("UPDATE decision_pending SET frozen_inputs_json = ?1 WHERE source_trade_id = ?2", rusqlite::params![frozen.to_string(), id.0]).unwrap();
        } else if matches!(mutation, "stamped" | "verified") {
            let group = paper.activity_group_state(&id).unwrap().unwrap();
            let mut proof: Value = serde_json::from_str(&group.proof_json).unwrap();
            proof["correction"][mutation] =
                serde_json::to_value(market_outcome(MARKET_C, 0)).unwrap();
            connection
                .execute(
                    "UPDATE activity_groups SET proof_json = ?1 WHERE source_trade_id = ?2",
                    rusqlite::params![proof.to_string(), id.0],
                )
                .unwrap();
        }
        let index =
            pe_service::risk_inputs::SourceReceiptIndex::replay(&dir.path().join("source.log"))
                .unwrap();
        let result = pe_service::bucket_commit::validate_open_continuations(&paper, &index);
        if matches!(mutation, "control" | "corrected") {
            assert_eq!(result.unwrap(), 1, "{mutation}");
        } else {
            assert_eq!(result.unwrap_err().source_trade_id, Some(id), "{mutation}");
        }
    }
    // Fresh control-message load: the engine records the correction before the owner loads it.
    let (dir, paper, engine) = fresh_anchored();
    drop(engine);
    paper
        .record_reconciled_history_status(&WalletHistoryStatusRecord {
            wallet: wallet(),
            complete: true,
            proof_json: "{}".to_owned(),
            updated_at_unix: 940,
        })
        .unwrap();
    let (read, commitment) = committed_source_read(
        &dir,
        &[position_row(
            "TRADE",
            "0xdynamic-corrected",
            MARKET_A,
            0,
            "BUY",
            "2",
            "0.4",
            950,
        )],
    );
    let id = read.aggregates[0].group_id.key().clone();
    let mut context = support::read_context(&read, commitment, 960);
    context.identity_overrides.insert(
        id.clone(),
        IdentityOverride {
            verified: market_outcome(MARKET_B, 1),
            evidence_hash: "recorded-gamma-proof".to_owned(),
        },
    );
    let (control, receiver) = tokio::sync::mpsc::channel(4);
    let orchestrator = support::continuation_orchestrator(
        Arc::clone(&paper),
        &dir.path().join("paper.log"),
        wallet(),
        receiver,
        support::continuation_hooks(960),
    );
    let run = tokio::spawn(orchestrator.run(std::future::pending::<()>()));
    let (committed, acknowledgement) = tokio::sync::oneshot::channel();
    control
        .send(
            pe_service::orchestrator_control::OrchestratorControl::CommitActivityBucket {
                aggregates: read.aggregates.into(),
                context: Arc::new(context),
                committed,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        acknowledgement.await.unwrap().unwrap().pending,
        vec![id.clone()]
    );
    drop(control);
    run.await.unwrap();
    let row = paper.decision_pending_for(&id).unwrap().unwrap();
    assert_eq!(row.state, DecisionPendingState::Terminal);
    let continuation = DecisionContinuationV3::from_durable(&row).unwrap();
    assert_eq!(
        continuation.facts.market_id,
        MarketId(VenueMarketId(MARKET_B.to_owned()))
    );
    assert_eq!(continuation.facts.outcome_id, OutcomeId(1));
}

#[test]
fn scoped_branch_matrix_preserves_membership_and_other_markets() {
    for route in ["ordinary", "revised", "partial", "late", "covered"] {
        for kind in [
            "TRADE",
            "SPLIT",
            "MERGE",
            "CONVERSION",
            "FUTURE_EFFECT",
            "REWARD",
        ] {
            for resolvable in [false, true] {
                let (_dir, paper, mut engine) = fresh_anchored();
                let mut setup = context(100, true);
                setup.copy_eligible = false;
                let raw = kind == "REWARD";
                let market = if resolvable { MARKET_A } else { "unresolved" };
                let make = |size: &str| {
                    if kind == "TRADE" {
                        position_row(kind, "0xmatrix", market, 0, "BUY", size, "0.5", 100)
                    } else {
                        pair_effect(kind, "0xmatrix", market, size, 100)
                    }
                };
                let original = make("1");
                let record = make(if route == "revised" { "2" } else { "1" });
                let ctx = context(100, true);
                if route == "revised" {
                    engine
                        .commit_read(vec![original], &setup, zero_basis())
                        .unwrap();
                } else if matches!(route, "partial" | "late") {
                    engine
                        .commit_read(
                            vec![pair_effect("REWARD", "0xprior", MARKET_C, "1", 100)],
                            &setup,
                            zero_basis(),
                        )
                        .unwrap();
                } else if route == "covered" {
                    install_anchor(&mut engine, &paper, 100, Vec::new(), 101);
                }
                let mut bucket = vec![record.clone()];
                if route == "partial" {
                    bucket.push(pair_effect("REWARD", "0xprior", MARKET_C, "1", 100));
                }
                if route == "ordinary" {
                    bucket.push(position_row(
                        "TRADE", "0xother", MARKET_B, 0, "BUY", "3", "0.5", 100,
                    ));
                }
                let result = engine.commit_read(bucket, &ctx, zero_basis()).unwrap();
                assert_eq!(result.newly_fenced, None, "{route}/{kind}/{resolvable}");
                assert!(!paper.is_wallet_fenced(&wallet()).unwrap());
                assert!(!paper.wallet_coverage(&wallet()).unwrap().reanchor_required);
                let drops = paper.wallet_scope_drops(&wallet(), 100).unwrap();
                let ordinary_problem = matches!(kind, "MERGE" | "CONVERSION" | "FUTURE_EFFECT");
                let should_drop = resolvable && !raw && (route != "ordinary" || ordinary_problem);
                assert_eq!(
                    !drops.is_empty(),
                    should_drop,
                    "{route}/{kind}/{resolvable}"
                );
                if should_drop {
                    assert!(drops.iter().all(|drop| drop.scope_id == MARKET_A));
                }
                if route == "ordinary" {
                    let other =
                        position_row("TRADE", "0xother", MARKET_B, 0, "BUY", "3", "0.5", 100);
                    assert_eq!(
                        result.dispositions[&other.group_id.key().0],
                        if should_drop {
                            "not_an_entry"
                        } else {
                            "decision_pending"
                        }
                    );
                    assert_eq!(state(&engine, MARKET_B, 0).atomic(), 3_000_000);
                }
                if route == "covered" && !resolvable {
                    let mut bracket = ctx.clone();
                    bracket.bracket_commit = true;
                    assert!(
                        engine
                            .commit_read(vec![record.clone()], &bracket, zero_basis())
                            .unwrap()
                            .already_committed
                    );
                }
                if !resolvable && !raw {
                    assert!(
                        paper
                            .market_history_record(
                                &wallet(),
                                &MarketId(VenueMarketId("unresolved".to_owned()))
                            )
                            .unwrap()
                            .is_none()
                    );
                }
                let later = position_row("TRADE", "0xhealthy", MARKET_C, 0, "BUY", "2", "0.5", 101);
                let later_id = later.group_id.key().clone();
                let result = engine
                    .commit_read(vec![later], &context(101, true), zero_basis())
                    .unwrap();
                assert_eq!(
                    result.dispositions[&later_id.0], "decision_pending",
                    "{route}/{kind}/{resolvable}"
                );
                assert_eq!(
                    replay_wallet_ledger(&paper, wallet()).unwrap().snapshots(),
                    engine.ledger().snapshots()
                );
            }
        }
    }
}

#[test]
fn missing_membership_is_atomic_on_ordinary_and_cold_late_routes() {
    for route in ["ordinary", "revised", "partial", "late", "covered"] {
        let (dir, paper, mut engine) = fresh_anchored();
        let cold = "0xcd";
        let record = pair_effect("MERGE", "0xcold", cold, "1", 100);
        let mut setup = context(100, true);
        setup.copy_eligible = false;
        if route == "revised" {
            engine
                .commit_read(
                    vec![pair_effect("MERGE", "0xcold", cold, "0", 100)],
                    &setup,
                    zero_basis(),
                )
                .unwrap();
        } else if matches!(route, "partial" | "late") {
            engine
                .commit_read(
                    vec![pair_effect("REWARD", "0xprior", MARKET_C, "1", 100)],
                    &setup,
                    zero_basis(),
                )
                .unwrap();
        } else if route == "covered" {
            install_anchor(&mut engine, &paper, 100, Vec::new(), 101);
        }
        let mut bucket = vec![record.clone()];
        if route == "partial" {
            bucket.push(pair_effect("REWARD", "0xprior", MARKET_C, "1", 100));
        }
        let before = paper.wallet_coverage(&wallet()).unwrap();
        let cursor = paper.cursor(&wallet()).unwrap();
        let groups = paper.activity_groups_after(&wallet(), 0).unwrap();
        let error = engine
            .commit_read(bucket.clone(), &context(100, true), zero_basis())
            .unwrap_err();
        assert!(
            matches!(error, pe_service::bucket_commit::BucketCommitError::MembershipRequired(markets) if markets == [cold]),
            "{route}"
        );
        assert_eq!(paper.wallet_coverage(&wallet()).unwrap(), before);
        assert_eq!(paper.cursor(&wallet()).unwrap(), cursor);
        assert_eq!(paper.activity_groups_after(&wallet(), 0).unwrap(), groups);
        assert!(paper.wallet_scope_drops(&wallet(), 100).unwrap().is_empty());
        assert_eq!(paper.scope_drop_boundary_unix().unwrap(), None);
        assert!(
            !paper
                .activity_revision_retained_without_fence(
                    record.group_id.key(),
                    record.semantic_revision.as_str()
                )
                .unwrap()
        );
        paper.record_market_group(cold, "").unwrap();
        let result = engine
            .commit_read(bucket.clone(), &context(100, true), zero_basis())
            .unwrap();
        assert_eq!(result.newly_fenced, None);
        assert_eq!(
            paper.wallet_scope_drops(&wallet(), 100).unwrap()[0].scope_id,
            cold
        );
        assert_eq!(
            result.dispositions[&record.group_id.key().0],
            if route == "covered" {
                "anchor_covered_late"
            } else {
                "scope_dropped"
            }
        );
        let reopened = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        let mut restarted =
            BucketCommitEngine::load(reopened.clone(), build_leader_ledger(&reopened).unwrap())
                .unwrap();
        assert!(
            restarted
                .commit_read(bucket, &context(100, true), zero_basis())
                .unwrap()
                .already_committed
        );
    }
}

#[test]
fn scoped_drop_savepoint_failure_rolls_back_boundary_history_and_revision() {
    let (dir, paper, mut engine) = fresh_anchored();
    let original = position_row(
        "TRADE",
        "0xrevision-fault",
        MARKET_A,
        0,
        "BUY",
        "1",
        "0.5",
        100,
    );
    let revised = position_row(
        "TRADE",
        "0xrevision-fault",
        MARKET_A,
        0,
        "BUY",
        "2",
        "0.5",
        100,
    );
    engine
        .commit_read(vec![original], &context(100, true), zero_basis())
        .unwrap();
    let capture = ledger_capture(engine.ledger(), &paper, wallet()).unwrap();
    let history = paper.gate_history().unwrap();
    let conn = rusqlite::Connection::open(dir.path().join("paper.db")).unwrap();
    conn.execute_batch("CREATE TRIGGER fail_revision BEFORE INSERT ON activity_group_revisions BEGIN SELECT RAISE(ABORT, 'injected revision failure'); END;").unwrap();
    assert!(
        engine
            .commit_read(vec![revised.clone()], &context(100, true), zero_basis())
            .is_err()
    );
    assert_eq!(
        ledger_capture(engine.ledger(), &paper, wallet())
            .unwrap()
            .hash,
        capture.hash
    );
    assert_eq!(paper.gate_history().unwrap(), history);
    assert_eq!(paper.scope_drop_boundary_unix().unwrap(), None);
    assert!(paper.wallet_scope_drops(&wallet(), 100).unwrap().is_empty());
    conn.execute_batch("DROP TRIGGER fail_revision").unwrap();
    let result = engine
        .commit_read(vec![revised], &context(100, true), zero_basis())
        .unwrap();
    assert!(result.retained_revision);
    assert_eq!(paper.scope_drop_boundary_unix().unwrap(), Some(120));
}

#[test]
fn scoped_and_raw_only_revisions_survive_restart_bracket_anchor_and_swap_out() {
    for scoped in [false, true] {
        let (dir, paper, mut engine) = fresh_anchored();
        let kind = if scoped { "TRADE" } else { "REWARD" };
        let original = position_row(kind, "0xrevision", MARKET_A, 0, "BUY", "1", "0.5", 100);
        let revised = position_row(kind, "0xrevision", MARKET_A, 0, "BUY", "2", "0.5", 100);
        let id = original.group_id.key().clone();
        let mut setup = context(100, true);
        setup.copy_eligible = false;
        engine
            .commit_read(vec![original], &setup, zero_basis())
            .unwrap();
        let immutable = paper.activity_group_state(&id).unwrap();
        let result = engine
            .commit_read(vec![revised.clone()], &context(100, true), zero_basis())
            .unwrap();
        assert!(result.retained_revision);
        assert_eq!(
            result.dispositions[&id.0],
            if scoped { "scope_dropped" } else { "raw_only" }
        );
        let generation = paper
            .wallet_coverage(&wallet())
            .unwrap()
            .coverage_generation;
        for anchored in [false, true] {
            let reopened = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
            engine =
                BucketCommitEngine::load(reopened.clone(), build_leader_ledger(&reopened).unwrap())
                    .unwrap();
            let mut ctx = context(100, true);
            ctx.bracket_commit = true;
            let result = engine
                .commit_read(vec![revised.clone()], &ctx, zero_basis())
                .unwrap();
            assert!(result.already_committed, "{scoped}/{anchored}");
            assert!(!result.retained_revision);
            assert_eq!(paper.activity_group_state(&id).unwrap(), immutable);
            assert_eq!(
                paper
                    .wallet_coverage(&wallet())
                    .unwrap()
                    .coverage_generation,
                generation
            );
            if !anchored {
                install_anchor(&mut engine, &paper, 101, Vec::new(), 121);
            }
        }
        let drops = paper.wallet_scope_drops(&wallet(), 200).unwrap();
        assert_eq!(
            engine.retire_wallet(wallet(), 1000, None).unwrap().result,
            None
        );
        for _ in 0..20 {
            if paper
                .drain_retired_wallet(wallet(), 100)
                .unwrap()
                .result
                .finished
            {
                break;
            }
        }
        assert_eq!(paper.wallet_scope_drops(&wallet(), 200).unwrap(), drops);
    }
}

#[test]
fn healthy_seconds_and_market_drops_need_no_membership_but_event_drops_do() {
    for kind in [None, Some("market"), Some("event")] {
        let (_dir, paper, mut engine) = fresh_anchored();
        let cold = "0xcd";
        if let Some(kind) = kind {
            paper
                .import_scope_drops(
                    1,
                    &[(
                        pe_paper_state::WalletScopeDrop {
                            wallet: wallet(),
                            scope_kind: kind.to_owned(),
                            scope_id: if kind == "market" {
                                MARKET_A.to_owned()
                            } else {
                                "0xee".to_owned()
                            },
                            cause: "imported".to_owned(),
                            source_trade_id: None,
                            dropped_at_epoch: 90,
                            proof_json: "{}".to_owned(),
                            recorded_at_unix: 99,
                        },
                        support::scenario_receipt(99),
                    )],
                    &Default::default(),
                )
                .unwrap();
        }
        let trade = position_row("TRADE", "0xhealthy-cold", cold, 0, "BUY", "1", "0.5", 100);
        let result = engine.commit_read(vec![trade.clone()], &context(100, true), zero_basis());
        if kind == Some("event") {
            assert!(
                matches!(result, Err(pe_service::bucket_commit::BucketCommitError::MembershipRequired(markets)) if markets == [cold])
            );
            paper.record_market_group(cold, "0xee").unwrap();
            let result = engine
                .commit_read(vec![trade], &context(100, true), zero_basis())
                .unwrap();
            assert!(
                result
                    .dispositions
                    .values()
                    .all(|disposition| disposition == "scope_dropped")
            );
        } else {
            assert_eq!(result.unwrap().pending.len(), 1);
        }
    }
}

#[test]
fn scoped_history_uses_problem_and_corrected_buy_witnesses_across_brackets() {
    for arithmetic in [false, true] {
        let (dir, paper, mut engine) = fresh_anchored();
        if arithmetic {
            install_anchor(
                &mut engine,
                &paper,
                0,
                vec![(
                    MarketId(VenueMarketId(MARKET_B.to_owned())),
                    OutcomeId(0),
                    ShareAmount::from_atomic(u64::MAX),
                )],
                1,
            );
        }
        let stamped = if arithmetic { MARKET_A } else { "\\xca" };
        let buy = position_row(
            "TRADE",
            "0xhistory-proof",
            stamped,
            0,
            "BUY",
            "1",
            "0.5",
            100,
        );
        let id = buy.group_id.key().clone();
        let mut ctx = context(100, true);
        ctx.identity_overrides.insert(
            id.clone(),
            IdentityOverride {
                verified: market_outcome(MARKET_B, 0),
                evidence_hash: "a".repeat(64),
            },
        );
        let result = engine
            .commit_read(vec![buy.clone()], &ctx, zero_basis())
            .unwrap();
        assert_eq!(result.dispositions[&id.0], "scope_dropped");
        let consumed = if arithmetic { MARKET_B } else { MARKET_A };
        let unconsumed = if arithmetic { MARKET_A } else { MARKET_B };
        let witness = paper
            .market_history_record(&wallet(), &MarketId(VenueMarketId(consumed.to_owned())))
            .unwrap()
            .unwrap();
        assert_eq!(witness.source_trade_id, id);
        assert_eq!(
            paper.wallet_scope_drops(&wallet(), 100).unwrap()[0].scope_id,
            MARKET_A
        );
        assert!(
            paper
                .market_history_record(&wallet(), &MarketId(VenueMarketId(unconsumed.to_owned())))
                .unwrap()
                .is_none()
        );
        for restart in [false, true] {
            if restart {
                let reopened = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
                engine = BucketCommitEngine::load(
                    reopened.clone(),
                    build_leader_ledger(&reopened).unwrap(),
                )
                .unwrap();
            }
            ctx.bracket_commit = true;
            assert!(
                engine
                    .commit_read(vec![buy.clone()], &ctx, zero_basis())
                    .unwrap()
                    .already_committed
            );
            assert_eq!(
                paper
                    .market_history_record(&wallet(), &witness.market_id)
                    .unwrap(),
                Some(witness.clone())
            );
            assert!(
                paper
                    .market_history_record(
                        &wallet(),
                        &MarketId(VenueMarketId(unconsumed.to_owned()))
                    )
                    .unwrap()
                    .is_none()
            );
        }
    }
}

#[test]
fn imported_certification_and_later_discovery_preserve_recorded_replay() {
    let (_dir, paper, mut engine) = fresh_anchored();
    let later = position_row(
        "TRADE",
        "0xlater-first",
        MARKET_A,
        0,
        "BUY",
        "1",
        "0.5",
        110,
    );
    engine
        .commit_read(vec![later.clone()], &context(110, true), zero_basis())
        .unwrap();
    let recorded = paper.activity_group_state(later.group_id.key()).unwrap();
    let drop = pe_paper_state::WalletScopeDrop {
        wallet: wallet(),
        scope_kind: "market".to_owned(),
        scope_id: MARKET_A.to_owned(),
        cause: "imported".to_owned(),
        source_trade_id: None,
        dropped_at_epoch: 100,
        proof_json: "{}".to_owned(),
        recorded_at_unix: 140,
    };
    paper
        .import_scope_drops(
            2,
            &[(drop, support::scenario_receipt(140))],
            &Default::default(),
        )
        .unwrap();
    // Certification remains effective without the original trigger in the service read.
    let other = position_row("TRADE", "0xcertified", MARKET_B, 0, "BUY", "2", "0.5", 100);
    let result = engine
        .commit_read(vec![other], &context(100, true), zero_basis())
        .unwrap();
    // S4's late record drops B; the earlier admitted A remains immutable.
    assert_eq!(result.newly_fenced, None);
    assert_eq!(
        paper.activity_group_state(later.group_id.key()).unwrap(),
        recorded
    );
    let applied_before = paper.activity_groups_after(&wallet(), 0).unwrap();
    let replayed = replay_wallet_ledger(&paper, wallet()).unwrap();
    let mut restarted = BucketCommitEngine::load(paper.clone(), replayed.clone()).unwrap();
    assert!(
        restarted
            .commit_read(vec![later], &context(110, true), zero_basis())
            .unwrap()
            .already_committed
    );
    assert_eq!(
        paper.activity_groups_after(&wallet(), 0).unwrap(),
        applied_before
    );
    assert_eq!(restarted.ledger().snapshots(), replayed.snapshots());
}

#[test]
fn same_side_component_overflow_is_a_scoped_order_problem() {
    let (_dir, paper, mut engine) = fresh_anchored();
    install_anchor(
        &mut engine,
        &paper,
        0,
        vec![(
            MarketId(VenueMarketId(MARKET_A.to_owned())),
            OutcomeId(0),
            ShareAmount::from_atomic(u64::MAX),
        )],
        1,
    );
    let buys = ["0xoverflow-a", "0xoverflow-b"]
        .into_iter()
        .map(|tx| position_row("TRADE", tx, MARKET_A, 0, "BUY", "1", "0.5", 100))
        .collect();
    let result = engine
        .commit_read(buys, &context(100, true), zero_basis())
        .unwrap();
    assert_eq!(result.newly_fenced, None);
    assert!(!paper.is_wallet_fenced(&wallet()).unwrap());
    assert_eq!(
        paper.wallet_scope_drops(&wallet(), 100).unwrap()[0].cause,
        "order_dependent_equal_second"
    );
    assert_eq!(state(&engine, MARKET_A, 0).atomic(), u64::MAX);
}

#[test]
fn raw_only_late_records_leave_their_market_copyable() {
    for route in ["partial", "late", "covered"] {
        let (_dir, paper, mut engine) = fresh_anchored();
        let prior = pair_effect("REWARD", "0xprior", MARKET_C, "1", 100);
        if route == "covered" {
            install_anchor(&mut engine, &paper, 100, Vec::new(), 101);
        } else {
            engine
                .commit_read(vec![prior.clone()], &context(100, true), zero_basis())
                .unwrap();
        }
        let raw = pair_effect("SPLIT", "0xzero", MARKET_A, "0", 100);
        let mut bucket = vec![raw];
        if route == "partial" {
            bucket.push(prior);
        }
        let result = engine
            .commit_read(bucket, &context(100, true), zero_basis())
            .unwrap();
        assert_eq!(result.newly_fenced, None);
        assert!(paper.wallet_scope_drops(&wallet(), 100).unwrap().is_empty());
        let first = position_row(
            "TRADE",
            "0xfirst-after-raw",
            MARKET_A,
            0,
            "BUY",
            "1",
            "0.5",
            101,
        );
        assert_eq!(
            engine
                .commit_read(vec![first], &context(101, true), zero_basis())
                .unwrap()
                .pending
                .len(),
            1
        );
    }
}

#[test]
fn reverse_order_discovery_keeps_earliest_drop_and_import_certifies_without_trigger() {
    let (_dir, paper, mut engine) = fresh_anchored();
    for epoch in [110, 100] {
        let problem = pair_effect("MERGE", &format!("0xproblem-{epoch}"), MARKET_A, "1", epoch);
        engine
            .commit_read(vec![problem], &context(epoch, true), zero_basis())
            .unwrap();
    }
    assert_eq!(
        paper.wallet_scope_drops(&wallet(), 200).unwrap()[0].dropped_at_epoch,
        100
    );
    let (_dir, paper, mut engine) = fresh_anchored();
    paper
        .import_scope_drops(
            1,
            &[(
                pe_paper_state::WalletScopeDrop {
                    wallet: wallet(),
                    scope_kind: "market".to_owned(),
                    scope_id: MARKET_A.to_owned(),
                    cause: "imported".to_owned(),
                    source_trade_id: None,
                    dropped_at_epoch: 100,
                    proof_json: "{}".to_owned(),
                    recorded_at_unix: 99,
                },
                support::scenario_receipt(99),
            )],
            &Default::default(),
        )
        .unwrap();
    let buy = position_row("TRADE", "0xno-trigger", MARKET_B, 0, "BUY", "1", "0.5", 100);
    let id = buy.group_id.key().clone();
    let result = engine
        .commit_read(vec![buy], &context(100, true), zero_basis())
        .unwrap();
    assert_eq!(result.dispositions[&id.0], "not_an_entry");
    assert_eq!(state(&engine, MARKET_B, 0).atomic(), 1_000_000);
    assert_eq!(
        paper
            .market_history_record(&wallet(), &MarketId(VenueMarketId(MARKET_B.to_owned())))
            .unwrap()
            .unwrap()
            .source_trade_id,
        id
    );
    let later = position_row(
        "TRADE",
        "0xafter-certification",
        MARKET_C,
        0,
        "BUY",
        "1",
        "0.5",
        101,
    );
    assert_eq!(
        engine
            .commit_read(vec![later], &context(101, true), zero_basis())
            .unwrap()
            .pending
            .len(),
        1
    );
}

#[test]
fn ambiguity_in_an_earlier_dropped_event_does_not_certify_its_carrier() {
    let (dir, paper, mut engine) = fresh_anchored();
    let conn = rusqlite::Connection::open(dir.path().join("paper.db")).unwrap();
    conn.execute(
        "DELETE FROM meta WHERE key = ?1",
        [format!("neg_risk_group:{MARKET_A}")],
    )
    .unwrap();
    let receipt = support::scenario_receipt(500);
    paper
        .import_scope_drops(
            1,
            &[(
                pe_paper_state::WalletScopeDrop {
                    wallet: wallet(),
                    scope_kind: "event".to_owned(),
                    scope_id: "0xee".to_owned(),
                    cause: "conversion".to_owned(),
                    source_trade_id: None,
                    dropped_at_epoch: 50,
                    proof_json: "{}".to_owned(),
                    recorded_at_unix: 60,
                },
                receipt,
            )],
            &Default::default(),
        )
        .unwrap();
    let implicated = position_row("TRADE", "0xambiguous", MARKET_A, 0, "BUY", "1", "0.5", 100);
    let healthy = position_row(
        "TRADE",
        "0xhealthy-carrier",
        MARKET_B,
        0,
        "BUY",
        "1",
        "0.5",
        100,
    );
    let id = healthy.group_id.key().clone();
    let mut ctx = context(100, true);
    ctx.decision_inputs_json = json!({"ambiguous_observations": [{
        "group_id": implicated.group_id.key(), "receipt": receipt, "source_epoch": 100,
        "candidates": [(implicated.group_id.key(), 100)], "market_id": MARKET_A,
    }]})
    .to_string();
    assert!(matches!(
        engine.commit_read(vec![implicated.clone(), healthy.clone()], &ctx, zero_basis()),
        Err(pe_service::bucket_commit::BucketCommitError::MembershipRequired(markets)) if markets.contains(&MARKET_A.to_owned())
    ));
    assert!(!paper.activity_observation_retired(receipt).unwrap());
    assert!(paper.activity_group_state(&id).unwrap().is_none());
    paper.record_market_group(MARKET_A, "0xee").unwrap();
    let result = engine
        .commit_read(
            vec![implicated.clone(), healthy.clone()],
            &ctx,
            zero_basis(),
        )
        .unwrap();
    assert_eq!(result.dispositions[&id.0], "decision_pending");
    assert_eq!(result.pending, vec![id]);
    assert_eq!(
        result.dispositions[&implicated.group_id.key().0],
        "scope_dropped"
    );
    assert_eq!(paper.wallet_scope_drops(&wallet(), 100).unwrap().len(), 1);
    assert!(paper.activity_observation_unbound_retired(receipt).unwrap());
    engine
        .commit_read(vec![implicated, healthy], &ctx, zero_basis())
        .unwrap();
    assert_eq!(paper.wallet_scope_drops(&wallet(), 100).unwrap().len(), 1);
}

#[test]
fn covered_late_buy_repairs_history_with_or_without_a_revision() {
    let mut expected = None;
    for revised in [false, true] {
        let (dir, mut paper, mut engine) = fresh_anchored();
        let original = position_row(
            "TRADE",
            "0xcovered-revision",
            MARKET_A,
            0,
            "BUY",
            "1",
            "0.5",
            100,
        );
        engine
            .commit_read(vec![original.clone()], &context(100, true), zero_basis())
            .unwrap();
        install_anchor(&mut engine, &paper, 100, Vec::new(), 101);
        let current = if revised {
            position_row(
                "TRADE",
                "0xcovered-revision",
                MARKET_A,
                0,
                "BUY",
                "2",
                "0.5",
                100,
            )
        } else {
            original
        };
        let late = position_row(
            "TRADE",
            "0xcovered-history",
            "\\xca",
            0,
            "BUY",
            "1",
            "0.5",
            100,
        );
        let mut ctx = context(100, true);
        ctx.identity_overrides.insert(
            late.group_id.key().clone(),
            IdentityOverride {
                verified: market_outcome(MARKET_B, 0),
                evidence_hash: "a".repeat(64),
            },
        );
        let market = market_outcome(MARKET_B, 0).market().clone();
        let result = engine
            .commit_read(vec![current.clone(), late.clone()], &ctx, zero_basis())
            .unwrap();
        assert_eq!(
            result.dispositions[&late.group_id.key().0],
            "anchor_covered_late"
        );
        assert!(result.pending.is_empty());
        let witness = paper
            .market_history_record(&wallet(), &market)
            .unwrap()
            .expect("covered BUY history");
        assert_eq!(witness.source_trade_id, *late.group_id.key());
        assert_eq!(witness.first_epoch, 100);
        if let Some(expected) = &expected {
            assert_eq!(&witness, expected);
        } else {
            expected = Some(witness.clone());
        }
        for restart in [false, true] {
            if restart {
                drop(engine);
                drop(paper);
                paper = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
                engine =
                    BucketCommitEngine::load(paper.clone(), build_leader_ledger(&paper).unwrap())
                        .unwrap();
            }
            engine
                .commit_read(vec![current.clone(), late.clone()], &ctx, zero_basis())
                .unwrap();
            assert_eq!(
                paper.market_history_record(&wallet(), &market).unwrap(),
                Some(witness.clone())
            );
            assert_eq!(state(&engine, MARKET_B, 0), ShareAmount::ZERO);
        }
        let later = position_row(
            "TRADE",
            "0xlater-covered",
            MARKET_B,
            0,
            "BUY",
            "1",
            "0.5",
            101,
        );
        let result = engine
            .commit_read(vec![later.clone()], &context(101, true), zero_basis())
            .unwrap();
        assert_eq!(
            result.dispositions[&later.group_id.key().0],
            "not_first_entry"
        );
        assert!(result.pending.is_empty());
    }
}

/// PASS: an earlier market drop retires an all-durable ambiguity carrier without membership,
/// even with an unrelated event drop; a later verified-ungrouped market still admits a BUY.
#[test]
fn durable_market_drop_retires_ambiguity_without_membership() {
    let (dir, paper, mut engine) = fresh_anchored();
    let receipt = support::scenario_receipt(500);
    let drops = [("market", MARKET_A, 50), ("event", "0xee", 60)].map(|(kind, scope, start)| {
        (
            pe_paper_state::WalletScopeDrop {
                wallet: wallet(),
                scope_kind: kind.to_owned(),
                scope_id: scope.to_owned(),
                cause: "underflow".to_owned(),
                source_trade_id: None,
                dropped_at_epoch: start,
                proof_json: "{}".to_owned(),
                recorded_at_unix: 70,
            },
            receipt,
        )
    });
    paper
        .import_scope_drops(1, &drops, &Default::default())
        .unwrap();
    rusqlite::Connection::open(dir.path().join("paper.db"))
        .unwrap()
        .execute(
            "DELETE FROM meta WHERE key = ?1",
            [format!("neg_risk_group:{MARKET_A}")],
        )
        .unwrap();
    let carrier = pair_effect("REWARD", "0xcarrier", MARKET_A, "1", 100);
    engine
        .commit_read(vec![carrier.clone()], &context(100, true), zero_basis())
        .unwrap();
    let mut ctx = context(100, true);
    ctx.decision_inputs_json = json!({"ambiguous_observations": [{
        "group_id": carrier.group_id.key(), "receipt": receipt, "source_epoch": 100,
        "candidates": [(carrier.group_id.key(), 100)], "market_id": MARKET_A,
    }]})
    .to_string();
    let result = engine
        .commit_read(vec![carrier], &ctx, zero_basis())
        .unwrap();
    assert!(result.already_committed);
    assert!(paper.activity_observation_unbound_retired(receipt).unwrap());
    assert_eq!(paper.market_membership(MARKET_A).unwrap(), None);
    let healthy = position_row("TRADE", "0xhealthy", MARKET_B, 0, "BUY", "1", "0.5", 102);
    let result = engine
        .commit_read(vec![healthy.clone()], &context(102, true), zero_basis())
        .unwrap();
    assert_eq!(result.pending, vec![healthy.group_id.key().clone()]);
    assert!(!paper.is_wallet_fenced(&wallet()).unwrap());
}

#[test]
fn revisions_classify_other_unseen_records_with_only_the_revised_scope_dropped() {
    for scoped in [false, true] {
        let (_dir, paper, mut engine) = fresh_anchored();
        let kind = if scoped { "TRADE" } else { "REWARD" };
        let original = position_row(
            kind,
            "0xrevision-with-other",
            MARKET_A,
            0,
            "BUY",
            "1",
            "0.5",
            100,
        );
        engine
            .commit_read(vec![original], &context(100, true), zero_basis())
            .unwrap();
        let revised = position_row(
            kind,
            "0xrevision-with-other",
            MARKET_A,
            0,
            "BUY",
            "2",
            "0.5",
            100,
        );
        let other = position_row(
            "TRADE",
            "0xother-unseen",
            MARKET_B,
            0,
            "BUY",
            "1",
            "0.5",
            100,
        );
        let id = other.group_id.key().clone();
        let result = engine
            .commit_read(vec![revised, other], &context(100, true), zero_basis())
            .unwrap();
        assert!(result.retained_revision);
        assert_eq!(
            result.dispositions[&id.0],
            if scoped {
                "not_an_entry"
            } else {
                "decision_pending"
            }
        );
        assert_eq!(
            paper.wallet_scope_drops(&wallet(), 100).unwrap().len(),
            usize::from(scoped)
        );
        assert_eq!(state(&engine, MARKET_B, 0).atomic(), 1_000_000);
        let later = position_row(
            "TRADE",
            "0xafter-revision",
            MARKET_C,
            0,
            "BUY",
            "1",
            "0.5",
            101,
        );
        assert_eq!(
            engine
                .commit_read(vec![later], &context(101, true), zero_basis())
                .unwrap()
                .pending
                .len(),
            1
        );
        assert_eq!(
            replay_wallet_ledger(&paper, wallet()).unwrap().snapshots(),
            engine.ledger().snapshots()
        );
    }
}

#[test]
fn routed_redeem_revision_cannot_enter_healthy_market_arithmetic() {
    let (_dir, paper, mut engine) = fresh_anchored();
    let original = position_row("REDEEM", "0xrouted-redeem", MARKET_A, 0, "", "0", "1", 100);
    engine
        .commit_read(vec![original], &context(100, true), zero_basis())
        .unwrap();
    let revised = position_row("REDEEM", "0xrouted-redeem", MARKET_A, 0, "", "1", "1", 100);
    let buy = position_row(
        "TRADE",
        "0xhealthy-buy",
        MARKET_B,
        0,
        "BUY",
        "1",
        "0.5",
        100,
    );
    let buy_id = buy.group_id.key().clone();
    let mut ctx = context(100, true);
    ctx.identity_overrides.insert(
        revised.group_id.key().clone(),
        IdentityOverride {
            verified: market_outcome(MARKET_B, 0),
            evidence_hash: "a".repeat(64),
        },
    );
    let result = engine
        .commit_read(vec![revised, buy], &ctx, zero_basis())
        .unwrap();
    assert_eq!(state(&engine, MARKET_B, 0).atomic(), 1_000_000);
    assert_eq!(result.dispositions[&buy_id.0], "not_an_entry");
    let drops = paper.wallet_scope_drops(&wallet(), 100).unwrap();
    assert_eq!(drops.len(), 1);
    assert_eq!(drops[0].scope_id, MARKET_A);
    assert_eq!(
        paper
            .market_history_record(&wallet(), &MarketId(VenueMarketId(MARKET_B.to_owned())))
            .unwrap()
            .unwrap()
            .source_trade_id,
        buy_id
    );
}

#[test]
fn bracket_raw_reward_does_not_repair_scoped_buy_history() {
    let (dir, paper, mut engine) = fresh_anchored();
    let buy = position_row("TRADE", "0xscoped-buy", "\\xca", 0, "BUY", "1", "0.5", 100);
    let reward = position_row("REWARD", "0xraw-reward", MARKET_A, 0, "", "1", "1", 100);
    let mut ctx = context(100, true);
    ctx.identity_overrides.insert(
        buy.group_id.key().clone(),
        IdentityOverride {
            verified: market_outcome(MARKET_B, 0),
            evidence_hash: "a".repeat(64),
        },
    );
    engine
        .commit_read(vec![buy.clone(), reward.clone()], &ctx, zero_basis())
        .unwrap();
    let witness = paper
        .market_history_record(&wallet(), &MarketId(VenueMarketId(MARKET_A.to_owned())))
        .unwrap()
        .unwrap();
    for restart in [false, true] {
        if restart {
            let reopened = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
            engine =
                BucketCommitEngine::load(reopened.clone(), build_leader_ledger(&reopened).unwrap())
                    .unwrap();
        }
        ctx.bracket_commit = true;
        engine
            .commit_read(vec![buy.clone(), reward.clone()], &ctx, zero_basis())
            .unwrap();
        assert_eq!(
            paper
                .market_history_record(&wallet(), &witness.market_id)
                .unwrap(),
            Some(witness.clone())
        );
        assert!(
            paper
                .market_history_record(&wallet(), &MarketId(VenueMarketId(MARKET_B.to_owned())))
                .unwrap()
                .is_none()
        );
    }
    let later = position_row(
        "TRADE",
        "0xfirst-healthy-b",
        MARKET_B,
        0,
        "BUY",
        "1",
        "0.5",
        101,
    );
    assert_eq!(
        engine
            .commit_read(vec![later], &context(101, true), zero_basis())
            .unwrap()
            .pending
            .len(),
        1
    );
}

#[test]
fn routed_corrected_buy_keeps_its_history_witness_across_retry_restart_and_bracket() {
    let (dir, paper, mut engine) = fresh_anchored();
    let original = position_row("TRADE", "0xrouted-buy", MARKET_A, 0, "BUY", "0", "0.5", 100);
    engine
        .commit_read(vec![original], &context(100, true), zero_basis())
        .unwrap();
    let revised = position_row("TRADE", "0xrouted-buy", MARKET_A, 0, "BUY", "1", "0.5", 100);
    let mut ctx = context(100, true);
    ctx.identity_overrides.insert(
        revised.group_id.key().clone(),
        IdentityOverride {
            verified: market_outcome(MARKET_B, 0),
            evidence_hash: "a".repeat(64),
        },
    );
    engine
        .commit_read(vec![revised.clone()], &ctx, zero_basis())
        .unwrap();
    let witness = paper
        .market_history_record(&wallet(), market_outcome(MARKET_B, 0).market())
        .unwrap()
        .unwrap();
    assert_eq!(witness.source_trade_id, *revised.group_id.key());
    for restart in [false, true] {
        if restart {
            let reopened = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
            engine =
                BucketCommitEngine::load(reopened.clone(), build_leader_ledger(&reopened).unwrap())
                    .unwrap();
        }
        ctx.bracket_commit = restart;
        assert!(
            engine
                .commit_read(vec![revised.clone()], &ctx, zero_basis())
                .unwrap()
                .already_committed
        );
        assert_eq!(
            paper
                .market_history_record(&wallet(), &witness.market_id)
                .unwrap(),
            Some(witness.clone())
        );
    }
    let later = position_row("TRADE", "0xlater-b", MARKET_B, 0, "BUY", "1", "0.5", 101);
    assert!(
        engine
            .commit_read(vec![later], &context(101, true), zero_basis())
            .unwrap()
            .pending
            .is_empty()
    );
}

#[test]
fn pre_anchor_event_drop_excludes_new_sibling_and_admits_another_market() {
    let (_dir, paper, mut engine) = fresh_anchored();
    install_anchor(&mut engine, &paper, 100, Vec::new(), 1);
    let sibling = "0xcdee";
    paper.record_market_group(sibling, "0xee").unwrap();
    paper
        .import_scope_drops(
            589,
            &[(
                pe_paper_state::WalletScopeDrop {
                    wallet: wallet(),
                    scope_kind: "event".to_owned(),
                    scope_id: "0xee".to_owned(),
                    cause: "underflow".to_owned(),
                    source_trade_id: None,
                    dropped_at_epoch: 50,
                    proof_json: "{}".to_owned(),
                    recorded_at_unix: 101,
                },
                support::scenario_receipt(99),
            )],
            &Default::default(),
        )
        .unwrap();
    let excluded = position_row("TRADE", "0xnew-sibling", sibling, 1, "BUY", "1", "0.5", 101);
    let healthy = position_row(
        "TRADE",
        "0xhealthy-elsewhere",
        MARKET_B,
        0,
        "BUY",
        "1",
        "0.5",
        101,
    );
    let excluded_id = excluded.group_id.key().clone();
    let healthy_id = healthy.group_id.key().clone();
    let result = engine
        .commit_read(vec![excluded, healthy], &context(101, true), zero_basis())
        .unwrap();
    assert_eq!(result.dispositions[&excluded_id.0], "scope_dropped");
    assert_eq!(result.pending, [healthy_id]);
    assert_eq!(state(&engine, sibling, 1), ShareAmount::ZERO);
    assert_eq!(state(&engine, MARKET_B, 0).atomic(), 1_000_000);
    assert_eq!(
        replay_wallet_ledger(&paper, wallet()).unwrap().snapshots(),
        engine.ledger().snapshots()
    );
}

#[test]
fn anomaly_only_seconds_commit_drops_and_cursors_once_across_restart_and_history_shortcuts() {
    use pe_service::bucket_commit::{BucketCommitError, ReconciliationBucket};
    use pe_source_polymarket_public::{
        ActivityRequestBounds, ActivityRowAcceptance, CompleteActivityRead,
        ReconciliationPageEvidence, canonical_page_hash, parse_activity_response_with_acceptance,
    };
    for history in ["fresh", "covered", "all_stored"] {
        for resolved in [false, true] {
            let (dir, paper, mut engine) = fresh();
            if history != "fresh" {
                paper.set_cursor(&wallet(), 0).unwrap();
                install_anchor(
                    &mut engine,
                    &paper,
                    if history == "covered" { 200 } else { 0 },
                    Vec::new(),
                    0,
                );
            }
            let stored = position_row("TRADE", "0xstored", MARKET_B, 0, "BUY", "1", "0.5", 100);
            let aggregates = if history == "all_stored" {
                engine
                    .commit_read(vec![stored.clone()], &context(100, true), zero_basis())
                    .unwrap();
                vec![stored.aggregate]
            } else {
                Vec::new()
            };
            let mut row = json!({"proxyWallet":wallet(), "timestamp":100, "type":"TRADE", "transactionHash":"0xanomaly", "price":"2", "size":"1", "usdcSize":"1", "side":"BUY", "isCombo":false, "outcomeIndex":0, "outcome":"Yes"});
            row["conditionId"] = json!("unknown-condition");
            row["asset"] = json!("missing-asset");
            if resolved {
                row["conditionId"] = json!("0xcd");
            }
            let payload = serde_json::to_vec(&[row]).unwrap();
            let now = time::OffsetDateTime::from_unix_timestamp(110).unwrap();
            let window = parse_activity_response_with_acceptance(
                &payload,
                wallet(),
                &pe_source_polymarket_public::ActivityParseContext {
                    source_id: pe_core_types::SourceId("test".to_owned()),
                    observed_at: pe_core_types::SourceTimestamp(now),
                    received_at: pe_core_types::ReceivedAt(now),
                    transport: pe_source_polymarket_public::ActivityTransport::Rest,
                },
                ActivityRowAcceptance::StrictWithAnomalies,
            )
            .unwrap();
            let read = CompleteActivityRead {
                requested_wallet: wallet(),
                fixed_end: 110,
                rows: Vec::new(),
                anomalies: window.anomalies.clone(),
                pages: vec![ReconciliationPageEvidence {
                    has_more: None,
                    next_cursor: None,
                    request_url: "test".to_owned(),
                    bounds: Some(ActivityRequestBounds {
                        start: Some(0),
                        end: 110,
                    }),
                    partition: None,
                    offset: 0,
                    row_count: 1,
                    canonical_page_hash: canonical_page_hash(&payload).unwrap(),
                    raw_page_hash: blake3::hash(&payload).to_hex().to_string(),
                    received_at: pe_core_types::ReceivedAt(now),
                    schema_version: 2,
                    parser_version: 2,
                    anomalies: window.anomalies,
                }],
            };
            let bucket = ReconciliationBucket {
                wallet: wallet(),
                source_epoch: 100,
                aggregates,
                anomalies: read.anomalies.clone(),
            };
            let mut ctx = context(100, true);
            ctx.bracket_commit = true;
            ctx.copy_eligible = false;
            ctx.decision_inputs_json = json!({"fixed_end":110,"pages":read.pages}).to_string();
            let before_cursor = paper.cursor(&wallet()).unwrap();
            if resolved {
                // The condition is syntactically canonical and has no recorded membership.
                let bucket = bucket.clone();
                let mut pages = read.pages.clone();
                pages[0].anomalies = bucket.anomalies.clone();
                ctx.decision_inputs_json = json!({"fixed_end":110,"pages":pages}).to_string();
                assert!(
                    matches!(engine.commit(bucket.clone(), &ctx, zero_basis()), Err(BucketCommitError::MembershipRequired(markets)) if markets == ["0xcd"])
                );
                assert_eq!(paper.cursor(&wallet()).unwrap(), before_cursor);
                assert!(paper.wallet_scope_drops(&wallet(), 100).unwrap().is_empty());
                paper.record_market_group("0xcd", "").unwrap();
                let first = engine.commit(bucket.clone(), &ctx, zero_basis()).unwrap();
                assert!(!first.already_committed);
                assert!(first.pending.is_empty());
                let drops = paper.wallet_scope_drops(&wallet(), 100).unwrap();
                assert_eq!(drops.len(), 1);
                assert_eq!(drops[0].cause, "unrepresentable_activity_row");
                drop(engine);
                let mut restarted =
                    BucketCommitEngine::load(paper.clone(), build_leader_ledger(&paper).unwrap())
                        .unwrap();
                let repeat = restarted.commit(bucket, &ctx, zero_basis()).unwrap();
                assert!(repeat.already_committed);
                assert_eq!(paper.wallet_scope_drops(&wallet(), 100).unwrap(), drops);
            } else {
                let first = engine.commit(bucket.clone(), &ctx, zero_basis()).unwrap();
                assert!(first.pending.is_empty());
                assert!(first.dispositions.is_empty());
                drop(engine);
                let reopened = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
                let mut restarted = BucketCommitEngine::load(
                    reopened.clone(),
                    build_leader_ledger(&reopened).unwrap(),
                )
                .unwrap();
                assert!(
                    restarted
                        .commit(bucket, &ctx, zero_basis())
                        .unwrap()
                        .already_committed
                );
                assert!(
                    reopened
                        .wallet_scope_drops(&wallet(), 100)
                        .unwrap()
                        .is_empty()
                );
            }
            assert!(paper.cursor(&wallet()).unwrap().unwrap() >= 100);
            assert!(
                paper.open_decision_pending().unwrap().len()
                    <= usize::from(history == "all_stored")
            );
        }
    }
}

#[test]
fn later_anomaly_second_keeps_existing_decisions_and_healthy_bucket_results() {
    let (_dir, paper, mut engine) = fresh_anchored();
    let original = position_row(
        "TRADE",
        "0xprior-decision",
        MARKET_A,
        0,
        "BUY",
        "1",
        "0.5",
        100,
    );
    let original_id = original.group_id.key().clone();
    engine
        .commit_read(vec![original.clone()], &context(100, true), zero_basis())
        .unwrap();
    let before = paper.decision_pending_for(&original_id).unwrap().unwrap();
    let gate = paper.entry_gate_result(&original_id).unwrap();
    let history = paper.gate_history().unwrap();
    let mut rows = original.rows.clone();
    rows[0]["usdcSize"] = json!("4.212e-7");
    let payload = serde_json::to_vec(&rows).unwrap();
    let (read, anomalies) = support::producer_shaped_anomaly_read(
        wallet(),
        &payload,
        100,
        120,
        support::scenario_receipt(10),
    );
    let mut ctx = context(100, true);
    ctx.decision_inputs_json = read.decision_inputs_json;
    let bucket = pe_service::bucket_commit::ReconciliationBucket {
        wallet: wallet(),
        source_epoch: 100,
        aggregates: Vec::new(),
        anomalies,
    };
    let result = engine.commit(bucket.clone(), &ctx, zero_basis()).unwrap();
    assert!(result.dispositions.is_empty());
    assert!(result.pending.is_empty());
    assert_eq!(
        paper.decision_pending_for(&original_id).unwrap().unwrap(),
        before
    );
    assert_eq!(paper.entry_gate_result(&original_id).unwrap(), gate);
    assert_eq!(paper.gate_history().unwrap(), history);
    assert!(
        engine
            .commit(bucket, &ctx, zero_basis())
            .unwrap()
            .already_committed
    );
    let healthy = position_row("TRADE", "0xhealthy", MARKET_B, 0, "BUY", "2", "0.5", 101);
    let healthy_id = healthy.group_id.key().clone();
    let result = engine
        .commit_read(vec![healthy], &context(101, true), zero_basis())
        .unwrap();
    assert_eq!(result.pending, [healthy_id]);
    assert_eq!(state(&engine, MARKET_A, 0).atomic(), 1_000_000);
    assert_eq!(state(&engine, MARKET_B, 0).atomic(), 2_000_000);
    assert_eq!(
        paper.decision_pending_for(&original_id).unwrap().unwrap(),
        before
    );
}
