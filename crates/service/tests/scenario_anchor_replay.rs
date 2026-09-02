#![cfg(feature = "scenario")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;

use pe_core_types::{
    MarketId, MarketOutcomeId, OutcomeId, ReceivedAt, ReconstructionQuality, ShareAmount, SourceId,
    SourceTimestamp, VenueMarketId, WalletAddress,
};
use pe_paper_state::{
    ActivityBucketCommit, ActivityDispositionRecord, PaperStateDb, WalletHistoryStatusRecord,
};
use pe_position_ledger::PositionLedger;
use pe_service::bucket_commit::{BucketCommitEngine, BucketDecisionContext};
use pe_service::paper_recovery::{
    WalletLedgerReplayError, build_leader_ledger, replay_wallet_ledger,
};
use pe_service::position_seeder::{AnchorExpectation, AnchorInstall, AnchorProof, ledger_capture};
use pe_source_polymarket_public::{
    ActivityAggregate, ActivityParseContext, ActivityTransport, parse_activity_response,
};
use serde_json::json;
use time::OffsetDateTime;

const WALLET: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

fn wallet() -> WalletAddress {
    WalletAddress::from_hex(WALLET).unwrap()
}

fn market(value: &str) -> MarketId {
    MarketId(VenueMarketId(value.to_owned()))
}

fn aggregate(
    transaction_hash: &str,
    market_id: &str,
    outcome: u16,
    side: &str,
    size: &str,
    epoch: i64,
) -> ActivityAggregate {
    let observed = OffsetDateTime::from_unix_timestamp(epoch).unwrap();
    let rows = json!([{
        "proxyWallet": WALLET,
        "timestamp": epoch,
        "conditionId": market_id,
        "type": "TRADE",
        "size": size,
        "usdcSize": "1",
        "transactionHash": transaction_hash,
        "price": "0.5",
        "asset": format!("{market_id}-{outcome}"),
        "side": side,
        "outcomeIndex": outcome,
        "outcome": if outcome == 0 { "Yes" } else { "No" },
        "isCombo": false,
    }]);
    parse_activity_response(
        &serde_json::to_vec(&rows).unwrap(),
        wallet(),
        &ActivityParseContext {
            source_id: SourceId("scenario".to_owned()),
            observed_at: SourceTimestamp(observed),
            received_at: ReceivedAt(observed),
            transport: ActivityTransport::Rest,
        },
    )
    .unwrap()
    .aggregates()
    .unwrap()
    .remove(0)
}

fn context(epoch: i64) -> BucketDecisionContext {
    BucketDecisionContext {
        applied_configuration: pe_service::runtime_config::RuntimeConfig::from_service_config(
            &pe_service::config::ServiceConfig::default(),
        ),
        decision_inputs_json: "{}".to_owned(),
        reconstruction_quality: ReconstructionQuality::new(100).unwrap(),
        signal_config: Default::default(),
        copy_eligible: false,
        recorded_at_unix: epoch,
        observation_provenance: HashMap::new(),
        no_copy_dispositions: HashMap::new(),
        history_status: Some(WalletHistoryStatusRecord {
            wallet: wallet(),
            complete: true,
            proof_json: "{}".to_owned(),
            updated_at_unix: epoch,
        }),
    }
}

fn zero_basis() -> pe_service::bucket_commit::FrozenDecisionBasis {
    pe_service::bucket_commit::FrozenDecisionBasis {
        win_rate_p: pe_core_types::Probability::ZERO,
        bankroll: rust_decimal::Decimal::ZERO,
    }
}

fn install(
    engine: &mut BucketCommitEngine,
    paper: &PaperStateDb,
    cutoff: i64,
    balances: Vec<(MarketId, OutcomeId, ShareAmount)>,
) {
    let captured = ledger_capture(engine.ledger(), paper, wallet()).unwrap();
    engine
        .install_anchors(&[AnchorInstall {
            wallet: wallet(),
            balances,
            cutoff,
            proof: AnchorProof {
                positions_proof_hash: format!("positions-{cutoff}"),
                activity_bounds_json: "[]".to_owned(),
                source_log_generation: "scenario".to_owned(),
                document: "{}".to_owned(),
                recorded_at_unix: cutoff,
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
fn two_anchors_and_ordered_effect_documents_replay_to_the_restart_mirror() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("paper.db");
    let paper = Arc::new(PaperStateDb::open(&path).unwrap());
    paper.set_cursor(&wallet(), 0).unwrap();
    let mut engine = BucketCommitEngine::load(Arc::clone(&paper), PositionLedger::new()).unwrap();
    install(
        &mut engine,
        &paper,
        100,
        vec![(
            market("market-a"),
            OutcomeId(0),
            ShareAmount::from_atomic(5_000_000),
        )],
    );
    engine
        .commit(
            vec![aggregate("0xbetween", "market-a", 0, "BUY", "2", 110)],
            &context(110),
            zero_basis(),
        )
        .unwrap();
    install(
        &mut engine,
        &paper,
        120,
        vec![
            (
                market("market-a"),
                OutcomeId(0),
                ShareAmount::from_atomic(9_000_000),
            ),
            (
                market("market-b"),
                OutcomeId(1),
                ShareAmount::from_atomic(3_000_000),
            ),
        ],
    );
    engine
        .commit(
            vec![aggregate("0xafter", "market-a", 0, "SELL", "4", 130)],
            &context(130),
            zero_basis(),
        )
        .unwrap();
    drop(engine);
    drop(paper);

    let restarted = PaperStateDb::open(&path).unwrap();
    let replayed = replay_wallet_ledger(&restarted, wallet()).unwrap();
    let mirrored = build_leader_ledger(&restarted).unwrap();
    assert_eq!(
        ledger_capture(&replayed, &restarted, wallet())
            .unwrap()
            .hash,
        ledger_capture(&mirrored, &restarted, wallet())
            .unwrap()
            .hash
    );
    let key = MarketOutcomeId::new(market("market-a"), OutcomeId(0));
    assert_eq!(
        replayed.position(&wallet()).unwrap().positions[&key]
            .long_contracts
            .atomic(),
        5_000_000
    );
}

fn replay_with_document(document: &str) -> WalletLedgerReplayError {
    let dir = tempfile::tempdir().unwrap();
    let paper = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
    paper.set_cursor(&wallet(), 0).unwrap();
    let mut engine = BucketCommitEngine::load(Arc::clone(&paper), PositionLedger::new()).unwrap();
    install(&mut engine, &paper, 100, Vec::new());
    let group = aggregate("0xinvalid-document", "market-a", 0, "BUY", "1", 110);
    paper
        .commit_activity_bucket(&ActivityBucketCommit {
            wallet: wallet(),
            source_epoch: 110,
            dispositions: vec![ActivityDispositionRecord {
                source_trade_id: group.group_id.key().clone(),
                transaction_hash: group.group_id.components().transaction_hash.clone(),
                wallet: wallet(),
                source_epoch: 110,
                semantic_revision: group.semantic_revision.as_str().to_owned(),
                activity_type: "TRADE".to_owned(),
                disposition: "applied".to_owned(),
                proof_json: document.to_owned(),
                no_copy: None,
            }],
            leader_positions: Vec::new(),
            gate_results: Vec::new(),
            history_effects: Vec::new(),
            history_status: None,
            pending: Vec::new(),
            fence: None,
            advance_cursor: true,
        })
        .unwrap();
    replay_wallet_ledger(&paper, wallet())
        .err()
        .expect("invalid effect document unexpectedly replayed")
}

#[test]
fn missing_and_unknown_effect_documents_are_typed_replay_failures() {
    assert!(matches!(
        replay_with_document("{}"),
        WalletLedgerReplayError::EffectDocument { .. }
    ));
    let unknown = replay_with_document(r#"{"effect":{"kind":"raw_only"},"version":2}"#);
    assert!(matches!(
        unknown,
        WalletLedgerReplayError::EffectDocument {
            source: pe_position_ledger::LedgerEffectDocumentError::UnknownVersion { version: 2 },
            ..
        }
    ));
}
