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
use pe_position_ledger::{LedgerEffect, PositionLedger};
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

fn non_trade_aggregate(
    activity_type: &str,
    transaction_hash: &str,
    market_id: &str,
    size: &str,
    epoch: i64,
) -> ActivityAggregate {
    let observed = OffsetDateTime::from_unix_timestamp(epoch).unwrap();
    let rows = json!([{
        "proxyWallet": WALLET,
        "timestamp": epoch,
        "conditionId": market_id,
        "type": activity_type,
        "size": size,
        "usdcSize": "0",
        "transactionHash": transaction_hash,
        "price": "0",
        "asset": "",
        "side": "",
        "outcomeIndex": 999,
        "outcome": "",
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
        page_occurrences: Vec::new(),
        observed_source_receipts: HashMap::new(),
        reconstruction_quality: ReconstructionQuality::new(100).unwrap(),
        read_commitment: None,

        signal_config: Default::default(),
        copy_eligible: false,
        bracket_commit: false,
        recorded_at_unix: epoch,
        observation_provenance: HashMap::new(),
        no_copy_dispositions: HashMap::new(),
        identity_overrides: HashMap::new(),
        identity_unresolved: Default::default(),
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

#[test]
fn successful_post_fence_bucket_replays_the_applied_effect() {
    let dir = tempfile::tempdir().unwrap();
    let paper = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
    paper.set_cursor(&wallet(), 0).unwrap();
    let mut engine = BucketCommitEngine::load(Arc::clone(&paper), PositionLedger::new()).unwrap();
    install(&mut engine, &paper, 100, Vec::new());
    engine
        .commit(
            vec![non_trade_aggregate(
                "CONVERSION",
                "0xfence-success",
                "market-a",
                "1",
                110,
            )],
            &context(110),
            zero_basis(),
        )
        .unwrap();
    let buy = aggregate("0xpost-fence-buy", "market-a", 0, "BUY", "2", 120);
    let buy_id = buy.group_id.key().clone();
    let result = engine
        .commit(vec![buy], &context(120), zero_basis())
        .unwrap();
    assert_eq!(result.dispositions[&buy_id.0], "wallet_fenced_applied");

    let replayed = replay_wallet_ledger(&paper, wallet()).unwrap();
    let mirrored = build_leader_ledger(&paper).unwrap();
    assert_eq!(
        ledger_capture(&replayed, &paper, wallet()).unwrap().hash,
        ledger_capture(&mirrored, &paper, wallet()).unwrap().hash
    );
}

#[test]
fn failed_post_fence_bucket_replays_without_the_rejected_effect() {
    let dir = tempfile::tempdir().unwrap();
    let paper = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
    paper.set_cursor(&wallet(), 0).unwrap();
    let mut engine = BucketCommitEngine::load(Arc::clone(&paper), PositionLedger::new()).unwrap();
    install(&mut engine, &paper, 100, Vec::new());
    engine
        .commit(
            vec![non_trade_aggregate(
                "CONVERSION",
                "0xfence-failed",
                "market-a",
                "1",
                110,
            )],
            &context(110),
            zero_basis(),
        )
        .unwrap();
    let merge = non_trade_aggregate("MERGE", "0xpost-fence-merge", "market-a", "1", 120);
    let merge_id = merge.group_id.key().clone();
    let result = engine
        .commit(vec![merge], &context(120), zero_basis())
        .unwrap();
    assert_eq!(result.dispositions[&merge_id.0], "wallet_fenced");

    let replayed = replay_wallet_ledger(&paper, wallet()).unwrap();
    let mirrored = build_leader_ledger(&paper).unwrap();
    assert_eq!(
        ledger_capture(&replayed, &paper, wallet()).unwrap().hash,
        ledger_capture(&mirrored, &paper, wallet()).unwrap().hash
    );
}

#[test]
fn equal_cutoff_is_covered_and_legacy_prefix_is_skipped() {
    let dir = tempfile::tempdir().unwrap();
    let paper = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
    paper.set_cursor(&wallet(), 0).unwrap();
    let legacy = aggregate("0xlegacy", "market-legacy", 0, "BUY", "1", 90);
    paper
        .commit_activity_bucket(&ActivityBucketCommit {
            wallet: wallet(),
            source_epoch: 90,
            dispositions: vec![ActivityDispositionRecord {
                source_trade_id: legacy.group_id.key().clone(),
                transaction_hash: legacy.group_id.components().transaction_hash.clone(),
                wallet: wallet(),
                source_epoch: 90,
                semantic_revision: legacy.semantic_revision.as_str().to_owned(),
                activity_type: "TRADE".to_owned(),
                disposition: "legacy_v0".to_owned(),
                proof_json: "{}".to_owned(),
                no_copy: None,
            }],
            leader_positions: Vec::new(),
            gate_results: Vec::new(),
            history_effects: Vec::new(),
            history_status: None,
            pending: Vec::new(),
            fence: None,
            reanchor: None,
            advance_cursor: true,
        })
        .unwrap();
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
    let equal = aggregate("0xequal", "market-a", 0, "BUY", "3", 100);
    let equal_id = equal.group_id.key().clone();
    let result = engine
        .commit(vec![equal], &context(100), zero_basis())
        .unwrap();
    assert_eq!(result.dispositions[&equal_id.0], "anchor_covered_late");

    let replayed = replay_wallet_ledger(&paper, wallet()).unwrap();
    let mirrored = build_leader_ledger(&paper).unwrap();
    assert_eq!(
        ledger_capture(&replayed, &paper, wallet()).unwrap().hash,
        ledger_capture(&mirrored, &paper, wallet()).unwrap().hash
    );
}

#[test]
fn legacy_fences_and_zero_conversion_documents_replay_without_reclassification() {
    for conversion in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let paper = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        paper.set_cursor(&wallet(), 0).unwrap();
        let mut engine =
            BucketCommitEngine::load(Arc::clone(&paper), PositionLedger::new()).unwrap();
        install(&mut engine, &paper, 100, Vec::new());
        let groups = if conversion {
            vec![non_trade_aggregate(
                "CONVERSION",
                "0xlegacy-zero",
                "market-a",
                "0",
                110,
            )]
        } else {
            (0..5)
                .map(|index| {
                    aggregate(
                        &format!("0xlegacy-wide-{index}"),
                        "market-a",
                        0,
                        "BUY",
                        "1",
                        110,
                    )
                })
                .collect()
        };
        let mutations = groups
            .iter()
            .map(pe_position_ledger::LedgerMutation::from_activity)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(matches!(
            pe_position_ledger::classify_complete_historical_second(
                engine.ledger(),
                wallet(),
                &mutations,
                ReconstructionQuality::new(100).unwrap(),
                &|_| false,
            )
            .unwrap(),
            pe_position_ledger::SecondVerdict::OrderIndependent { .. }
        ));
        let cause = if conversion {
            "conversion_unknown_conditions"
        } else {
            "order_dependent_equal_second"
        };
        let fence = pe_paper_state::WalletFenceRecord {
            wallet: wallet(),
            source_trade_id: groups[0].group_id.key().clone(),
            cause: cause.to_owned(),
            proof_json: "{}".to_owned(),
            fenced_at_unix: 110,
        };
        let records = groups
            .iter()
            .enumerate()
            .map(|(index, group)| ActivityDispositionRecord {
                source_trade_id: group.group_id.key().clone(),
                transaction_hash: group.group_id.components().transaction_hash.clone(),
                wallet: wallet(),
                source_epoch: 110,
                semantic_revision: group.semantic_revision.as_str().to_owned(),
                activity_type: if conversion { "CONVERSION" } else { "TRADE" }.to_owned(),
                disposition: if index == 0 { cause } else { "wallet_fenced" }.to_owned(),
                // Frozen old mapping: zero conversion was a refused conversion effect.
                proof_json: if conversion {
                    r#"{"effect":{"kind":"conversion"},"version":1}"#.to_owned()
                } else {
                    mutations[index].effect.to_document().unwrap()
                },
                no_copy: None,
            })
            .collect();
        paper
            .commit_activity_bucket(&ActivityBucketCommit {
                wallet: wallet(),
                source_epoch: 110,
                dispositions: records,
                leader_positions: Vec::new(),
                gate_results: Vec::new(),
                history_effects: Vec::new(),
                history_status: None,
                pending: Vec::new(),
                fence: Some(fence.clone()),
                reanchor: None,
                advance_cursor: true,
            })
            .unwrap();
        let replayed = replay_wallet_ledger(&paper, wallet()).unwrap();
        let mirrored = build_leader_ledger(&paper).unwrap();
        assert_eq!(
            ledger_capture(&replayed, &paper, wallet()).unwrap().hash,
            ledger_capture(&mirrored, &paper, wallet()).unwrap().hash
        );
        assert_eq!(paper.wallet_fences().unwrap(), vec![fence]);
        assert!(paper.gate_history().unwrap().is_empty());
        assert!(paper.open_decision_pending().unwrap().is_empty());
    }
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
            reanchor: None,
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
    let unknown = replay_with_document(r#"{"effect":{"kind":"raw_only"},"version":4}"#);
    assert!(matches!(
        unknown,
        WalletLedgerReplayError::EffectDocument {
            source: pe_position_ledger::LedgerEffectDocumentError::UnknownVersion { version: 4 },
            ..
        }
    ));
}

#[test]
fn unknown_disposition_version_is_a_typed_replay_failure() {
    let replay = {
        let dir = tempfile::tempdir().unwrap();
        let paper = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        paper.set_cursor(&wallet(), 0).unwrap();
        let mut engine =
            BucketCommitEngine::load(Arc::clone(&paper), PositionLedger::new()).unwrap();
        install(&mut engine, &paper, 100, Vec::new());
        let group = aggregate("0xunknown-disposition", "market-a", 0, "BUY", "1", 110);
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
                    disposition: "wallet_fenced_v2".to_owned(),
                    proof_json: LedgerEffect::RawOnly.to_document().unwrap(),
                    no_copy: None,
                }],
                leader_positions: Vec::new(),
                gate_results: Vec::new(),
                history_effects: Vec::new(),
                history_status: None,
                pending: Vec::new(),
                fence: None,
                reanchor: None,
                advance_cursor: true,
            })
            .unwrap();
        replay_wallet_ledger(&paper, wallet())
    };
    assert!(matches!(
        replay,
        Err(WalletLedgerReplayError::UnknownDisposition { disposition, .. })
            if disposition == "wallet_fenced_v2"
    ));
}
