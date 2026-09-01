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
use pe_paper_state::{DecisionPendingState, PaperStateDb, WalletHistoryStatusRecord};
use pe_position_ledger::{PositionLedger, WalletFenceCause};
use pe_service::bucket_commit::{
    BucketCommitEngine, BucketDecisionContext, DecisionContinuationV2,
};
use pe_service::decision_replay::{
    AuthorityEvidence, DecisionClockEvidence, DecisionPostBoundaryEvidence,
    DecisionPostBoundaryEvidenceBody, TerminalDispositionEvidence, replay_decision_pending,
};
use pe_service::paper_recovery::build_leader_ledger;
use pe_source_polymarket_public::{
    ActivityAggregate, ActivityParseContext, ActivityTransport, parse_activity_response,
};
use serde_json::{Value, json};
use time::OffsetDateTime;

const WALLET_HEX: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const MARKET_A: &str = "0xcondition-a";
const MARKET_B: &str = "0xcondition-b";

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
        recorded_at_unix: epoch + 20,
        observation_provenance: HashMap::new(),
        no_copy_dispositions: HashMap::new(),
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

fn fresh() -> (tempfile::TempDir, Arc<PaperStateDb>, BucketCommitEngine) {
    let dir = tempfile::tempdir().unwrap();
    let paper = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
    let engine = BucketCommitEngine::load(Arc::clone(&paper), PositionLedger::new()).unwrap();
    (dir, paper, engine)
}

#[test]
fn split_merge_redeem_preserve_exact_fractional_balances_atomically() {
    let (_dir, paper, mut engine) = fresh();
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
    let (_dir, paper, mut engine) = fresh();
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
        let (_dir, paper, mut engine) = fresh();
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

    let (dir, paper, mut engine) = fresh();
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
        let (_dir, paper, mut engine) = fresh();
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
        let (_dir, paper, mut engine) = fresh();
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
    let (dir, paper, mut engine) = fresh();
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
    let (_dir, paper, mut engine) = fresh();
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
            .all(|value| value == "wallet_fenced")
    );
    assert_eq!(state(&engine, MARKET_B, 0).atomic(), 1);

    let (_dir, paper, mut engine) = fresh();
    let result = engine
        .commit(
            vec![pair_effect("MERGE", "0x61", MARKET_A, "0.000001", 500)],
            &context(500, true),
            zero_basis(),
        )
        .unwrap();
    assert_eq!(result.newly_fenced, Some(WalletFenceCause::Underflow));
    assert!(engine.ledger().position(&wallet()).is_none());
    assert!(paper.is_wallet_fenced(&wallet()).unwrap());

    let (_dir, paper, mut engine) = fresh();
    let result = engine
        .commit(
            vec![combo_effect("FUTURE_POSITION_EFFECT", "0x62", 501)],
            &context(501, true),
            zero_basis(),
        )
        .unwrap();
    assert_eq!(result.newly_fenced, Some(WalletFenceCause::UnknownEffect));
    assert!(paper.is_wallet_fenced(&wallet()).unwrap());

    let (_dir, paper, mut engine) = fresh();
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
fn equal_second_validity_is_order_independent_or_the_whole_bucket_fences() {
    let (_dir, paper, mut engine) = fresh();
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

    let (_dir, paper, mut engine) = fresh();
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
    let (_dir, paper, mut engine) = fresh();
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

    let (_dir, paper, mut engine) = fresh();
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
