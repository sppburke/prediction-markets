#![cfg(feature = "scenario")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use pe_core_types::{
    MarketId, OutcomeId, ReceivedAt, ReconstructionQuality, ShareAmount, SourceId, SourceTimestamp,
    VenueMarketId, WalletAddress,
};
use pe_paper_state::{PaperStateDb, WalletHistoryStatusRecord};
use pe_position_ledger::PositionLedger;
use pe_service::bucket_commit::{BucketCommitEngine, BucketDecisionContext};
use pe_service::orchestrator_control::OrchestratorControl;
use pe_service::paper_recovery::build_leader_ledger;
use pe_service::position_seeder::{
    AnchorExpectation, AnchorInstall, AnchorProof, CausalPositionError, CausalPositionValidator,
    is_deferred_causal_position_error, ledger_capture,
};
use pe_service::watchlist_admission::{AdmissionError, AdmissionPreparer};
use pe_source_core::SourceError;
use pe_source_polymarket_public::{
    ActivityParseContext, ActivityReadError, ActivityTransport, PageFetcher, PolymarketEndpoint,
    PositionPartition, PositionReadError, aggregate_activity_rows, parse_activity_response,
};
use serde_json::{Value, json};
use tempfile::TempDir;
use time::OffsetDateTime;
use tokio::sync::mpsc;

const BASE: &str = "https://api.example.com";
const END: i64 = 100;

struct QueueFetcher {
    responses: Mutex<HashMap<String, VecDeque<Vec<u8>>>>,
}

impl QueueFetcher {
    fn new(responses: HashMap<String, Vec<Vec<u8>>>) -> Self {
        Self {
            responses: Mutex::new(
                responses
                    .into_iter()
                    .map(|(url, responses)| (url, responses.into()))
                    .collect(),
            ),
        }
    }
}

impl PageFetcher for QueueFetcher {
    async fn fetch_page(&self, url: &str) -> Result<Vec<u8>, SourceError> {
        self.responses
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_mut(url)
            .and_then(VecDeque::pop_front)
            .ok_or_else(|| SourceError::Fatal {
                message: format!("no queued fixture for {url}"),
            })
    }
}

fn wallet(byte: u8) -> WalletAddress {
    WalletAddress([byte; 20])
}

fn condition(byte: u8) -> String {
    format!("0xcondition{byte:02x}")
}

fn asset(byte: u8) -> String {
    format!("asset-{byte:02x}")
}

fn activity(wallet: WalletAddress, byte: u8, amount: &str, tx: &str, epoch: i64) -> Value {
    json!({
        "proxyWallet": wallet,
        "timestamp": epoch,
        "conditionId": condition(byte),
        "type": "TRADE",
        "size": amount,
        "usdcSize": "0.500000",
        "transactionHash": tx,
        "price": "0.500000",
        "asset": asset(byte),
        "side": "BUY",
        "outcomeIndex": 0,
        "outcome": "Yes",
        "isCombo": false
    })
}

fn position(wallet: WalletAddress, byte: u8, amount: &str) -> Value {
    json!({
        "proxyWallet": wallet,
        "asset": asset(byte),
        "conditionId": condition(byte),
        "size": amount,
        "outcomeIndex": 0,
        "cashPnl": "presentation-only",
        "negativeRisk": true
    })
}

fn activity_url(wallet: WalletAddress) -> String {
    PolymarketEndpoint::UserPositionActivityPage {
        user: wallet.to_string(),
        end: END,
        start: None,
        offset: 0,
    }
    .url(BASE)
}

fn position_url(wallet: WalletAddress, partition: PositionPartition) -> String {
    PolymarketEndpoint::CurrentPositionsReconciliationPage {
        user: wallet.to_string(),
        partition,
        offset: 0,
    }
    .url(BASE)
}

fn stable_responses(specs: &[(WalletAddress, u8, &str)]) -> HashMap<String, Vec<Vec<u8>>> {
    let mut responses = HashMap::new();
    for (wallet, byte, amount) in specs {
        let activity = serde_json::to_vec(&vec![activity(
            *wallet,
            *byte,
            "1.000000",
            &format!("0xbase{byte}"),
            10,
        )])
        .unwrap();
        let positions = serde_json::to_vec(&vec![position(*wallet, *byte, amount)]).unwrap();
        responses.insert(
            activity_url(*wallet),
            vec![activity.clone(), activity.clone(), activity],
        );
        responses.insert(
            position_url(*wallet, PositionPartition::NotRedeemable),
            vec![positions.clone(), positions],
        );
        responses.insert(
            position_url(*wallet, PositionPartition::Redeemable),
            vec![b"[]".to_vec(), b"[]".to_vec()],
        );
    }
    responses
}

fn validator(responses: HashMap<String, Vec<Vec<u8>>>) -> CausalPositionValidator {
    CausalPositionValidator::new(
        Arc::new(QueueFetcher::new(responses)),
        BASE,
        "source-generation-test",
    )
    .with_clock(Arc::new(|| END))
}

fn fresh(wallets: &[WalletAddress]) -> (TempDir, Arc<PaperStateDb>, BucketCommitEngine) {
    let dir = tempfile::tempdir().unwrap();
    let paper = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
    for wallet in wallets {
        paper
            .record_reconciled_history_status(&WalletHistoryStatusRecord {
                wallet: *wallet,
                complete: true,
                proof_json: "{\"complete\":true}".to_owned(),
                updated_at_unix: 1,
            })
            .unwrap();
    }
    let engine = BucketCommitEngine::load(Arc::clone(&paper), PositionLedger::new()).unwrap();
    (dir, paper, engine)
}

fn install_empty_anchor(
    engine: &mut BucketCommitEngine,
    paper: &PaperStateDb,
    wallet: WalletAddress,
    cutoff: i64,
) {
    paper.set_cursor(&wallet, cutoff).unwrap();
    let captured = ledger_capture(engine.ledger(), paper, wallet).unwrap();
    engine
        .install_anchors(&[AnchorInstall {
            wallet,
            balances: Vec::new(),
            cutoff,
            proof: AnchorProof {
                positions_proof_hash: "empty".to_owned(),
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

fn zero_basis() -> pe_service::bucket_commit::FrozenDecisionBasis {
    pe_service::bucket_commit::FrozenDecisionBasis {
        win_rate_p: pe_core_types::Probability::ZERO,
        bankroll: rust_decimal::Decimal::ZERO,
    }
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
        history_status: None,
    }
}

fn aggregate(row: Value, wallet: WalletAddress) -> pe_source_polymarket_public::ActivityAggregate {
    let now = OffsetDateTime::from_unix_timestamp(END).unwrap();
    let parsed = parse_activity_response(
        &serde_json::to_vec(&vec![row]).unwrap(),
        wallet,
        &ActivityParseContext {
            source_id: SourceId("test".to_owned()),
            observed_at: SourceTimestamp(now),
            received_at: ReceivedAt(now),
            transport: ActivityTransport::Rest,
        },
    )
    .unwrap();
    aggregate_activity_rows(&parsed.rows).unwrap().remove(0)
}

fn spawn_control_actor(
    mut control_rx: mpsc::Receiver<OrchestratorControl>,
    mut engine: BucketCommitEngine,
    paper: Arc<PaperStateDb>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(message) = control_rx.recv().await {
            match message {
                OrchestratorControl::PrepareAdmissions { acknowledged, .. } => {
                    let _ = acknowledged.send(());
                }
                OrchestratorControl::InstallAnchors {
                    installs,
                    acknowledged,
                } => {
                    let result = engine
                        .install_anchors(&installs)
                        .map_err(|error| error.to_string());
                    let _ = acknowledged.send(result);
                }
                OrchestratorControl::CaptureAdmissionLedger { wallet, captured } => {
                    let _ = captured.send(
                        ledger_capture(engine.ledger(), &paper, wallet)
                            .map_err(|error| error.to_string()),
                    );
                }
                OrchestratorControl::CommitActivityBucket {
                    aggregates,
                    context,
                    committed,
                } => {
                    let _ = committed.send(
                        engine
                            .commit(aggregates, &context, zero_basis())
                            .map_err(|error| error.to_string()),
                    );
                }
            }
        }
    })
}

#[tokio::test]
async fn stable_complete_bracket_installs_and_restart_is_identical() {
    let wallet = wallet(0x11);
    let (_dir, paper, mut engine) = fresh(&[wallet]);
    let first = validator(stable_responses(&[(wallet, 1, "1.000000")]))
        .validate_direct(&[wallet], &mut engine, &paper)
        .await
        .unwrap();
    let durable = paper.position_validation(&wallet).unwrap().unwrap();
    assert_eq!(
        durable.positions_proof_hash,
        first[0].proof.positions_proof_hash
    );
    assert_eq!(durable.source_log_generation, "source-generation-test");

    let restarted_ledger = build_leader_ledger(&paper).unwrap();
    let mut restarted = BucketCommitEngine::load(Arc::clone(&paper), restarted_ledger).unwrap();
    let second = validator(stable_responses(&[(wallet, 1, "1")]))
        .validate_direct(&[wallet], &mut restarted, &paper)
        .await
        .unwrap();
    assert_eq!(first[0].balances, second[0].balances);
    assert_eq!(
        first[0].proof.positions_proof_hash,
        second[0].proof.positions_proof_hash
    );
}

#[tokio::test]
async fn mutation_between_each_bracket_step_installs_nothing() {
    for target_step in 1..=5 {
        let wallet = wallet(u8::try_from(0x20 + target_step).unwrap());
        let (_dir, paper, mut engine) = fresh(&[wallet]);
        install_empty_anchor(&mut engine, &paper, wallet, 0);
        let mutation = aggregate(
            activity(
                wallet,
                9,
                "1.000000",
                &format!("0xmutation{target_step}"),
                20,
            ),
            wallet,
        );
        let hook = Arc::new(move |step: usize, engine: &mut BucketCommitEngine| {
            if step == target_step {
                engine
                    .commit(vec![mutation.clone()], &context(20), zero_basis())
                    .unwrap();
            }
        });
        let error = validator(stable_responses(&[(wallet, 1, "1.000000")]))
            .with_step_hook(hook)
            .validate_direct(&[wallet], &mut engine, &paper)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            CausalPositionError::LedgerRevision { .. } | CausalPositionError::AnchorInstall(_)
        ));
        assert_eq!(
            ledger_capture(engine.ledger(), &paper, wallet)
                .unwrap()
                .positive_ordinary_balances
                .get(&(condition(9), 0))
                .copied(),
            Some(ShareAmount::from_atomic(1_000_000)),
            "the intervening poller BUY must survive the rejected anchor"
        );
        assert_eq!(paper.position_anchors(&wallet).unwrap().len(), 1);
        assert!(paper.position_validation(&wallet).unwrap().is_none());
        assert!(!paper.is_wallet_fenced(&wallet).unwrap());
    }
}

#[tokio::test]
async fn changed_activity_revision_fences_and_installs_nothing() {
    let wallet = wallet(0x31);
    let (_dir, paper, mut engine) = fresh(&[wallet]);
    let first =
        serde_json::to_vec(&vec![activity(wallet, 1, "1.000000", "0xrevision", 10)]).unwrap();
    let changed =
        serde_json::to_vec(&vec![activity(wallet, 1, "2.000000", "0xrevision", 10)]).unwrap();
    let positions = serde_json::to_vec(&vec![position(wallet, 1, "1.000000")]).unwrap();
    let responses = HashMap::from([
        (activity_url(wallet), vec![first, changed]),
        (
            position_url(wallet, PositionPartition::NotRedeemable),
            vec![positions],
        ),
        (
            position_url(wallet, PositionPartition::Redeemable),
            vec![b"[]".to_vec()],
        ),
    ]);
    let accepted = validator(responses)
        .validate_direct(&[wallet], &mut engine, &paper)
        .await
        .expect("a deterministic durable fence is quarantined, not boot-fatal");
    assert!(accepted.is_empty());
    assert!(paper.is_wallet_fenced(&wallet).unwrap());
    assert!(paper.position_validation(&wallet).unwrap().is_none());
}

#[tokio::test]
async fn stable_positions_replace_the_activity_balance_without_fencing() {
    let wallet = wallet(0x41);
    let (_dir, paper, mut engine) = fresh(&[wallet]);
    let accepted = validator(stable_responses(&[(wallet, 1, "2.000000")]))
        .validate_direct(&[wallet], &mut engine, &paper)
        .await
        .expect("stable positions are the authoritative anchor");
    assert_eq!(accepted.len(), 1);
    assert!(!paper.is_wallet_fenced(&wallet).unwrap());
    assert!(paper.position_validation(&wallet).unwrap().is_some());
    assert_eq!(accepted[0].balances[0].2.atomic(), 2_000_000);

    let second = validator(stable_responses(&[(wallet, 1, "2.000000")]))
        .validate_direct(&[wallet], &mut engine, &paper)
        .await
        .expect("the authoritative balance re-installs idempotently");
    assert_eq!(second.len(), 1);
    assert!(!paper.is_wallet_fenced(&wallet).unwrap());
    assert!(paper.position_validation(&wallet).unwrap().is_some());
}

#[tokio::test]
async fn changed_position_revision_retries_without_fencing() {
    let wallet = wallet(0x42);
    let (_dir, paper, mut engine) = fresh(&[wallet]);
    let activity =
        serde_json::to_vec(&vec![activity(wallet, 1, "1.000000", "0xbase", 10)]).unwrap();
    let first = serde_json::to_vec(&vec![position(wallet, 1, "1.000000")]).unwrap();
    let second = serde_json::to_vec(&vec![position(wallet, 1, "2.000000")]).unwrap();
    let responses = HashMap::from([
        (
            activity_url(wallet),
            vec![activity.clone(), activity.clone(), activity],
        ),
        (
            position_url(wallet, PositionPartition::NotRedeemable),
            vec![first, second],
        ),
        (
            position_url(wallet, PositionPartition::Redeemable),
            vec![b"[]".to_vec(), b"[]".to_vec()],
        ),
    ]);
    let accepted = validator(responses)
        .validate_direct(&[wallet], &mut engine, &paper)
        .await
        .expect("changed position semantics defer the wallet for a later retry");
    assert!(accepted.is_empty());
    assert!(!paper.is_wallet_fenced(&wallet).unwrap());
    assert!(paper.position_validation(&wallet).unwrap().is_none());
}

#[tokio::test]
async fn backwards_activity_fixed_end_retries_without_installing() {
    let wallet = wallet(0x44);
    let (_dir, paper, mut engine) = fresh(&[wallet]);
    let activity =
        serde_json::to_vec(&vec![activity(wallet, 1, "1.000000", "0xbase", 10)]).unwrap();
    let positions = serde_json::to_vec(&vec![position(wallet, 1, "1.000000")]).unwrap();
    let responses = HashMap::from([
        (
            PolymarketEndpoint::UserPositionActivityPage {
                user: wallet.to_string(),
                end: 100,
                start: None,
                offset: 0,
            }
            .url(BASE),
            vec![activity.clone()],
        ),
        (
            PolymarketEndpoint::UserPositionActivityPage {
                user: wallet.to_string(),
                end: 99,
                start: None,
                offset: 0,
            }
            .url(BASE),
            vec![activity.clone()],
        ),
        (
            PolymarketEndpoint::UserPositionActivityPage {
                user: wallet.to_string(),
                end: 101,
                start: None,
                offset: 0,
            }
            .url(BASE),
            vec![activity],
        ),
        (
            position_url(wallet, PositionPartition::NotRedeemable),
            vec![positions.clone(), positions],
        ),
        (
            position_url(wallet, PositionPartition::Redeemable),
            vec![b"[]".to_vec(), b"[]".to_vec()],
        ),
    ]);
    let clock = Arc::new(Mutex::new(VecDeque::from([100_i64, 99, 101])));
    let validator = CausalPositionValidator::new(
        Arc::new(QueueFetcher::new(responses)),
        BASE,
        "source-generation-test",
    )
    .with_clock(Arc::new(move || {
        clock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pop_front()
            .unwrap()
    }));

    assert!(matches!(
        validator
            .validate_direct(&[wallet], &mut engine, &paper)
            .await,
        Err(CausalPositionError::NonMonotonicActivityBounds { .. })
    ));
    assert!(paper.position_validation(&wallet).unwrap().is_none());
    assert!(!paper.is_wallet_fenced(&wallet).unwrap());
}

#[tokio::test]
async fn later_activity_atomically_invalidates_an_accepted_proof() {
    let wallet = wallet(0x43);
    let (_dir, paper, mut engine) = fresh(&[wallet]);
    validator(stable_responses(&[(wallet, 1, "1.000000")]))
        .validate_direct(&[wallet], &mut engine, &paper)
        .await
        .unwrap();
    assert!(paper.position_validation_current(&wallet).unwrap());

    let mutation = aggregate(activity(wallet, 9, "1.000000", "0xlater", 20), wallet);
    engine
        .commit(vec![mutation], &context(20), zero_basis())
        .unwrap();
    assert!(
        !paper.position_validation_current(&wallet).unwrap(),
        "the activity commit and validation invalidation share one SQLite transaction"
    );
}

#[tokio::test]
async fn boot_bracket_installs_each_stable_authoritative_balance() {
    let first = wallet(0x51);
    let second = wallet(0x52);
    let (_dir, paper, mut engine) = fresh(&[first, second]);
    let accepted = validator(stable_responses(&[
        (first, 1, "1.000000"),
        (second, 2, "9.000000"),
    ]))
    .validate_direct(&[first, second], &mut engine, &paper)
    .await
    .expect("stable authoritative positions install for the whole boot batch");
    assert_eq!(accepted.len(), 2);
    assert_eq!(accepted[0].wallet, first);
    assert!(paper.position_validation(&first).unwrap().is_some());
    assert!(paper.position_validation(&second).unwrap().is_some());
    assert!(!paper.is_wallet_fenced(&second).unwrap());
}

#[tokio::test]
async fn positions_before_activity_retry_then_converge_without_fence() {
    let wallet = wallet(0x61);
    let (_dir, paper, mut engine) = fresh(&[wallet]);
    let positions = serde_json::to_vec(&vec![position(wallet, 1, "1.000000")]).unwrap();
    let unavailable = HashMap::from([
        (
            activity_url(wallet),
            vec![b"[]".to_vec(), b"[]".to_vec(), b"[]".to_vec()],
        ),
        (
            position_url(wallet, PositionPartition::NotRedeemable),
            vec![positions.clone()],
        ),
        (
            position_url(wallet, PositionPartition::Redeemable),
            vec![b"[]".to_vec()],
        ),
    ]);
    assert!(
        validator(unavailable)
            .validate_direct(&[wallet], &mut engine, &paper)
            .await
            .expect("missing activity mapping is a deferred boot outcome")
            .is_empty()
    );
    assert!(!paper.is_wallet_fenced(&wallet).unwrap());

    validator(stable_responses(&[(wallet, 1, "1.000000")]))
        .validate_direct(&[wallet], &mut engine, &paper)
        .await
        .unwrap();
    assert!(paper.position_validation_current(&wallet).unwrap());

    let restarted_ledger = build_leader_ledger(&paper).unwrap();
    let mut restarted = BucketCommitEngine::load(Arc::clone(&paper), restarted_ledger).unwrap();
    validator(stable_responses(&[(wallet, 1, "1.000000")]))
        .validate_direct(&[wallet], &mut restarted, &paper)
        .await
        .unwrap();
    assert!(paper.position_validation_current(&wallet).unwrap());
}

#[tokio::test]
async fn presentation_receipt_raw_hash_and_partition_layout_changes_still_accept() {
    let wallet = wallet(0x62);
    let (_dir, paper, mut engine) = fresh(&[wallet]);
    let activity =
        serde_json::to_vec(&vec![activity(wallet, 1, "1.000000", "0xpresentation", 10)]).unwrap();
    let first = serde_json::to_vec(&vec![position(wallet, 1, "1.000000")]).unwrap();
    let second = serde_json::to_vec(&vec![json!({
        "title": "changed presentation",
        "percentPnl": -999,
        "cashPnl": 123,
        "currentValue": 456,
        "avgPrice": 0.01,
        "negativeRisk": false,
        "outcomeIndex": 0,
        "size": "1",
        "conditionId": condition(1),
        "asset": asset(1),
        "proxyWallet": wallet,
    })])
    .unwrap();
    let responses = HashMap::from([
        (
            activity_url(wallet),
            vec![activity.clone(), activity.clone(), activity],
        ),
        (
            position_url(wallet, PositionPartition::NotRedeemable),
            vec![first, b"[]".to_vec()],
        ),
        (
            position_url(wallet, PositionPartition::Redeemable),
            vec![b"[]".to_vec(), second],
        ),
    ]);

    validator(responses)
        .validate_direct(&[wallet], &mut engine, &paper)
        .await
        .expect("provenance-only changes must not alter semantic equality");
    assert!(paper.position_validation_current(&wallet).unwrap());
}

#[tokio::test]
async fn combo_identity_comes_from_activity_and_is_excluded_from_ordinary_comparison() {
    let wallet = wallet(0x63);
    let (_dir, paper, mut engine) = fresh(&[wallet]);
    let mut combo = activity(wallet, 1, "1.000000", "0xcombo", 10);
    combo["isCombo"] = json!(true);
    let activity = serde_json::to_vec(&vec![combo]).unwrap();
    let mut positions = position(wallet, 1, "7.500000");
    positions["negativeRisk"] = json!(false);
    let positions = serde_json::to_vec(&vec![positions]).unwrap();
    let responses = HashMap::from([
        (
            activity_url(wallet),
            vec![activity.clone(), activity.clone(), activity],
        ),
        (
            position_url(wallet, PositionPartition::NotRedeemable),
            vec![positions.clone(), positions],
        ),
        (
            position_url(wallet, PositionPartition::Redeemable),
            vec![b"[]".to_vec(), b"[]".to_vec()],
        ),
    ]);

    validator(responses)
        .validate_direct(&[wallet], &mut engine, &paper)
        .await
        .expect("negativeRisk must not turn an activity-proven combo into ordinary inventory");
    assert!(paper.position_validation_current(&wallet).unwrap());
}

#[tokio::test]
async fn exact_zero_ordinary_balance_is_omitted_without_resolution_filtering() {
    let wallet = wallet(0x64);
    let (_dir, paper, mut engine) = fresh(&[wallet]);
    let buy = activity(wallet, 1, "1.000000", "0xbuy", 10);
    let mut sell = activity(wallet, 1, "1.000000", "0xsell", 20);
    sell["side"] = json!("SELL");
    let activity = serde_json::to_vec(&vec![sell, buy]).unwrap();
    let positions = serde_json::to_vec(&vec![position(wallet, 1, "0.000000")]).unwrap();
    let responses = HashMap::from([
        (
            activity_url(wallet),
            vec![activity.clone(), activity.clone(), activity],
        ),
        (
            position_url(wallet, PositionPartition::NotRedeemable),
            vec![positions.clone(), positions],
        ),
        (
            position_url(wallet, PositionPartition::Redeemable),
            vec![b"[]".to_vec(), b"[]".to_vec()],
        ),
    ]);

    validator(responses)
        .validate_direct(&[wallet], &mut engine, &paper)
        .await
        .expect("exact zero must be omitted from the balance comparison");
    assert!(paper.position_validation_current(&wallet).unwrap());
}

#[tokio::test]
async fn serialized_admission_preparer_runs_the_bracket_before_acknowledgement() {
    let wallet = wallet(0x65);
    let (_dir, paper, mut engine) = fresh(&[wallet]);
    let (control_tx, mut control_rx) = mpsc::channel(2);
    let actor_paper = Arc::clone(&paper);
    let actor = tokio::spawn(async move {
        while let Some(message) = control_rx.recv().await {
            match message {
                OrchestratorControl::PrepareAdmissions { acknowledged, .. } => {
                    let _ = acknowledged.send(());
                }
                OrchestratorControl::InstallAnchors {
                    installs,
                    acknowledged,
                } => {
                    let result = engine
                        .install_anchors(&installs)
                        .map_err(|error| error.to_string());
                    let _ = acknowledged.send(result);
                }
                OrchestratorControl::CaptureAdmissionLedger { wallet, captured } => {
                    let _ = captured.send(
                        ledger_capture(engine.ledger(), &actor_paper, wallet)
                            .map_err(|error| error.to_string()),
                    );
                }
                OrchestratorControl::CommitActivityBucket {
                    aggregates,
                    context,
                    committed,
                } => {
                    let _ = committed.send(
                        engine
                            .commit(
                                aggregates,
                                &context,
                                pe_service::bucket_commit::FrozenDecisionBasis {
                                    win_rate_p: pe_core_types::Probability::ZERO,
                                    bankroll: rust_decimal::Decimal::ZERO,
                                },
                            )
                            .map_err(|error| error.to_string()),
                    );
                }
            }
        }
    });
    let preparer = AdmissionPreparer::with_validator(
        control_tx,
        Arc::clone(&paper),
        validator(stable_responses(&[(wallet, 1, "1.000000")])),
    );

    preparer.prepare(&[wallet]).await.unwrap();
    assert!(paper.position_validation_current(&wallet).unwrap());
    drop(preparer);
    actor.await.unwrap();
}

#[test]
fn anchor_install_is_durable_before_swap_and_rejects_a_regressed_cutoff() {
    let wallet = wallet(0x66);
    let (_dir, paper, mut engine) = fresh(&[wallet]);
    paper.set_cursor(&wallet, 10).unwrap();
    let before = ledger_capture(engine.ledger(), &paper, wallet).unwrap();
    let balance = (
        MarketId(VenueMarketId(condition(1))),
        OutcomeId(0),
        ShareAmount::from_atomic(3_000_000),
    );
    let mut install = AnchorInstall {
        wallet,
        balances: vec![balance.clone()],
        cutoff: 10,
        proof: AnchorProof {
            positions_proof_hash: "positions".to_owned(),
            activity_bounds_json: "[]".to_owned(),
            source_log_generation: "scenario".to_owned(),
            document: "{".to_owned(),
            recorded_at_unix: 100,
        },
        expected: AnchorExpectation {
            ledger_hash: before.hash.clone(),
            cursor: before.cursor,
            anchor_seq: before.anchor_seq,
            coverage_generation: before.coverage_generation,
        },
    };

    assert!(engine.install_anchors(&[install.clone()]).is_err());
    assert_eq!(
        ledger_capture(engine.ledger(), &paper, wallet)
            .unwrap()
            .hash,
        before.hash
    );
    assert!(paper.position_anchors(&wallet).unwrap().is_empty());
    assert!(paper.leader_positions().unwrap().is_empty());
    assert!(paper.position_validation(&wallet).unwrap().is_none());

    install.proof.document = "{}".to_owned();
    engine.install_anchors(&[install]).unwrap();
    let installed = ledger_capture(engine.ledger(), &paper, wallet).unwrap();
    assert_eq!(
        installed
            .positive_ordinary_balances
            .get(&(condition(1), 0))
            .copied(),
        Some(balance.2)
    );
    let durable_before_regression = (
        paper.position_anchors(&wallet).unwrap(),
        paper.leader_positions().unwrap(),
        paper.position_validation(&wallet).unwrap(),
        paper.wallet_coverage(&wallet).unwrap(),
    );
    let regressed = AnchorInstall {
        wallet,
        balances: Vec::new(),
        cutoff: 9,
        proof: AnchorProof {
            positions_proof_hash: "regressed".to_owned(),
            activity_bounds_json: "[]".to_owned(),
            source_log_generation: "scenario".to_owned(),
            document: "{}".to_owned(),
            recorded_at_unix: 101,
        },
        expected: AnchorExpectation {
            ledger_hash: installed.hash.clone(),
            cursor: installed.cursor,
            anchor_seq: installed.anchor_seq,
            coverage_generation: installed.coverage_generation,
        },
    };
    assert!(matches!(
        engine.install_anchors(&[regressed]),
        Err(pe_service::bucket_commit::AnchorInstallError::CutoffRegression { .. })
    ));
    assert_eq!(
        ledger_capture(engine.ledger(), &paper, wallet)
            .unwrap()
            .hash,
        installed.hash
    );
    assert_eq!(
        (
            paper.position_anchors(&wallet).unwrap(),
            paper.leader_positions().unwrap(),
            paper.position_validation(&wallet).unwrap(),
            paper.wallet_coverage(&wallet).unwrap(),
        ),
        durable_before_regression
    );
}

#[test]
fn covered_late_generation_change_rejects_an_otherwise_unchanged_anchor() {
    let wallet = wallet(0x67);
    let (_dir, paper, mut engine) = fresh(&[wallet]);
    install_empty_anchor(&mut engine, &paper, wallet, 100);
    let captured = ledger_capture(engine.ledger(), &paper, wallet).unwrap();
    let candidate = AnchorInstall {
        wallet,
        balances: Vec::new(),
        cutoff: 100,
        proof: AnchorProof {
            positions_proof_hash: "candidate".to_owned(),
            activity_bounds_json: "[]".to_owned(),
            source_log_generation: "scenario".to_owned(),
            document: "{}".to_owned(),
            recorded_at_unix: 101,
        },
        expected: AnchorExpectation {
            ledger_hash: captured.hash.clone(),
            cursor: captured.cursor,
            anchor_seq: captured.anchor_seq,
            coverage_generation: captured.coverage_generation,
        },
    };
    engine
        .commit(
            vec![aggregate(
                activity(wallet, 2, "1.000000", "0xcovered-late", 90),
                wallet,
            )],
            &context(101),
            zero_basis(),
        )
        .unwrap();

    assert!(matches!(
        engine.install_anchors(&[candidate]),
        Err(pe_service::bucket_commit::AnchorInstallError::CoverageGenerationChanged { .. })
    ));
    let after = ledger_capture(engine.ledger(), &paper, wallet).unwrap();
    assert_eq!(after.hash, captured.hash);
    assert_eq!(after.cursor, captured.cursor);
    assert_eq!(paper.position_anchors(&wallet).unwrap().len(), 1);
    assert!(paper.position_validation(&wallet).unwrap().is_none());
    assert!(paper.wallet_coverage(&wallet).unwrap().reanchor_required);
}

#[test]
fn deferred_position_predicate_is_exact() {
    let wallet = wallet(0x68);
    let retryable = vec![
        CausalPositionError::PositionRevision { wallet },
        CausalPositionError::InterveningActivity { wallet },
        CausalPositionError::Activity {
            wallet,
            source: ActivityReadError::SaturatedTerminalSecond {
                end: 100,
                offset: 3_000,
            },
        },
        CausalPositionError::Activity {
            wallet,
            source: ActivityReadError::Fetch {
                url: "activity".to_owned(),
                source: SourceError::Transient {
                    message: "retry".to_owned(),
                },
            },
        },
        CausalPositionError::Activity {
            wallet,
            source: ActivityReadError::Fetch {
                url: "activity".to_owned(),
                source: SourceError::RateLimited {
                    retry_after_secs: 1,
                },
            },
        },
        CausalPositionError::Positions {
            wallet,
            source: PositionReadError::Fetch {
                url: "positions".to_owned(),
                source: SourceError::Transient {
                    message: "retry".to_owned(),
                },
            },
        },
        CausalPositionError::Positions {
            wallet,
            source: PositionReadError::Fetch {
                url: "positions".to_owned(),
                source: SourceError::RateLimited {
                    retry_after_secs: 1,
                },
            },
        },
        CausalPositionError::Positions {
            wallet,
            source: PositionReadError::MissingActivityMapping {
                asset: "missing".to_owned(),
            },
        },
        CausalPositionError::Positions {
            wallet,
            source: PositionReadError::ConflictingActivityMapping {
                asset: "activity-conflict".to_owned(),
            },
        },
        CausalPositionError::Positions {
            wallet,
            source: PositionReadError::ConflictingOutcomeMapping {
                condition_id: "condition".to_owned(),
                outcome: 0,
            },
        },
        CausalPositionError::Positions {
            wallet,
            source: PositionReadError::PositionMappingConflict {
                asset: "position-conflict".to_owned(),
            },
        },
        CausalPositionError::Positions {
            wallet,
            source: PositionReadError::DuplicateAsset {
                asset: "duplicate".to_owned(),
            },
        },
        CausalPositionError::Positions {
            wallet,
            source: PositionReadError::SaturatedTerminalPage {
                partition: PositionPartition::NotRedeemable,
                offset: 3_000,
            },
        },
    ];
    assert!(retryable.iter().all(is_deferred_causal_position_error));

    let fatal = [
        CausalPositionError::Activity {
            wallet,
            source: ActivityReadError::Fetch {
                url: "activity".to_owned(),
                source: SourceError::Fatal {
                    message: "fatal".to_owned(),
                },
            },
        },
        CausalPositionError::Positions {
            wallet,
            source: PositionReadError::Fetch {
                url: "positions".to_owned(),
                source: SourceError::Fatal {
                    message: "fatal".to_owned(),
                },
            },
        },
        CausalPositionError::LedgerRevision { wallet },
    ];
    assert!(
        fatal
            .iter()
            .all(|error| !is_deferred_causal_position_error(error))
    );
}

#[tokio::test]
async fn parse_and_fatal_fetch_errors_remain_boot_fatal() {
    for (suffix, payload) in [(0x69, Some(b"{".to_vec())), (0x6a, None)] {
        let wallet = wallet(suffix);
        let (_dir, paper, mut engine) = fresh(&[wallet]);
        let responses = payload.map_or_else(HashMap::new, |payload| {
            HashMap::from([(activity_url(wallet), vec![payload])])
        });
        let error = validator(responses)
            .validate_direct(&[wallet], &mut engine, &paper)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            CausalPositionError::Activity {
                source: ActivityReadError::Parse(_)
                    | ActivityReadError::Fetch {
                        source: SourceError::Fatal { .. },
                        ..
                    },
                ..
            }
        ));
        assert!(!paper.is_wallet_fenced(&wallet).unwrap());
        assert!(paper.position_anchors(&wallet).unwrap().is_empty());
    }
}

#[tokio::test]
async fn multi_wallet_prepare_preserves_typed_deferral_and_installs_none() {
    let healthy = wallet(0x6b);
    let deferred = wallet(0x6c);
    let (_dir, paper, engine) = fresh(&[healthy, deferred]);
    let mut responses = stable_responses(&[(healthy, 1, "1.000000")]);
    responses.insert(
        activity_url(deferred),
        vec![b"[]".to_vec(), b"[]".to_vec(), b"[]".to_vec()],
    );
    responses.insert(
        position_url(deferred, PositionPartition::NotRedeemable),
        vec![serde_json::to_vec(&vec![position(deferred, 2, "1")]).unwrap()],
    );
    responses.insert(
        position_url(deferred, PositionPartition::Redeemable),
        vec![b"[]".to_vec()],
    );
    let (control_tx, control_rx) = mpsc::channel(2);
    let actor = spawn_control_actor(control_rx, engine, Arc::clone(&paper));
    let preparer =
        AdmissionPreparer::with_validator(control_tx, Arc::clone(&paper), validator(responses));
    let error = preparer.prepare(&[healthy, deferred]).await.unwrap_err();
    assert!(matches!(
        error,
        AdmissionError::PositionValidation(CausalPositionError::Positions {
            source: PositionReadError::MissingActivityMapping { .. },
            ..
        })
    ));
    assert!(paper.position_anchors(&healthy).unwrap().is_empty());
    assert!(paper.position_anchors(&deferred).unwrap().is_empty());
    drop(preparer);
    actor.await.unwrap();
}

#[tokio::test]
async fn boot_bracket_quarantines_a_newly_fenced_wallet_and_accepts_the_rest() {
    // Live shape from the #544 activation rehearsal: one wallet's visible
    // activity cannot reconstruct its holdings (position_underflow), the
    // commit fences it durably, and the boot bracket must continue with the
    // remaining universe instead of aborting the one-shot activation.
    let fenced_wallet = wallet(0x71);
    let healthy = wallet(0x72);
    let (_dir, paper, mut engine) = fresh(&[fenced_wallet, healthy]);
    install_empty_anchor(&mut engine, &paper, fenced_wallet, 0);
    install_empty_anchor(&mut engine, &paper, healthy, 0);
    let mut responses = stable_responses(&[(healthy, 2, "1.000000")]);
    let mut underflow_redeem = activity(fenced_wallet, 1, "2.000000", "0xunderflow", 10);
    underflow_redeem["type"] = json!("REDEEM");
    responses.insert(
        activity_url(fenced_wallet),
        vec![serde_json::to_vec(&vec![underflow_redeem]).unwrap()],
    );

    let accepted = validator(responses)
        .validate_direct(&[fenced_wallet, healthy], &mut engine, &paper)
        .await
        .expect("a per-wallet fence must not abort the boot bracket");

    assert_eq!(accepted.len(), 1);
    assert_eq!(accepted[0].wallet, healthy);
    assert!(paper.is_wallet_fenced(&fenced_wallet).unwrap());
    assert!(paper.position_validation(&fenced_wallet).unwrap().is_none());
    assert!(paper.position_validation(&healthy).unwrap().is_some());
}
