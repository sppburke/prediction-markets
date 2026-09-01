#![cfg(feature = "scenario")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use pe_core_types::{ReceivedAt, ReconstructionQuality, SourceId, SourceTimestamp, WalletAddress};
use pe_paper_state::{PaperStateDb, WalletHistoryStatusRecord};
use pe_position_ledger::PositionLedger;
use pe_service::bucket_commit::{BucketCommitEngine, BucketDecisionContext};
use pe_service::orchestrator_control::OrchestratorControl;
use pe_service::paper_recovery::build_leader_ledger;
use pe_service::position_seeder::{CausalPositionError, CausalPositionValidator, ledger_capture};
use pe_service::watchlist_admission::AdmissionPreparer;
use pe_source_core::SourceError;
use pe_source_polymarket_public::{
    ActivityParseContext, ActivityTransport, PageFetcher, PolymarketEndpoint, PositionPartition,
    aggregate_activity_rows, parse_activity_response,
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

#[tokio::test]
async fn stable_complete_bracket_installs_and_restart_is_identical() {
    let wallet = wallet(0x11);
    let (_dir, paper, mut engine) = fresh(&[wallet]);
    let first = validator(stable_responses(&[(wallet, 1, "1.000000")]))
        .validate_direct(&[wallet], &mut engine, &paper)
        .await
        .unwrap();
    let durable = paper.position_validation(&wallet).unwrap().unwrap();
    assert_eq!(durable.ledger_hash, first[0].validation.ledger_hash);
    assert_eq!(durable.source_log_generation, "source-generation-test");

    let restarted_ledger = build_leader_ledger(&paper).unwrap();
    let mut restarted = BucketCommitEngine::load(Arc::clone(&paper), restarted_ledger).unwrap();
    let second = validator(stable_responses(&[(wallet, 1, "1")]))
        .validate_direct(&[wallet], &mut restarted, &paper)
        .await
        .unwrap();
    assert_eq!(
        first[0].validation.ledger_hash,
        second[0].validation.ledger_hash
    );
    assert_eq!(
        first[0].validation.positions_proof_hash,
        second[0].validation.positions_proof_hash
    );
}

#[tokio::test]
async fn mutation_between_each_bracket_step_installs_nothing() {
    for target_step in 1..=4 {
        let wallet = wallet(u8::try_from(0x20 + target_step).unwrap());
        let (_dir, paper, mut engine) = fresh(&[wallet]);
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
                engine.commit(vec![mutation.clone()], &context(20)).unwrap();
            }
        });
        let error = validator(stable_responses(&[(wallet, 1, "1.000000")]))
            .with_step_hook(hook)
            .validate_direct(&[wallet], &mut engine, &paper)
            .await
            .expect_err("every intervening mutation must reject the bracket");
        assert!(matches!(
            error,
            CausalPositionError::LedgerRevision { .. } | CausalPositionError::StableMismatch { .. }
        ));
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
    let error = validator(responses)
        .validate_direct(&[wallet], &mut engine, &paper)
        .await
        .expect_err("a changed applied revision must fence");
    assert!(matches!(error, CausalPositionError::Fenced { .. }));
    assert!(paper.is_wallet_fenced(&wallet).unwrap());
    assert!(paper.position_validation(&wallet).unwrap().is_none());
}

#[tokio::test]
async fn stable_unexplained_mismatch_retries_without_fencing() {
    let wallet = wallet(0x41);
    let (_dir, paper, mut engine) = fresh(&[wallet]);
    let error = validator(stable_responses(&[(wallet, 1, "2.000000")]))
        .validate_direct(&[wallet], &mut engine, &paper)
        .await
        .expect_err("a stable unexplained mismatch stays unavailable");
    assert!(matches!(error, CausalPositionError::StableMismatch { .. }));
    assert!(!paper.is_wallet_fenced(&wallet).unwrap());
    assert!(paper.position_validation(&wallet).unwrap().is_none());

    let second = validator(stable_responses(&[(wallet, 1, "2.000000")]))
        .validate_direct(&[wallet], &mut engine, &paper)
        .await
        .expect_err("a never-converging mismatch remains unavailable on retry");
    assert!(matches!(second, CausalPositionError::StableMismatch { .. }));
    assert!(!paper.is_wallet_fenced(&wallet).unwrap());
    assert!(paper.position_validation(&wallet).unwrap().is_none());
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
    let error = validator(responses)
        .validate_direct(&[wallet], &mut engine, &paper)
        .await
        .expect_err("changed position semantics must retry");
    assert!(matches!(
        error,
        CausalPositionError::PositionRevision { .. }
    ));
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
    engine.commit(vec![mutation], &context(20)).unwrap();
    assert!(
        !paper.position_validation_current(&wallet).unwrap(),
        "the activity commit and validation invalidation share one SQLite transaction"
    );
}

#[tokio::test]
async fn multi_wallet_attempt_installs_no_partial_acceptance() {
    let first = wallet(0x51);
    let second = wallet(0x52);
    let (_dir, paper, mut engine) = fresh(&[first, second]);
    let error = validator(stable_responses(&[
        (first, 1, "1.000000"),
        (second, 2, "9.000000"),
    ]))
    .validate_direct(&[first, second], &mut engine, &paper)
    .await
    .expect_err("one unavailable wallet rejects the attempted generation");
    assert!(matches!(error, CausalPositionError::StableMismatch { .. }));
    assert!(paper.position_validation(&first).unwrap().is_none());
    assert!(paper.position_validation(&second).unwrap().is_none());
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
    assert!(matches!(
        validator(unavailable)
            .validate_direct(&[wallet], &mut engine, &paper)
            .await,
        Err(CausalPositionError::Positions { .. })
    ));
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
                OrchestratorControl::PrepareValidatedAdmissions {
                    validations,
                    acknowledged,
                } => {
                    let result = validations
                        .iter()
                        .try_for_each(|validation| {
                            let capture = ledger_capture(engine.ledger(), validation.wallet)
                                .map_err(|error| error.to_string())?;
                            if capture.hash == validation.ledger_hash {
                                Ok(())
                            } else {
                                Err("ledger changed".to_owned())
                            }
                        })
                        .and_then(|()| {
                            actor_paper
                                .record_position_validations(&validations)
                                .map_err(|error| error.to_string())
                        });
                    let _ = acknowledged.send(result);
                }
                OrchestratorControl::CaptureAdmissionLedger { wallet, captured } => {
                    let _ = captured.send(
                        ledger_capture(engine.ledger(), wallet).map_err(|error| error.to_string()),
                    );
                }
                OrchestratorControl::CommitActivityBucket {
                    aggregates,
                    context,
                    committed,
                } => {
                    let _ = committed.send(
                        engine
                            .commit(aggregates, &context)
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
