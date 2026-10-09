#![cfg(feature = "scenario")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "support/golden.rs"]
mod golden;

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use pe_copy_signal_engine::PositionState;
use pe_core_types::{
    MarketId, MarketOutcomeId, OutcomeId, ReceivedAt, ReconstructionQuality, ShareAmount, SourceId,
    SourceTimestamp, VenueMarketId, WalletAddress,
};
use pe_event_log::Reader;
use pe_paper_state::{AnchorInstallRecord, PaperStateDb, WalletHistoryStatusRecord};
use pe_position_ledger::PositionLedger;
use pe_service::asset_identity::AssetIdentityResolver;
use pe_service::bucket_commit::{BucketCommitEngine, BucketDecisionContext};
use pe_service::orchestrator_control::OrchestratorControl;
use pe_service::paper_recovery::{build_leader_ledger, replay_wallet_ledger};
use pe_service::position_seeder::{
    AnchorExpectation, AnchorInstall, AnchorProof, CausalPositionError, CausalPositionValidator,
    anchor_proves_full_history, is_deferred_causal_position_error, ledger_capture,
};
use pe_service::source_event_sink::SourceEventSink;
use pe_service::watchlist_admission::{AdmissionError, AdmissionPreparer, AnchorRefreshOutcome};
use pe_source_core::SourceError;
use pe_source_polymarket_public::{
    ActivityIdentityError, ActivityParseContext, ActivityParseError, ActivityReadError,
    ActivityTransport, ActivityValidationError, ActivityWindowInvalidation, GAMMA_BATCH_SIZE,
    GAMMA_MARKETS_SOURCE_ID, PageFetcher, PolymarketEndpoint, PositionPartition, PositionReadError,
    ReconciliationFetcher, aggregate_activity_rows, parse_activity_response,
};
use serde_json::{Value, json};
use tempfile::TempDir;
use time::OffsetDateTime;
use tokio::sync::mpsc;

const BASE: &str = "https://api.example.com";
const END: i64 = 100;

struct QueueFetcher {
    responses: Mutex<HashMap<String, VecDeque<Vec<u8>>>>,
    urls: Mutex<Vec<String>>,
    gamma_response: Option<Vec<u8>>,
}

impl QueueFetcher {
    fn new(responses: HashMap<String, Vec<Vec<u8>>>) -> Self {
        Self::with_gamma(responses, None)
    }

    fn with_gamma(
        responses: HashMap<String, Vec<Vec<u8>>>,
        gamma_response: Option<Vec<u8>>,
    ) -> Self {
        Self {
            responses: Mutex::new(
                responses
                    .into_iter()
                    .map(|(url, responses)| (url, responses.into()))
                    .collect(),
            ),
            urls: Mutex::new(Vec::new()),
            gamma_response,
        }
    }

    fn urls(&self) -> Vec<String> {
        self.urls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl PageFetcher for QueueFetcher {
    async fn fetch_page(&self, url: &str) -> Result<Vec<u8>, SourceError> {
        self.urls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(url.to_owned());
        if url.contains("/markets?clob_token_ids=") {
            if let Some(response) = &self.gamma_response {
                return Ok(response.clone());
            }
            let markets = url
                .split('?')
                .nth(1)
                .into_iter()
                .flat_map(|query| query.split('&'))
                .filter_map(|part| part.strip_prefix("clob_token_ids="))
                .filter_map(|token| {
                    let byte = token.strip_prefix("asset-")?;
                    u8::from_str_radix(byte, 16).ok().map(|byte| {
                        json!({
                            "conditionId": condition(byte),
                            "clobTokenIds": [token],
                            "closed": url.contains("closed=true")
                        })
                    })
                })
                .collect::<Vec<_>>();
            return serde_json::to_vec(&markets).map_err(|error| SourceError::Fatal {
                message: error.to_string(),
            });
        }
        if url.contains("/activity?") && !url.contains("start=") {
            return Ok(b"[]".to_vec());
        }
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

struct GatedFetcher {
    inner: QueueFetcher,
    wallets: Vec<WalletAddress>,
    first_activity: Mutex<HashSet<WalletAddress>>,
    barrier: tokio::sync::Barrier,
    yields: HashMap<WalletAddress, usize>,
}

impl GatedFetcher {
    fn new(
        responses: HashMap<String, Vec<Vec<u8>>>,
        wallets: Vec<WalletAddress>,
        yields: HashMap<WalletAddress, usize>,
        gamma_response: Option<Vec<u8>>,
    ) -> Self {
        Self {
            inner: QueueFetcher::with_gamma(responses, gamma_response),
            barrier: tokio::sync::Barrier::new(wallets.len()),
            wallets,
            first_activity: Mutex::new(HashSet::new()),
            yields,
        }
    }
}

impl PageFetcher for GatedFetcher {
    async fn fetch_page(&self, url: &str) -> Result<Vec<u8>, SourceError> {
        let wallet = self
            .wallets
            .iter()
            .copied()
            .find(|wallet| url.contains(&wallet.to_string()));
        let gate = wallet.is_some_and(|wallet| {
            url.contains("/activity?")
                && self
                    .first_activity
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(wallet)
        });
        if gate {
            self.barrier.wait().await;
            if let Some(wallet) = wallet {
                for _ in 0..self.yields.get(&wallet).copied().unwrap_or_default() {
                    tokio::task::yield_now().await;
                }
            }
        }
        self.inner.fetch_page(url).await
    }
}

struct OrderedBracketFetcher {
    inner: QueueFetcher,
    wallets: [WalletAddress; 2],
    preferred: WalletAddress,
    activity_calls: Mutex<HashMap<WalletAddress, usize>>,
    first_activity: tokio::sync::Barrier,
    preferred_first_commit: Arc<tokio::sync::Semaphore>,
    other_first_commit: Arc<tokio::sync::Semaphore>,
    preferred_completion: Arc<tokio::sync::Semaphore>,
}

impl PageFetcher for OrderedBracketFetcher {
    async fn fetch_page(&self, url: &str) -> Result<Vec<u8>, SourceError> {
        let wallet = self
            .wallets
            .iter()
            .copied()
            .find(|wallet| url.contains(&wallet.to_string()));
        if url.contains("/activity?")
            && let Some(wallet) = wallet
        {
            let call = {
                let mut calls = self
                    .activity_calls
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let call = calls.entry(wallet).or_default();
                *call += 1;
                *call
            };
            match (wallet == self.preferred, call) {
                (preferred, 1) => {
                    self.first_activity.wait().await;
                    if !preferred {
                        self.preferred_first_commit
                            .acquire()
                            .await
                            .map_err(|error| SourceError::Fatal {
                                message: error.to_string(),
                            })?
                            .forget();
                    }
                }
                (true, 2) => {
                    self.other_first_commit
                        .acquire()
                        .await
                        .map_err(|error| SourceError::Fatal {
                            message: error.to_string(),
                        })?
                        .forget();
                }
                (false, 3) => {
                    self.preferred_completion
                        .acquire()
                        .await
                        .map_err(|error| SourceError::Fatal {
                            message: error.to_string(),
                        })?
                        .forget();
                }
                _ => {}
            }
        }
        self.inner.fetch_page(url).await
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

fn split_merge_activity(wallet: WalletAddress, byte: u8, combo: bool) -> Vec<Value> {
    [("MERGE", "1", 20), ("SPLIT", "2", 10)]
        .into_iter()
        .map(|(kind, size, epoch)| {
            json!({
                "proxyWallet": wallet,
                "timestamp": epoch,
                "conditionId": condition(byte),
                "type": kind,
                "size": size,
                "usdcSize": size,
                "transactionHash": format!("0x{kind}"),
                "price": "0",
                "isCombo": combo
            })
        })
        .collect()
}

fn condition_metadata(byte: u8, closed: bool) -> Vec<u8> {
    serde_json::to_vec(&json!([{
        "conditionId": condition(byte),
        "clobTokenIds": [asset(byte), format!("{}-no", asset(byte))],
        "closed": closed
    }]))
    .unwrap()
}

fn condition_url(byte: u8, closed: bool) -> String {
    let filter = if closed { "&closed=true" } else { "" };
    format!(
        "{BASE}/markets?condition_ids={}{filter}&limit=500",
        condition(byte)
    )
}

fn bracket_failure(
    result: Result<pe_service::position_seeder::DirectValidationOutcome, CausalPositionError>,
) -> CausalPositionError {
    match result {
        Err(error) => error,
        Ok(mut outcome) => {
            assert!(outcome.accepted.is_empty());
            assert_eq!(outcome.deferred.len(), 1);
            outcome.deferred.pop().unwrap().1
        }
    }
}

/// PASS: SPLIT/MERGE-only history discovers both outcomes and accepts either position, including closed markets.
/// FAIL: discovery is absent from metadata_reads, skips recording, or omits the closed fallback.
#[tokio::test]
async fn split_merge_only_positions_accept_both_outcomes_with_recorded_discovery() {
    for closed in [false, true] {
        for outcome in [0, 1] {
            let wallet = wallet(0xa1);
            let (_dir, paper, mut engine) = fresh(&[]);
            let mut positions = position(wallet, 1, "1");
            positions["outcomeIndex"] = json!(outcome);
            positions["asset"] = if outcome == 0 {
                json!(asset(1))
            } else {
                json!(format!("{}-no", asset(1)))
            };
            let metadata = condition_metadata(1, closed);
            let mut responses = HashMap::from([
                (
                    activity_url(wallet),
                    vec![serde_json::to_vec(&split_merge_activity(wallet, 1, false)).unwrap(); 3],
                ),
                (
                    position_url(wallet, PositionPartition::NotRedeemable),
                    vec![serde_json::to_vec(&vec![positions]).unwrap(); 2],
                ),
                (
                    position_url(wallet, PositionPartition::Redeemable),
                    vec![b"[]".to_vec(); 2],
                ),
                (
                    condition_url(1, false),
                    vec![
                        if closed {
                            b"[]".to_vec()
                        } else {
                            metadata.clone()
                        };
                        3
                    ],
                ),
            ]);
            if closed {
                responses.insert(condition_url(1, true), vec![metadata.clone(); 3]);
            }
            let fetcher = Arc::new(QueueFetcher::new(responses));
            let (_log_dir, source_path, validator) = recording_validator(fetcher.clone());
            let installs = validator
                .validate_direct(&[wallet], &mut engine, &paper)
                .await
                .unwrap();
            assert_eq!(installs.len(), 1);
            assert_eq!(
                installs[0].balances,
                vec![(
                    MarketId(VenueMarketId(condition(1))),
                    OutcomeId(outcome),
                    ShareAmount::from_atomic(1_000_000)
                )]
            );
            let proof: Value = serde_json::from_str(&installs[0].proof.document).unwrap();
            let reads = proof["metadata_reads"].as_array().unwrap();
            assert_eq!(
                reads.len(),
                2,
                "both discovered outcomes carry metadata provenance"
            );
            let entries = Reader::replay(&source_path)
                .unwrap()
                .map(Result::unwrap)
                .collect::<Vec<_>>();
            for read in reads {
                let sequence = read["source_log_sequence"].as_u64().unwrap();
                let (_, envelope) = entries.iter().find(|(seq, _)| seq.0 == sequence).unwrap();
                assert_eq!(envelope.source_id.0, GAMMA_MARKETS_SOURCE_ID);
                assert_eq!(envelope.payload, metadata);
                assert_eq!(
                    read["canonical_page_hash"],
                    pe_source_polymarket_public::canonical_page_hash(&envelope.payload).unwrap()
                );
            }
            let urls = fetcher.urls();
            let lookups = urls
                .iter()
                .filter(|url| url.contains("condition_ids="))
                .collect::<Vec<_>>();
            assert_eq!(lookups.len(), if closed { 6 } else { 3 });
            if closed {
                for pair in lookups.chunks(2) {
                    assert_eq!(pair[0], &condition_url(1, false));
                    assert_eq!(pair[1], &condition_url(1, true));
                }
            }
            assert!(!urls.iter().any(|url| url.contains("clob_token_ids=")));
            assert_eq!(paper.position_anchors(&wallet).unwrap().len(), 1);
        }
    }
}

/// PASS: activity on another SPLIT/MERGE market does not map a position-only market.
/// FAIL: discovering one market guesses identities for a market with no wallet activity.
#[tokio::test]
async fn split_merge_discovery_keeps_no_activity_market_wallet_persistent() {
    let wallet = wallet(0xa2);
    let (_dir, paper, mut engine) = fresh(&[]);
    let responses = HashMap::from([
        (
            activity_url(wallet),
            vec![serde_json::to_vec(&split_merge_activity(wallet, 1, false)).unwrap(); 3],
        ),
        (
            position_url(wallet, PositionPartition::NotRedeemable),
            vec![serde_json::to_vec(&vec![position(wallet, 2, "1")]).unwrap()],
        ),
        (
            position_url(wallet, PositionPartition::Redeemable),
            vec![b"[]".to_vec()],
        ),
        (
            condition_url(1, false),
            vec![condition_metadata(1, false); 3],
        ),
    ]);
    let fetcher = Arc::new(QueueFetcher::new(responses));
    let error = validator_from_fetcher(fetcher.clone())
        .validate_direct_with_deferrals(&[wallet], &mut engine, &paper)
        .await;
    let error = bracket_failure(error);
    assert!(
        matches!(&error, CausalPositionError::Positions { source: PositionReadError::MissingActivityMapping { asset: missing }, .. } if missing == &asset(2))
    );
    assert_eq!(
        error.class(),
        pe_service::position_seeder::FailureClass::WalletPersistent
    );
    assert!(!fetcher.urls().contains(&condition_url(2, false)));
    assert!(paper.position_anchors(&wallet).unwrap().is_empty());
}

/// PASS: conflicting SPLIT/MERGE combo flags leave the condition unresolved and positions deferred.
/// FAIL: metadata manufactures a unanimous classification for conflicting wallet activity.
#[tokio::test]
async fn split_merge_conflicting_combo_flags_do_not_discover_positions() {
    let wallet = wallet(0xa3);
    let (_dir, paper, mut engine) = fresh(&[]);
    let mut rows = split_merge_activity(wallet, 1, false);
    rows[0]["isCombo"] = json!(true);
    let responses = HashMap::from([
        (
            activity_url(wallet),
            vec![serde_json::to_vec(&rows).unwrap(); 3],
        ),
        (
            position_url(wallet, PositionPartition::NotRedeemable),
            vec![serde_json::to_vec(&vec![position(wallet, 1, "1")]).unwrap()],
        ),
        (
            position_url(wallet, PositionPartition::Redeemable),
            vec![b"[]".to_vec()],
        ),
    ]);
    let fetcher = Arc::new(QueueFetcher::new(responses));
    let error = validator_from_fetcher(fetcher.clone())
        .validate_direct_with_deferrals(&[wallet], &mut engine, &paper)
        .await;
    let error = bracket_failure(error);
    assert!(matches!(
        error,
        CausalPositionError::Positions {
            source: PositionReadError::MissingActivityMapping { .. },
            ..
        }
    ));
    assert!(!fetcher.urls().iter().any(|url| url.contains("/markets?")));
    assert!(paper.position_anchors(&wallet).unwrap().is_empty());
}

/// PASS: cache conflicts, durable token rejections, and condition mismatches leave split positions unmapped.
/// FAIL: condition discovery bypasses a rejection or accepts a different market's identity.
#[tokio::test]
async fn split_merge_discovery_rejections_and_condition_mismatch_fail_closed() {
    for case in ["warmed", "warmed_activity", "rejected", "mismatch"] {
        let wallet = wallet(0xa4);
        let (_dir, paper, mut engine) = fresh(&[]);
        let mut discovery = condition_metadata(1, false);
        let seed = if case == "rejected" {
            serde_json::to_vec(&json!([
                {"conditionId": condition(3), "clobTokenIds": [asset(1)]},
                {"conditionId": condition(4), "clobTokenIds": [asset(1)]}
            ]))
            .unwrap()
        } else {
            condition_metadata(1, false)
        };
        if case.starts_with("warmed") {
            discovery = serde_json::to_vec(&json!([{
                "conditionId": condition(1), "clobTokenIds": [format!("{}-no", asset(1)), asset(1)]
            }]))
            .unwrap();
        } else if case == "mismatch" {
            discovery = condition_metadata(2, false);
        }
        let mut rows = split_merge_activity(wallet, 1, false);
        if case == "warmed_activity" {
            rows.push(activity(wallet, 1, "1", "0xtrade", 5));
        }
        let responses = HashMap::from([
            (
                activity_url(wallet),
                vec![serde_json::to_vec(&rows).unwrap(); 3],
            ),
            (
                position_url(wallet, PositionPartition::NotRedeemable),
                vec![serde_json::to_vec(&vec![position(wallet, 1, "1")]).unwrap()],
            ),
            (
                position_url(wallet, PositionPartition::Redeemable),
                vec![b"[]".to_vec()],
            ),
            (condition_url(1, false), vec![discovery; 3]),
            (condition_url(1, true), vec![b"[]".to_vec(); 3]),
        ]);
        let fetcher = Arc::new(QueueFetcher::with_gamma(responses, Some(seed)));
        let log_dir = tempfile::tempdir().unwrap();
        let path = log_dir.path().join("source.log");
        let sink = Arc::new(tokio::sync::Mutex::new(
            SourceEventSink::open(&path).unwrap(),
        ));
        let resolver = Arc::new(
            AssetIdentityResolver::new(fetcher.clone(), BASE.into(), GAMMA_BATCH_SIZE, sink)
                .with_paper_state(
                    paper.clone(),
                    "installed".into(),
                    pe_service::risk_inputs::SourceReceiptIndex::replay(&path).unwrap(),
                ),
        );
        if case != "mismatch" {
            let seeded = resolver
                .resolve_live([pe_core_types::PolymarketTokenId(asset(1))])
                .await
                .unwrap();
            assert_eq!(seeded.verified.is_empty(), case == "rejected");
        }
        let validator =
            CausalPositionValidator::new(fetcher, BASE, "source-generation-test", resolver)
                .with_clock(Arc::new(|| END));
        let error = validator
            .validate_direct_with_deferrals(&[wallet], &mut engine, &paper)
            .await;
        let error = bracket_failure(error);
        if case == "warmed_activity" {
            assert!(
                matches!(
                    error,
                    CausalPositionError::Identity {
                        source: SourceError::Fatal { .. },
                        ..
                    }
                ),
                "{case}: {error}"
            );
        } else {
            assert!(
                matches!(
                    error,
                    CausalPositionError::Positions {
                        source: PositionReadError::MissingActivityMapping { .. },
                        ..
                    }
                ),
                "{case}: {error}"
            );
        }
        assert!(paper.position_anchors(&wallet).unwrap().is_empty());
    }
}

/// PASS: a token first discovered on the next read remains InterveningActivity through the bounded retry.
/// FAIL: a later discovery silently repairs the earlier positions read and installs an anchor.
#[tokio::test]
async fn split_merge_identity_appearing_later_remains_intervening_activity() {
    let wallet = wallet(0xa5);
    let (_dir, paper, mut engine) = fresh(&[]);
    let metadata = condition_metadata(1, false);
    let responses = HashMap::from([
        (
            activity_url(wallet),
            vec![serde_json::to_vec(&split_merge_activity(wallet, 1, false)).unwrap(); 4],
        ),
        (
            position_url(wallet, PositionPartition::NotRedeemable),
            vec![serde_json::to_vec(&vec![position(wallet, 1, "1")]).unwrap(); 2],
        ),
        (
            position_url(wallet, PositionPartition::Redeemable),
            vec![b"[]".to_vec(); 2],
        ),
        (
            condition_url(1, false),
            vec![b"[]".to_vec(), metadata.clone(), b"[]".to_vec(), metadata],
        ),
        (condition_url(1, true), vec![b"[]".to_vec(); 2]),
    ]);
    let error = validator(responses)
        .validate_direct_with_deferrals(&[wallet], &mut engine, &paper)
        .await;
    let error = bracket_failure(error);
    assert!(
        matches!(error, CausalPositionError::InterveningActivity { .. }),
        "{error}"
    );
    assert!(paper.position_anchors(&wallet).unwrap().is_empty());
}

/// PASS: a condition discovery transport failure keeps the existing Identity/Transient classification.
/// FAIL: a source outage becomes a persistent missing-activity failure or an accepted bracket.
#[tokio::test]
async fn split_merge_discovery_source_failure_is_identity_transient() {
    struct UnavailableConditions(QueueFetcher);
    impl PageFetcher for UnavailableConditions {
        async fn fetch_page(&self, url: &str) -> Result<Vec<u8>, SourceError> {
            if url.contains("condition_ids=") {
                return Err(SourceError::Transient {
                    message: "fixture unavailable".into(),
                });
            }
            self.0.fetch_page(url).await
        }
    }
    let wallet = wallet(0xa6);
    let (_dir, paper, mut engine) = fresh(&[]);
    let fetcher = Arc::new(UnavailableConditions(QueueFetcher::new(HashMap::from([(
        activity_url(wallet),
        vec![serde_json::to_vec(&split_merge_activity(wallet, 1, false)).unwrap()],
    )]))));
    let error = validator_from_reconciliation(fetcher)
        .validate_direct_with_deferrals(&[wallet], &mut engine, &paper)
        .await;
    let error = bracket_failure(error);
    assert!(matches!(
        &error,
        CausalPositionError::Identity {
            source: SourceError::Transient { .. },
            ..
        }
    ));
    assert_eq!(
        error.class(),
        pe_service::position_seeder::FailureClass::WalletTransient
    );
    assert!(paper.position_anchors(&wallet).unwrap().is_empty());
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
    activity_url_at(wallet, END)
}

fn assert_full_history_proof(document: &str) {
    assert!(anchor_proves_full_history(document));
    let proof: Value = serde_json::from_str(document).unwrap();
    assert!(proof.get("baseline_walk").is_none());
    let walks = proof["activity_walks"].as_array().unwrap();
    assert_eq!(walks.len(), 3);
    for walk in walks {
        assert!(walk["pages"].as_array().unwrap().iter().any(|page| {
            page["offset"] == 0
                && page["bounds"]["start"] == 0
                && page["bounds"]["end"] == walk["fixed_end"]
                && page["request_url"].as_str().unwrap().ends_with("&start=1")
        }));
    }
}

fn older_market_responses(wallet: WalletAddress, attempts: usize) -> HashMap<String, Vec<Vec<u8>>> {
    let rows = (1..=3)
        .rev()
        .map(|byte| {
            activity(
                wallet,
                byte,
                "1",
                &format!("0xbase{byte}"),
                i64::from(byte) * 10,
            )
        })
        .collect::<Vec<_>>();
    let positions = (1..=3)
        .map(|byte| position(wallet, byte, "1"))
        .collect::<Vec<_>>();
    HashMap::from([
        (
            activity_url(wallet),
            vec![serde_json::to_vec(&rows).unwrap(); 3 * attempts],
        ),
        (
            position_url(wallet, PositionPartition::NotRedeemable),
            vec![serde_json::to_vec(&positions).unwrap(); 2 * attempts],
        ),
        (
            position_url(wallet, PositionPartition::Redeemable),
            vec![b"[]".to_vec(); 2 * attempts],
        ),
    ])
}

fn activity_url_at(wallet: WalletAddress, end: i64) -> String {
    PolymarketEndpoint::UserPositionActivityPage {
        user: wallet.to_string(),
        end,
        start: Some(1),
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

fn stable_responses_for_attempts(
    specs: &[(WalletAddress, u8, &str)],
    attempts: usize,
) -> HashMap<String, Vec<Vec<u8>>> {
    let mut responses = stable_responses(specs);
    for pages in responses.values_mut() {
        let original = pages.clone();
        for _ in 1..attempts {
            pages.extend(original.clone());
        }
    }
    responses
}

fn validator(responses: HashMap<String, Vec<Vec<u8>>>) -> CausalPositionValidator {
    validator_from_fetcher(Arc::new(QueueFetcher::new(responses)))
}

fn validator_from_fetcher(fetcher: Arc<QueueFetcher>) -> CausalPositionValidator {
    let fetcher: Arc<dyn ReconciliationFetcher> = fetcher;
    validator_from_reconciliation(fetcher)
}

fn validator_from_reconciliation(
    fetcher: Arc<dyn ReconciliationFetcher>,
) -> CausalPositionValidator {
    let dir = tempfile::tempdir().unwrap();
    let source_log = Arc::new(tokio::sync::Mutex::new(
        SourceEventSink::open(dir.path().join("source.log")).unwrap(),
    ));
    let asset_identity = Arc::new(AssetIdentityResolver::new(
        Arc::clone(&fetcher),
        BASE.to_owned(),
        GAMMA_BATCH_SIZE,
        source_log,
    ));
    CausalPositionValidator::new(fetcher, BASE, "source-generation-test", asset_identity)
        .with_clock(Arc::new(|| END))
}

fn recording_validator(
    fetcher: Arc<dyn ReconciliationFetcher>,
) -> (TempDir, std::path::PathBuf, CausalPositionValidator) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("source.log");
    let sink = Arc::new(tokio::sync::Mutex::new(
        SourceEventSink::open(&path).unwrap(),
    ));
    let asset_identity = Arc::new(AssetIdentityResolver::new(
        Arc::clone(&fetcher),
        BASE.to_owned(),
        GAMMA_BATCH_SIZE,
        Arc::clone(&sink),
    ));
    let validator = CausalPositionValidator::new_recording(
        fetcher,
        BASE,
        "source-generation-test",
        sink,
        asset_identity,
    )
    .with_clock(Arc::new(|| END));
    (dir, path, validator)
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
            newest_activity_unix: None,
            fresh_history: Vec::new(),
            expected_fence: None,
            history_status: None,
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
        verified_read: None,
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
        restamp_twins: Default::default(),
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
    control_rx: mpsc::Receiver<OrchestratorControl>,
    engine: BucketCommitEngine,
    paper: Arc<PaperStateDb>,
) -> tokio::task::JoinHandle<()> {
    spawn_counted_control_actor(control_rx, engine, paper, None)
}

fn spawn_counted_control_actor(
    mut control_rx: mpsc::Receiver<OrchestratorControl>,
    mut engine: BucketCommitEngine,
    paper: Arc<PaperStateDb>,
    commits: Option<Arc<AtomicUsize>>,
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
                    let result = engine.install_anchors(&installs);
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
                    if let Some(commits) = &commits {
                        commits.fetch_add(1, Ordering::SeqCst);
                    }
                    let _ = committed.send(
                        engine
                            .commit(aggregates, context.as_ref(), zero_basis())
                            .map_err(|error| error.to_string()),
                    );
                }
                _ => {}
            }
        }
    })
}

#[test]
fn canonical_capture_hash_matches_sorted_anchored_balances() {
    let wallet = wallet(0x10);
    let (_dir, paper, _engine) = fresh(&[wallet]);
    paper.set_cursor(&wallet, 42).unwrap();
    let mut ledger = PositionLedger::new();
    ledger.replace_wallet_snapshot(
        wallet,
        HashMap::from([
            (
                MarketOutcomeId::new(MarketId(VenueMarketId(condition(2))), OutcomeId(1)),
                PositionState {
                    long_contracts: ShareAmount::ZERO,
                    short_contracts: ShareAmount::from_atomic(2_500_000),
                },
            ),
            (
                MarketOutcomeId::new(MarketId(VenueMarketId(condition(1))), OutcomeId(0)),
                PositionState {
                    long_contracts: ShareAmount::from_atomic(1_000_000),
                    short_contracts: ShareAmount::ZERO,
                },
            ),
        ]),
    );

    let capture = ledger_capture(&ledger, &paper, wallet).unwrap();
    assert_eq!(
        capture.hash,
        "c08a170362315f92f6ee39615f7db11e308c4a7db8f2b1b044813fe8a67113c4"
    );
    assert_eq!(capture.cursor, Some(42));
    assert_eq!(capture.anchor_seq, None);
    assert_eq!(capture.coverage_generation, 0);
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

fn stored_activity_snapshot(path: &std::path::Path) -> Vec<Vec<Vec<rusqlite::types::Value>>> {
    let connection = rusqlite::Connection::open(path).unwrap();
    [
        "activity_groups",
        "activity_group_revisions",
        "no_copy_dispositions",
        "seen_trades",
    ]
    .into_iter()
    .map(|table| {
        let mut statement = connection
            .prepare(&format!("SELECT * FROM {table} ORDER BY 1, 2"))
            .unwrap();
        let columns = statement.column_count();
        statement
            .query_map([], |row| (0..columns).map(|index| row.get(index)).collect())
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    })
    .collect()
}

fn require_reanchor(path: &std::path::Path, wallet: WalletAddress) {
    rusqlite::Connection::open(path)
        .unwrap()
        .execute(
            "UPDATE poll_cursors SET reanchor_required = 1 WHERE wallet_hex = ?1",
            [wallet.to_string()],
        )
        .unwrap();
}

fn store_old_late_groups(engine: &mut BucketCommitEngine, wallet: WalletAddress) {
    for byte in 1..=3 {
        let epoch = i64::from(byte) * 10;
        let result = engine
            .commit(
                vec![aggregate(
                    activity(wallet, byte, "1", &format!("0xbase{byte}"), epoch),
                    wallet,
                )],
                &context(epoch),
                zero_basis(),
            )
            .unwrap();
        assert!(
            result
                .dispositions
                .values()
                .all(|value| value == "reanchor_required_late_group")
        );
    }
}

#[tokio::test]
async fn full_history_repairs_all_older_markets_and_preserves_old_records_on_restart() {
    for kind in ["anchored", "reanchor", "stored", "seeded"] {
        let wallet = wallet(0x91);
        let (dir, paper, mut engine) = fresh(&[wallet]);
        let path = dir.path().join("paper.db");
        if kind == "seeded" {
            paper
                .record_reconciled_history_status(&WalletHistoryStatusRecord {
                    wallet,
                    complete: false,
                    proof_json: "{\"seed\":true}".to_owned(),
                    updated_at_unix: 1,
                })
                .unwrap();
        }
        let old_status = paper.wallet_history_status(&wallet).unwrap();
        install_empty_anchor(&mut engine, &paper, wallet, 50);
        let original_anchor = paper.position_anchors(&wallet).unwrap()[0].clone();
        if matches!(kind, "reanchor" | "stored") {
            require_reanchor(&path, wallet);
        }
        if kind == "stored" {
            store_old_late_groups(&mut engine, wallet);
            assert!(
                paper
                    .gate_history()
                    .unwrap()
                    .get(&wallet)
                    .is_none_or(HashSet::is_empty)
            );
        }
        let before_groups = stored_activity_snapshot(&path);
        let before_financial = paper.financial_snapshot(END).unwrap();
        let fetcher = Arc::new(QueueFetcher::new(older_market_responses(wallet, 2)));
        let (_source_dir, source_path, validator) = recording_validator(fetcher.clone());
        let accepted = validator
            .validate_direct(&[wallet], &mut engine, &paper)
            .await
            .unwrap();
        assert_eq!(accepted.len(), 1, "{kind}");
        assert!(accepted[0].history_status.is_none());
        assert_full_history_proof(&accepted[0].proof.document);
        let expected = (1..=3)
            .map(|byte| MarketId(VenueMarketId(condition(byte))))
            .collect::<HashSet<_>>();
        assert_eq!(paper.gate_history().unwrap()[&wallet], expected, "{kind}");
        assert!(paper.wallet_history_complete(&wallet).unwrap());
        if kind != "seeded" {
            assert_eq!(paper.wallet_history_status(&wallet).unwrap(), old_status);
        }
        assert_eq!(paper.position_anchors(&wallet).unwrap()[0], original_anchor);
        if kind == "stored" {
            assert_eq!(stored_activity_snapshot(&path), before_groups);
        }
        assert_eq!(paper.financial_snapshot(END).unwrap(), before_financial);
        assert!(paper.decision_pending_history().unwrap().is_empty());
        let groups = stored_activity_snapshot(&path);
        let balances = ledger_capture(engine.ledger(), &paper, wallet)
            .unwrap()
            .hash;
        validator
            .validate_direct(&[wallet], &mut engine, &paper)
            .await
            .unwrap();
        assert_eq!(stored_activity_snapshot(&path), groups);
        assert_eq!(
            ledger_capture(engine.ledger(), &paper, wallet)
                .unwrap()
                .hash,
            balances
        );
        // Every market rejects a later entry through the running gate, before restart.
        for byte in 1..=3 {
            for (side, delta) in [("SELL", 0), ("BUY", 1)] {
                let epoch = END + i64::from(byte) * 2 + delta;
                let mut row = activity(wallet, byte, "1", &format!("0xlater-{byte}-{side}"), epoch);
                row["side"] = json!(side);
                let mut ordinary = context(epoch);
                ordinary.copy_eligible = true;
                let result = engine
                    .commit(vec![aggregate(row, wallet)], &ordinary, zero_basis())
                    .unwrap();
                assert!(result.pending.is_empty());
                if side == "BUY" {
                    assert!(
                        result
                            .dispositions
                            .values()
                            .all(|value| value == "not_first_entry")
                    );
                }
            }
        }
        assert_eq!(paper.financial_snapshot(END).unwrap(), before_financial);
        drop(validator);
        assert_eq!(
            Reader::replay(&source_path)
                .unwrap()
                .filter(|entry| {
                    entry.as_ref().unwrap().1.source_id.0
                        == pe_service::trade_poller::ACTIVITY_POLL_SOURCE_ID
                })
                .count(),
            14
        );
        drop(engine);
        drop(paper);
        let reopened = Arc::new(PaperStateDb::open(&path).unwrap());
        let ledger = build_leader_ledger(&reopened).unwrap();
        let replayed = replay_wallet_ledger(&reopened, wallet).unwrap();
        assert_eq!(
            ledger_capture(&ledger, &reopened, wallet).unwrap().hash,
            balances
        );
        assert_eq!(
            ledger_capture(&replayed, &reopened, wallet).unwrap().hash,
            balances
        );
        let mut engine = BucketCommitEngine::load(reopened.clone(), ledger).unwrap();
        assert!(engine.history_complete(&wallet));
        assert_eq!(reopened.gate_history().unwrap()[&wallet], expected);
        for byte in 1..=3 {
            let epoch = END + 10 + i64::from(byte) * 2;
            for (side, epoch) in [("SELL", epoch), ("BUY", epoch + 1)] {
                let mut row = activity(
                    wallet,
                    byte,
                    "1",
                    &format!("0xrestart-{byte}-{side}"),
                    epoch,
                );
                row["side"] = json!(side);
                let mut ordinary = context(epoch);
                ordinary.copy_eligible = true;
                let result = engine
                    .commit(vec![aggregate(row, wallet)], &ordinary, zero_basis())
                    .unwrap();
                assert!(result.pending.is_empty());
                if side == "BUY" {
                    assert!(
                        result
                            .dispositions
                            .values()
                            .all(|value| value == "not_first_entry")
                    );
                }
            }
        }
    }
}

#[tokio::test]
async fn stored_market_recovered_in_second_read_counts_as_activity_and_retries() {
    let wallet = wallet(0x92);
    let (dir, paper, mut engine) = fresh(&[wallet]);
    let path = dir.path().join("paper.db");
    install_empty_anchor(&mut engine, &paper, wallet, 0);
    require_reanchor(&path, wallet);
    store_old_late_groups(&mut engine, wallet);
    assert_eq!(paper.cursor(&wallet).unwrap(), Some(0));
    assert_eq!(paper.activity(&wallet).unwrap(), None);
    let before = stored_activity_snapshot(&path);
    let mut responses = older_market_responses(wallet, 2);
    let first = serde_json::to_vec(&vec![
        activity(wallet, 1, "1", "0xbase1", 10),
        activity(wallet, 2, "1", "0xbase2", 20),
    ])
    .unwrap();
    responses.get_mut(&activity_url(wallet)).unwrap()[0] = first;
    responses.insert(
        position_url(wallet, PositionPartition::NotRedeemable),
        vec![b"[]".to_vec(); 4],
    );
    let fetcher = Arc::new(QueueFetcher::new(responses));
    let accepted = validator_from_fetcher(fetcher.clone())
        .validate_direct(&[wallet], &mut engine, &paper)
        .await
        .unwrap();
    assert_eq!(accepted.len(), 1);
    assert_eq!(
        fetcher
            .urls()
            .iter()
            .filter(|url| url.contains("/activity?"))
            .count(),
        5
    );
    assert_eq!(paper.gate_history().unwrap()[&wallet].len(), 3);
    assert_eq!(paper.cursor(&wallet).unwrap(), Some(30));
    assert_eq!(paper.activity(&wallet).unwrap(), Some(30));
    assert_eq!(stored_activity_snapshot(&path), before);
    assert!(paper.decision_pending_history().unwrap().is_empty());
}

#[test]
fn fenced_bracket_records_new_covered_purchases_without_repairing_stored_groups() {
    let wallet = wallet(0x98);
    let (dir, paper, mut engine) = fresh(&[wallet]);
    let path = dir.path().join("paper.db");
    install_empty_anchor(&mut engine, &paper, wallet, 50);
    require_reanchor(&path, wallet);
    store_old_late_groups(&mut engine, wallet);
    let stored = aggregate(activity(wallet, 3, "1", "0xbase3", 30), wallet);
    let stored_before = paper.activity_group_state(stored.group_id.key()).unwrap();
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute(
            "INSERT INTO wallet_fences \
             (wallet_hex, source_trade_id, cause, proof_json, fenced_at_unix) \
             VALUES (?1, 'test', 'invalid_mapping', '{}', 50)",
            [wallet.to_string()],
        )
        .unwrap();
    let mut engine =
        BucketCommitEngine::load(paper.clone(), build_leader_ledger(&paper).unwrap()).unwrap();
    assert!(engine.is_fenced(&wallet));
    let history_before = paper.gate_history().unwrap();
    let mut bracket = context(END);
    bracket.bracket_commit = true;
    let result = engine
        .commit(vec![stored.clone()], &bracket, zero_basis())
        .unwrap();
    assert!(result.already_committed);
    assert!(paper.gate_history().unwrap()[&wallet].is_empty());

    // A bracket already in flight can contain both a stored group and an unseen BUY below cutoff.
    let new = aggregate(activity(wallet, 4, "1", "0xnew-covered", 30), wallet);
    let new_id = new.group_id.key().0.clone();
    let groups = vec![stored.clone(), new];
    let result = engine
        .commit(groups.clone(), &bracket, zero_basis())
        .unwrap();
    assert_eq!(result.dispositions[&new_id], "anchor_covered_late");
    assert_eq!(
        result.dispositions[&stored.group_id.key().0],
        "already_committed"
    );
    assert!(result.pending.is_empty());
    assert!(!result.already_committed);
    // All fenced bracket history waits for an eligible serialized recovery installation.
    assert_eq!(paper.gate_history().unwrap(), history_before);
    assert_eq!(
        paper.activity_group_state(stored.group_id.key()).unwrap(),
        stored_before
    );

    let retry = engine.commit(groups, &bracket, zero_basis()).unwrap();
    assert!(retry.already_committed);
    assert_eq!(paper.gate_history().unwrap(), history_before);
    assert!(paper.is_wallet_fenced(&wallet).unwrap());
    assert!(paper.decision_pending_history().unwrap().is_empty());
}

#[test]
fn cursor_write_failure_after_stored_repair_keeps_the_projection_published() {
    let wallet = wallet(0x99);
    let (dir, paper, mut engine) = fresh(&[wallet]);
    let path = dir.path().join("paper.db");
    install_empty_anchor(&mut engine, &paper, wallet, 0);
    require_reanchor(&path, wallet);
    store_old_late_groups(&mut engine, wallet);
    let stored = aggregate(activity(wallet, 3, "1", "0xbase3", 30), wallet);
    let stored_before = paper.activity_group_state(stored.group_id.key()).unwrap();
    assert_eq!(paper.cursor(&wallet).unwrap(), Some(0));
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute_batch(
        "CREATE TRIGGER fail_cursor BEFORE UPDATE ON poll_cursors          BEGIN SELECT RAISE(FAIL, 'cursor fault'); END;",
    )
    .unwrap();
    let mut bracket = context(END);
    bracket.bracket_commit = true;
    // The stored-group repair commits history, publishes it, then fails to
    // move the delivery cursor: durable history and the projection agree.
    engine
        .commit(vec![stored.clone()], &bracket, zero_basis())
        .unwrap_err();
    let expected = HashSet::from([MarketId(VenueMarketId(condition(3)))]);
    assert_eq!(paper.gate_history().unwrap()[&wallet], expected);
    assert_eq!(paper.cursor(&wallet).unwrap(), Some(0));
    assert_eq!(
        paper.activity_group_state(stored.group_id.key()).unwrap(),
        stored_before
    );
    conn.execute_batch("DROP TRIGGER fail_cursor").unwrap();
    // With the projection already published, the retry is the plain
    // all-stored shortcut: no second repair, cursor advanced.
    let retry = engine
        .commit(vec![stored.clone()], &bracket, zero_basis())
        .unwrap();
    assert!(retry.already_committed);
    assert_eq!(paper.gate_history().unwrap()[&wallet], expected);
    assert_eq!(paper.cursor(&wallet).unwrap(), Some(30));
    assert_eq!(paper.activity(&wallet).unwrap(), None);
    assert!(paper.decision_pending_history().unwrap().is_empty());
}

#[tokio::test]
async fn failed_history_repair_rolls_back_projection_and_restart_converges() {
    for stored in [false, true] {
        let wallet = wallet(0x93);
        let (dir, paper, mut engine) = fresh(&[wallet]);
        let path = dir.path().join("paper.db");
        install_empty_anchor(&mut engine, &paper, wallet, 50);
        require_reanchor(&path, wallet);
        if stored {
            store_old_late_groups(&mut engine, wallet);
        }
        let before = stored_activity_snapshot(&path);
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch("CREATE TRIGGER fail_repair BEFORE INSERT ON wallet_market_history_v2 BEGIN SELECT RAISE(FAIL, 'history repair fault'); END;").unwrap();
        let error = validator(older_market_responses(wallet, 1))
            .validate_direct(&[wallet], &mut engine, &paper)
            .await
            .unwrap_err();
        assert!(matches!(error, CausalPositionError::BucketCommit { .. }));
        assert_eq!(stored_activity_snapshot(&path), before);
        assert!(
            paper
                .gate_history()
                .unwrap()
                .get(&wallet)
                .is_none_or(HashSet::is_empty)
        );
        assert_eq!(paper.position_anchors(&wallet).unwrap().len(), 1);
        conn.execute_batch("DROP TRIGGER fail_repair").unwrap();
        // A successful retry in the same engine proves that failed writes did not publish history.
        validator(older_market_responses(wallet, 1))
            .validate_direct(&[wallet], &mut engine, &paper)
            .await
            .unwrap();
        assert_eq!(paper.gate_history().unwrap()[&wallet].len(), 3);
        drop(engine);
        drop(paper);
        let paper = Arc::new(PaperStateDb::open(&path).unwrap());
        let mut engine =
            BucketCommitEngine::load(paper.clone(), build_leader_ledger(&paper).unwrap()).unwrap();
        validator(older_market_responses(wallet, 1))
            .validate_direct(&[wallet], &mut engine, &paper)
            .await
            .unwrap();
        assert_eq!(paper.gate_history().unwrap()[&wallet].len(), 3);
        assert!(paper.decision_pending_history().unwrap().is_empty());
    }
}

#[tokio::test]
async fn deferred_old_complete_wallet_restarts_from_partial_bracket_without_recopying() {
    let wallet = wallet(0x96);
    let (dir, paper, mut engine) = fresh(&[wallet]);
    let path = dir.path().join("paper.db");
    install_empty_anchor(&mut engine, &paper, wallet, 50);
    let old_status = paper.wallet_history_status(&wallet).unwrap();
    let old_anchor = paper.position_anchors(&wallet).unwrap();
    let mut responses = older_market_responses(wallet, 2);
    let stable = responses[&position_url(wallet, PositionPartition::NotRedeemable)][0].clone();
    let mut revised: Value = serde_json::from_slice(&stable).unwrap();
    revised[0]["size"] = json!("2");
    let revised = serde_json::to_vec(&revised).unwrap();
    responses.insert(
        position_url(wallet, PositionPartition::NotRedeemable),
        vec![stable.clone(), revised.clone(), stable, revised],
    );
    let accepted = validator(responses)
        .validate_direct(&[wallet], &mut engine, &paper)
        .await
        .unwrap();
    assert!(
        accepted.is_empty(),
        "an old complete flag cannot turn deferral into acceptance"
    );
    assert_eq!(paper.position_anchors(&wallet).unwrap(), old_anchor);
    assert_eq!(paper.wallet_history_status(&wallet).unwrap(), old_status);
    assert_eq!(paper.gate_history().unwrap()[&wallet].len(), 3);
    let groups = stored_activity_snapshot(&path);
    drop(engine);
    drop(paper);
    let paper = Arc::new(PaperStateDb::open(&path).unwrap());
    let mut engine =
        BucketCommitEngine::load(paper.clone(), build_leader_ledger(&paper).unwrap()).unwrap();
    let accepted = validator(older_market_responses(wallet, 1))
        .validate_direct(&[wallet], &mut engine, &paper)
        .await
        .unwrap();
    assert_eq!(accepted.len(), 1);
    assert_full_history_proof(&accepted[0].proof.document);
    assert_eq!(stored_activity_snapshot(&path), groups);
    assert_eq!(paper.gate_history().unwrap()[&wallet].len(), 3);
    assert!(paper.decision_pending_history().unwrap().is_empty());
    assert!(paper.list_fills().unwrap().is_empty());
}

#[tokio::test]
async fn reanchor_history_repair_excludes_sells_and_unresolved_assets() {
    let wallet = wallet(0x97);
    let (dir, paper, mut engine) = fresh(&[wallet]);
    install_empty_anchor(&mut engine, &paper, wallet, 50);
    require_reanchor(&dir.path().join("paper.db"), wallet);
    let mut responses = older_market_responses(wallet, 1);
    let mut rows: Vec<Value> =
        serde_json::from_slice(&responses[&activity_url(wallet)][0]).unwrap();
    let mut sell = activity(wallet, 4, "1", "0xsell-only", 40);
    sell["side"] = json!("SELL");
    let mut unresolved = activity(wallet, 5, "1", "0xunresolved", 45);
    unresolved["asset"] = json!("unmapped-token");
    rows.extend([sell, unresolved]);
    responses.insert(
        activity_url(wallet),
        vec![serde_json::to_vec(&rows).unwrap(); 3],
    );
    let accepted = validator(responses)
        .validate_direct(&[wallet], &mut engine, &paper)
        .await
        .unwrap();
    assert_eq!(accepted.len(), 1);
    assert_eq!(
        paper.gate_history().unwrap()[&wallet],
        (1..=3)
            .map(|byte| MarketId(VenueMarketId(condition(byte))))
            .collect()
    );
    assert!(paper.decision_pending_history().unwrap().is_empty());
    assert!(!paper.is_wallet_fenced(&wallet).unwrap());
}

#[tokio::test]
async fn corrected_bracket_records_metadata_reuses_provenance_and_keeps_full_reads() {
    let wallet = wallet(0x12);
    let (_paper_dir, paper, mut engine) = fresh(&[wallet]);
    let gamma = serde_json::to_vec(&vec![json!({
        "conditionId": condition(9),
        "clobTokenIds": ["other-outcome", asset(1)]
    })])
    .unwrap();
    let fetcher = Arc::new(QueueFetcher::with_gamma(
        stable_responses_for_attempts(&[(wallet, 1, "1.000000")], 2),
        Some(gamma),
    ));
    let fetcher_trait: Arc<dyn ReconciliationFetcher> = fetcher.clone();
    let (_source_dir, source_path, validator) = recording_validator(fetcher_trait);

    let first = validator
        .validate_direct(&[wallet], &mut engine, &paper)
        .await
        .unwrap();
    let second = validator
        .validate_direct(&[wallet], &mut engine, &paper)
        .await
        .unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(second.len(), 1);
    assert_eq!(
        first[0].balances,
        vec![(
            MarketId(VenueMarketId(condition(9))),
            OutcomeId(1),
            ShareAmount::from_atomic(1_000_000),
        )]
    );
    let first_proof: Value = serde_json::from_str(&first[0].proof.document).unwrap();
    let second_proof: Value = serde_json::from_str(&second[0].proof.document).unwrap();
    assert_eq!(
        first_proof["metadata_reads"],
        second_proof["metadata_reads"]
    );
    let metadata_reads = first_proof["metadata_reads"].as_array().unwrap();
    assert_eq!(metadata_reads.len(), 1);
    let sequence = metadata_reads[0]["source_log_sequence"].as_u64().unwrap();
    let canonical_hash = metadata_reads[0]["canonical_page_hash"].as_str().unwrap();

    let urls = fetcher.urls();
    let activity_urls = urls
        .iter()
        .filter(|url| url.contains("/activity?"))
        .collect::<Vec<_>>();
    assert_eq!(activity_urls.len(), 6);
    assert!(activity_urls.iter().all(|url| url.ends_with("&start=1")));
    assert_eq!(
        urls.iter()
            .filter(|url| url.contains("/markets?clob_token_ids="))
            .count(),
        1,
        "the anchored bracket reuses the process cache"
    );
    assert_eq!(first[0].cutoff, END);
    assert_eq!(second[0].cutoff, END);

    let groups = paper.activity_groups_after(&wallet, -1).unwrap();
    assert_eq!(groups.len(), 1);
    let effect = pe_position_ledger::LedgerEffect::from_document(&groups[0].proof_json).unwrap();
    assert!(effect.correction().is_some());
    assert_eq!(
        serde_json::from_str::<Value>(&groups[0].proof_json).unwrap()["version"],
        2
    );

    drop(validator);
    let metadata_entries = Reader::replay(&source_path)
        .unwrap()
        .map(|entry| entry.unwrap())
        .filter(|(_, envelope)| envelope.source_id.0 == GAMMA_MARKETS_SOURCE_ID)
        .collect::<Vec<_>>();
    assert_eq!(metadata_entries.len(), 1);
    assert_eq!(metadata_entries[0].0.0, sequence);
    let canonical_payload = serde_json::to_vec(
        &serde_json::from_slice::<Value>(&metadata_entries[0].1.payload).unwrap(),
    )
    .unwrap();
    assert_eq!(
        blake3::hash(&canonical_payload).to_hex().as_str(),
        canonical_hash
    );
}

fn durable_bracket_validator(
    fetcher: Arc<dyn ReconciliationFetcher>,
    paper: Arc<PaperStateDb>,
    path: &std::path::Path,
) -> CausalPositionValidator {
    let sink = Arc::new(tokio::sync::Mutex::new(
        SourceEventSink::open(path).unwrap(),
    ));
    let receipts = pe_service::risk_inputs::SourceReceiptIndex::replay(path).unwrap();
    let identity = Arc::new(
        AssetIdentityResolver::new(
            fetcher.clone(),
            BASE.to_owned(),
            GAMMA_BATCH_SIZE,
            sink.clone(),
        )
        .with_paper_state(paper, "installed-identity-generation".into(), receipts),
    );
    CausalPositionValidator::new_recording(fetcher, BASE, "source-generation-test", sink, identity)
        .with_clock(Arc::new(|| END))
}

#[tokio::test]
async fn durable_bracket_restart_skips_gamma_for_absent_and_rejected_tokens() {
    for rejection_case in 0..3 {
        let wallet = wallet(0x74);
        let (dir, paper, engine) = fresh(&[wallet]);
        let db_path = dir.path().join("paper.db");
        let path = dir.path().join("source.log");
        let token = pe_core_types::PolymarketTokenId(asset(1));
        let generation = "installed-identity-generation";
        let sink = Arc::new(tokio::sync::Mutex::new(
            SourceEventSink::open(&path).unwrap(),
        ));
        let receipts = pe_service::risk_inputs::SourceReceiptIndex::replay(&path).unwrap();
        let rejected = rejection_case != 0;
        let gamma = if rejection_case == 2 {
            serde_json::to_vec(&json!([
                {"conditionId": condition(9), "clobTokenIds": [asset(1)]},
                {"conditionId": condition(10), "clobTokenIds": [asset(1)]}
            ]))
            .unwrap()
        } else if rejected {
            serde_json::to_vec(&json!([{
                "conditionId": condition(9), "clobTokenIds": [asset(1), "sibling"]
            }]))
            .unwrap()
        } else {
            b"[]".to_vec()
        };
        let seed = Arc::new(QueueFetcher::with_gamma(HashMap::new(), Some(gamma)));
        let resolver =
            AssetIdentityResolver::new(seed.clone(), BASE.into(), GAMMA_BATCH_SIZE, sink)
                .with_paper_state(paper.clone(), generation.into(), receipts);
        let initial = resolver
            .resolve_historical_for_bracket([token.clone()])
            .await
            .unwrap();
        assert_eq!(initial.verified.contains_key(&token), rejection_case == 1);
        drop(resolver);
        if rejection_case == 1 {
            let sink = Arc::new(tokio::sync::Mutex::new(
                SourceEventSink::open(&path).unwrap(),
            ));
            let receipts = pe_service::risk_inputs::SourceReceiptIndex::replay(&path).unwrap();
            let gamma = serde_json::to_vec(&json!([{
                "conditionId": condition(9), "clobTokenIds": ["different-token", asset(2)]
            }]))
            .unwrap();
            let seed = Arc::new(QueueFetcher::with_gamma(HashMap::new(), Some(gamma)));
            let resolver = AssetIdentityResolver::new(seed, BASE.into(), GAMMA_BATCH_SIZE, sink)
                .with_paper_state(paper.clone(), generation.into(), receipts);
            let conflicting = resolver
                .resolve_live([pe_core_types::PolymarketTokenId(asset(2))])
                .await
                .unwrap();
            assert!(conflicting.verified.is_empty());
            assert!(
                paper
                    .asset_identity_condition_rejection(generation, &condition(9))
                    .unwrap()
                    .is_some()
            );
        } else if rejection_case == 0 {
            assert_eq!(
                paper
                    .absent_asset_tokens(generation, std::slice::from_ref(&token))
                    .unwrap()
                    .len(),
                1
            );
        }
        if rejected {
            assert!(
                paper
                    .rejected_asset_tokens(generation, std::slice::from_ref(&token))
                    .unwrap()
                    .contains_key(&token)
            );
        }
        drop(engine);
        drop(paper);

        let paper = Arc::new(PaperStateDb::open(&db_path).unwrap());
        let mut engine =
            BucketCommitEngine::load(paper.clone(), build_leader_ledger(&paper).unwrap()).unwrap();
        let activity =
            serde_json::to_vec(&vec![activity(wallet, 1, "1.000000", "0xunverified", 10)]).unwrap();
        let fetcher = Arc::new(QueueFetcher::with_gamma(
            HashMap::from([
                (activity_url(wallet), vec![activity; 3]),
                (
                    position_url(wallet, PositionPartition::NotRedeemable),
                    vec![b"[]".to_vec(); 2],
                ),
                (
                    position_url(wallet, PositionPartition::Redeemable),
                    vec![b"[]".to_vec(); 2],
                ),
            ]),
            Some(b"[]".to_vec()),
        ));
        let validator = durable_bracket_validator(fetcher.clone(), paper.clone(), &path);
        let accepted = validator
            .validate_direct(&[wallet], &mut engine, &paper)
            .await
            .unwrap();
        assert_eq!(accepted.len(), 1);
        assert_eq!(
            fetcher
                .urls()
                .iter()
                .filter(|url| url.contains("/markets?"))
                .count(),
            0
        );
        let groups = paper.activity_groups_after(&wallet, -1).unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].disposition, "raw_only");
        assert_eq!(
            paper
                .no_copy_disposition(&groups[0].source_trade_id)
                .unwrap()
                .unwrap()
                .2,
            "identity_unresolved"
        );
        assert!(
            paper
                .gate_history()
                .unwrap()
                .get(&wallet)
                .is_none_or(HashSet::is_empty)
        );
        assert!(paper.decision_pending_history().unwrap().is_empty());
        if rejected {
            assert!(
                paper
                    .asset_identity_condition_rejection(generation, &condition(9))
                    .unwrap()
                    .is_some()
            );
        }
    }
}

#[tokio::test]
async fn durable_bracket_identity_restart_authenticates_cache_and_refetches_altered_rows() {
    for altered in ["outcome", "page_hash"] {
        let wallet = wallet(0x12);
        let (paper_dir, paper, mut engine) = fresh(&[wallet]);
        let source_dir = tempfile::tempdir().unwrap();
        let path = source_dir.path().join("source.log");
        let gamma = serde_json::to_vec(&vec![json!({
            "conditionId": condition(9),
            "clobTokenIds": ["other-outcome", asset(1)]
        })])
        .unwrap();
        let fetcher = Arc::new(QueueFetcher::with_gamma(
            stable_responses_for_attempts(&[(wallet, 1, "1.000000")], 3),
            Some(gamma),
        ));
        let validator = durable_bracket_validator(fetcher.clone(), paper.clone(), &path);
        let original = validator
            .validate_direct(&[wallet], &mut engine, &paper)
            .await
            .unwrap();
        assert_eq!(original.len(), 1);
        let original_proof: Value = serde_json::from_str(&original[0].proof.document).unwrap();
        drop(validator);

        let validator = durable_bracket_validator(fetcher.clone(), paper.clone(), &path);
        let restored = validator
            .validate_direct(&[wallet], &mut engine, &paper)
            .await
            .unwrap();
        assert_eq!(restored.len(), 1);
        let restored_proof: Value = serde_json::from_str(&restored[0].proof.document).unwrap();
        assert_eq!(
            restored_proof["metadata_reads"],
            original_proof["metadata_reads"]
        );
        assert_eq!(
            fetcher
                .urls()
                .iter()
                .filter(|url| url.contains("/markets?"))
                .count(),
            1,
            "a new bracket resolver uses saved identities without Gamma requests"
        );
        drop(validator);

        let conn = rusqlite::Connection::open(paper_dir.path().join("paper.db")).unwrap();
        if altered == "outcome" {
            assert_eq!(
                conn.execute(
                    "UPDATE asset_identities SET outcome = 0 WHERE token = ?1",
                    [asset(1)]
                )
                .unwrap(),
                1
            );
        } else {
            assert_eq!(
                conn.execute(
                    "UPDATE asset_identities SET canonical_page_hash = ?1 WHERE token = ?2",
                    ["0".repeat(64), asset(1)],
                )
                .unwrap(),
                1
            );
        }
        drop(conn);
        let validator = durable_bracket_validator(fetcher.clone(), paper.clone(), &path);
        let corrected = validator
            .validate_direct(&[wallet], &mut engine, &paper)
            .await
            .unwrap();
        assert_eq!(corrected.len(), 1, "{altered}");
        assert_eq!(corrected[0].balances, original[0].balances, "{altered}");
        assert_eq!(corrected[0].balances[0].1, OutcomeId(1), "{altered}");
        let corrected_proof: Value = serde_json::from_str(&corrected[0].proof.document).unwrap();
        assert_ne!(
            corrected_proof["metadata_reads"], original_proof["metadata_reads"],
            "{altered}"
        );
        let saved = paper
            .asset_identities(
                "installed-identity-generation",
                &[pe_core_types::PolymarketTokenId(asset(1))],
            )
            .unwrap();
        assert_eq!(saved.len(), 1);
        assert_eq!(saved[0].outcome, 1);
        assert_eq!(
            saved[0].canonical_page_hash,
            original_proof["metadata_reads"][0]["canonical_page_hash"]
        );
        assert_eq!(
            fetcher
                .urls()
                .iter()
                .filter(|url| url.contains("/markets?"))
                .count(),
            2,
            "{altered}"
        );
        assert_eq!(
            fetcher
                .urls()
                .iter()
                .filter(|url| url.contains("/activity?"))
                .count(),
            9
        );
    }
}

#[tokio::test]
async fn failed_boot_bracket_keeps_saved_identities_without_repeat_gamma_after_restart() {
    let wallet = wallet(0x12);
    let (dir, paper, mut engine) = fresh(&[wallet]);
    let db_path = dir.path().join("paper.db");
    let path = dir.path().join("source.log");
    let fetcher = Arc::new(QueueFetcher::new(stable_responses_for_attempts(
        &[(wallet, 1, "1.000000")],
        2,
    )));
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    conn.execute_batch("CREATE TRIGGER fail_identity_bracket BEFORE INSERT ON wallet_market_history_v2 BEGIN SELECT RAISE(FAIL, 'bracket fault'); END;").unwrap();
    let validator = durable_bracket_validator(fetcher.clone(), paper.clone(), &path);
    let error = validator
        .validate_direct(&[wallet], &mut engine, &paper)
        .await
        .unwrap_err();
    assert!(matches!(error, CausalPositionError::BucketCommit { .. }));
    let token = pe_core_types::PolymarketTokenId(asset(1));
    let saved = paper
        .asset_identities(
            "installed-identity-generation",
            std::slice::from_ref(&token),
        )
        .unwrap();
    assert_eq!(saved.len(), 1);
    assert!(paper.position_anchors(&wallet).unwrap().is_empty());
    drop(validator);
    drop(engine);
    drop(paper);
    drop(conn);

    let paper = Arc::new(PaperStateDb::open(&db_path).unwrap());
    assert_eq!(
        paper
            .asset_identities("installed-identity-generation", &[token])
            .unwrap(),
        saved
    );
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    conn.execute_batch("DROP TRIGGER fail_identity_bracket")
        .unwrap();
    drop(conn);
    let mut engine =
        BucketCommitEngine::load(paper.clone(), build_leader_ledger(&paper).unwrap()).unwrap();
    let validator = durable_bracket_validator(fetcher.clone(), paper.clone(), &path);
    let accepted = validator
        .validate_direct(&[wallet], &mut engine, &paper)
        .await
        .unwrap();
    assert_eq!(accepted.len(), 1);
    assert_eq!(
        fetcher
            .urls()
            .iter()
            .filter(|url| url.contains("/markets?"))
            .count(),
        1
    );
    let proof: Value = serde_json::from_str(&accepted[0].proof.document).unwrap();
    assert_eq!(
        proof["metadata_reads"][0]["canonical_page_hash"],
        saved[0].canonical_page_hash
    );
}

#[tokio::test]
async fn direct_bracket_uses_three_full_history_walks_and_step_three_cutoff() {
    let wallet = wallet(0x76);
    let (_dir, paper, mut engine) = fresh(&[wallet]);
    let activity = serde_json::to_vec(&vec![activity(
        wallet,
        1,
        "1.000000",
        "0xvarying-direct",
        90,
    )])
    .unwrap();
    let positions = serde_json::to_vec(&vec![position(wallet, 1, "1.000000")]).unwrap();
    let responses = HashMap::from([
        (activity_url_at(wallet, 100), vec![activity.clone()]),
        (activity_url_at(wallet, 101), vec![activity.clone()]),
        (activity_url_at(wallet, 102), vec![activity]),
        (
            position_url(wallet, PositionPartition::NotRedeemable),
            vec![positions.clone(), positions],
        ),
        (
            position_url(wallet, PositionPartition::Redeemable),
            vec![b"[]".to_vec(), b"[]".to_vec()],
        ),
    ]);
    let fetcher = Arc::new(QueueFetcher::new(responses));
    let ends = Arc::new(Mutex::new(VecDeque::from([100, 101, 102, 102])));
    let clock_ends = Arc::clone(&ends);
    let validator = validator_from_fetcher(Arc::clone(&fetcher)).with_clock(Arc::new(move || {
        clock_ends
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pop_front()
            .unwrap_or(102)
    }));

    let accepted = validator
        .validate_direct(&[wallet], &mut engine, &paper)
        .await
        .unwrap();
    assert_eq!(accepted[0].cutoff, 101);
    assert_full_history_proof(&accepted[0].proof.document);
    let activity_urls = fetcher
        .urls()
        .into_iter()
        .filter(|url| url.contains("/activity?"))
        .collect::<Vec<_>>();
    assert_eq!(
        activity_urls,
        vec![
            activity_url_at(wallet, 100),
            activity_url_at(wallet, 101),
            activity_url_at(wallet, 102),
        ]
    );
    assert!(activity_urls.iter().all(|url| url.ends_with("&start=1")));
}

#[tokio::test]
async fn row_older_than_the_previous_end_is_fetched_and_triggers_the_bounded_retry() {
    let wallet = wallet(0x77);
    let (_dir, paper, mut engine) = fresh(&[wallet]);
    let baseline = activity(wallet, 1, "1.000000", "0xbaseline", 90);
    let intervening = activity(wallet, 2, "1.000000", "0xolder-intervening", 80);
    let baseline_page = serde_json::to_vec(&vec![baseline.clone()]).unwrap();
    let both_page = serde_json::to_vec(&vec![baseline, intervening.clone()]).unwrap();
    let first_positions = serde_json::to_vec(&vec![position(wallet, 1, "1.000000")]).unwrap();
    let both_positions = serde_json::to_vec(&vec![
        position(wallet, 1, "1.000000"),
        position(wallet, 2, "1.000000"),
    ])
    .unwrap();
    let responses = HashMap::from([
        (
            activity_url_at(wallet, 100),
            vec![baseline_page, both_page.clone()],
        ),
        (
            activity_url_at(wallet, 101),
            vec![both_page.clone(), both_page.clone()],
        ),
        (activity_url_at(wallet, 102), vec![both_page]),
        (
            position_url(wallet, PositionPartition::NotRedeemable),
            vec![first_positions, both_positions.clone(), both_positions],
        ),
        (
            position_url(wallet, PositionPartition::Redeemable),
            vec![b"[]".to_vec(), b"[]".to_vec(), b"[]".to_vec()],
        ),
    ]);
    let fetcher = Arc::new(QueueFetcher::new(responses));
    let ends = Arc::new(Mutex::new(VecDeque::from([100, 101, 100, 101, 102, 102])));
    let clock_ends = Arc::clone(&ends);
    let validator = validator_from_fetcher(Arc::clone(&fetcher)).with_clock(Arc::new(move || {
        clock_ends
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pop_front()
            .unwrap_or(102)
    }));
    let intervening_id = aggregate(intervening, wallet).group_id.key().clone();

    let accepted = validator
        .validate_direct(&[wallet], &mut engine, &paper)
        .await
        .unwrap();

    assert_eq!(accepted[0].cutoff, 101);
    assert_full_history_proof(&accepted[0].proof.document);
    let activity_urls = fetcher
        .urls()
        .into_iter()
        .filter(|url| url.contains("/activity?"))
        .collect::<Vec<_>>();
    assert_eq!(
        activity_urls,
        vec![
            activity_url_at(wallet, 100),
            activity_url_at(wallet, 101),
            activity_url_at(wallet, 100),
            activity_url_at(wallet, 101),
            activity_url_at(wallet, 102),
        ],
        "the second read detected the inserted old row and forced one full retry"
    );
    assert!(activity_urls.iter().all(|url| url.ends_with("&start=1")));
    assert!(
        paper
            .activity_group_state(&intervening_id)
            .unwrap()
            .is_some()
    );
    assert!(!paper.is_wallet_fenced(&wallet).unwrap());
}

#[tokio::test]
async fn runtime_control_bracket_uses_three_full_history_walks() {
    let wallet = wallet(0x78);
    let (_dir, paper, engine) = fresh(&[wallet]);
    let activity = serde_json::to_vec(&vec![activity(
        wallet,
        1,
        "1.000000",
        "0xvarying-control",
        90,
    )])
    .unwrap();
    let positions = serde_json::to_vec(&vec![position(wallet, 1, "1.000000")]).unwrap();
    let responses = HashMap::from([
        (activity_url_at(wallet, 100), vec![activity.clone()]),
        (activity_url_at(wallet, 101), vec![activity.clone()]),
        (activity_url_at(wallet, 102), vec![activity]),
        (
            position_url(wallet, PositionPartition::NotRedeemable),
            vec![positions.clone(), positions],
        ),
        (
            position_url(wallet, PositionPartition::Redeemable),
            vec![b"[]".to_vec(), b"[]".to_vec()],
        ),
    ]);
    let fetcher = Arc::new(QueueFetcher::new(responses));
    let ends = Arc::new(Mutex::new(VecDeque::from([100, 101, 102, 102])));
    let clock_ends = Arc::clone(&ends);
    let validator = validator_from_fetcher(Arc::clone(&fetcher)).with_clock(Arc::new(move || {
        clock_ends
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pop_front()
            .unwrap_or(102)
    }));
    let (control_tx, control_rx) = mpsc::channel(2);
    let actor = spawn_control_actor(control_rx, engine, Arc::clone(&paper));

    let accepted = validator
        .validate_via_control(
            &[wallet],
            &control_tx,
            &paper,
            pe_service::position_seeder::ValidationPurpose::CatchUp,
            None,
        )
        .await;
    assert!(accepted.shared.is_none());
    assert!(accepted.deferred.is_empty());
    assert_eq!(accepted.accepted[0].cutoff, 101);
    assert_full_history_proof(&accepted.accepted[0].proof.document);
    let activity_urls = fetcher
        .urls()
        .into_iter()
        .filter(|url| url.contains("/activity?"))
        .collect::<Vec<_>>();
    assert_eq!(
        activity_urls,
        vec![
            activity_url_at(wallet, 100),
            activity_url_at(wallet, 101),
            activity_url_at(wallet, 102),
        ]
    );
    assert!(activity_urls.iter().all(|url| url.ends_with("&start=1")));
    drop(control_tx);
    actor.await.unwrap();
}

#[tokio::test]
async fn verified_anchor_replaces_a_preexisting_wrong_identity_ledger_row() {
    let wallet = wallet(0x18);
    let (_dir, paper, mut engine) = fresh(&[wallet]);
    install_empty_anchor(&mut engine, &paper, wallet, 0);
    let raw = activity(wallet, 1, "1.000000", "0xbase1", 10);
    engine
        .commit(vec![aggregate(raw, wallet)], &context(10), zero_basis())
        .unwrap();
    assert_eq!(
        engine
            .ledger()
            .position(&wallet)
            .and_then(|snapshot| {
                snapshot.positions.get(&MarketOutcomeId::new(
                    MarketId(VenueMarketId(condition(1))),
                    OutcomeId(0),
                ))
            })
            .map(|position| position.long_contracts.atomic()),
        Some(1_000_000)
    );

    let gamma = serde_json::to_vec(&vec![json!({
        "conditionId": condition(9),
        "clobTokenIds": ["other-outcome", asset(1)]
    })])
    .unwrap();
    let fetcher = Arc::new(QueueFetcher::with_gamma(
        stable_responses(&[(wallet, 1, "1.000000")]),
        Some(gamma),
    ));
    let validator = validator_from_fetcher(fetcher);
    let accepted = validator
        .validate_direct(&[wallet], &mut engine, &paper)
        .await
        .unwrap();

    assert_eq!(accepted.len(), 1);
    let snapshot = engine.ledger().position(&wallet).unwrap();
    assert!(!snapshot.positions.contains_key(&MarketOutcomeId::new(
        MarketId(VenueMarketId(condition(1))),
        OutcomeId(0),
    )));
    assert_eq!(
        snapshot.positions
            [&MarketOutcomeId::new(MarketId(VenueMarketId(condition(9))), OutcomeId(1),)]
            .long_contracts
            .atomic(),
        1_000_000
    );

    let replayed = build_leader_ledger(&paper).unwrap();
    let replayed = replayed.position(&wallet).unwrap();
    assert!(!replayed.positions.contains_key(&MarketOutcomeId::new(
        MarketId(VenueMarketId(condition(1))),
        OutcomeId(0),
    )));
    assert_eq!(
        replayed.positions
            [&MarketOutcomeId::new(MarketId(VenueMarketId(condition(9))), OutcomeId(1),)]
            .long_contracts
            .atomic(),
        1_000_000
    );
}

#[tokio::test]
async fn assets_first_seen_in_second_or_final_activity_read_are_resolved_before_commit() {
    for (suffix, appearance) in [(0x19, 2_usize), (0x1a, 3_usize)] {
        let wallet = wallet(suffix);
        let (_dir, paper, mut engine) = fresh(&[wallet]);
        let first_row = activity(wallet, 1, "1.000000", "0xfirst-asset", 10);
        let second_row = activity(wallet, 2, "2.000000", "0xlater-asset", 20);
        let second_group = aggregate(second_row.clone(), wallet).group_id.key().clone();
        let first_activity = serde_json::to_vec(&vec![first_row.clone()]).unwrap();
        let both_activity = serde_json::to_vec(&vec![second_row, first_row]).unwrap();
        let activity_pages = if appearance == 2 {
            vec![
                first_activity,
                both_activity.clone(),
                both_activity.clone(),
                both_activity.clone(),
                both_activity,
            ]
        } else {
            vec![
                first_activity.clone(),
                first_activity,
                both_activity.clone(),
                both_activity.clone(),
                both_activity.clone(),
                both_activity,
            ]
        };
        let first_positions = serde_json::to_vec(&vec![position(wallet, 1, "1")]).unwrap();
        let both_positions =
            serde_json::to_vec(&vec![position(wallet, 1, "1"), position(wallet, 2, "2")]).unwrap();
        let position_reads = if appearance == 2 { 3 } else { 4 };
        let mut not_redeemable = vec![first_positions; appearance - 1];
        not_redeemable.extend((0..2).map(|_| both_positions.clone()));
        assert_eq!(not_redeemable.len(), position_reads);
        let responses = HashMap::from([
            (activity_url(wallet), activity_pages),
            (
                position_url(wallet, PositionPartition::NotRedeemable),
                not_redeemable,
            ),
            (
                position_url(wallet, PositionPartition::Redeemable),
                (0..position_reads).map(|_| b"[]".to_vec()).collect(),
            ),
        ]);
        let gamma = serde_json::to_vec(&vec![
            json!({"conditionId":condition(1),"clobTokenIds":[asset(1)]}),
            json!({
                "conditionId":condition(9),
                "clobTokenIds":["other-outcome",asset(2)]
            }),
        ])
        .unwrap();
        let fetcher = Arc::new(QueueFetcher::with_gamma(responses, Some(gamma)));

        let accepted = validator_from_fetcher(fetcher)
            .validate_direct(&[wallet], &mut engine, &paper)
            .await
            .unwrap();
        assert_eq!(accepted.len(), 1);
        assert!(accepted[0].balances.contains(&(
            MarketId(VenueMarketId(condition(9))),
            OutcomeId(1),
            ShareAmount::from_atomic(2_000_000),
        )));
        let corrected = paper
            .activity_groups_after(&wallet, -1)
            .unwrap()
            .into_iter()
            .find(|group| group.source_trade_id == second_group)
            .expect("later asset is committed during the read where it first appears");
        assert_eq!(
            serde_json::from_str::<Value>(&corrected.proof_json).unwrap()["version"],
            2
        );
    }
}

#[tokio::test]
async fn concurrent_brackets_record_both_orders_install_once_and_attribute_distinct_pages() {
    let first = wallet(0x13);
    let second = wallet(0x14);
    let mut outcomes = Vec::new();
    let mut commit_orders = Vec::new();
    let mut completion_orders = Vec::new();
    for preferred in [first, second] {
        let (_paper_dir, paper, mut engine) = fresh(&[first, second]);
        let preferred_first_commit = Arc::new(tokio::sync::Semaphore::new(0));
        let other_first_commit = Arc::new(tokio::sync::Semaphore::new(0));
        let preferred_completion = Arc::new(tokio::sync::Semaphore::new(0));
        let fetcher = Arc::new(OrderedBracketFetcher {
            inner: QueueFetcher::new(stable_responses(&[
                (first, 1, "1.000000"),
                (second, 2, "2.000000"),
            ])),
            wallets: [first, second],
            preferred,
            activity_calls: Mutex::new(HashMap::new()),
            first_activity: tokio::sync::Barrier::new(2),
            preferred_first_commit: Arc::clone(&preferred_first_commit),
            other_first_commit: Arc::clone(&other_first_commit),
            preferred_completion: Arc::clone(&preferred_completion),
        });
        let fetcher_trait: Arc<dyn ReconciliationFetcher> = fetcher.clone();
        let (_source_dir, source_path, validator) = recording_validator(fetcher_trait);
        let commit_order = Arc::new(Mutex::new(Vec::new()));
        let completion_order = Arc::new(Mutex::new(Vec::new()));
        let install_calls = Arc::new(AtomicUsize::new(0));
        let observed_commit_order = Arc::clone(&commit_order);
        let observed_completion_order = Arc::clone(&completion_order);
        let step_hook = Arc::new(
            move |wallet: WalletAddress, step: usize, _engine: &mut BucketCommitEngine| {
                if step == 1 {
                    observed_commit_order
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push(wallet);
                    if wallet == preferred {
                        preferred_first_commit.add_permits(1);
                    } else {
                        other_first_commit.add_permits(1);
                    }
                }
                if step == 5 {
                    observed_completion_order
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push(wallet);
                    if wallet == preferred {
                        preferred_completion.add_permits(1);
                    }
                }
            },
        );
        let observed_install_calls = Arc::clone(&install_calls);
        let install_hook = Arc::new(move |_installs: &[AnchorInstall]| {
            observed_install_calls.fetch_add(1, Ordering::SeqCst);
        });
        let validator = validator
            .with_step_hook(step_hook)
            .with_install_hook(install_hook);

        let accepted = validator
            .validate_direct(&[first, second], &mut engine, &paper)
            .await
            .unwrap();
        assert_eq!(accepted.len(), 2);
        assert_eq!(install_calls.load(Ordering::SeqCst), 1);
        commit_orders.push(
            commit_order
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
        );
        completion_orders.push(
            completion_order
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
        );
        let metadata = accepted
            .iter()
            .map(|install| {
                serde_json::from_str::<Value>(&install.proof.document).unwrap()["metadata_reads"]
                    .clone()
            })
            .collect::<Vec<_>>();
        assert_ne!(metadata[0], metadata[1]);
        assert_eq!(metadata[0][0]["asset"], asset(1));
        assert_eq!(metadata[1][0]["asset"], asset(2));

        drop(validator);
        let metadata_entries = Reader::replay(&source_path)
            .unwrap()
            .map(|entry| entry.unwrap())
            .filter(|(_, envelope)| envelope.source_id.0 == GAMMA_MARKETS_SOURCE_ID)
            .collect::<HashMap<_, _>>();
        assert_eq!(metadata_entries.len(), 2);
        for proof in &metadata {
            let provenance = &proof[0];
            let sequence = provenance["source_log_sequence"].as_u64().unwrap();
            let canonical_hash = provenance["canonical_page_hash"].as_str().unwrap();
            let envelope = &metadata_entries[&pe_core_types::EventSeq(sequence)];
            let canonical_payload =
                serde_json::to_vec(&serde_json::from_slice::<Value>(&envelope.payload).unwrap())
                    .unwrap();
            assert_eq!(
                blake3::hash(&canonical_payload).to_hex().as_str(),
                canonical_hash
            );
        }

        let replayed = build_leader_ledger(&paper).unwrap();
        let replayed_engine = BucketCommitEngine::load(Arc::clone(&paper), replayed).unwrap();
        outcomes.push(
            [first, second]
                .into_iter()
                .map(|wallet| {
                    replayed_engine
                        .ledger()
                        .position(&wallet)
                        .and_then(|snapshot| {
                            snapshot.positions.get(&MarketOutcomeId::new(
                                MarketId(VenueMarketId(condition(if wallet == first {
                                    1
                                } else {
                                    2
                                }))),
                                OutcomeId(0),
                            ))
                        })
                        .map(|position| position.long_contracts.atomic())
                })
                .collect::<Vec<_>>(),
        );
    }
    assert_eq!(
        commit_orders,
        vec![vec![first, second], vec![second, first]]
    );
    assert_eq!(completion_orders, commit_orders);
    assert_eq!(outcomes[0], outcomes[1]);
    assert_eq!(outcomes[0], vec![Some(1_000_000), Some(2_000_000)]);
}

#[tokio::test]
async fn concurrent_shared_corrected_asset_replays_identically_in_both_completion_orders() {
    let first = wallet(0x79);
    let second = wallet(0x7a);
    let gamma = serde_json::to_vec(&vec![json!({
        "conditionId": condition(9),
        "clobTokenIds": ["other-outcome", asset(1)]
    })])
    .unwrap();
    let mut completion_orders = Vec::new();
    let mut replayed_balances = Vec::new();
    for preferred in [first, second] {
        let preferred_first_commit = Arc::new(tokio::sync::Semaphore::new(0));
        let other_first_commit = Arc::new(tokio::sync::Semaphore::new(0));
        let preferred_completion = Arc::new(tokio::sync::Semaphore::new(0));
        let fetcher = Arc::new(OrderedBracketFetcher {
            inner: QueueFetcher::with_gamma(
                stable_responses(&[(first, 1, "1.000000"), (second, 1, "2.000000")]),
                Some(gamma.clone()),
            ),
            wallets: [first, second],
            preferred,
            activity_calls: Mutex::new(HashMap::new()),
            first_activity: tokio::sync::Barrier::new(2),
            preferred_first_commit: Arc::clone(&preferred_first_commit),
            other_first_commit: Arc::clone(&other_first_commit),
            preferred_completion: Arc::clone(&preferred_completion),
        });
        let fetcher_trait: Arc<dyn ReconciliationFetcher> = fetcher.clone();
        let (_source_dir, source_path, validator) = recording_validator(fetcher_trait);
        let completion_order = Arc::new(Mutex::new(Vec::new()));
        let observed_completion_order = Arc::clone(&completion_order);
        let step_hook = Arc::new(
            move |wallet: WalletAddress, step: usize, _engine: &mut BucketCommitEngine| {
                if step == 1 {
                    if wallet == preferred {
                        preferred_first_commit.add_permits(1);
                    } else {
                        other_first_commit.add_permits(1);
                    }
                }
                if step == 5 {
                    observed_completion_order
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push(wallet);
                    if wallet == preferred {
                        preferred_completion.add_permits(1);
                    }
                }
            },
        );
        let validator = validator.with_step_hook(step_hook);
        let (_paper_dir, paper, mut engine) = fresh(&[first, second]);

        let accepted = validator
            .validate_direct(&[first, second], &mut engine, &paper)
            .await
            .unwrap();
        assert_eq!(accepted.len(), 2);
        completion_orders.push(
            completion_order
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
        );

        let metadata = accepted
            .iter()
            .map(|install| {
                serde_json::from_str::<Value>(&install.proof.document).unwrap()["metadata_reads"]
                    .clone()
            })
            .collect::<Vec<_>>();
        assert_eq!(metadata[0], metadata[1]);
        assert_eq!(metadata[0][0]["asset"], asset(1));
        assert_eq!(
            fetcher
                .inner
                .urls()
                .iter()
                .filter(|url| url.contains("/markets?clob_token_ids="))
                .count(),
            1
        );

        let verified_key =
            MarketOutcomeId::new(MarketId(VenueMarketId(condition(9))), OutcomeId(1));
        let stamped_key = MarketOutcomeId::new(MarketId(VenueMarketId(condition(1))), OutcomeId(0));
        let mut balances = Vec::new();
        for (wallet, amount) in [(first, 1_000_000), (second, 2_000_000)] {
            let groups = paper.activity_groups_after(&wallet, -1).unwrap();
            assert_eq!(groups.len(), 1);
            let document = serde_json::from_str::<Value>(&groups[0].proof_json).unwrap();
            assert_eq!(document["version"], 2);
            assert_eq!(document["correction"]["stamped"]["market"], condition(1));
            assert_eq!(document["correction"]["stamped"]["outcome"], 0);
            assert_eq!(document["correction"]["verified"]["market"], condition(9));
            assert_eq!(document["correction"]["verified"]["outcome"], 1);
            assert_eq!(document["effect"]["market"], condition(9));
            assert_eq!(document["effect"]["outcome"], 1);

            let replayed = replay_wallet_ledger(&paper, wallet).unwrap();
            let snapshot = replayed.position(&wallet).unwrap();
            assert!(!snapshot.positions.contains_key(&stamped_key));
            let position = snapshot.positions.get(&verified_key).unwrap();
            assert_eq!(position.long_contracts.atomic(), amount);
            assert_eq!(position.short_contracts, ShareAmount::ZERO);
            balances.push(position.long_contracts.atomic());
        }
        replayed_balances.push(balances);

        drop(validator);
        let provenance = &metadata[0][0];
        let sequence = provenance["source_log_sequence"].as_u64().unwrap();
        let canonical_hash = provenance["canonical_page_hash"].as_str().unwrap();
        let metadata_entry = Reader::replay(&source_path)
            .unwrap()
            .map(|entry| entry.unwrap())
            .find(|(event_sequence, envelope)| {
                event_sequence.0 == sequence && envelope.source_id.0 == GAMMA_MARKETS_SOURCE_ID
            })
            .expect("both proofs reference the one appended Gamma page");
        let canonical_payload = serde_json::to_vec(
            &serde_json::from_slice::<Value>(&metadata_entry.1.payload).unwrap(),
        )
        .unwrap();
        assert_eq!(
            blake3::hash(&canonical_payload).to_hex().as_str(),
            canonical_hash
        );
    }
    assert_eq!(
        completion_orders,
        vec![vec![first, second], vec![second, first]]
    );
    assert_eq!(replayed_balances[0], replayed_balances[1]);
    assert_eq!(replayed_balances[0], vec![1_000_000, 2_000_000]);
}

#[tokio::test]
async fn concurrent_fenced_and_deferred_wallets_do_not_abort_healthy_install() {
    let fenced = wallet(0x15);
    let deferred = wallet(0x16);
    let healthy = wallet(0x17);
    let (_dir, paper, mut engine) = fresh(&[fenced, deferred, healthy]);
    install_empty_anchor(&mut engine, &paper, fenced, 0);
    let mut responses = stable_responses(&[(healthy, 3, "4.000000")]);
    let mut underflow = activity(fenced, 1, "2.000000", "0xconcurrent-underflow", 10);
    underflow["type"] = json!("REDEEM");
    responses.insert(
        activity_url(fenced),
        vec![serde_json::to_vec(&vec![underflow]).unwrap()],
    );
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
    let fetcher = Arc::new(GatedFetcher::new(
        responses,
        vec![fenced, deferred, healthy],
        HashMap::from([(fenced, 4), (deferred, 2), (healthy, 0)]),
        None,
    ));
    let fetcher: Arc<dyn ReconciliationFetcher> = fetcher;
    let validator = validator_from_reconciliation(fetcher);

    let accepted = validator
        .validate_direct(&[fenced, deferred, healthy], &mut engine, &paper)
        .await
        .unwrap();
    assert_eq!(accepted.len(), 1);
    assert_eq!(accepted[0].wallet, healthy);
    assert!(paper.is_wallet_fenced(&fenced).unwrap());
    assert!(paper.position_validation(&fenced).unwrap().is_none());
    assert!(paper.position_validation(&deferred).unwrap().is_none());
    assert!(paper.position_validation(&healthy).unwrap().is_some());
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
        let hook = Arc::new(
            move |_wallet, step: usize, engine: &mut BucketCommitEngine| {
                if step == target_step {
                    engine
                        .commit(vec![mutation.clone()], &context(20), zero_basis())
                        .unwrap();
                }
            },
        );
        let result = validator(stable_responses(&[(wallet, 1, "1.000000")]))
            .with_step_hook(hook)
            .validate_direct_with_deferrals(&[wallet], &mut engine, &paper)
            .await;
        let error = if target_step == 5 {
            result
                .err()
                .expect("the atomic install race still fails boot")
        } else {
            let outcome = result.expect("wallet-scoped ledger changes defer before installation");
            assert!(outcome.accepted.is_empty());
            assert_eq!(outcome.deferred.len(), 1);
            let (deferred, error) = outcome.deferred.into_iter().next().unwrap();
            assert_eq!(deferred, wallet);
            assert_eq!(
                error.class(),
                pe_service::position_seeder::FailureClass::WalletTransient
            );
            error
        };
        assert!(matches!(
            error,
            CausalPositionError::LedgerRevision { .. } | CausalPositionError::AnchorInstall(_)
        ));
        assert_eq!(
            engine
                .ledger()
                .position(&wallet)
                .and_then(|snapshot| {
                    snapshot.positions.get(&MarketOutcomeId::new(
                        MarketId(VenueMarketId(condition(9))),
                        OutcomeId(0),
                    ))
                })
                .map(|state| state.long_contracts),
            Some(ShareAmount::from_atomic(1_000_000)),
            "the intervening poller BUY must survive the rejected anchor"
        );
        assert_eq!(paper.position_anchors(&wallet).unwrap().len(), 1);
        assert!(paper.position_validation(&wallet).unwrap().is_none());
        assert!(!paper.is_wallet_fenced(&wallet).unwrap());
    }
}

#[tokio::test]
async fn intervening_once_retries_then_accepts_and_clears_reanchor() {
    let wallet = wallet(0x2f);
    let (_dir, paper, mut engine) = fresh(&[wallet]);
    install_empty_anchor(&mut engine, &paper, wallet, 0);
    let mut redeem = activity(wallet, 9, "0", "0xreanchor-between-captures", 20);
    redeem["type"] = json!("REDEEM");
    redeem["usdcSize"] = json!("0");
    redeem["price"] = json!("0");
    redeem["side"] = json!("");
    let mutation = aggregate(redeem, wallet);
    let hook = Arc::new(
        move |_wallet, step: usize, engine: &mut BucketCommitEngine| {
            if step == 4 {
                engine
                    .commit(vec![mutation.clone()], &context(20), zero_basis())
                    .unwrap();
            }
        },
    );

    let accepted = validator(stable_responses_for_attempts(&[(wallet, 1, "1.000000")], 2))
        .with_step_hook(hook)
        .validate_direct(&[wallet], &mut engine, &paper)
        .await
        .unwrap();

    assert_eq!(accepted.len(), 1);
    assert_eq!(paper.position_anchors(&wallet).unwrap().len(), 2);
    assert!(!paper.wallet_coverage(&wallet).unwrap().reanchor_required);
    assert!(paper.position_validation(&wallet).unwrap().is_some());
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
            vec![
                activity.clone(),
                activity.clone(),
                activity.clone(),
                activity.clone(),
                activity.clone(),
                activity,
            ],
        ),
        (
            position_url(wallet, PositionPartition::NotRedeemable),
            vec![first, second.clone(), second.clone(), second],
        ),
        (
            position_url(wallet, PositionPartition::Redeemable),
            vec![
                b"[]".to_vec(),
                b"[]".to_vec(),
                b"[]".to_vec(),
                b"[]".to_vec(),
            ],
        ),
    ]);
    let accepted = validator(responses)
        .validate_direct(&[wallet], &mut engine, &paper)
        .await
        .expect("the one bounded retry accepts stable position semantics");
    assert_eq!(accepted.len(), 1);
    assert!(!paper.is_wallet_fenced(&wallet).unwrap());
    assert!(paper.position_validation(&wallet).unwrap().is_some());
}

#[tokio::test]
async fn position_revision_twice_defers_after_one_retry() {
    let wallet = wallet(0x45);
    let (_dir, paper, mut engine) = fresh(&[wallet]);
    let activity =
        serde_json::to_vec(&vec![activity(wallet, 1, "1.000000", "0xbase", 10)]).unwrap();
    let positions = ["1", "2", "3", "4"]
        .into_iter()
        .map(|amount| serde_json::to_vec(&vec![position(wallet, 1, amount)]).unwrap())
        .collect::<Vec<_>>();
    let responses = HashMap::from([
        (
            activity_url(wallet),
            vec![
                activity.clone(),
                activity.clone(),
                activity.clone(),
                activity.clone(),
                activity.clone(),
                activity,
            ],
        ),
        (
            position_url(wallet, PositionPartition::NotRedeemable),
            positions,
        ),
        (
            position_url(wallet, PositionPartition::Redeemable),
            vec![
                b"[]".to_vec(),
                b"[]".to_vec(),
                b"[]".to_vec(),
                b"[]".to_vec(),
            ],
        ),
    ]);

    let outcome = validator(responses)
        .validate_direct_with_deferrals(&[wallet], &mut engine, &paper)
        .await
        .unwrap();
    assert!(outcome.accepted.is_empty());
    assert_eq!(outcome.deferred.len(), 1);
    assert_eq!(outcome.deferred[0].0, wallet);
    assert_eq!(
        outcome.deferred[0].1.class(),
        pe_service::position_seeder::FailureClass::WalletTransient
    );
    assert!(paper.position_validation(&wallet).unwrap().is_none());
    assert!(!paper.is_wallet_fenced(&wallet).unwrap());
}

#[tokio::test]
async fn intervening_twice_defers_after_one_retry() {
    let wallet = wallet(0x46);
    let (_dir, paper, mut engine) = fresh(&[wallet]);
    install_empty_anchor(&mut engine, &paper, wallet, 0);
    let mutations = [("0xintervening-first", 20), ("0xintervening-second", 21)]
        .into_iter()
        .map(|(transaction_hash, epoch)| {
            let mut redeem = activity(wallet, 9, "0", transaction_hash, epoch);
            redeem["type"] = json!("REDEEM");
            redeem["usdcSize"] = json!("0");
            redeem["price"] = json!("0");
            redeem["side"] = json!("");
            aggregate(redeem, wallet)
        })
        .collect::<VecDeque<_>>();
    let mutations = Arc::new(Mutex::new(mutations));
    let hook = Arc::new(
        move |_wallet, step: usize, engine: &mut BucketCommitEngine| {
            if step == 4
                && let Some(mutation) = mutations
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .pop_front()
            {
                let epoch = mutation.source_time.0.unix_timestamp();
                engine
                    .commit(vec![mutation], &context(epoch), zero_basis())
                    .unwrap();
            }
        },
    );

    let accepted = validator(stable_responses_for_attempts(&[(wallet, 1, "1.000000")], 2))
        .with_step_hook(hook)
        .validate_direct(&[wallet], &mut engine, &paper)
        .await
        .unwrap();
    assert!(accepted.is_empty());
    assert!(paper.position_validation(&wallet).unwrap().is_none());
    assert!(!paper.wallet_coverage(&wallet).unwrap().reanchor_required);
    assert!(!paper.is_wallet_fenced(&wallet).unwrap());
}

#[tokio::test]
async fn durable_fence_between_attempts_stops_the_retry() {
    let wallet = wallet(0x47);
    let (dir, paper, mut engine) = fresh(&[wallet]);
    let activity =
        serde_json::to_vec(&vec![activity(wallet, 1, "1.000000", "0xbase", 10)]).unwrap();
    let first = serde_json::to_vec(&vec![position(wallet, 1, "1")]).unwrap();
    let second = serde_json::to_vec(&vec![position(wallet, 1, "2")]).unwrap();
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
    let database_path = dir.path().join("paper.db");
    let inserted = Arc::new(Mutex::new(false));
    let hook = Arc::new(
        move |_wallet, step: usize, _engine: &mut BucketCommitEngine| {
            let mut inserted = inserted
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if step == 4 && !*inserted {
                rusqlite::Connection::open(&database_path)
                    .unwrap()
                    .execute(
                        "INSERT INTO wallet_fences
                     (wallet_hex, source_trade_id, cause, proof_json, fenced_at_unix)
                     VALUES (?1, 'g2:test-fence', 'test_fence', '{}', 100)",
                        [wallet.to_string()],
                    )
                    .unwrap();
                *inserted = true;
            }
        },
    );

    let accepted = validator(responses)
        .with_step_hook(hook)
        .validate_direct(&[wallet], &mut engine, &paper)
        .await
        .unwrap();
    assert!(accepted.is_empty());
    assert!(paper.is_wallet_fenced(&wallet).unwrap());
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
                start: Some(1),
                offset: 0,
            }
            .url(BASE),
            vec![activity.clone()],
        ),
        (
            PolymarketEndpoint::UserPositionActivityPage {
                user: wallet.to_string(),
                end: 99,
                start: Some(1),
                offset: 0,
            }
            .url(BASE),
            vec![activity.clone()],
        ),
        (
            PolymarketEndpoint::UserPositionActivityPage {
                user: wallet.to_string(),
                end: 101,
                start: Some(1),
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
    let validator = validator(responses).with_clock(Arc::new(move || {
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

fn mapping_arrives_on_next_walk(wallet: WalletAddress) -> HashMap<String, Vec<Vec<u8>>> {
    let row = serde_json::to_vec(&vec![activity(wallet, 1, "1.000000", "0xlate-map", 10)]).unwrap();
    let positions = serde_json::to_vec(&vec![position(wallet, 1, "1.000000")]).unwrap();
    HashMap::from([
        (
            activity_url(wallet),
            vec![b"[]".to_vec(), row.clone(), row.clone(), row.clone(), row],
        ),
        (
            position_url(wallet, PositionPartition::NotRedeemable),
            vec![positions.clone(), positions.clone(), positions],
        ),
        (
            position_url(wallet, PositionPartition::Redeemable),
            vec![b"[]".to_vec(); 3],
        ),
    ])
}

#[tokio::test]
async fn direct_missing_mapping_waits_for_next_walk_then_retries_without_changing_proof() {
    let wallet = wallet(0x60);
    let (dir, paper, mut engine) = fresh(&[wallet]);
    let fetcher = Arc::new(QueueFetcher::new(mapping_arrives_on_next_walk(wallet)));
    let accepted = validator_from_fetcher(Arc::clone(&fetcher))
        .validate_direct(&[wallet], &mut engine, &paper)
        .await
        .unwrap();
    assert_eq!(accepted.len(), 1);
    assert!(anchor_proves_full_history(&accepted[0].proof.document));
    assert_eq!(
        fetcher
            .urls()
            .iter()
            .filter(|url| **url == activity_url(wallet))
            .count(),
        5
    );
    assert!(!paper.position_anchors(&wallet).unwrap().is_empty());
    drop(engine);
    drop(paper);
    let reopened = PaperStateDb::open(&dir.path().join("paper.db")).unwrap();
    assert!(reopened.position_validation_current(&wallet).unwrap());
}

#[tokio::test]
async fn control_missing_mapping_waits_for_next_walk_then_retries() {
    let wallet = wallet(0x63);
    let (_dir, paper, engine) = fresh(&[wallet]);
    let (control_tx, control_rx) = mpsc::channel(8);
    let actor = spawn_control_actor(control_rx, engine, Arc::clone(&paper));
    let fetcher = Arc::new(QueueFetcher::new(mapping_arrives_on_next_walk(wallet)));
    let outcomes = validator_from_fetcher(Arc::clone(&fetcher))
        .validate_via_control(
            &[wallet],
            &control_tx,
            &paper,
            pe_service::position_seeder::ValidationPurpose::CatchUp,
            None,
        )
        .await;
    assert!(outcomes.shared.is_none());
    assert!(outcomes.deferred.is_empty());
    assert_eq!(outcomes.accepted.len(), 1);
    assert!(anchor_proves_full_history(
        &outcomes.accepted[0].proof.document
    ));
    assert_eq!(
        fetcher
            .urls()
            .iter()
            .filter(|url| **url == activity_url(wallet))
            .count(),
        5
    );
    drop(control_tx);
    actor.await.unwrap();
}

/// PASS: receipt, raw-payload, numeric presentation, and field-order changes within one explicit
/// position partition preserve the semantic proof and install the anchor.
/// FAIL: non-semantic wire presentation changes trigger a retry, fence, or failed installation.
#[tokio::test]
async fn presentation_receipt_raw_hash_and_field_layout_changes_still_accept() {
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
        "negativeRisk": true,
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
            vec![first, second],
        ),
        (
            position_url(wallet, PositionPartition::Redeemable),
            vec![b"[]".to_vec(), b"[]".to_vec()],
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
async fn mixed_activity_combo_classification_defers_with_named_asset() {
    let wallet = wallet(0x60);
    let (_dir, paper, engine) = fresh(&[wallet]);
    let ordinary = activity(wallet, 1, "1.000000", "0xmixed-ordinary", 10);
    let mut combo = activity(wallet, 1, "1.000000", "0xmixed-combo", 11);
    combo["isCombo"] = json!(true);
    let activity = serde_json::to_vec(&vec![ordinary, combo]).unwrap();
    let responses = HashMap::from([(
        activity_url(wallet),
        vec![activity.clone(), activity.clone(), activity],
    )]);
    let (control_tx, control_rx) = mpsc::channel(2);
    let actor = spawn_control_actor(control_rx, engine, Arc::clone(&paper));
    let preparer =
        AdmissionPreparer::with_validator(control_tx, Arc::clone(&paper), validator(responses));

    let outcome = preparer.prepare(&[wallet]).await.unwrap();
    assert!(outcome.admitted.is_empty());
    assert_eq!(outcome.deferred.len(), 1);
    assert_eq!(
        outcome.deferred[0].class,
        pe_service::position_seeder::FailureClass::WalletPersistent
    );
    assert!(paper.position_validation(&wallet).unwrap().is_none());
    drop(preparer);
    actor.await.unwrap();
}

#[tokio::test]
async fn metadata_unresolved_activity_only_asset_is_raw_only_and_wallet_anchors() {
    let wallet = wallet(0x74);
    let (_dir, paper, engine) = fresh(&[wallet]);
    let activity = serde_json::to_vec(&vec![activity(
        wallet,
        1,
        "1.000000",
        "0xmetadata-unresolved",
        10,
    )])
    .unwrap();
    let responses = HashMap::from([
        (
            activity_url(wallet),
            vec![activity.clone(), activity.clone(), activity],
        ),
        (
            position_url(wallet, PositionPartition::NotRedeemable),
            vec![b"[]".to_vec(), b"[]".to_vec()],
        ),
        (
            position_url(wallet, PositionPartition::Redeemable),
            vec![b"[]".to_vec(), b"[]".to_vec()],
        ),
    ]);
    let fetcher = Arc::new(QueueFetcher::with_gamma(responses, Some(b"[]".to_vec())));
    let (control_tx, control_rx) = mpsc::channel(2);
    let actor = spawn_control_actor(control_rx, engine, Arc::clone(&paper));
    let preparer = AdmissionPreparer::with_validator(
        control_tx,
        Arc::clone(&paper),
        validator_from_fetcher(fetcher),
    );

    preparer.prepare(&[wallet]).await.unwrap();
    let groups = paper.activity_groups_after(&wallet, -1).unwrap();
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].disposition, "raw_only");
    assert_eq!(
        paper
            .no_copy_disposition(&groups[0].source_trade_id)
            .unwrap()
            .unwrap()
            .2,
        "identity_unresolved"
    );
    let coverage = paper.wallet_coverage(&wallet).unwrap();
    assert_eq!(coverage.coverage_generation, 0);
    assert!(!coverage.reanchor_required);
    assert_eq!(coverage.anchor_seq, Some(0));
    assert!(paper.position_validation_current(&wallet).unwrap());
    drop(preparer);
    actor.await.unwrap();
}

#[tokio::test]
async fn conflicting_stamped_identities_without_metadata_are_raw_only_and_anchor() {
    let wallet = wallet(0x75);
    let (_dir, paper, engine) = fresh(&[wallet]);
    let first = activity(wallet, 1, "1.000000", "0xstamped-zero", 10);
    let mut second = activity(wallet, 1, "1.000000", "0xstamped-one", 11);
    second["outcomeIndex"] = json!(1);
    let activity = serde_json::to_vec(&vec![first, second]).unwrap();
    let responses = HashMap::from([
        (
            activity_url(wallet),
            vec![activity.clone(), activity.clone(), activity],
        ),
        (
            position_url(wallet, PositionPartition::NotRedeemable),
            vec![b"[]".to_vec(), b"[]".to_vec()],
        ),
        (
            position_url(wallet, PositionPartition::Redeemable),
            vec![b"[]".to_vec(), b"[]".to_vec()],
        ),
    ]);
    let fetcher = Arc::new(QueueFetcher::with_gamma(responses, Some(b"[]".to_vec())));
    let (control_tx, control_rx) = mpsc::channel(2);
    let actor = spawn_control_actor(control_rx, engine, Arc::clone(&paper));
    let preparer = AdmissionPreparer::with_validator(
        control_tx,
        Arc::clone(&paper),
        validator_from_fetcher(fetcher),
    );

    preparer.prepare(&[wallet]).await.unwrap();
    let groups = paper.activity_groups_after(&wallet, -1).unwrap();
    assert_eq!(groups.len(), 2);
    assert!(groups.iter().all(|group| group.disposition == "raw_only"));
    assert!(groups.iter().all(|group| {
        paper
            .no_copy_disposition(&group.source_trade_id)
            .unwrap()
            .is_some_and(|(_, _, reason)| reason == "identity_unresolved")
    }));
    let coverage = paper.wallet_coverage(&wallet).unwrap();
    assert_eq!(coverage.coverage_generation, 0);
    assert!(!coverage.reanchor_required);
    assert_eq!(coverage.anchor_seq, Some(0));
    drop(preparer);
    actor.await.unwrap();
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
                    let result = engine.install_anchors(&installs);
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
                                context.as_ref(),
                                pe_service::bucket_commit::FrozenDecisionBasis {
                                    win_rate_p: pe_core_types::Probability::ZERO,
                                    bankroll: rust_decimal::Decimal::ZERO,
                                },
                            )
                            .map_err(|error| error.to_string()),
                    );
                }
                _ => {}
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

#[tokio::test]
async fn periodic_refresh_skips_an_existing_fence_but_genuine_admission_stays_fatal() {
    let wallet = wallet(0x76);
    let (dir, paper, _engine) = fresh(&[wallet]);
    let connection = rusqlite::Connection::open(dir.path().join("paper.db")).unwrap();
    connection
        .execute(
            "INSERT INTO wallet_fences \
             (wallet_hex, source_trade_id, cause, proof_json, fenced_at_unix) \
             VALUES (?1, 'group', 'test', '{}', 1)",
            [wallet.to_string()],
        )
        .unwrap();
    let (control_tx, _control_rx) = mpsc::channel(1);
    let preparer = AdmissionPreparer::new(control_tx, Arc::clone(&paper));

    assert_eq!(
        preparer.prepare_if_due(wallet, END, 1).await.unwrap(),
        AnchorRefreshOutcome::Skipped
    );
    let outcome = preparer.prepare(&[wallet]).await.unwrap();
    assert!(outcome.admitted.is_empty());
    assert_eq!(outcome.deferred[0].kind, "fence.active");
}

#[tokio::test]
async fn periodic_refresh_skips_a_wallet_newly_fenced_inside_the_bracket() {
    let wallet = wallet(0x77);
    let (_dir, paper, mut engine) = fresh(&[wallet]);
    install_empty_anchor(&mut engine, &paper, wallet, 0);
    let mut revised = activity(wallet, 1, "1.000000", "0xrefresh-revision", 10);
    engine
        .commit(
            vec![aggregate(revised.clone(), wallet)],
            &context(10),
            zero_basis(),
        )
        .unwrap();
    revised["size"] = json!("2.000000");
    let mut retry_revision = revised.clone();
    retry_revision["size"] = json!("3.000000");
    let responses = HashMap::from([(
        activity_url(wallet),
        vec![
            serde_json::to_vec(&vec![revised]).unwrap(),
            serde_json::to_vec(&vec![retry_revision]).unwrap(),
        ],
    )]);
    let fetcher = Arc::new(QueueFetcher::new(responses));
    let (control_tx, control_rx) = mpsc::channel(2);
    let actor = spawn_control_actor(control_rx, engine, Arc::clone(&paper));
    let preparer = AdmissionPreparer::with_validator(
        control_tx,
        Arc::clone(&paper),
        validator_from_fetcher(Arc::clone(&fetcher)),
    );
    let anchors_before = paper.position_anchors(&wallet).unwrap();

    // A recoverable fence permits one retry; another novel revision defers without installing.
    assert_eq!(
        preparer.prepare_if_due(wallet, END, 1).await.unwrap(),
        AnchorRefreshOutcome::Deferred
    );
    assert!(paper.is_wallet_fenced(&wallet).unwrap());
    assert_eq!(paper.position_anchors(&wallet).unwrap(), anchors_before);
    assert_eq!(
        fetcher
            .urls()
            .iter()
            .filter(|url| url.contains("/activity?"))
            .count(),
        2
    );
    drop(preparer);
    actor.await.unwrap();
}

#[tokio::test]
async fn paper_service_rollout_periodic_refresh_recovers_a_stable_revision_on_bounded_retry() {
    let wallet = wallet(0x77);
    let (_dir, paper, mut engine) = fresh(&[wallet]);
    install_empty_anchor(&mut engine, &paper, wallet, 0);
    let mut revised = activity(wallet, 1, "1.000000", "0xrefresh-revision", 10);
    let original = aggregate(revised.clone(), wallet);
    let id = original.group_id.key().clone();
    engine
        .commit(vec![original], &context(10), zero_basis())
        .unwrap();
    let original_state = paper.activity_group_state(&id).unwrap();
    let pending = paper.decision_pending_history().unwrap();
    let anchors_before = paper.position_anchors(&wallet).unwrap();
    revised["size"] = json!("2.000000");
    let revision = aggregate(revised.clone(), wallet).semantic_revision;
    let mut responses = stable_responses(&[(wallet, 1, "2.000000")]);
    responses.insert(
        activity_url(wallet),
        vec![serde_json::to_vec(&vec![revised]).unwrap(); 4],
    );
    let fetcher = Arc::new(QueueFetcher::new(responses));
    let (control_tx, control_rx) = mpsc::channel(2);
    let actor = spawn_control_actor(control_rx, engine, Arc::clone(&paper));
    let preparer = AdmissionPreparer::with_validator(
        control_tx,
        Arc::clone(&paper),
        validator_from_fetcher(Arc::clone(&fetcher)),
    );

    // The first read retains R1 and fences; the bounded retry sees only that disposed revision.
    assert_eq!(
        preparer.prepare_if_due(wallet, END, 1).await.unwrap(),
        AnchorRefreshOutcome::Anchored
    );
    assert!(paper.wallet_fence(&wallet).unwrap().is_none());
    let anchors = paper.position_anchors(&wallet).unwrap();
    assert_eq!(anchors.len(), anchors_before.len() + 1);
    assert_eq!(&anchors[..anchors_before.len()], anchors_before);
    let anchor = anchors.last().unwrap();
    assert_eq!(anchor.activity_cutoff_unix, END);
    assert_full_history_proof(&anchor.proof_json);
    let proof: Value = serde_json::from_str(&anchor.proof_json).unwrap();
    assert_eq!(proof["cleared_fence"]["cause"], "revised_applied_aggregate");
    assert_eq!(proof["cleared_fence"]["source_trade_id"], id.0);
    assert!(paper.position_validation_current(&wallet).unwrap());
    let coverage = paper.wallet_coverage(&wallet).unwrap();
    assert_eq!(coverage.activity_cutoff_unix, Some(END));
    assert_eq!(coverage.anchor_seq, Some(anchor.anchor_seq));
    assert!(!coverage.reanchor_required);
    let ledger = build_leader_ledger(&paper).unwrap();
    assert_eq!(
        ledger.position(&wallet).unwrap().positions
            [&MarketOutcomeId::new(MarketId(VenueMarketId(condition(1))), OutcomeId(0),)]
            .long_contracts,
        ShareAmount::from_atomic(2_000_000)
    );
    assert_eq!(paper.activity_group_state(&id).unwrap(), original_state);
    assert_eq!(
        paper
            .activity_revision_state(&id, revision.as_str())
            .unwrap()
            .unwrap()
            .disposition,
        "revised_applied_aggregate"
    );
    assert_eq!(paper.decision_pending_history().unwrap(), pending);
    assert_eq!(
        fetcher
            .urls()
            .iter()
            .filter(|url| url.contains("/activity?"))
            .count(),
        4
    );
    drop(preparer);
    actor.await.unwrap();
}

#[test]
fn anchor_transaction_failure_preserves_engine_before_retry_and_rejects_regressed_cutoff() {
    let wallet = wallet(0x66);
    let (dir, paper, mut engine) = fresh(&[wallet]);
    paper.set_cursor(&wallet, 10).unwrap();
    let before = ledger_capture(engine.ledger(), &paper, wallet).unwrap();
    let balance = (
        MarketId(VenueMarketId(condition(1))),
        OutcomeId(0),
        ShareAmount::from_atomic(3_000_000),
    );
    let install = AnchorInstall {
        newest_activity_unix: None,
        fresh_history: Vec::new(),
        expected_fence: None,
        history_status: None,
        wallet,
        balances: vec![balance.clone()],
        cutoff: 10,
        proof: AnchorProof {
            positions_proof_hash: "positions".to_owned(),
            activity_bounds_json: "[]".to_owned(),
            source_log_generation: "scenario".to_owned(),
            document: "{}".to_owned(),
            recorded_at_unix: 100,
        },
        expected: AnchorExpectation {
            ledger_hash: before.hash.clone(),
            cursor: before.cursor,
            anchor_seq: before.anchor_seq,
            coverage_generation: before.coverage_generation,
        },
    };

    let trigger = rusqlite::Connection::open(dir.path().join("paper.db")).unwrap();
    trigger
        .execute_batch(
            "CREATE TRIGGER fail_anchor_validation
             BEFORE INSERT ON position_validations
             BEGIN SELECT RAISE(FAIL, 'injected anchor transaction failure'); END;",
        )
        .unwrap();
    assert!(matches!(
        engine.install_anchors(std::slice::from_ref(&install)),
        Err(pe_service::bucket_commit::AnchorInstallError::Durability(_))
    ));
    assert_eq!(
        ledger_capture(engine.ledger(), &paper, wallet)
            .unwrap()
            .hash,
        before.hash
    );
    assert!(paper.position_anchors(&wallet).unwrap().is_empty());
    assert!(paper.leader_positions().unwrap().is_empty());
    assert!(paper.position_validation(&wallet).unwrap().is_none());

    trigger
        .execute_batch("DROP TRIGGER fail_anchor_validation;")
        .unwrap();
    engine.install_anchors(&[install]).unwrap();
    let installed = ledger_capture(engine.ledger(), &paper, wallet).unwrap();
    assert_eq!(
        engine
            .ledger()
            .position(&wallet)
            .and_then(|snapshot| {
                snapshot.positions.get(&MarketOutcomeId::new(
                    MarketId(VenueMarketId(condition(1))),
                    OutcomeId(0),
                ))
            })
            .map(|state| state.long_contracts),
        Some(balance.2)
    );
    let durable_before_regression = (
        paper.position_anchors(&wallet).unwrap(),
        paper.leader_positions().unwrap(),
        paper.position_validation(&wallet).unwrap(),
        paper.wallet_coverage(&wallet).unwrap(),
    );
    let regressed = AnchorInstall {
        newest_activity_unix: None,
        fresh_history: Vec::new(),
        expected_fence: None,
        history_status: None,
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
fn cursor_and_anchor_sequence_cas_reject_stale_expectations() {
    let cursor_wallet = wallet(0x6d);
    let (_dir, paper, mut engine) = fresh(&[cursor_wallet]);
    paper.set_cursor(&cursor_wallet, 10).unwrap();
    let captured = ledger_capture(engine.ledger(), &paper, cursor_wallet).unwrap();
    let candidate = AnchorInstall {
        newest_activity_unix: None,
        fresh_history: Vec::new(),
        expected_fence: None,
        history_status: None,
        wallet: cursor_wallet,
        balances: Vec::new(),
        cutoff: 20,
        proof: AnchorProof {
            positions_proof_hash: "cursor".to_owned(),
            activity_bounds_json: "[]".to_owned(),
            source_log_generation: "scenario".to_owned(),
            document: "{}".to_owned(),
            recorded_at_unix: 20,
        },
        expected: AnchorExpectation {
            ledger_hash: captured.hash,
            cursor: captured.cursor,
            anchor_seq: captured.anchor_seq,
            coverage_generation: captured.coverage_generation,
        },
    };
    paper.set_cursor(&cursor_wallet, 11).unwrap();
    assert!(matches!(
        engine.install_anchors(&[candidate]),
        Err(pe_service::bucket_commit::AnchorInstallError::CursorChanged { .. })
    ));

    let wallet = wallet(0x6e);
    let (_dir, paper, mut engine) = fresh(&[wallet]);
    install_empty_anchor(&mut engine, &paper, wallet, 10);
    let captured = ledger_capture(engine.ledger(), &paper, wallet).unwrap();
    let candidate = AnchorInstall {
        newest_activity_unix: None,
        fresh_history: Vec::new(),
        expected_fence: None,
        history_status: None,
        wallet,
        balances: Vec::new(),
        cutoff: 10,
        proof: AnchorProof {
            positions_proof_hash: "anchor-sequence".to_owned(),
            activity_bounds_json: "[]".to_owned(),
            source_log_generation: "scenario".to_owned(),
            document: "{}".to_owned(),
            recorded_at_unix: 11,
        },
        expected: AnchorExpectation {
            ledger_hash: captured.hash,
            cursor: captured.cursor,
            anchor_seq: captured.anchor_seq,
            coverage_generation: captured.coverage_generation,
        },
    };
    install_empty_anchor(&mut engine, &paper, wallet, 10);
    assert!(matches!(
        engine.install_anchors(&[candidate]),
        Err(pe_service::bucket_commit::AnchorInstallError::AnchorSeqChanged { .. })
    ));
}

#[test]
fn covered_late_generation_change_rejects_an_otherwise_unchanged_anchor() {
    let wallet = wallet(0x67);
    let (_dir, paper, mut engine) = fresh(&[wallet]);
    install_empty_anchor(&mut engine, &paper, wallet, 100);
    let captured = ledger_capture(engine.ledger(), &paper, wallet).unwrap();
    let candidate = AnchorInstall {
        newest_activity_unix: None,
        fresh_history: Vec::new(),
        expected_fence: None,
        history_status: None,
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
        // Issue #594: the observed venue row (price 3.1968021978) defers only that wallet.
        CausalPositionError::Activity {
            wallet,
            source: ActivityReadError::Parse(ActivityParseError::InvalidRow {
                row_index: 419,
                source: ActivityValidationError::InvalidPrice {
                    value: rust_decimal::Decimal::from_str_exact("3.1968021978").unwrap(),
                    reason: "Price value out of range".to_owned(),
                },
            }),
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
            source: PositionReadError::MixedActivityClassification {
                asset: "mixed-classification".to_owned(),
            },
        },
        CausalPositionError::Positions {
            wallet,
            source: PositionReadError::MetadataUnresolved {
                asset: "metadata-unresolved".to_owned(),
                reason: "absent".to_owned(),
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
        CausalPositionError::Activity {
            wallet,
            source: ActivityReadError::Parse(ActivityParseError::Json {
                message: "truncated".to_owned(),
            }),
        },
        CausalPositionError::Activity {
            wallet,
            source: ActivityReadError::Parse(ActivityParseError::WindowInvalidated(
                ActivityWindowInvalidation::MissingWallet { row_index: 0 },
            )),
        },
        CausalPositionError::Activity {
            wallet,
            source: ActivityReadError::Parse(ActivityParseError::InvalidRow {
                row_index: 1,
                source: ActivityValidationError::InvalidActivityType {
                    value: "MYSTERY".to_owned(),
                },
            }),
        },
        CausalPositionError::Activity {
            wallet,
            source: ActivityReadError::Aggregate(
                pe_source_polymarket_public::activity::ActivityAggregationError::Identity(
                    ActivityIdentityError::ComponentTooLong,
                ),
            ),
        },
        CausalPositionError::Activity {
            wallet,
            source: ActivityReadError::Identity(ActivityIdentityError::ComponentTooLong),
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
async fn paper_state_transaction_error_remains_boot_fatal_without_swapping() {
    let wallet = wallet(0x73);
    let (dir, paper, mut engine) = fresh(&[wallet]);
    let before = ledger_capture(engine.ledger(), &paper, wallet).unwrap();
    let trigger = rusqlite::Connection::open(dir.path().join("paper.db")).unwrap();
    trigger
        .execute_batch(
            "CREATE TRIGGER fail_activity_group
             BEFORE INSERT ON activity_groups
             BEGIN SELECT RAISE(FAIL, 'injected activity transaction failure'); END;",
        )
        .unwrap();

    let error = validator(stable_responses(&[(wallet, 1, "1.000000")]))
        .validate_direct(&[wallet], &mut engine, &paper)
        .await
        .unwrap_err();
    assert!(matches!(error, CausalPositionError::BucketCommit { .. }));
    assert_eq!(
        ledger_capture(engine.ledger(), &paper, wallet)
            .unwrap()
            .hash,
        before.hash
    );
    assert_eq!(paper.cursor(&wallet).unwrap(), None);
    assert!(paper.position_anchors(&wallet).unwrap().is_empty());
}

#[tokio::test]
async fn five_wallet_prepare_defers_mapping_failure_and_preserves_accepted_order() {
    let healthy = [wallet(0x6b), wallet(0x6f), wallet(0x71)];
    let deferred = wallet(0x6c);
    let raced = wallet(0x70);
    let (_dir, paper, engine) = fresh(&[]);
    let mut responses = stable_responses(&healthy.map(|wallet| (wallet, 1, "1.000000")));
    let raced_activity =
        serde_json::to_vec(&vec![activity(raced, 1, "1.000000", "0xraced", 10)]).unwrap();
    responses.insert(activity_url(raced), vec![raced_activity; 6]);
    responses.insert(
        position_url(raced, PositionPartition::NotRedeemable),
        ["1", "2", "3", "4"]
            .into_iter()
            .map(|amount| serde_json::to_vec(&vec![position(raced, 1, amount)]).unwrap())
            .collect(),
    );
    responses.insert(
        position_url(raced, PositionPartition::Redeemable),
        vec![b"[]".to_vec(); 4],
    );
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
    let fetcher = Arc::new(QueueFetcher::new(responses));
    let preparer = AdmissionPreparer::with_validator(
        control_tx,
        Arc::clone(&paper),
        validator_from_fetcher(Arc::clone(&fetcher)),
    );
    let outcome = preparer
        .prepare(&[healthy[0], deferred, raced, healthy[1], healthy[2]])
        .await
        .unwrap();
    assert_eq!(outcome.admitted, healthy);
    assert_eq!(outcome.deferred.len(), 2);
    assert_eq!(outcome.deferred[0].wallet, deferred);
    assert_eq!(
        outcome.deferred[0].kind,
        "positions.missing_activity_mapping"
    );
    assert_eq!(outcome.deferred[1].wallet, raced);
    assert_eq!(outcome.deferred[1].kind, "validation.position_revision");
    for wallet in healthy {
        assert!(!paper.position_anchors(&wallet).unwrap().is_empty());
        assert!(paper.wallet_history_complete(&wallet).unwrap());
    }
    assert!(paper.position_anchors(&deferred).unwrap().is_empty());
    assert!(!paper.wallet_history_complete(&deferred).unwrap());
    assert!(paper.position_anchors(&raced).unwrap().is_empty());
    let urls = fetcher.urls();
    for wallet in [healthy[0], deferred, raced, healthy[1], healthy[2]] {
        let activity = activity_url(wallet);
        assert_eq!(
            urls.iter()
                .filter(|url| url.as_str() == activity.as_str())
                .count(),
            if wallet == deferred {
                2 // The missing mapping is checked against the next required full walk.
            } else if wallet == raced {
                6
            } else {
                3
            },
            "wallet {wallet} was bracketed more than once"
        );
    }
    let raced_positions = position_url(raced, PositionPartition::NotRedeemable);
    assert_eq!(
        urls.iter()
            .filter(|url| url.as_str() == raced_positions.as_str())
            .count(),
        4,
        "the raced wallet was retried more than once"
    );
    drop(preparer);
    actor.await.unwrap();
}

#[tokio::test]
async fn anchor_install_ledger_race_defers_one_wallet_and_durability_aborts() {
    for durability in [false, true] {
        let (raced, healthy) = (wallet(0x72), wallet(0x73));
        let (_dir, paper, mut engine) = fresh(&[]);
        let (control_tx, mut control_rx) = mpsc::channel(2);
        let actor_paper = Arc::clone(&paper);
        let actor = tokio::spawn(async move {
            let mut install_calls = Vec::new();
            while let Some(command) = control_rx.recv().await {
                match command {
                    OrchestratorControl::PrepareAdmissions { acknowledged, .. } => {
                        acknowledged.send(()).unwrap();
                    }
                    OrchestratorControl::CaptureAdmissionLedger { wallet, captured } => {
                        captured
                            .send(
                                ledger_capture(engine.ledger(), &actor_paper, wallet)
                                    .map_err(|error| error.to_string()),
                            )
                            .unwrap();
                    }
                    OrchestratorControl::CommitActivityBucket {
                        aggregates,
                        context,
                        committed,
                    } => {
                        committed
                            .send(
                                engine
                                    .commit(aggregates, context.as_ref(), zero_basis())
                                    .map_err(|error| error.to_string()),
                            )
                            .unwrap();
                    }
                    OrchestratorControl::InstallAnchors {
                        installs,
                        acknowledged,
                    } => {
                        install_calls.push(
                            installs
                                .iter()
                                .map(|install| install.wallet)
                                .collect::<Vec<_>>(),
                        );
                        let result = if durability {
                            Err(pe_service::bucket_commit::AnchorInstallError::Durability(
                                "injected persistence failure".to_owned(),
                            ))
                        } else if install_calls.len() == 1 {
                            Err(
                                pe_service::bucket_commit::AnchorInstallError::LedgerHashChanged {
                                    wallet: raced,
                                },
                            )
                        } else {
                            engine.install_anchors(&installs)
                        };
                        acknowledged.send(result).unwrap();
                    }
                    _ => unreachable!("admission test sent an unrelated control command"),
                }
            }
            install_calls
        });
        let preparer = AdmissionPreparer::with_validator(
            control_tx,
            Arc::clone(&paper),
            validator(stable_responses(&[(raced, 1, "1"), (healthy, 2, "1")])),
        );
        if durability {
            let abort = preparer.prepare(&[raced, healthy]).await.unwrap_err();
            assert!(matches!(abort.cause, AdmissionError::ValidationInstall(_)));
            assert!(abort.deferred.is_empty());
            assert!(paper.position_anchors(&raced).unwrap().is_empty());
            assert!(paper.position_anchors(&healthy).unwrap().is_empty());
        } else {
            let outcome = preparer.prepare(&[raced, healthy]).await.unwrap();
            assert_eq!(outcome.admitted, vec![healthy]);
            assert_eq!(outcome.deferred.len(), 1);
            assert_eq!(outcome.deferred[0].wallet, raced);
            assert_eq!(outcome.deferred[0].kind, "anchor.ledger_hash_changed");
            assert!(paper.position_anchors(&raced).unwrap().is_empty());
            assert_eq!(paper.position_anchors(&healthy).unwrap().len(), 1);
        }
        drop(preparer);
        assert_eq!(
            actor.await.unwrap(),
            if durability {
                vec![vec![raced, healthy]]
            } else {
                vec![vec![raced, healthy], vec![healthy]]
            }
        );
    }
}

#[tokio::test]
async fn wallet_deferral_survives_a_shared_transient_prepare_abort() {
    struct TransientOne {
        inner: QueueFetcher,
        url: String,
    }

    impl PageFetcher for TransientOne {
        async fn fetch_page(&self, url: &str) -> Result<Vec<u8>, SourceError> {
            if url == self.url {
                return Err(SourceError::Transient {
                    message: "exhausted fixture".to_owned(),
                });
            }
            self.inner.fetch_page(url).await
        }
    }

    let deferred = wallet(0x6d);
    let shared = wallet(0x6e);
    let (_dir, paper, engine) = fresh(&[]);
    let mut responses = HashMap::new();
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
    let fetcher: Arc<dyn ReconciliationFetcher> = Arc::new(TransientOne {
        inner: QueueFetcher::new(responses),
        url: activity_url(shared),
    });
    let (control_tx, control_rx) = mpsc::channel(2);
    let actor = spawn_control_actor(control_rx, engine, Arc::clone(&paper));
    let preparer = AdmissionPreparer::with_validator(
        control_tx,
        Arc::clone(&paper),
        validator_from_reconciliation(fetcher),
    );
    let abort = preparer.prepare(&[deferred, shared]).await.unwrap_err();
    assert!(matches!(
        abort.cause,
        AdmissionError::PositionValidation(CausalPositionError::Activity {
            source: ActivityReadError::Fetch {
                source: SourceError::Transient { .. },
                ..
            },
            ..
        })
    ));
    assert_eq!(abort.deferred.len(), 1);
    assert_eq!(abort.deferred[0].wallet, deferred);
    assert_eq!(abort.deferred[0].kind, "positions.missing_activity_mapping");
    assert!(paper.position_anchors(&deferred).unwrap().is_empty());
    drop(preparer);
    actor.await.unwrap();
}

#[tokio::test]
async fn each_source_error_variant_aborts_admission_preparation() {
    struct RejectingFetcher {
        variant: u8,
    }

    impl PageFetcher for RejectingFetcher {
        async fn fetch_page(&self, _url: &str) -> Result<Vec<u8>, SourceError> {
            Err(match self.variant {
                0 => SourceError::Transient {
                    message: "exhausted fixture".to_owned(),
                },
                1 => SourceError::RateLimited {
                    retry_after_secs: 1,
                },
                2 => SourceError::Fatal {
                    message: "invalid fixture".to_owned(),
                },
                _ => unreachable!(),
            })
        }
    }

    for variant in 0..3 {
        let candidate = wallet(0x6e);
        let (_dir, paper, engine) = fresh(&[]);
        let (control_tx, control_rx) = mpsc::channel(2);
        let actor = spawn_control_actor(control_rx, engine, Arc::clone(&paper));
        let fetcher: Arc<dyn ReconciliationFetcher> = Arc::new(RejectingFetcher { variant });
        let preparer = AdmissionPreparer::with_validator(
            control_tx,
            Arc::clone(&paper),
            validator_from_reconciliation(fetcher),
        );
        let abort = preparer.prepare(&[candidate]).await.unwrap_err();
        assert!(
            matches!(
                &abort.cause,
                AdmissionError::PositionValidation(CausalPositionError::Activity {
                    source: ActivityReadError::Fetch { .. },
                    ..
                })
            ),
            "variant {variant} did not abort preparation: {abort:?}"
        );
        assert!(abort.deferred.is_empty());
        assert!(paper.position_anchors(&candidate).unwrap().is_empty());
        drop(preparer);
        actor.await.unwrap();
    }
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

fn financial_preparer(
    h: &golden::BracketFinancialHarness,
    paper: &Arc<PaperStateDb>,
    fetcher: Arc<QueueFetcher>,
) -> AdmissionPreparer {
    let fetcher: Arc<dyn ReconciliationFetcher> = fetcher;
    let identity = Arc::new(AssetIdentityResolver::new_runtime(
        fetcher.clone(),
        BASE.to_owned(),
        GAMMA_BATCH_SIZE,
        h.source.clone(),
    ));
    AdmissionPreparer::with_validator(
        h.control.clone(),
        paper.clone(),
        CausalPositionValidator::new(fetcher, BASE, "bracket-financial", identity)
            .with_clock(Arc::new(|| END)),
    )
    .with_source_log(h.source.clone())
}

async fn routine_refresh_defers_read(read_index: usize) {
    let wallet = wallet(0x81);
    let (dir, paper, mut engine) = fresh(&[wallet]);
    install_empty_anchor(&mut engine, &paper, wallet, 1);
    drop(engine);
    let h = golden::BracketFinancialHarness::new(dir.path(), paper.clone(), &[wallet]).await;
    let entry_payload = golden::BracketFinancialHarness::entry_payload(wallet, 90);
    let row: Vec<Value> = serde_json::from_slice(&entry_payload).unwrap();
    let covered = activity(wallet, 9, "1", "0xunseen-covered", 1);
    let covered_id = aggregate(covered.clone(), wallet).group_id.key().clone();
    let payload = serde_json::to_vec(&vec![covered, row[0].clone()]).unwrap();
    let id = aggregate(row[0].clone(), wallet).group_id.key().clone();
    let mut reads = vec![b"[]".to_vec(); read_index];
    reads.push(payload.clone());
    let mut responses = HashMap::from([
        (activity_url(wallet), reads),
        (
            position_url(wallet, PositionPartition::NotRedeemable),
            vec![b"[]".to_vec(); 2],
        ),
        (
            position_url(wallet, PositionPartition::Redeemable),
            vec![b"[]".to_vec(); 2],
        ),
    ]);
    let fetcher = Arc::new(QueueFetcher::new(std::mem::take(&mut responses)));
    let preparer = financial_preparer(&h, &paper, fetcher.clone());
    assert_eq!(
        preparer.prepare_if_due(wallet, END, 1).await.unwrap(),
        AnchorRefreshOutcome::Deferred
    );
    assert_eq!(
        fetcher
            .urls()
            .iter()
            .filter(|url| url.contains("/activity?"))
            .count(),
        read_index + 1
    );
    assert!(paper.activity_group_state(&id).unwrap().is_none());
    assert!(
        paper.activity_group_state(&covered_id).unwrap().is_none(),
        "preflight must precede even the first covered bucket in this read"
    );
    let conn = rusqlite::Connection::open(dir.path().join("paper.db")).unwrap();
    let gates: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM entry_gate_results WHERE source_trade_id = ?1",
            [&id.0],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(gates, 0);
    assert!(paper.gate_history().unwrap()[&wallet].is_empty());
    assert!(paper.decision_pending_history().unwrap().is_empty());
    assert_eq!(h.mutations(), 0);
    assert_eq!(paper.cursor(&wallet).unwrap(), Some(1));
    let committed = h.ordinary_entry(wallet, 90).await;
    assert_eq!(committed.pending.len(), 1);
    assert_eq!(h.mutations(), 1);
    assert_eq!(
        paper.decision_pending_history().unwrap()[0]
            .terminal_disposition
            .as_deref(),
        Some("fill")
    );
    let replayed = h.ordinary_entry(wallet, 90).await;
    assert!(replayed.already_committed);
    assert!(replayed.pending.is_empty());
    assert_eq!(h.mutations(), 1);
    let positions = serde_json::to_vec(&json!([{
        "proxyWallet": wallet, "conditionId": row[0]["conditionId"],
        "asset": row[0]["asset"], "size": "5", "outcomeIndex": 0, "negativeRisk": false
    }]))
    .unwrap();
    let stable = Arc::new(QueueFetcher::with_gamma(
        HashMap::from([
            (activity_url(wallet), vec![payload; 3]),
            (
                position_url(wallet, PositionPartition::NotRedeemable),
                vec![positions; 2],
            ),
            (
                position_url(wallet, PositionPartition::Redeemable),
                vec![b"[]".to_vec(); 2],
            ),
        ]),
        Some(golden::BracketFinancialHarness::entry_gamma()),
    ));
    let retry = financial_preparer(&h, &paper, stable);
    assert_eq!(
        retry.prepare_if_due(wallet, END, 1).await.unwrap(),
        AnchorRefreshOutcome::Anchored
    );
    assert_eq!(paper.position_anchors(&wallet).unwrap().len(), 2);
    assert_eq!(
        paper.wallet_coverage(&wallet).unwrap().activity_cutoff_unix,
        Some(END)
    );
    assert_eq!(paper.decision_pending_history().unwrap().len(), 1);
    assert_eq!(h.mutations(), 1);
    drop((preparer, retry));
    h.shutdown().await;
}

#[tokio::test]
async fn routine_refresh_defers_first_read_entry_until_ordinary_commit() {
    routine_refresh_defers_read(0).await;
}

#[tokio::test]
async fn routine_refresh_defers_second_read_entry_until_ordinary_commit() {
    routine_refresh_defers_read(1).await;
}

#[tokio::test]
async fn routine_refresh_defers_final_read_entry_until_ordinary_commit() {
    routine_refresh_defers_read(2).await;
}

/// PASS: preflight reaches an unseen group after a known post-anchor group, within and across
/// buckets, at each of the three reads; it writes neither covered history nor the unseen entry.
#[tokio::test]
async fn routine_refresh_preflights_every_group_after_known_activity() {
    for read_index in 0..3 {
        for same_bucket in [false, true] {
            let wallet = wallet(0x91);
            let (_dir, paper, mut engine) = fresh(&[wallet]);
            install_empty_anchor(&mut engine, &paper, wallet, 1);
            let known_epoch = if same_bucket { 90 } else { 89 };
            let first = activity(wallet, 1, "1", "0xfirst", known_epoch);
            let second = activity(wallet, 2, "1", "0xsecond", 90);
            // CompleteActivityRead orders equal-second groups by their canonical identity.
            let (known, unseen) = if same_bucket
                && aggregate(first.clone(), wallet).group_id.key().0
                    > aggregate(second.clone(), wallet).group_id.key().0
            {
                (second, first)
            } else {
                (first, second)
            };
            let known_group = aggregate(known.clone(), wallet);
            let unseen_id = aggregate(unseen.clone(), wallet).group_id.key().clone();
            engine
                .commit(
                    vec![known_group.clone()],
                    &context(known_epoch),
                    zero_basis(),
                )
                .unwrap();
            let before = paper
                .activity_group_state(known_group.group_id.key())
                .unwrap();
            let history = paper.gate_history().unwrap();
            let covered = activity(wallet, 9, "1", "0xcovered", 1);
            let covered_id = aggregate(covered.clone(), wallet).group_id.key().clone();
            let mut reads = vec![serde_json::to_vec(&vec![known.clone()]).unwrap(); read_index];
            reads.push(serde_json::to_vec(&vec![covered, known.clone(), unseen]).unwrap());
            let held = serde_json::to_vec(&json!([{
                "proxyWallet": wallet, "asset": known["asset"],
                "conditionId": known["conditionId"], "size": "1", "outcomeIndex": 0,
                "negativeRisk": true
            }]))
            .unwrap();
            let fetcher = Arc::new(QueueFetcher::new(HashMap::from([
                (activity_url(wallet), reads),
                (
                    position_url(wallet, PositionPartition::NotRedeemable),
                    vec![held; 2],
                ),
                (
                    position_url(wallet, PositionPartition::Redeemable),
                    vec![b"[]".to_vec(); 2],
                ),
            ])));
            let identity = Arc::new(AssetIdentityResolver::new(
                fetcher.clone(),
                BASE.to_owned(),
                GAMMA_BATCH_SIZE,
                Arc::new(tokio::sync::Mutex::new(
                    SourceEventSink::open(_dir.path().join("source.log")).unwrap(),
                )),
            ));
            let validator =
                CausalPositionValidator::new(fetcher.clone(), BASE, "preflight", identity)
                    .with_clock(Arc::new(|| END));
            let (tx, rx) = mpsc::channel(2);
            let actor = spawn_control_actor(rx, engine, paper.clone());
            let preparer = AdmissionPreparer::with_validator(tx, paper.clone(), validator);
            assert_eq!(
                preparer.prepare_if_due(wallet, END, 1).await.unwrap(),
                AnchorRefreshOutcome::Deferred
            );
            assert_eq!(
                fetcher
                    .urls()
                    .iter()
                    .filter(|url| url.contains("/activity?"))
                    .count(),
                read_index + 1
            );
            assert_eq!(
                paper
                    .activity_group_state(known_group.group_id.key())
                    .unwrap(),
                before
            );
            assert!(paper.activity_group_state(&unseen_id).unwrap().is_none());
            assert!(paper.activity_group_state(&covered_id).unwrap().is_none());
            assert_eq!(paper.gate_history().unwrap(), history);
            assert!(paper.decision_pending_history().unwrap().is_empty());
            assert_eq!(paper.position_anchors(&wallet).unwrap().len(), 1);
            assert!(!paper.is_wallet_fenced(&wallet).unwrap());
            drop(preparer);
            actor.await.unwrap();
        }
    }
}

async fn runtime_completion_case(
    incomplete: bool,
    repeat: bool,
    restart: bool,
    fault: Option<&str>,
) {
    let incumbent = wallet(0x82);
    let newcomer = wallet(0x83);
    let (dir, paper, mut engine) = fresh(&[incumbent]);
    install_empty_anchor(&mut engine, &paper, incumbent, 0);
    drop(engine);
    if incomplete {
        paper
            .record_reconciled_history_status(&WalletHistoryStatusRecord {
                wallet: newcomer,
                complete: false,
                proof_json: "{\"seed\":true}".to_owned(),
                updated_at_unix: 1,
            })
            .unwrap();
    }
    let before = paper.wallet_history_status(&newcomer).unwrap();
    let h = golden::BracketFinancialHarness::new(dir.path(), paper.clone(), &[incumbent]).await;
    let fetcher = Arc::new(QueueFetcher::new(stable_responses_for_attempts(
        &[(newcomer, 1, "1")],
        3,
    )));
    let preparer = financial_preparer(&h, &paper, fetcher.clone());
    let conn = rusqlite::Connection::open(dir.path().join("paper.db")).unwrap();
    if let Some(operation) = fault {
        conn.execute_batch(&format!("CREATE TRIGGER fail_history BEFORE {operation} ON wallet_history_status_v2 BEGIN SELECT RAISE(FAIL, 'completion fault'); END;")).unwrap();
        assert!(matches!(
            preparer.prepare(&[newcomer]).await,
            Err(pe_service::watchlist_admission::AdmissionAbort {
                cause: AdmissionError::ValidationInstall(_),
                ..
            })
        ));
        assert_eq!(paper.wallet_history_status(&newcomer).unwrap(), before);
        assert!(paper.position_anchors(&newcomer).unwrap().is_empty());
        assert!(paper.position_validation(&newcomer).unwrap().is_none());
        assert_eq!(
            paper
                .wallet_coverage(&newcomer)
                .unwrap()
                .activity_cutoff_unix,
            None
        );
        assert_eq!(h.live.snapshot().entries.len(), 1);
        assert_eq!(h.live.snapshot().entries[0].wallet, incumbent);
        assert_eq!(h.mutations(), 0);
        conn.execute_batch("DROP TRIGGER fail_history").unwrap();
    }
    preparer.prepare(&[newcomer]).await.unwrap();
    let complete = paper.wallet_history_status(&newcomer).unwrap().unwrap();
    assert!(complete.complete);
    assert_eq!(complete.updated_at_unix, END);
    assert_eq!(
        complete.proof_json,
        paper
            .position_validation(&newcomer)
            .unwrap()
            .unwrap()
            .proof_json
    );
    let proof: Value = serde_json::from_str(&complete.proof_json).unwrap();
    assert_eq!(proof["activity_walks"].as_array().unwrap().len(), 3);
    assert_full_history_proof(&complete.proof_json);
    assert_eq!(
        h.live.snapshot().entries.len(),
        1,
        "completion precedes publication"
    );
    assert!(
        paper.gate_history().unwrap()[&newcomer].contains(&MarketId(VenueMarketId(condition(1))))
    );
    assert!(paper.decision_pending_history().unwrap().is_empty());
    if repeat {
        preparer.prepare(&[newcomer]).await.unwrap();
        assert_ne!(
            paper
                .position_validation(&newcomer)
                .unwrap()
                .unwrap()
                .proof_json,
            complete.proof_json,
            "the next acceptance must exercise preservation against a different proof"
        );
        assert_eq!(
            paper.wallet_history_status(&newcomer).unwrap(),
            Some(complete.clone())
        );
    }
    preparer
        .scenario_publish_ranking(
            &h.live,
            &h.writer_lock,
            golden::BracketFinancialHarness::entries(&[newcomer]),
            &HashMap::from([(newcomer, 10)]),
            1,
        )
        .await
        .unwrap();
    assert_eq!(h.live.snapshot().entries[0].wallet, newcomer);
    let result = h.ordinary_entry(newcomer, END + 1).await;
    assert_eq!(result.pending.len(), 1);
    assert_eq!(h.mutations(), 1);
    let rows = paper.decision_pending_history().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].terminal_disposition.as_deref(), Some("fill"));
    pe_service::decision_replay::replay_decision_pending(&rows[0]).unwrap();
    assert_eq!(
        paper.wallet_history_status(&newcomer).unwrap(),
        Some(complete)
    );
    assert_eq!(paper.gate_history().unwrap()[&newcomer].len(), 2);
    if incomplete {
        let historical = aggregate(activity(newcomer, 1, "1", "0xbase1", 10), newcomer);
        let (committed, ack) = tokio::sync::oneshot::channel();
        h.control
            .send(OrchestratorControl::CommitActivityBucket {
                aggregates: vec![historical],
                context: Arc::new(context(END + 1)),
                committed,
            })
            .await
            .unwrap();
        assert!(ack.await.unwrap().unwrap().already_committed);
        // Close and reopen the historical market with new immutable groups: a duplicate
        // group alone cannot prove that the running first-entry gate survived installation.
        for (side, epoch) in [("SELL", END + 2), ("BUY", END + 3)] {
            let mut row = activity(newcomer, 1, "1", &format!("0xhistorical-{side}"), epoch);
            row["side"] = json!(side);
            let aggregate = aggregate(row, newcomer);
            let id = aggregate.group_id.key().clone();
            let mut ordinary = context(epoch);
            ordinary.copy_eligible = true;
            let (committed, ack) = tokio::sync::oneshot::channel();
            h.control
                .send(OrchestratorControl::CommitActivityBucket {
                    aggregates: vec![aggregate],
                    context: Arc::new(ordinary),
                    committed,
                })
                .await
                .unwrap();
            let result = ack.await.unwrap().unwrap();
            assert!(result.pending.is_empty());
            if side == "BUY" {
                assert_eq!(result.dispositions[&id.0], "not_first_entry");
            }
        }
        assert_eq!(h.mutations(), 1);
    }
    let expected_membership = serde_json::to_vec(&h.live.snapshot().entries).unwrap();
    let expected_history = paper.gate_history().unwrap();
    let expected_status = paper.wallet_history_status(&newcomer).unwrap();
    let source_path = h.source_path.clone();
    let paper_path = h.paper_path.clone();
    let initial = h.initial.clone();
    drop(preparer);
    h.shutdown().await;
    if restart {
        drop(conn);
        drop(paper);
        let reopened = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        let ledger = build_leader_ledger(&reopened).unwrap();
        let replayed = replay_wallet_ledger(&reopened, newcomer).unwrap();
        assert_eq!(
            ledger_capture(&ledger, &reopened, newcomer).unwrap().hash,
            ledger_capture(&replayed, &reopened, newcomer).unwrap().hash
        );
        let engine = BucketCommitEngine::load(reopened.clone(), ledger).unwrap();
        assert!(engine.history_complete(&newcomer));
        assert_eq!(reopened.gate_history().unwrap(), expected_history);
        assert_eq!(
            reopened.wallet_history_status(&newcomer).unwrap(),
            expected_status
        );
        let era = pe_service::paper_recovery::paper_era(
            pe_service::paper_recovery::scan_paper_log(&paper_path).unwrap(),
        );
        let restored = pe_service::paper_recovery::replay_membership(&era, initial, &source_path)
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::to_vec(&restored.watchlist.entries).unwrap(),
            expected_membership
        );
        assert_eq!(restored.last_ranking_batch_id, 546);
    }
}

#[tokio::test]
async fn runtime_repairs_stored_history_before_membership_and_preserves_financial_replay() {
    for complete in [false, true] {
        let incumbent = wallet(0x94);
        let newcomer = wallet(0x95);
        let (dir, paper, mut engine) = fresh(&[incumbent]);
        let path = dir.path().join("paper.db");
        paper
            .record_reconciled_history_status(&WalletHistoryStatusRecord {
                wallet: newcomer,
                complete,
                proof_json: "{\"old\":true}".to_owned(),
                updated_at_unix: 1,
            })
            .unwrap();
        install_empty_anchor(&mut engine, &paper, incumbent, 0);
        install_empty_anchor(&mut engine, &paper, newcomer, 50);
        require_reanchor(&path, newcomer);
        store_old_late_groups(&mut engine, newcomer);
        drop(engine);
        let old_groups = paper.activity_groups_after(&newcomer, -1).unwrap();
        let old_status = paper.wallet_history_status(&newcomer).unwrap();
        let h = golden::BracketFinancialHarness::new(dir.path(), paper.clone(), &[incumbent]).await;
        assert_eq!(h.ordinary_entry(incumbent, END + 1).await.pending.len(), 1);
        let financial = paper.financial_snapshot(END + 1).unwrap();
        let prior_decisions = paper.decision_pending_history().unwrap();
        let fetcher = Arc::new(QueueFetcher::new(older_market_responses(newcomer, 2)));
        let preparer = financial_preparer(&h, &paper, fetcher);
        preparer.prepare(&[newcomer]).await.unwrap();
        assert_eq!(paper.gate_history().unwrap()[&newcomer].len(), 3);
        assert!(paper.wallet_history_complete(&newcomer).unwrap());
        if complete {
            assert_eq!(paper.wallet_history_status(&newcomer).unwrap(), old_status);
        }
        assert_eq!(
            paper.activity_groups_after(&newcomer, -1).unwrap(),
            old_groups
        );
        assert_full_history_proof(
            &paper
                .position_validation(&newcomer)
                .unwrap()
                .unwrap()
                .proof_json,
        );
        assert_eq!(h.live.snapshot().entries[0].wallet, incumbent);
        assert_eq!(h.mutations(), 1);
        preparer
            .scenario_publish_ranking(
                &h.live,
                &h.writer_lock,
                golden::BracketFinancialHarness::entries(&[newcomer]),
                &HashMap::from([(newcomer, 10)]),
                1,
            )
            .await
            .unwrap();
        for byte in 1..=3 {
            for (side, delta) in [("SELL", 0), ("BUY", 1)] {
                let epoch = END + i64::from(byte) * 2 + delta;
                let mut row = activity(
                    newcomer,
                    byte,
                    "1",
                    &format!("0xruntime-{byte}-{side}"),
                    epoch,
                );
                row["side"] = json!(side);
                let mut ordinary = context(epoch);
                ordinary.copy_eligible = true;
                let (committed, ack) = tokio::sync::oneshot::channel();
                h.control
                    .send(OrchestratorControl::CommitActivityBucket {
                        aggregates: vec![aggregate(row, newcomer)],
                        context: Arc::new(ordinary),
                        committed,
                    })
                    .await
                    .unwrap();
                let result = ack.await.unwrap().unwrap();
                assert!(result.pending.is_empty());
                if side == "BUY" {
                    assert!(
                        result
                            .dispositions
                            .values()
                            .all(|value| value == "not_first_entry")
                    );
                }
            }
        }
        assert_eq!(paper.financial_snapshot(END + 1).unwrap(), financial);
        assert_eq!(paper.decision_pending_history().unwrap(), prior_decisions);
        assert_eq!(h.mutations(), 1);
        let membership = serde_json::to_vec(&h.live.snapshot().entries).unwrap();
        let initial = h.initial.clone();
        let source_path = h.source_path.clone();
        let paper_path = h.paper_path.clone();
        drop(preparer);
        h.shutdown().await;
        drop(paper);
        let paper = Arc::new(PaperStateDb::open(&path).unwrap());
        let ledger = build_leader_ledger(&paper).unwrap();
        let replayed = replay_wallet_ledger(&paper, newcomer).unwrap();
        assert_eq!(
            ledger_capture(&ledger, &paper, newcomer).unwrap().hash,
            ledger_capture(&replayed, &paper, newcomer).unwrap().hash
        );
        assert_eq!(paper.gate_history().unwrap()[&newcomer].len(), 3);
        assert_eq!(paper.financial_snapshot(END + 1).unwrap(), financial);
        for row in paper.decision_pending_history().unwrap() {
            pe_service::decision_replay::replay_decision_pending(&row).unwrap();
        }
        let era = pe_service::paper_recovery::paper_era(
            pe_service::paper_recovery::scan_paper_log(&paper_path).unwrap(),
        );
        let restored = pe_service::paper_recovery::replay_membership(&era, initial, &source_path)
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::to_vec(&restored.watchlist.entries).unwrap(),
            membership
        );
    }
}

#[tokio::test]
async fn runtime_absent_history_completes_before_publication_and_fresh_entry() {
    runtime_completion_case(false, false, false, None).await;
}

#[tokio::test]
async fn runtime_incomplete_history_completes_and_preserves_consumed_markets() {
    runtime_completion_case(true, false, false, None).await;
}

#[tokio::test]
async fn runtime_completion_transaction_failure_publishes_neither_completion_nor_membership() {
    runtime_completion_case(false, false, false, Some("INSERT")).await;
    runtime_completion_case(true, false, false, Some("UPDATE")).await;
    // Inspect the running engine at the rejected transaction boundary as well as the
    // end-to-end publication/retry assertions above.
    for operation in ["INSERT", "UPDATE"] {
        let wallet = wallet(0x8a);
        let (dir, paper, mut engine) = fresh(&[]);
        paper.set_cursor(&wallet, 10).unwrap();
        if operation == "UPDATE" {
            paper
                .record_reconciled_history_status(&WalletHistoryStatusRecord {
                    wallet,
                    complete: false,
                    proof_json: "{}".to_owned(),
                    updated_at_unix: 1,
                })
                .unwrap();
        }
        let captured = ledger_capture(engine.ledger(), &paper, wallet).unwrap();
        let install = AnchorInstall {
            newest_activity_unix: None,
            fresh_history: Vec::new(),
            expected_fence: None,
            wallet,
            balances: Vec::new(),
            cutoff: END,
            history_status: Some(WalletHistoryStatusRecord {
                wallet,
                complete: true,
                proof_json: "{}".to_owned(),
                updated_at_unix: END,
            }),
            proof: AnchorProof {
                positions_proof_hash: "empty".to_owned(),
                activity_bounds_json: "[]".to_owned(),
                source_log_generation: "scenario".to_owned(),
                document: "{}".to_owned(),
                recorded_at_unix: END,
            },
            expected: AnchorExpectation {
                ledger_hash: captured.hash.clone(),
                cursor: captured.cursor,
                anchor_seq: captured.anchor_seq,
                coverage_generation: captured.coverage_generation,
            },
        };
        let conn = rusqlite::Connection::open(dir.path().join("paper.db")).unwrap();
        conn.execute_batch(&format!("CREATE TRIGGER fail_completion BEFORE {operation} ON wallet_history_status_v2 BEGIN SELECT RAISE(FAIL, 'completion fault'); END;")).unwrap();
        assert!(matches!(
            engine.install_anchors(std::slice::from_ref(&install)),
            Err(pe_service::bucket_commit::AnchorInstallError::Durability(_))
        ));
        assert!(!engine.history_complete(&wallet));
        assert_eq!(
            ledger_capture(engine.ledger(), &paper, wallet).unwrap(),
            captured
        );
        assert!(!paper.wallet_history_complete(&wallet).unwrap());
        assert!(paper.position_anchors(&wallet).unwrap().is_empty());
        conn.execute_batch("DROP TRIGGER fail_completion").unwrap();
        engine.install_anchors(&[install]).unwrap();
        assert!(engine.history_complete(&wallet));
        assert!(paper.wallet_history_complete(&wallet).unwrap());
    }
}

#[tokio::test]
async fn runtime_repeated_preparation_preserves_complete_history_proof() {
    runtime_completion_case(false, true, false, None).await;
}

#[tokio::test]
async fn runtime_history_completion_restart_and_membership_replay_are_exact() {
    runtime_completion_case(false, false, true, None).await;
}

#[tokio::test]
async fn runtime_empty_history_and_positions_refuses_without_initializing_cursor() {
    let wallet = wallet(0x84);
    let (_dir, paper, engine) = fresh(&[]);
    let responses = HashMap::from([
        (activity_url(wallet), vec![b"[]".to_vec(); 3]),
        (
            position_url(wallet, PositionPartition::NotRedeemable),
            vec![b"[]".to_vec(); 2],
        ),
        (
            position_url(wallet, PositionPartition::Redeemable),
            vec![b"[]".to_vec(); 2],
        ),
    ]);
    let (tx, rx) = mpsc::channel(2);
    let actor = spawn_control_actor(rx, engine, paper.clone());
    let preparer = AdmissionPreparer::with_validator(tx, paper.clone(), validator(responses));
    let error = preparer.prepare(&[wallet]).await.unwrap_err();
    assert!(
        matches!(&error.cause, AdmissionError::ValidationInstall(message) if message.contains("requires an existing wallet cursor"))
    );
    assert!(paper.cursor(&wallet).unwrap().is_none());
    assert!(paper.wallet_history_status(&wallet).unwrap().is_none());
    assert!(paper.position_anchors(&wallet).unwrap().is_empty());
    assert!(paper.decision_pending_history().unwrap().is_empty());
    drop(preparer);
    actor.await.unwrap();
}

#[tokio::test]
async fn boot_newcomer_and_required_reanchor_history_never_copy() {
    for kind in [
        "boot_unseeded",
        "boot_seeded",
        "newcomer",
        "required_reanchor",
    ] {
        let wallet = wallet(0x85);
        let (dir, paper, mut engine) = fresh(&[]);
        if kind == "boot_seeded" || kind == "required_reanchor" {
            paper
                .record_reconciled_history_status(&WalletHistoryStatusRecord {
                    wallet,
                    complete: kind == "required_reanchor",
                    proof_json: "{}".to_owned(),
                    updated_at_unix: 1,
                })
                .unwrap();
        }
        if kind == "required_reanchor" {
            install_empty_anchor(&mut engine, &paper, wallet, 0);
            let conn = rusqlite::Connection::open(dir.path().join("paper.db")).unwrap();
            conn.execute(
                "UPDATE poll_cursors SET reanchor_required = 1 WHERE wallet_hex = ?1",
                [wallet.to_string()],
            )
            .unwrap();
        }
        let validator = validator(stable_responses_for_attempts(&[(wallet, 1, "1")], 2));
        if kind.starts_with("boot_") {
            let installs = validator
                .validate_direct(&[wallet], &mut engine, &paper)
                .await
                .unwrap();
            assert_eq!(installs.len(), 1);
            assert!(installs[0].history_status.is_none());
            assert_eq!(
                paper.wallet_history_complete(&wallet).unwrap(),
                kind == "boot_seeded"
            );
        } else {
            let (tx, rx) = mpsc::channel(2);
            let actor = spawn_control_actor(rx, engine, paper.clone());
            let preparer = AdmissionPreparer::with_validator(tx, paper.clone(), validator);
            if kind == "newcomer" {
                preparer.prepare(&[wallet]).await.unwrap();
            } else {
                assert_eq!(
                    preparer.prepare_if_due(wallet, END, 1).await.unwrap(),
                    AnchorRefreshOutcome::Anchored
                );
            }
            drop(preparer);
            actor.await.unwrap();
        }
        assert!(
            paper.decision_pending_history().unwrap().is_empty(),
            "{kind}"
        );
        assert!(paper.list_fills().unwrap().is_empty(), "{kind}");
        if kind == "required_reanchor" {
            assert!(
                paper
                    .activity_groups_after(&wallet, -1)
                    .unwrap()
                    .iter()
                    .all(|group| group.disposition == "reanchor_required_late_group")
            );
        }
        assert!(
            paper.gate_history().unwrap()[&wallet].contains(&MarketId(VenueMarketId(condition(1))))
        );
        assert!(!paper.wallet_coverage(&wallet).unwrap().reanchor_required);
    }
}

#[tokio::test]
async fn runtime_rejected_brackets_do_not_complete_history() {
    for kind in [
        "partial",
        "failed",
        "deferred",
        "invalidated",
        "fenced",
        "cas_fenced",
        "cas_cursor",
        "cas_anchor",
        "cas_generation",
        "cas_cutoff",
    ] {
        let wallet = wallet(0x86);
        let (dir, paper, mut engine) = fresh(&[]);
        let conn = rusqlite::Connection::open(dir.path().join("paper.db")).unwrap();
        let mut responses = stable_responses_for_attempts(&[(wallet, 1, "1")], 2);
        match kind {
            "partial" => {
                // A full first page followed by a failed continuation is not complete history.
                let rows = vec![activity(wallet, 1, "1", "0xpartial", 10); 500];
                responses.insert(
                    activity_url(wallet),
                    vec![serde_json::to_vec(&rows).unwrap()],
                );
            }
            "failed" => {
                responses.clear();
            }
            "deferred" => {
                responses.insert(
                    position_url(wallet, PositionPartition::NotRedeemable),
                    ["1", "2", "3", "4"]
                        .iter()
                        .map(|size| serde_json::to_vec(&vec![position(wallet, 1, size)]).unwrap())
                        .collect(),
                );
            }
            "fenced" => {
                conn.execute("INSERT INTO wallet_fences (wallet_hex, source_trade_id, cause, proof_json, fenced_at_unix) VALUES (?1, 'test', 'invalid_mapping', '{}', 1)", [wallet.to_string()]).unwrap();
            }
            "invalidated" | "cas_cutoff" | "cas_fenced" => {
                install_empty_anchor(&mut engine, &paper, wallet, 0);
            }
            _ => {}
        }
        let fetcher = Arc::new(QueueFetcher::new(responses));
        let validator = validator_from_fetcher(fetcher.clone());
        let (tx, mut rx) = mpsc::channel(2);
        let actor_paper = paper.clone();
        let state_path = dir.path().join("paper.db");
        let actor = tokio::spawn(async move {
            while let Some(message) = rx.recv().await {
                match message {
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
                    OrchestratorControl::CaptureAdmissionLedger { wallet, captured } => {
                        let _ = captured.send(
                            ledger_capture(engine.ledger(), &actor_paper, wallet)
                                .map_err(|error| error.to_string()),
                        );
                    }
                    OrchestratorControl::InstallAnchors {
                        mut installs,
                        acknowledged,
                    } => {
                        match kind {
                            "invalidated" => {
                                let mutation = aggregate(
                                    activity(wallet, 9, "1", "0xintervening", 110),
                                    wallet,
                                );
                                engine
                                    .commit(vec![mutation], &context(110), zero_basis())
                                    .unwrap();
                            }
                            "cas_fenced" => {
                                let mut row = activity(wallet, 9, "2", "0xinstall-fence", 110);
                                row["type"] = json!("REDEEM");
                                engine
                                    .commit(
                                        vec![aggregate(row, wallet)],
                                        &context(110),
                                        zero_basis(),
                                    )
                                    .unwrap();
                                assert!(engine.is_fenced(&wallet));
                            }
                            "cas_cursor" => {
                                actor_paper.set_cursor(&wallet, 11).unwrap();
                            }
                            "cas_anchor" => {
                                install_empty_anchor(&mut engine, &actor_paper, wallet, 10);
                            }
                            "cas_generation" => {
                                let conn = rusqlite::Connection::open(&state_path).unwrap();
                                conn.execute("UPDATE poll_cursors SET coverage_generation = coverage_generation + 1 WHERE wallet_hex = ?1", [wallet.to_string()]).unwrap();
                            }
                            "cas_cutoff" => {
                                installs[0].cutoff = -1;
                            }
                            _ => {}
                        }
                        let result = engine.install_anchors(&installs);
                        assert!(result.is_err(), "{kind}");
                        assert!(!engine.history_complete(&wallet), "{kind}");
                        let _ = acknowledged.send(result);
                    }
                    _ => {}
                }
            }
        });
        let preparer = AdmissionPreparer::with_validator(tx, paper.clone(), validator);
        let result = preparer.prepare(&[wallet]).await;
        match kind {
            "partial" => {
                assert!(matches!(
                    result,
                    Err(pe_service::watchlist_admission::AdmissionAbort {
                        cause: AdmissionError::PositionValidation(CausalPositionError::Activity {
                            source: ActivityReadError::Fetch { .. },
                            ..
                        }),
                        ..
                    })
                ));
                assert_eq!(
                    fetcher.urls().len(),
                    2,
                    "the first page succeeded before the continuation failed"
                );
            }
            "failed" => assert!(matches!(
                result,
                Err(pe_service::watchlist_admission::AdmissionAbort {
                    cause: AdmissionError::PositionValidation(CausalPositionError::Activity {
                        source: ActivityReadError::Fetch { .. },
                        ..
                    }),
                    ..
                })
            )),
            "deferred" => assert_eq!(result.unwrap().deferred.len(), 1),
            "fenced" => {
                assert_eq!(result.unwrap().deferred[0].kind, "fence.active");
                assert!(fetcher.urls().is_empty());
            }
            _ => assert_eq!(result.unwrap().deferred.len(), 1, "{kind}"),
        }
        assert!(
            paper.wallet_history_status(&wallet).unwrap().is_none(),
            "{kind}"
        );
        assert!(
            paper.decision_pending_history().unwrap().is_empty(),
            "{kind}"
        );
        drop(preparer);
        actor.await.unwrap();
    }
}

async fn queued_bucket_before_membership(invalidate_admission: bool) {
    let (incumbent, newcomer) = (wallet(0x91), wallet(0x92));
    let (dir, paper, mut engine) = fresh(&[incumbent]);
    install_empty_anchor(&mut engine, &paper, incumbent, 0);
    drop(engine);
    let h = golden::BracketFinancialHarness::new(dir.path(), paper.clone(), &[incumbent]).await;
    let preparer = financial_preparer(
        &h,
        &paper,
        Arc::new(QueueFetcher::new(stable_responses(&[(newcomer, 1, "1")]))),
    );
    preparer.prepare(&[newcomer]).await.unwrap();
    assert!(paper.position_validation_current(&newcomer).unwrap());
    drop(preparer);

    // This bounded relay is the publication barrier: maintenance has computed its exact
    // mutation, and its acknowledgement stays pending while we queue the bucket first.
    let (publication_tx, mut publication_rx) = mpsc::channel(1);
    let publisher =
        AdmissionPreparer::new(publication_tx, paper.clone()).with_source_log(h.source.clone());
    let live = h.live.clone();
    let writer_lock = h.writer_lock.clone();
    let publication = tokio::spawn(async move {
        publisher
            .scenario_publish_ranking(
                &live,
                &writer_lock,
                golden::BracketFinancialHarness::entries(&[newcomer]),
                &HashMap::from([(newcomer, 10)]),
                1,
            )
            .await
    });
    let message = publication_rx.recv().await.unwrap();
    let OrchestratorControl::PublishMembership {
        change,
        replacements,
        checks,
        acknowledged: maintenance_ack,
    } = message
    else {
        unreachable!("the publication barrier received an unrelated control");
    };
    let expected_record = serde_json::to_vec(&change.clone().into_record()).unwrap();
    let bucket_wallet = if invalidate_admission {
        newcomer
    } else {
        incumbent
    };
    let mut bucket_ack = h.queue_entry(bucket_wallet, END + 1).await;
    let (acknowledged, publication_ack) = tokio::sync::oneshot::channel();
    h.control
        .send(OrchestratorControl::PublishMembership {
            change,
            replacements,
            checks,
            acknowledged,
        })
        .await
        .unwrap();
    let result = publication_ack.await.unwrap();
    // The serialized owner must acknowledge the earlier bucket before this publication.
    let bucket = bucket_ack.try_recv().unwrap().unwrap();
    assert_eq!(bucket.wallet, bucket_wallet);
    assert!(!bucket.already_committed);
    assert!(!paper.position_validation_current(&bucket_wallet).unwrap());
    let membership_records = Reader::replay(&h.paper_path)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .into_iter()
        .filter(|(_, envelope)| envelope.payload == expected_record)
        .collect::<Vec<_>>();
    if invalidate_admission {
        assert!(
            matches!(result.as_ref().unwrap_err(), pe_service::watchlist_maintenance::PublishError::Wallet { wallet, cause: pe_service::watchlist_maintenance::WalletPublishCause::UnvalidatedPosition } if *wallet == newcomer)
        );
        assert_eq!(h.live.snapshot().entries[0].wallet, incumbent);
        assert_eq!(h.live.structural_membership(), HashSet::from([incumbent]));
        assert!(membership_records.is_empty());
    } else {
        let receipt = result.as_ref().unwrap();
        assert_eq!(h.live.snapshot().entries[0].wallet, newcomer);
        assert_eq!(h.live.structural_membership(), HashSet::from([newcomer]));
        assert!(paper.cursor(&newcomer).unwrap().is_some());
        assert_eq!(membership_records.len(), 1);
        assert_eq!(membership_records[0].0, receipt.sequence);
    }
    maintenance_ack.send(result).unwrap();
    let result = publication.await.unwrap();
    if invalidate_admission {
        assert!(
            matches!(result.unwrap_err(), pe_service::watchlist_maintenance::MembershipApplyError::Publication(pe_service::watchlist_maintenance::PublishError::Wallet { wallet, cause: pe_service::watchlist_maintenance::WalletPublishCause::UnvalidatedPosition }) if wallet == newcomer)
        );
    } else {
        result.unwrap();
    }
    h.shutdown().await;
}

/// PASS: the earlier bucket is acknowledged before maintenance publishes with the writer lock
/// enabled, and the durable record retains the exact MembershipChange bytes.
#[tokio::test]
async fn queued_bucket_before_membership_publishes_without_writer_deadlock() {
    tokio::time::timeout(
        std::time::Duration::from_secs(20),
        queued_bucket_before_membership(false),
    )
    .await
    .expect("bucket/publication lock cycle stalled");
}

/// PASS: a queued bucket invalidates the admitted wallet's bracket before the final recheck;
/// publication returns the existing rejection text and preserves the incumbent and paper log.
#[tokio::test]
async fn queued_bucket_before_membership_rejects_invalidated_bracket() {
    tokio::time::timeout(
        std::time::Duration::from_secs(20),
        queued_bucket_before_membership(true),
    )
    .await
    .expect("bucket/publication lock cycle stalled");
}

/// PASS: only the current request commits its capacity epoch before acknowledgement;
/// superseded requests and old maintenance epochs append no record and leave membership intact.
#[tokio::test]
async fn membership_handler_rechecks_capacity_and_commits_epoch_before_ack() {
    use pe_service::config_poller::capacity_request_channel;
    use pe_service::paper_recovery::{MembershipChange, MembershipReason};
    use pe_service::runtime_config::AppliedWatchlistCapacity;
    use pe_service::watchlist_maintenance::{
        MembershipCapacityCheck, MembershipCommit, PublicationBinding, PublishError,
    };

    let incumbent = wallet(0x93);
    let (dir, paper, mut engine) = fresh(&[incumbent]);
    install_empty_anchor(&mut engine, &paper, incumbent, 0);
    drop(engine);
    let h = golden::BracketFinancialHarness::new(dir.path(), paper.clone(), &[incumbent]).await;
    let publisher = AdmissionPreparer::new(h.control.clone(), paper);
    let applied = AppliedWatchlistCapacity::new(1);
    let original = applied.load();
    let (requests, desired) = capacity_request_channel(1, h.writer_lock.clone());
    let request = requests.request(2).await;
    let newer = requests.request(3).await;
    let record_count = || Reader::replay(&h.paper_path).unwrap().count();
    let before = record_count();
    for attempted in [request, newer] {
        let result = publisher
            .publish_membership(
                MembershipChange {
                    reason: MembershipReason::CapacityChange,
                    removed: Vec::new(),
                    added: Vec::new(),
                    capacity: attempted.target,
                    ranking_batch_id: None,
                    evidence: json!({"scenario": "capacity-publication"}),
                },
                h.initial.entries.clone(),
                MembershipCommit {
                    seeds: Vec::new(),
                    reentries: Vec::new(),
                    capacity: Some(MembershipCapacityCheck::Transition {
                        applied: applied.clone(),
                        desired: desired.clone(),
                        request: attempted,
                    }),
                    binding: PublicationBinding {
                        structural: h.live.structural_membership(),
                        digests: Vec::new(),
                    },
                },
            )
            .await;
        if attempted == request {
            assert!(matches!(&result, Err(PublishError::CapacitySuperseded)));
            assert_eq!(applied.load(), original);
            assert_eq!(record_count(), before);
        } else {
            result.unwrap();
            assert_eq!(applied.load(), newer);
            assert_eq!(record_count(), before + 1);
        }
    }
    let result = publisher
        .publish_membership(
            MembershipChange {
                reason: MembershipReason::FullRerank,
                removed: vec![incumbent],
                added: Vec::new(),
                capacity: original.target,
                ranking_batch_id: Some(546),
                evidence: json!({"scenario": "stale-maintenance"}),
            },
            Vec::new(),
            MembershipCommit {
                seeds: Vec::new(),
                reentries: Vec::new(),
                capacity: Some(MembershipCapacityCheck::Unchanged {
                    applied: applied.clone(),
                    expected: original,
                }),
                binding: PublicationBinding {
                    structural: h.live.structural_membership(),
                    digests: Vec::new(),
                },
            },
        )
        .await;
    assert!(matches!(result, Err(PublishError::StaleCapacity)));
    assert_eq!(
        serde_json::to_value(&h.live.snapshot().entries).unwrap(),
        serde_json::to_value(&h.initial.entries).unwrap(),
    );
    assert_eq!(applied.load(), newer);
    assert_eq!(record_count(), before + 1);
    drop(publisher);
    h.shutdown().await;
}

#[tokio::test]
async fn fence_after_prepare_invalidates_selected_vector() {
    let (incumbent, selected, survivor) = (wallet(0x87), wallet(0x88), wallet(0x89));
    let (dir, paper, mut engine) = fresh(&[incumbent]);
    install_empty_anchor(&mut engine, &paper, incumbent, 0);
    drop(engine);
    let h = golden::BracketFinancialHarness::new(dir.path(), paper.clone(), &[incumbent]).await;
    let preparer = financial_preparer(
        &h,
        &paper,
        Arc::new(QueueFetcher::new(stable_responses(&[
            (selected, 1, "1"),
            (survivor, 2, "1"),
        ]))),
    );
    preparer.prepare(&[selected]).await.unwrap();
    let conn = rusqlite::Connection::open(dir.path().join("paper.db")).unwrap();
    conn.execute("INSERT INTO wallet_fences (wallet_hex, source_trade_id, cause, proof_json, fenced_at_unix) VALUES (?1, 'test', 'invalid_mapping', '{}', 1)", [selected.to_string()]).unwrap();
    let error = preparer
        .scenario_publish_ranking(
            &h.live,
            &h.writer_lock,
            golden::BracketFinancialHarness::entries(&[selected]),
            &HashMap::from([(selected, 10)]),
            1,
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, pe_service::watchlist_maintenance::MembershipApplyError::Publication(pe_service::watchlist_maintenance::PublishError::Wallet { wallet, cause: pe_service::watchlist_maintenance::WalletPublishCause::FencedAdmission }) if wallet == selected)
    );
    assert_eq!(h.live.snapshot().entries[0].wallet, incumbent);
    let mut bench = h.initial.clone();
    bench.entries = golden::BracketFinancialHarness::entries(&[selected, survivor]);
    let (retry, times) = pe_service::supabase_reader::select_membership(
        bench,
        HashMap::from([(selected, 10), (survivor, 10)]),
        &HashSet::from([selected]),
        1,
    );
    assert_eq!(retry.entries[0].wallet, survivor);
    preparer.prepare(&[survivor]).await.unwrap();
    preparer
        .scenario_publish_ranking(&h.live, &h.writer_lock, retry.entries, &times, 1)
        .await
        .unwrap();
    assert_eq!(h.live.snapshot().entries[0].wallet, survivor);
    drop(preparer);
    h.shutdown().await;
}

#[tokio::test]
async fn proof_changed_at_locked_publication_recaptures_cited_manifest() {
    use pe_service::paper_recovery::{
        PaperLogRecord, paper_era, replay_membership, scan_paper_log,
    };
    use pe_service::watchlist_maintenance::{
        MembershipApplyError, PublishError, WalletPublishCause,
    };

    let (incumbent, selected) = (wallet(0x96), wallet(0x97));
    let (dir, paper, mut engine) = fresh(&[incumbent]);
    install_empty_anchor(&mut engine, &paper, incumbent, 0);
    drop(engine);
    let h = golden::BracketFinancialHarness::new(dir.path(), paper.clone(), &[incumbent]).await;
    let preparer = financial_preparer(
        &h,
        &paper,
        Arc::new(QueueFetcher::new(stable_responses(&[(selected, 1, "1")]))),
    );
    assert_eq!(
        preparer.prepare(&[selected]).await.unwrap().admitted,
        vec![selected]
    );

    let (relay_tx, mut relay_rx) = mpsc::channel(1);
    let publisher =
        AdmissionPreparer::new(relay_tx, paper.clone()).with_source_log(h.source.clone());
    let first = publisher.clone();
    let live = h.live.clone();
    let writer_lock = h.writer_lock.clone();
    let first_try = tokio::spawn(async move {
        first
            .scenario_publish_ranking(
                &live,
                &writer_lock,
                golden::BracketFinancialHarness::entries(&[selected]),
                &HashMap::from([(selected, 10)]),
                1,
            )
            .await
    });
    let publication = relay_rx.recv().await.unwrap();
    let anchor = paper.position_anchors(&selected).unwrap().pop().unwrap();
    let validation = paper.position_validation(&selected).unwrap().unwrap();
    paper
        .install_anchors(&[AnchorInstallRecord {
            repaired_history: Vec::new(),
            expected_fence: None,
            history_status: None,
            wallet: selected,
            balances: Vec::new(),
            activity_cutoff_unix: anchor.activity_cutoff_unix,
            anchored_at_unix: anchor.anchored_at_unix + 1,
            ledger_hash_after: anchor.ledger_hash_after,
            positions_proof_hash: validation.positions_proof_hash,
            activity_bounds_json: validation.activity_bounds_json,
            source_log_generation: validation.source_log_generation,
            proof_json: anchor.proof_json,
            recorded_at_unix: validation.recorded_at_unix + 1,
        }])
        .unwrap();
    h.control.send(publication).await.unwrap();
    assert!(
        matches!(first_try.await.unwrap(), Err(MembershipApplyError::Publication(PublishError::Wallet { wallet, cause: WalletPublishCause::ProofChanged })) if wallet == selected)
    );
    assert_eq!(h.live.structural_membership(), HashSet::from([incumbent]));

    let second = publisher.clone();
    let live = h.live.clone();
    let writer_lock = h.writer_lock.clone();
    let second_try = tokio::spawn(async move {
        second
            .scenario_publish_ranking(
                &live,
                &writer_lock,
                golden::BracketFinancialHarness::entries(&[selected]),
                &HashMap::from([(selected, 10)]),
                1,
            )
            .await
    });
    let recaptured_publication = relay_rx.recv().await.unwrap();
    if let OrchestratorControl::PublishMembership { checks, .. } = &recaptured_publication {
        let current = pe_service::watchlist_maintenance::PublicationBinding::for_additions(
            h.live.structural_membership(),
            &paper,
            &[selected],
        )
        .unwrap();
        assert_eq!(checks.binding.digests, current.digests);
    } else {
        unreachable!("recaptured attempt sent a non-publication command");
    }
    h.control.send(recaptured_publication).await.unwrap();
    second_try.await.unwrap().unwrap();
    let admissions = Reader::replay(&h.source_path)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .into_iter()
        .filter(|(_, frame)| frame.source_id.0 == "pe-service.watchlist-admission")
        .map(|(_, frame)| frame.this_hash)
        .collect::<Vec<_>>();
    assert_eq!(admissions.len(), 2);
    let records = Reader::replay(&h.paper_path)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .into_iter()
        .filter_map(|(_, frame)| serde_json::from_slice::<PaperLogRecord>(&frame.payload).ok())
        .filter_map(|record| match record {
            PaperLogRecord::MembershipChanged { evidence, .. } => Some(evidence),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(records.len(), 1);
    assert!(
        serde_json::to_string(&records[0])
            .unwrap()
            .contains(&admissions[1].to_hex().to_string())
    );
    let era = paper_era(scan_paper_log(&h.paper_path).unwrap());
    let replayed = replay_membership(&era, h.initial.clone(), &h.source_path)
        .unwrap()
        .unwrap();
    assert_eq!(replayed.watchlist.entries[0].wallet, selected);
    drop(preparer);
    drop(publisher);
    h.shutdown().await;
}

#[tokio::test]
async fn locked_structural_change_rejects_prepared_publication_and_retry_replays() {
    use pe_service::paper_recovery::{paper_era, replay_membership, scan_paper_log};
    use pe_service::watchlist_maintenance::{MembershipApplyError, PublishError};

    let (incumbent, selected) = (wallet(0x98), wallet(0x99));
    let (dir, paper, mut engine) = fresh(&[incumbent]);
    install_empty_anchor(&mut engine, &paper, incumbent, 0);
    drop(engine);
    let h = golden::BracketFinancialHarness::new(dir.path(), paper.clone(), &[incumbent]).await;
    let preparer = financial_preparer(
        &h,
        &paper,
        Arc::new(QueueFetcher::new(stable_responses(&[(selected, 1, "1")]))),
    );
    assert_eq!(
        preparer.prepare(&[selected]).await.unwrap().admitted,
        vec![selected]
    );
    let (relay_tx, mut relay_rx) = mpsc::channel(1);
    let publisher = AdmissionPreparer::new(relay_tx, paper).with_source_log(h.source.clone());
    let first = publisher.clone();
    let live = h.live.clone();
    let writer_lock = h.writer_lock.clone();
    let pending = tokio::spawn(async move {
        first
            .scenario_publish_ranking(
                &live,
                &writer_lock,
                golden::BracketFinancialHarness::entries(&[selected]),
                &HashMap::from([(selected, 10)]),
                1,
            )
            .await
    });
    let message = relay_rx.recv().await.unwrap();
    h.live.scenario_commit_structural_change(&[incumbent], &[]);
    h.control.send(message).await.unwrap();
    assert!(matches!(
        pending.await.unwrap(),
        Err(MembershipApplyError::Publication(
            PublishError::StaleStructure
        ))
    ));
    assert_eq!(
        Reader::replay(&h.paper_path)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|(_, frame)| {
                serde_json::from_slice::<pe_service::paper_recovery::PaperLogRecord>(&frame.payload)
                    .is_ok_and(|record| {
                        matches!(
                            record,
                            pe_service::paper_recovery::PaperLogRecord::MembershipChanged { .. }
                        )
                    })
            })
            .count(),
        0
    );

    h.live.scenario_commit_structural_change(&[], &[incumbent]);
    let second = publisher.clone();
    let live = h.live.clone();
    let writer_lock = h.writer_lock.clone();
    let retry = tokio::spawn(async move {
        second
            .scenario_publish_ranking(
                &live,
                &writer_lock,
                golden::BracketFinancialHarness::entries(&[selected]),
                &HashMap::from([(selected, 10)]),
                1,
            )
            .await
    });
    h.control
        .send(relay_rx.recv().await.unwrap())
        .await
        .unwrap();
    retry.await.unwrap().unwrap();
    let era = paper_era(scan_paper_log(&h.paper_path).unwrap());
    let replayed = replay_membership(&era, h.initial.clone(), &h.source_path)
        .unwrap()
        .unwrap();
    assert_eq!(replayed.watchlist.entries[0].wallet, selected);
    drop(preparer);
    drop(publisher);
    h.shutdown().await;
}

#[tokio::test]
async fn locked_history_loss_is_a_typed_wallet_rejection() {
    use pe_service::watchlist_maintenance::{
        MembershipApplyError, PublishError, WalletPublishCause,
    };

    let (incumbent, selected) = (wallet(0x9a), wallet(0x9b));
    let (dir, paper, mut engine) = fresh(&[incumbent]);
    install_empty_anchor(&mut engine, &paper, incumbent, 0);
    drop(engine);
    let h = golden::BracketFinancialHarness::new(dir.path(), paper.clone(), &[incumbent]).await;
    let preparer = financial_preparer(
        &h,
        &paper,
        Arc::new(QueueFetcher::new(stable_responses(&[(selected, 1, "1")]))),
    );
    assert_eq!(
        preparer.prepare(&[selected]).await.unwrap().admitted,
        vec![selected]
    );
    let (relay_tx, mut relay_rx) = mpsc::channel(1);
    let publisher = AdmissionPreparer::new(relay_tx, paper).with_source_log(h.source.clone());
    let live = h.live.clone();
    let writer_lock = h.writer_lock.clone();
    let pending = tokio::spawn(async move {
        publisher
            .scenario_publish_ranking(
                &live,
                &writer_lock,
                golden::BracketFinancialHarness::entries(&[selected]),
                &HashMap::from([(selected, 10)]),
                1,
            )
            .await
    });
    let message = relay_rx.recv().await.unwrap();
    rusqlite::Connection::open(dir.path().join("paper.db"))
        .unwrap()
        .execute(
            "UPDATE wallet_history_status_v2 SET complete = 0 WHERE wallet_hex = ?1",
            [selected.to_string()],
        )
        .unwrap();
    h.control.send(message).await.unwrap();
    assert!(
        matches!(pending.await.unwrap(), Err(MembershipApplyError::Publication(PublishError::Wallet { wallet, cause: WalletPublishCause::IncompleteHistory })) if wallet == selected)
    );
    assert_eq!(h.live.structural_membership(), HashSet::from([incumbent]));
    drop(preparer);
    h.shutdown().await;
}

#[tokio::test]
async fn routine_refresh_retries_covered_history_intervention() {
    let wallet = wallet(0x8b);
    let (_dir, paper, mut engine) = fresh(&[wallet]);
    install_empty_anchor(&mut engine, &paper, wallet, 20);
    let late = serde_json::to_vec(&vec![activity(wallet, 1, "1", "0xcovered-late", 10)]).unwrap();
    let responses = HashMap::from([
        (
            activity_url(wallet),
            vec![
                b"[]".to_vec(),
                late.clone(),
                late.clone(),
                late.clone(),
                late,
            ],
        ),
        (
            position_url(wallet, PositionPartition::NotRedeemable),
            vec![b"[]".to_vec(); 3],
        ),
        (
            position_url(wallet, PositionPartition::Redeemable),
            vec![b"[]".to_vec(); 3],
        ),
    ]);
    let fetcher = Arc::new(QueueFetcher::new(responses));
    let (tx, rx) = mpsc::channel(2);
    let actor = spawn_control_actor(rx, engine, paper.clone());
    let preparer = AdmissionPreparer::with_validator(
        tx,
        paper.clone(),
        validator_from_fetcher(fetcher.clone()),
    );
    assert_eq!(
        preparer.prepare_if_due(wallet, END, 1).await.unwrap(),
        AnchorRefreshOutcome::Anchored
    );
    assert_eq!(
        fetcher
            .urls()
            .iter()
            .filter(|url| **url == activity_url(wallet))
            .count(),
        5
    );
    assert!(paper.decision_pending_history().unwrap().is_empty());
    assert_eq!(
        paper.wallet_coverage(&wallet).unwrap().activity_cutoff_unix,
        Some(END)
    );
    drop(preparer);
    actor.await.unwrap();
}

#[tokio::test]
async fn negative_share_amount_defers_only_that_wallet_and_promotes_only_seeded_healthy_history() {
    let healthy = wallet(0x74);
    let corrupt = wallet(0x75);
    for seeded in [true, false] {
        let (_dir, paper, mut engine) = fresh(&[]);
        for wallet in [healthy, corrupt].into_iter().filter(|_| seeded) {
            paper
                .record_reconciled_history_status(&WalletHistoryStatusRecord {
                    wallet,
                    complete: false,
                    proof_json: "{\"seed\":true}".to_owned(),
                    updated_at_unix: 1,
                })
                .unwrap();
        }
        let corrupt_history = paper.wallet_history_status(&corrupt).unwrap();
        let mut responses = stable_responses(&[(healthy, 1, "1.000000")]);
        let row = activity(corrupt, 2, "-1.000000", "0xnegative-size", 10);
        responses.insert(
            activity_url(corrupt),
            vec![serde_json::to_vec(&[row]).unwrap()],
        );

        let outcome = validator(responses)
            .validate_direct_with_deferrals(&[corrupt, healthy], &mut engine, &paper)
            .await
            .unwrap();
        assert_eq!(outcome.accepted.len(), 1);
        assert_eq!(outcome.accepted[0].wallet, healthy);
        assert_eq!(outcome.deferred.len(), 1);
        let (wallet, error) = &outcome.deferred[0];
        assert_eq!(*wallet, corrupt);
        assert!(matches!(
            error,
            CausalPositionError::Activity {
                source: ActivityReadError::Parse(ActivityParseError::InvalidRow {
                    source: ActivityValidationError::InvalidShareAmount { .. },
                    ..
                }),
                ..
            }
        ));
        assert!(!is_deferred_causal_position_error(error));
        assert_eq!(
            error.class(),
            pe_service::position_seeder::FailureClass::WalletPersistent
        );
        assert_eq!(paper.position_anchors(&healthy).unwrap().len(), 1);
        assert!(paper.position_validation(&healthy).unwrap().is_some());
        assert!(paper.position_anchors(&corrupt).unwrap().is_empty());
        assert!(paper.position_validation(&corrupt).unwrap().is_none());
        assert!(!paper.is_wallet_fenced(&corrupt).unwrap());
        assert_eq!(paper.wallet_history_complete(&healthy).unwrap(), seeded);
        assert!(!paper.wallet_history_complete(&corrupt).unwrap());
        assert_eq!(
            paper.wallet_history_status(&corrupt).unwrap(),
            corrupt_history
        );
    }
}

#[tokio::test]
async fn unparseable_venue_row_defers_only_that_wallet_at_boot() {
    // Issue #594: rehearsal attempt 9 aborted the whole boot on one wallet's venue TRADE row with
    // price 3.1968021978. Boot leaves that wallet unvalidated and anchors the others.
    let healthy = wallet(0x74);
    let corrupt = wallet(0x75);
    let (_dir, paper, mut engine) = fresh(&[healthy, corrupt]);
    let mut responses = stable_responses(&[(healthy, 1, "1.000000"), (corrupt, 2, "1.000000")]);
    let mut row = activity(corrupt, 2, "0.91", "0xcorrupt", 10);
    row["side"] = json!("SELL");
    row["price"] = json!("3.1968021978");
    row["usdcSize"] = json!("2.909090");
    let page = serde_json::to_vec(&vec![row]).unwrap();
    responses.insert(
        activity_url(corrupt),
        vec![page.clone(), page.clone(), page],
    );

    let outcome = validator(responses)
        .validate_direct_with_deferrals(&[healthy, corrupt], &mut engine, &paper)
        .await
        .unwrap();
    let installs = outcome.accepted;

    assert_eq!(installs.len(), 1);
    assert_eq!(installs[0].wallet, healthy);
    assert_eq!(outcome.deferred.len(), 1);
    assert_eq!(outcome.deferred[0].0, corrupt);
    assert_eq!(
        outcome.deferred[0].1.class(),
        pe_service::position_seeder::FailureClass::WalletPersistent
    );
    assert!(paper.position_validation(&healthy).unwrap().is_some());
    assert!(paper.position_validation(&corrupt).unwrap().is_none());
    assert!(paper.position_anchors(&corrupt).unwrap().is_empty());
    assert!(!paper.is_wallet_fenced(&corrupt).unwrap());
}

#[tokio::test]
async fn periodic_refresh_defers_a_wallet_with_an_unparseable_venue_row() {
    // Issue #594: once such a wallet is live, the hourly anchor refresh must report `Deferred`
    // instead of surfacing an error that would end the poll round.
    let wallet = wallet(0x77);
    let (_dir, paper, engine) = fresh(&[wallet]);
    let (control_tx, control_rx) = mpsc::channel(8);
    let actor = spawn_control_actor(control_rx, engine, Arc::clone(&paper));
    let mut responses = stable_responses(&[(wallet, 1, "1.000000")]);
    let mut row = activity(wallet, 1, "0.91", "0xcorrupt", 10);
    row["side"] = json!("SELL");
    row["price"] = json!("3.1968021978");
    row["usdcSize"] = json!("2.909090");
    let page = serde_json::to_vec(&vec![row]).unwrap();
    responses.insert(activity_url(wallet), vec![page.clone(), page.clone(), page]);
    let preparer =
        AdmissionPreparer::with_validator(control_tx, Arc::clone(&paper), validator(responses));

    assert_eq!(
        preparer.prepare_if_due(wallet, END, 1).await.unwrap(),
        AnchorRefreshOutcome::Deferred
    );
    assert!(paper.position_anchors(&wallet).unwrap().is_empty());
    assert!(!paper.is_wallet_fenced(&wallet).unwrap());
    drop(preparer);
    actor.await.unwrap();
}

fn rollout_history(dir: &TempDir) -> Vec<pe_paper_state::MarketHistoryRecord> {
    let conn = rusqlite::Connection::open(dir.path().join("paper.db")).unwrap();
    let mut query = conn.prepare("SELECT wallet_hex, market_id, first_epoch, source_trade_id FROM wallet_market_history_v2 ORDER BY wallet_hex, market_id").unwrap();
    query
        .query_map([], |row| {
            Ok(pe_paper_state::MarketHistoryRecord {
                wallet: WalletAddress::from_hex(&row.get::<_, String>(0)?).unwrap(),
                market_id: MarketId(VenueMarketId(row.get(1)?)),
                first_epoch: row.get(2)?,
                source_trade_id: pe_core_types::SourceTradeId(row.get(3)?),
            })
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

fn rollout_record_effect(
    paper: &PaperStateDb,
    row: &pe_source_polymarket_public::ActivityAggregate,
    effect: &pe_position_ledger::LedgerEffect,
    disposition: &str,
) {
    rollout_record_effect_with_cursor(paper, row, effect, disposition, true);
}

fn rollout_record_effect_with_cursor(
    paper: &PaperStateDb,
    row: &pe_source_polymarket_public::ActivityAggregate,
    effect: &pe_position_ledger::LedgerEffect,
    disposition: &str,
    advance_cursor: bool,
) {
    paper
        .commit_activity_bucket(&pe_paper_state::ActivityBucketCommit {
            wallet: row.group_id.components().wallet,
            source_epoch: row.source_time.0.unix_timestamp(),
            dispositions: vec![pe_paper_state::ActivityDispositionRecord {
                source_trade_id: row.group_id.key().clone(),
                transaction_hash: row.group_id.components().transaction_hash.clone(),
                wallet: row.group_id.components().wallet,
                source_epoch: row.source_time.0.unix_timestamp(),
                semantic_revision: row.semantic_revision.as_str().to_owned(),
                activity_type: row.group_id.components().activity_type.as_str().to_owned(),
                disposition: disposition.to_owned(),
                proof_json: effect.to_document().unwrap(),
                no_copy: None,
            }],
            leader_positions: Vec::new(),
            gate_results: Vec::new(),
            history_effects: Vec::new(),
            history_status: None,
            pending: Vec::new(),
            fence: None,
            reanchor: None,
            advance_cursor,
        })
        .unwrap();
}

fn rollout_fence(
    dir: &TempDir,
    wallet: WalletAddress,
    id: &pe_core_types::SourceTradeId,
    cause: &str,
    epoch: i64,
) {
    rusqlite::Connection::open(dir.path().join("paper.db"))
        .unwrap()
        .execute(
            "INSERT INTO wallet_fences VALUES (?1, ?2, ?3, ?4, 99)",
            rusqlite::params![
                wallet.to_string(),
                id.0,
                cause,
                json!({"bucket_epoch":epoch}).to_string()
            ],
        )
        .unwrap();
}

fn rollout_empty_reads(wallet: WalletAddress, attempts: usize) -> HashMap<String, Vec<Vec<u8>>> {
    HashMap::from([
        (activity_url(wallet), vec![b"[]".to_vec(); 3 * attempts]),
        (
            position_url(wallet, PositionPartition::NotRedeemable),
            vec![b"[]".to_vec(); 2 * attempts],
        ),
        (
            position_url(wallet, PositionPartition::Redeemable),
            vec![b"[]".to_vec(); 2 * attempts],
        ),
    ])
}

/// PASS: every immutable effect participates in installation; a zero current revision cannot
/// erase a prior BUY or hide a prior conversion/unknown effect. Exact secondary revisions remain
/// idempotent after clearance, restart, and a full-history refresh.
#[tokio::test]
async fn paper_service_rollout_recorded_union_repairs_buys_and_refuses_unsafe_effects() {
    use pe_position_ledger::{LedgerEffect, LedgerMutation};
    for unsafe_effect in [
        None,
        Some(LedgerEffect::Conversion),
        Some(LedgerEffect::UnknownEffect),
    ] {
        let wallet = wallet(0xd1);
        let (dir, paper, _) = fresh(&[wallet]);
        let original = aggregate(activity(wallet, 1, "1", "0xunion", 10), wallet);
        let original_effect = unsafe_effect
            .clone()
            .unwrap_or_else(|| LedgerMutation::from_activity(&original).unwrap().effect);
        rollout_record_effect(&paper, &original, &original_effect, "applied");
        rollout_fence(
            &dir,
            wallet,
            original.group_id.key(),
            "position_underflow",
            10,
        );
        let mut zero = activity(wallet, 1, "0", "0xunion", 10);
        zero["usdcSize"] = json!("0");
        let revised = aggregate(zero.clone(), wallet);
        rollout_record_effect(&paper, &revised, &LedgerEffect::RawOnly, "wallet_fenced");
        let stored = paper.activity_group_state(original.group_id.key()).unwrap();
        let generation = paper.wallet_coverage(&wallet).unwrap().coverage_generation;
        let engine =
            BucketCommitEngine::load(paper.clone(), build_leader_ledger(&paper).unwrap()).unwrap();
        let (tx, rx) = mpsc::channel(4);
        let actor = spawn_control_actor(rx, engine, paper.clone());
        let mut reads = rollout_empty_reads(wallet, 1);
        reads.insert(
            activity_url(wallet),
            vec![serde_json::to_vec(&vec![zero.clone()]).unwrap(); 3],
        );
        let preparer =
            AdmissionPreparer::with_validator(tx.clone(), paper.clone(), validator(reads));
        let result = preparer.prepare(&[wallet]).await.unwrap();
        if unsafe_effect.is_some() {
            assert!(result.admitted.is_empty());
            assert_eq!(result.deferred[0].kind, "anchor.unsafe_recovery");
            assert!(paper.is_wallet_fenced(&wallet).unwrap());
            assert!(paper.position_anchors(&wallet).unwrap().is_empty());
            assert!(rollout_history(&dir).is_empty());
        } else {
            assert_eq!(result.admitted, vec![wallet]);
            assert!(!paper.is_wallet_fenced(&wallet).unwrap());
            let history = rollout_history(&dir);
            assert_eq!(history.len(), 1);
            assert_eq!(history[0].first_epoch, 10);
            assert_eq!(history[0].source_trade_id, *original.group_id.key());
            let proof: Value =
                serde_json::from_str(&paper.position_anchors(&wallet).unwrap()[0].proof_json)
                    .unwrap();
            assert_eq!(proof["version"], 1);
            assert_eq!(proof["cleared_fence"]["cause"], "position_underflow");
            assert!(proof.get("repaired_history").is_none());
        }
        assert_eq!(
            paper.activity_group_state(original.group_id.key()).unwrap(),
            stored
        );
        assert_eq!(
            paper.wallet_coverage(&wallet).unwrap().coverage_generation,
            generation
        );
        drop(preparer);
        drop(tx);
        actor.await.unwrap();
        if unsafe_effect.is_none() {
            let mut engine =
                BucketCommitEngine::load(paper.clone(), build_leader_ledger(&paper).unwrap())
                    .unwrap();
            let mut reads = rollout_empty_reads(wallet, 1);
            reads.insert(
                activity_url(wallet),
                vec![serde_json::to_vec(&vec![zero]).unwrap(); 3],
            );
            validator(reads)
                .validate_direct(&[wallet], &mut engine, &paper)
                .await
                .unwrap();
            assert!(!paper.is_wallet_fenced(&wallet).unwrap());
            assert_eq!(
                paper.activity_group_state(original.group_id.key()).unwrap(),
                stored
            );
            assert_eq!(
                paper.wallet_coverage(&wallet).unwrap().coverage_generation,
                generation
            );
            assert_eq!(rollout_history(&dir).len(), 1);
        }
    }
}

#[tokio::test]
async fn paper_service_rollout_fresh_metadata_rejects_previously_raw_only_unsafe_effects() {
    use pe_position_ledger::LedgerEffect;
    for activity_type in ["CONVERSION", "OTHER"] {
        let wallet = wallet(0xd2);
        let (dir, paper, _) = fresh(&[wallet]);
        let mut row = activity(wallet, 1, "1", "0xfresh-unsafe", 10);
        row["type"] = json!(activity_type);
        let original = aggregate(row.clone(), wallet);
        rollout_record_effect(&paper, &original, &LedgerEffect::RawOnly, "raw_only");
        rollout_fence(
            &dir,
            wallet,
            original.group_id.key(),
            "position_underflow",
            10,
        );
        let engine = BucketCommitEngine::load(paper.clone(), PositionLedger::new()).unwrap();
        let (tx, rx) = mpsc::channel(4);
        let actor = spawn_control_actor(rx, engine, paper.clone());
        let mut reads = rollout_empty_reads(wallet, 1);
        reads.insert(
            activity_url(wallet),
            vec![serde_json::to_vec(&vec![row]).unwrap(); 3],
        );
        let preparer =
            AdmissionPreparer::with_validator(tx.clone(), paper.clone(), validator(reads));
        let result = preparer.prepare(&[wallet]).await.unwrap();
        assert!(result.admitted.is_empty());
        assert_eq!(result.deferred[0].kind, "validation.unsafe_recovery");
        assert!(paper.is_wallet_fenced(&wallet).unwrap());
        assert!(paper.position_anchors(&wallet).unwrap().is_empty());
        assert!(rollout_history(&dir).is_empty());
        drop(preparer);
        drop(tx);
        actor.await.unwrap();
    }
}

#[tokio::test]
async fn paper_service_rollout_fence_allowlist_cutoff_and_install_races_fail_closed() {
    use pe_position_ledger::LedgerEffect;
    for (cause, epoch, succeeds) in [
        ("position_underflow", 10, true),
        ("position_overflow", 10, true),
        ("order_dependent_equal_second", 10, true),
        ("late_group_after_bucket_commit", 10, true),
        ("position_underflow", END, false),
        ("invalid_mapping", 10, false),
        ("conversion_unknown_conditions", 10, false),
        ("unknown_activity_effect", 10, false),
        ("revised_applied_aggregate", 10, false),
    ] {
        let wallet = wallet(0xd3);
        let (dir, paper, _) = fresh(&[wallet]);
        let original = aggregate(activity(wallet, 1, "1", "0xallowlist", 10), wallet);
        rollout_record_effect(&paper, &original, &LedgerEffect::RawOnly, "raw_only");
        rollout_fence(&dir, wallet, original.group_id.key(), cause, epoch);
        let engine = BucketCommitEngine::load(paper.clone(), PositionLedger::new()).unwrap();
        let (tx, rx) = mpsc::channel(4);
        let actor = spawn_control_actor(rx, engine, paper.clone());
        let preparer = AdmissionPreparer::with_validator(
            tx.clone(),
            paper.clone(),
            validator(rollout_empty_reads(wallet, 1)),
        );
        let result = preparer.prepare(&[wallet]).await.unwrap();
        assert_eq!(!result.admitted.is_empty(), succeeds, "{cause}/{epoch}");
        assert_eq!(paper.is_wallet_fenced(&wallet).unwrap(), !succeeds);
        drop(preparer);
        drop(tx);
        actor.await.unwrap();
    }
    for proof in ["{}", "{\"bucket_epoch\":\"10\"}", "malformed"] {
        let wallet = wallet(0xd5);
        let (dir, paper, _) = fresh(&[wallet]);
        let original = aggregate(activity(wallet, 1, "1", "0xmalformed", 10), wallet);
        rollout_record_effect(&paper, &original, &LedgerEffect::RawOnly, "raw_only");
        rollout_fence(
            &dir,
            wallet,
            original.group_id.key(),
            "position_underflow",
            10,
        );
        rusqlite::Connection::open(dir.path().join("paper.db"))
            .unwrap()
            .execute("UPDATE wallet_fences SET proof_json=?1", [proof])
            .unwrap();
        let (tx, rx) = mpsc::channel(4);
        let actor = spawn_control_actor(
            rx,
            BucketCommitEngine::load(paper.clone(), PositionLedger::new()).unwrap(),
            paper.clone(),
        );
        let preparer = AdmissionPreparer::with_validator(
            tx.clone(),
            paper.clone(),
            validator(rollout_empty_reads(wallet, 1)),
        );
        assert!(
            preparer
                .prepare(&[wallet])
                .await
                .unwrap()
                .admitted
                .is_empty()
        );
        assert!(paper.is_wallet_fenced(&wallet).unwrap());
        assert!(paper.position_anchors(&wallet).unwrap().is_empty());
        assert!(rollout_history(&dir).is_empty());
        drop(preparer);
        drop(tx);
        actor.await.unwrap();
    }
    for unsafe_revision in [false, true] {
        let wallet = wallet(0xd4);
        let (dir, paper, _) = fresh(&[wallet]);
        let original = aggregate(activity(wallet, 1, "1", "0xrace", 10), wallet);
        rollout_record_effect(&paper, &original, &LedgerEffect::RawOnly, "raw_only");
        rollout_fence(
            &dir,
            wallet,
            original.group_id.key(),
            "position_underflow",
            10,
        );
        let mut engine = BucketCommitEngine::load(paper.clone(), PositionLedger::new()).unwrap();
        let (tx, rx) = mpsc::channel(4);
        let actor = spawn_control_actor(
            rx,
            BucketCommitEngine::load(paper.clone(), PositionLedger::new()).unwrap(),
            paper.clone(),
        );
        let outcome = validator(rollout_empty_reads(wallet, 1))
            .validate_via_control(
                &[wallet],
                &tx,
                &paper,
                pe_service::position_seeder::ValidationPurpose::CatchUp,
                None,
            )
            .await;
        let mut revised = activity(wallet, 1, "2", "0xrace", 10);
        revised["usdcSize"] = json!("1");
        let revised = aggregate(revised, wallet);
        rollout_record_effect(
            &paper,
            &revised,
            &if unsafe_revision {
                LedgerEffect::Conversion
            } else {
                LedgerEffect::RawOnly
            },
            "wallet_fenced",
        );
        assert!(matches!(
            engine.install_anchors(&outcome.accepted),
            Err(pe_service::bucket_commit::AnchorInstallError::CoverageGenerationChanged { .. })
        ));
        assert!(paper.position_anchors(&wallet).unwrap().is_empty());
        assert!(rollout_history(&dir).is_empty());
        assert!(paper.is_wallet_fenced(&wallet).unwrap());
        drop(tx);
        actor.await.unwrap();
    }
}

/// PASS: an internally monotonic bracket after UTC rollback cannot clear a fence below a
/// disposed revision's epoch. Admission defers and exact retries remain nonfatal while fenced.
#[tokio::test]
async fn paper_service_rollout_recovery_cutoff_before_recorded_revision_defers() {
    let wallet = wallet(0xd6);
    let (dir, paper, mut engine) = fresh(&[wallet]);
    let older_row = activity(wallet, 1, "1", "0xolder-fence", 100);
    let older = aggregate(older_row.clone(), wallet);
    engine
        .commit(vec![older.clone()], &context(100), zero_basis())
        .unwrap();
    rollout_fence(
        &dir,
        wallet,
        older.group_id.key(),
        "position_underflow",
        100,
    );
    let fence = paper.wallet_fence(&wallet).unwrap();
    let mut engine = BucketCommitEngine::load(paper.clone(), engine.into_ledger()).unwrap();
    let later_row = activity(wallet, 1, "1", "0xlater-revision", 200);
    engine
        .commit(
            vec![aggregate(later_row, wallet)],
            &context(200),
            zero_basis(),
        )
        .unwrap();
    let mut revised_row = activity(wallet, 1, "2", "0xlater-revision", 200);
    revised_row["usdcSize"] = json!("1");
    let revised = aggregate(revised_row, wallet);
    let retained = engine
        .commit(vec![revised.clone()], &context(200), zero_basis())
        .unwrap();
    assert!(retained.retained_revision);
    let stored_revision = paper
        .activity_revision_state(revised.group_id.key(), revised.semantic_revision.as_str())
        .unwrap()
        .unwrap();
    assert_eq!(stored_revision.disposition, "wallet_fenced");
    assert_eq!(
        ledger_capture(engine.ledger(), &paper, wallet)
            .unwrap()
            .cursor,
        Some(200)
    );
    let coverage = paper.wallet_coverage(&wallet).unwrap();
    let history = rollout_history(&dir);

    // All three new bounds increase, but the accepted second-read cutoff is only 150.
    // The older row retains the same asset's metadata mapping below that cutoff.
    let mut responses = rollout_empty_reads(wallet, 1);
    responses.remove(&activity_url(wallet));
    for end in [149, 150, 151] {
        responses.insert(
            activity_url_at(wallet, end),
            vec![serde_json::to_vec(&vec![older_row.clone()]).unwrap()],
        );
    }
    let fetcher = Arc::new(QueueFetcher::new(responses));
    let ends = Arc::new(Mutex::new(VecDeque::from([149, 150, 151, 151])));
    let validator = validator_from_fetcher(fetcher.clone()).with_clock(Arc::new(move || {
        ends.lock().unwrap().pop_front().unwrap_or(151)
    }));
    let (tx, rx) = mpsc::channel(4);
    let actor = spawn_control_actor(rx, engine, paper.clone());
    let preparer = AdmissionPreparer::with_validator(tx.clone(), paper.clone(), validator);
    let outcome = preparer.prepare(&[wallet]).await.unwrap();
    assert!(outcome.admitted.is_empty());
    assert_eq!(outcome.deferred.len(), 1);
    assert_eq!(outcome.deferred[0].stage, "anchor_install");
    assert_eq!(outcome.deferred[0].kind, "anchor.cutoff_regression");
    assert_eq!(
        outcome.deferred[0].class,
        pe_service::position_seeder::FailureClass::WalletTransient
    );
    assert!(outcome.deferred[0].message.contains("from 200 to 150"));
    assert_eq!(
        fetcher
            .urls()
            .into_iter()
            .filter(|url| url.contains("/activity?"))
            .collect::<Vec<_>>(),
        [149, 150, 151].map(|end| activity_url_at(wallet, end))
    );
    assert_eq!(paper.wallet_fence(&wallet).unwrap(), fence);
    assert!(paper.position_anchors(&wallet).unwrap().is_empty());
    assert!(paper.position_validation(&wallet).unwrap().is_none());
    assert_eq!(paper.wallet_coverage(&wallet).unwrap(), coverage);
    assert_eq!(rollout_history(&dir), history);

    // An ordinary retry through the same serialized owner remains an exact disposed retry.
    let (committed, acknowledgement) = tokio::sync::oneshot::channel();
    tx.send(OrchestratorControl::CommitActivityBucket {
        aggregates: vec![revised.clone()],
        context: Arc::new(context(200)),
        committed,
    })
    .await
    .unwrap();
    let retried = acknowledgement.await.unwrap().unwrap();
    assert!(retried.already_committed);
    assert!(!retried.retained_revision);
    assert_eq!(
        paper
            .activity_revision_state(revised.group_id.key(), revised.semantic_revision.as_str())
            .unwrap(),
        Some(stored_revision)
    );
    assert_eq!(paper.wallet_fence(&wallet).unwrap(), fence);
    assert_eq!(paper.wallet_coverage(&wallet).unwrap(), coverage);
    drop(preparer);
    drop(tx);
    actor.await.unwrap();
}

struct RolloutDeadlineFetcher {
    inner: QueueFetcher,
    starts: mpsc::UnboundedSender<WalletAddress>,
    first: Mutex<HashSet<WalletAddress>>,
    release: Arc<tokio::sync::Semaphore>,
}
impl PageFetcher for RolloutDeadlineFetcher {
    async fn fetch_page(&self, url: &str) -> Result<Vec<u8>, SourceError> {
        if url.contains("/activity?") {
            let wallet = url
                .split("user=")
                .nth(1)
                .unwrap()
                .split('&')
                .next()
                .unwrap();
            let wallet = WalletAddress::from_hex(wallet).unwrap();
            if self.first.lock().unwrap().insert(wallet) {
                self.starts.send(wallet).unwrap();
                self.release.acquire().await.unwrap().forget();
            }
        }
        self.inner.fetch_page(url).await
    }
}

#[tokio::test(start_paused = true)]
async fn paper_service_rollout_deadline_drains_four_and_leaves_fifth_unstarted() {
    let wallets = (0xe1..=0xe5).map(wallet).collect::<Vec<_>>();
    let (_dir, paper, engine) = fresh(&wallets);
    let specs = wallets
        .iter()
        .enumerate()
        .map(|(n, w)| (*w, u8::try_from(n + 1).unwrap(), "1"))
        .collect::<Vec<_>>();
    let (starts, mut started) = mpsc::unbounded_channel();
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let fetcher = Arc::new(RolloutDeadlineFetcher {
        inner: QueueFetcher::new(stable_responses(&specs)),
        starts,
        first: Mutex::new(HashSet::new()),
        release: release.clone(),
    });
    let validator = validator_from_reconciliation(fetcher);
    let (tx, rx) = mpsc::channel(4);
    let actor = spawn_control_actor(rx, engine, paper.clone());
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    let paper_task = paper.clone();
    let wallets_task = wallets.clone();
    let tx_task = tx.clone();
    let task = tokio::spawn(async move {
        validator
            .validate_via_control(
                &wallets_task,
                &tx_task,
                &paper_task,
                pe_service::position_seeder::ValidationPurpose::CatchUp,
                Some(deadline),
            )
            .await
    });
    let mut launched = HashSet::new();
    for _ in 0..4 {
        launched.insert(started.recv().await.unwrap());
    }
    assert_eq!(launched, wallets[..4].iter().copied().collect());
    tokio::time::advance(std::time::Duration::from_secs(11)).await;
    release.add_permits(4);
    let outcome = task.await.unwrap();
    assert_eq!(outcome.started_prefix, 4);
    assert_eq!(
        outcome
            .accepted
            .iter()
            .map(|install| install.wallet)
            .collect::<Vec<_>>(),
        wallets[..4]
    );
    assert!(outcome.deferred.is_empty());
    assert!(outcome.shared.is_none());
    assert!(started.try_recv().is_err());
    assert!(paper.cursor(&wallets[4]).unwrap().is_none());
    drop(tx);
    actor.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn paper_service_rollout_deadline_suppresses_optional_retry_and_maps_filtered_prefix() {
    let wallets = (0xd0..=0xd5).map(wallet).collect::<Vec<_>>();
    let (dir, paper, engine) = fresh(&wallets);
    rollout_fence(
        &dir,
        wallets[0],
        &pe_core_types::SourceTradeId("g2:filtered".to_owned()),
        "conversion_requires_reanchor",
        1,
    );
    let specs = wallets[1..]
        .iter()
        .enumerate()
        .map(|(n, w)| (*w, u8::try_from(n + 1).unwrap(), "1"))
        .collect::<Vec<_>>();
    let mut responses = stable_responses(&specs);
    responses.insert(
        position_url(wallets[1], PositionPartition::NotRedeemable),
        ["1", "2"]
            .into_iter()
            .map(|amount| serde_json::to_vec(&vec![position(wallets[1], 1, amount)]).unwrap())
            .collect(),
    );
    let (starts, mut started) = mpsc::unbounded_channel();
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let fetcher = Arc::new(RolloutDeadlineFetcher {
        inner: QueueFetcher::new(responses),
        starts,
        first: Mutex::new(HashSet::new()),
        release: release.clone(),
    });
    let validator = validator_from_reconciliation(fetcher.clone());
    let (tx, rx) = mpsc::channel(4);
    let actor = spawn_control_actor(rx, engine, paper.clone());
    let preparer = AdmissionPreparer::with_validator(tx.clone(), paper.clone(), validator);
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    let input = wallets.clone();
    let task = tokio::spawn(async move {
        preparer
            .scenario_prepare_until(&input, deadline)
            .await
            .unwrap()
    });
    let mut seen = HashSet::new();
    for _ in 0..4 {
        seen.insert(started.recv().await.unwrap());
    }
    assert_eq!(seen, wallets[1..5].iter().copied().collect());
    tokio::time::advance(std::time::Duration::from_secs(11)).await;
    release.add_permits(4);
    let outcome = task.await.unwrap();
    assert_eq!(outcome.started, wallets[1..5]);
    assert_eq!(outcome.unstarted, vec![wallets[5]]);
    assert_eq!(outcome.admitted, wallets[2..5]);
    assert_eq!(outcome.deferred.len(), 2);
    assert_eq!(outcome.deferred[0].wallet, wallets[0]);
    assert_eq!(outcome.deferred[1].wallet, wallets[1]);
    assert_eq!(outcome.deferred[1].kind, "validation.position_revision");
    assert_eq!(
        fetcher
            .inner
            .urls()
            .iter()
            .filter(|url| url.contains("/activity?") && url.contains(&wallets[1].to_string()))
            .count(),
        3
    );
    assert!(started.try_recv().is_err());
    assert!(paper.position_anchors(&wallets[5]).unwrap().is_empty());
    drop(tx);
    actor.await.unwrap();
}

#[tokio::test]
async fn paper_service_rollout_novel_revision_on_first_read_refuses_then_safe_retry_recovers() {
    let wallet = wallet(0xc1);
    let (dir, paper, _) = fresh(&[wallet]);
    let original = aggregate(activity(wallet, 1, "1", "0xretained-first", 10), wallet);
    rollout_record_effect(
        &paper,
        &original,
        &pe_position_ledger::LedgerEffect::RawOnly,
        "raw_only",
    );
    rollout_fence(
        &dir,
        wallet,
        original.group_id.key(),
        "position_underflow",
        10,
    );
    let mut engine = BucketCommitEngine::load(
        paper.clone(),
        pe_service::paper_recovery::build_leader_ledger(&paper).unwrap(),
    )
    .unwrap();
    let revised =
        serde_json::to_vec(&vec![activity(wallet, 1, "2", "0xretained-first", 10)]).unwrap();
    let mut responses = rollout_empty_reads(wallet, 2);
    responses.insert(activity_url(wallet), vec![revised; 4]);
    let fetcher = Arc::new(QueueFetcher::new(responses));
    let validator = validator_from_reconciliation(fetcher.clone());
    let before = paper.wallet_coverage(&wallet).unwrap().coverage_generation;
    // The first attempt retains the revision and refuses; its bounded retry sees the disposed
    // secondary and obtains a genuinely fresh bracket, without replaying historical balances.
    let (tx, rx) = mpsc::channel(4);
    let actor = spawn_control_actor(rx, engine, paper.clone());
    let outcomes = validator
        .validate_via_control(
            &[wallet],
            &tx,
            &paper,
            pe_service::position_seeder::ValidationPurpose::CatchUp,
            None,
        )
        .await;
    assert_eq!(
        outcomes.accepted.len(),
        1,
        "deferred={:?} shared={:?}",
        outcomes.deferred,
        outcomes.shared
    );
    assert_eq!(
        paper.wallet_coverage(&wallet).unwrap().coverage_generation,
        before + 1
    );
    assert!(paper.is_wallet_fenced(&wallet).unwrap());
    assert!(paper.position_anchors(&wallet).unwrap().is_empty());
    assert!(rollout_history(&dir).is_empty());
    assert_eq!(
        fetcher
            .urls()
            .iter()
            .filter(|url| url.contains("/activity?"))
            .count(),
        4
    );
    drop(tx);
    actor.await.unwrap();
    engine = BucketCommitEngine::load(
        paper.clone(),
        pe_service::paper_recovery::build_leader_ledger(&paper).unwrap(),
    )
    .unwrap();
    engine.install_anchors(&outcomes.accepted).unwrap();
    assert!(!paper.is_wallet_fenced(&wallet).unwrap());
    assert_eq!(rollout_history(&dir).len(), 1);
}

fn tail_url(wallet: WalletAddress, baseline_end: i64, end: i64) -> String {
    PolymarketEndpoint::UserPositionActivityPage {
        user: wallet.to_string(),
        end,
        start: Some(baseline_end - pe_service::position_seeder::REENTRY_HISTORY_OVERLAP_SECS + 1),
        offset: 0,
    }
    .url(BASE)
}

fn reentry_reads(
    wallet: WalletAddress,
    baseline_end: i64,
    ends: [i64; 3],
    baseline: &[Value],
    tails: [&[Value]; 3],
    positions: &[Value],
) -> HashMap<String, Vec<Vec<u8>>> {
    let mut responses = HashMap::from([(
        activity_url_at(wallet, baseline_end),
        vec![serde_json::to_vec(baseline).unwrap()],
    )]);
    for (end, rows) in ends.into_iter().zip(tails) {
        responses
            .entry(tail_url(wallet, baseline_end, end))
            .or_default()
            .push(serde_json::to_vec(rows).unwrap());
    }
    responses.insert(
        position_url(wallet, PositionPartition::NotRedeemable),
        vec![serde_json::to_vec(positions).unwrap(); 2],
    );
    responses.insert(
        position_url(wallet, PositionPartition::Redeemable),
        vec![b"[]".to_vec(); 2],
    );
    responses
}

fn clocked_validator(
    fetcher: Arc<dyn ReconciliationFetcher>,
    ends: Vec<i64>,
) -> CausalPositionValidator {
    let fallback = *ends.last().unwrap();
    let ends = Mutex::new(VecDeque::from(ends));
    validator_from_reconciliation(fetcher).with_clock(Arc::new(move || {
        ends.lock().unwrap().pop_front().unwrap_or(fallback)
    }))
}

/// PASS: re-entry routes through one full baseline and three overlapping tails, catches up a
/// baseline-time trade, and maps an older position using cached baseline identity evidence.
/// FAIL: extra full walks, repeated baseline metadata fetches, union-row commits, or lost old positions.
#[tokio::test]
async fn reentry_baseline_and_three_tails_install_older_positions_and_catch_up() {
    let wallet = wallet(0xe1);
    let (dir, paper, engine) = fresh(&[wallet]);
    let old = activity(wallet, 1, "1", "0xbaseline-old", 10);
    let during_baseline = activity(wallet, 2, "1", "0xduring-baseline", 4000);
    let tails = vec![during_baseline.clone()];
    let fetcher = Arc::new(QueueFetcher::new(reentry_reads(
        wallet,
        4000,
        [4001, 4002, 4003],
        std::slice::from_ref(&old),
        [&tails; 3],
        &[position(wallet, 1, "1"), position(wallet, 2, "1")],
    )));
    let (tx, rx) = mpsc::channel(2);
    let commits = Arc::new(AtomicUsize::new(0));
    let actor = spawn_counted_control_actor(rx, engine, paper.clone(), Some(commits.clone()));
    let preparer = AdmissionPreparer::with_validator(
        tx,
        paper.clone(),
        clocked_validator(fetcher.clone(), vec![4000, 4001, 4002, 4003, 4003]),
    );
    let result = preparer
        .scenario_prepare_reentry(&[wallet], &HashMap::from([(wallet, 10)]))
        .await
        .unwrap();
    assert_eq!(result.admitted, vec![wallet]);
    let anchor = paper.position_anchors(&wallet).unwrap().remove(0);
    assert_eq!(anchor.activity_cutoff_unix, 4002);
    let proof: Value = serde_json::from_str(&anchor.proof_json).unwrap();
    assert_eq!(proof["baseline_walk"]["fixed_end"], 4000);
    assert_eq!(
        proof["baseline_walk"]["pages"][0]["bounds"],
        json!({"start":0,"end":4000})
    );
    assert_eq!(proof["metadata_reads"].as_array().unwrap().len(), 2);
    for (walk, end) in proof["activity_walks"]
        .as_array()
        .unwrap()
        .iter()
        .zip([4001, 4002, 4003])
    {
        assert_eq!(walk["fixed_end"], end);
        assert_eq!(walk["pages"][0]["bounds"], json!({"start":400,"end":end}));
    }
    assert!(anchor_proves_full_history(&anchor.proof_json));
    let urls = fetcher.urls();
    assert_eq!(
        urls.iter()
            .filter(|url| url.contains("/activity?"))
            .cloned()
            .collect::<Vec<_>>(),
        vec![
            activity_url_at(wallet, 4000),
            tail_url(wallet, 4000, 4001),
            tail_url(wallet, 4000, 4002),
            tail_url(wallet, 4000, 4003)
        ]
    );
    let gamma = urls
        .iter()
        .filter(|url| url.contains("/markets?"))
        .collect::<Vec<_>>();
    assert_eq!(gamma.len(), 2);
    assert_eq!(
        gamma.iter().filter(|url| url.contains(&asset(1))).count(),
        1
    );
    assert_eq!(
        commits.load(Ordering::SeqCst),
        4,
        "baseline is committed once; each tail commits only its own group"
    );
    assert!(
        paper
            .activity_group_state(aggregate(during_baseline, wallet).group_id.key())
            .unwrap()
            .is_some()
    );
    let path = dir.path().join("paper.db");
    drop(preparer);
    actor.await.unwrap();
    drop(paper);
    let reopened = PaperStateDb::open(&path).unwrap();
    let installed = reopened.position_anchors(&wallet).unwrap().remove(0);
    assert_eq!(installed, anchor);
    assert!(anchor_proves_full_history(&installed.proof_json));
    assert_eq!(
        ledger_capture(&build_leader_ledger(&reopened).unwrap(), &reopened, wallet)
            .unwrap()
            .hash,
        ledger_capture(
            &replay_wallet_ledger(&reopened, wallet).unwrap(),
            &reopened,
            wallet
        )
        .unwrap()
        .hash
    );
    assert_eq!(reopened.leader_positions().unwrap().len(), 2);
}

/// PASS: new activity after the first tail invalidates stability and the one bounded retry
/// repeats the baseline and succeeds; the failed attempt installs no anchor.
/// FAIL: a changed second tail is accepted, or retry uses stale baseline bounds/evidence.
#[tokio::test]
async fn reentry_intervening_tail_activity_retries_from_a_new_baseline() {
    let wallet = wallet(0xe2);
    let (_dir, paper, engine) = fresh(&[wallet]);
    let old = activity(wallet, 1, "1", "0xretry-old", 10);
    let late = activity(wallet, 2, "1", "0xretry-new", 4002);
    let tail = vec![late.clone()];
    let mut reads = reentry_reads(
        wallet,
        4000,
        [4001, 4002, 4003],
        std::slice::from_ref(&old),
        [&[], &tail, &tail],
        &[position(wallet, 1, "1")],
    );
    let retry = reentry_reads(
        wallet,
        4003,
        [4004, 4005, 4006],
        &[late, old],
        [&tail; 3],
        &[position(wallet, 1, "1"), position(wallet, 2, "1")],
    );
    reads
        .get_mut(&position_url(wallet, PositionPartition::NotRedeemable))
        .unwrap()
        .truncate(1);
    reads
        .get_mut(&position_url(wallet, PositionPartition::Redeemable))
        .unwrap()
        .truncate(1);
    for (url, pages) in retry {
        reads.entry(url).or_default().extend(pages);
    }
    let fetcher = Arc::new(QueueFetcher::new(reads));
    let validator = clocked_validator(
        fetcher.clone(),
        vec![4000, 4001, 4002, 4003, 4004, 4005, 4006, 4006],
    );
    let (tx, rx) = mpsc::channel(2);
    let actor = spawn_control_actor(rx, engine, paper.clone());
    let bytes = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::fmt()
        .json()
        .without_time()
        .with_writer(BracketLogs(bytes.clone()))
        .finish();
    use tracing::instrument::WithSubscriber;
    let outcomes = validator
        .validate_via_control(
            &[wallet],
            &tx,
            &paper,
            pe_service::position_seeder::ValidationPurpose::Reentry,
            None,
        )
        .with_subscriber(subscriber)
        .await;
    let logged = bytes.lock().unwrap().clone();
    let attempts = std::str::from_utf8(&logged)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .filter(|line| line["fields"]["message"] == "reentry bracket attempt")
        .map(|line| line["fields"]["outcome"].as_str().unwrap().to_owned())
        .collect::<Vec<_>>();
    assert_eq!(attempts, ["validation.intervening_activity", "accepted"]);
    assert!(outcomes.shared.is_none());
    assert!(outcomes.deferred.is_empty());
    assert_eq!(outcomes.accepted.len(), 1);
    assert!(paper.position_anchors(&wallet).unwrap().is_empty());
    let proof: Value = serde_json::from_str(&outcomes.accepted[0].proof.document).unwrap();
    assert_eq!(proof["baseline_walk"]["fixed_end"], 4003);
    assert_eq!(outcomes.accepted[0].cutoff, 4005);
    assert_eq!(
        fetcher
            .urls()
            .iter()
            .filter(|url| url.contains("/activity?"))
            .count(),
        7
    );
    let (acknowledged, ack) = tokio::sync::oneshot::channel();
    tx.send(OrchestratorControl::InstallAnchors {
        installs: outcomes.accepted,
        acknowledged,
    })
    .await
    .unwrap();
    ack.await.unwrap().unwrap();
    drop(tx);
    actor.await.unwrap();
}

/// PASS: late-group recovery clears a fence only when the second tail ends strictly after
/// its epoch, even if the final tail ends later; successful proof retains the baseline.
/// FAIL: the baseline or final tail substitutes for the second tail's fence boundary.
#[tokio::test]
async fn reentry_late_group_recovery_enforces_second_tail_fence_epoch() {
    for (fence_epoch, accepted) in [(3999, true), (4002, false)] {
        let wallet = wallet(0xe3);
        let (dir, paper, _engine) = fresh(&[wallet]);
        let row = activity(wallet, 1, "1", "0xlate-recovery", 3999);
        let group = aggregate(row.clone(), wallet);
        rollout_record_effect(
            &paper,
            &group,
            &pe_position_ledger::LedgerEffect::RawOnly,
            "reanchor_required_late_group",
        );
        rollout_fence(
            &dir,
            wallet,
            group.group_id.key(),
            "late_group_after_bucket_commit",
            fence_epoch,
        );
        let engine = BucketCommitEngine::load(paper.clone(), PositionLedger::new()).unwrap();
        let tails = vec![row.clone()];
        let fetcher = Arc::new(QueueFetcher::new(reentry_reads(
            wallet,
            4000,
            [4001, 4002, 4003],
            &[row],
            [&tails; 3],
            &[position(wallet, 1, "1")],
        )));
        let (tx, rx) = mpsc::channel(2);
        let actor = spawn_control_actor(rx, engine, paper.clone());
        let preparer = AdmissionPreparer::with_validator(
            tx,
            paper.clone(),
            clocked_validator(fetcher, vec![4000, 4001, 4002, 4003, 4003]),
        );
        let outcome = preparer
            .scenario_prepare_reentry(&[wallet], &HashMap::from([(wallet, 10)]))
            .await
            .unwrap();
        assert_eq!(outcome.admitted == vec![wallet], accepted);
        assert_eq!(paper.is_wallet_fenced(&wallet).unwrap(), !accepted);
        if !accepted {
            assert_eq!(outcome.deferred[0].kind, "validation.fenced");
            assert!(rollout_history(&dir).is_empty());
        }
        drop(preparer);
        actor.await.unwrap();
    }
}

struct HistoryInspectingFetcher {
    inner: QueueFetcher,
    path: std::path::PathBuf,
    fenced: bool,
    wallet: WalletAddress,
    old_id: pe_core_types::SourceTradeId,
}
impl PageFetcher for HistoryInspectingFetcher {
    async fn fetch_page(&self, url: &str) -> Result<Vec<u8>, SourceError> {
        if url.contains("/activity?") && url.contains("&start=401") {
            let conn = rusqlite::Connection::open(&self.path).unwrap();
            let history=conn.query_row("SELECT first_epoch,source_trade_id FROM wallet_market_history_v2 WHERE wallet_hex=?1",[self.wallet.to_string()],|row|Ok((row.get::<_,i64>(0)?,row.get::<_,String>(1)?)));
            if self.fenced {
                assert!(matches!(history, Err(rusqlite::Error::QueryReturnedNoRows)));
            } else {
                assert_eq!(history.unwrap(), (10, self.old_id.0.clone()));
            }
        }
        self.inner.fetch_page(url).await
    }
}

/// PASS: the unfenced baseline repairs history before tails; fenced recovery retains the
/// baseline-only oldest BUY until installation; failed fenced brackets repair nothing.
/// FAIL: a later tail BUY consumes the first entry, or a failed recovery repairs history early.
#[tokio::test]
async fn reentry_baseline_repairs_earliest_purchase_in_both_paths() {
    for (fenced, fail) in [(false, false), (true, false), (true, true)] {
        let wallet = wallet(0xe4);
        let (dir, paper, _engine) = fresh(&[wallet]);
        let old = activity(wallet, 1, "1", "0xearliest", 10);
        let old_group = aggregate(old.clone(), wallet);
        rollout_record_effect(
            &paper,
            &old_group,
            &pe_position_ledger::LedgerEffect::RawOnly,
            "raw_only",
        );
        if fenced {
            rollout_fence(
                &dir,
                wallet,
                old_group.group_id.key(),
                "late_group_after_bucket_commit",
                10,
            );
        }
        let engine = BucketCommitEngine::load(paper.clone(), PositionLedger::new()).unwrap();
        let tail = vec![activity(wallet, 1, "1", "0xlater", 3990)];
        let mut reads = reentry_reads(
            wallet,
            4000,
            [4000; 3],
            &[old],
            [&tail; 3],
            &[position(wallet, 1, "2")],
        );
        if fail {
            let positions = reads
                .get_mut(&position_url(wallet, PositionPartition::NotRedeemable))
                .unwrap();
            positions[0] = serde_json::to_vec(&vec![position(wallet, 1, "1")]).unwrap();
            for pages in reads.values_mut() {
                pages.extend(pages.clone());
            }
        }
        let fetcher = Arc::new(HistoryInspectingFetcher {
            inner: QueueFetcher::new(reads),
            path: dir.path().join("paper.db"),
            fenced,
            wallet,
            old_id: old_group.group_id.key().clone(),
        });
        let validator = clocked_validator(fetcher, vec![4000]);
        let (tx, rx) = mpsc::channel(2);
        let actor = spawn_control_actor(rx, engine, paper.clone());
        let preparer = AdmissionPreparer::with_validator(tx, paper.clone(), validator);
        let outcome = preparer
            .scenario_prepare_reentry(&[wallet], &HashMap::from([(wallet, 10)]))
            .await
            .unwrap();
        if fail {
            assert!(outcome.admitted.is_empty());
            assert_eq!(outcome.deferred[0].kind, "validation.position_revision");
            assert!(rollout_history(&dir).is_empty());
            assert!(paper.is_wallet_fenced(&wallet).unwrap());
        } else {
            assert_eq!(outcome.admitted, vec![wallet]);
            let history = rollout_history(&dir);
            assert_eq!(history.len(), 1);
            assert_eq!(history[0].first_epoch, 10);
            assert_eq!(history[0].source_trade_id, *old_group.group_id.key());
            assert!(!paper.is_wallet_fenced(&wallet).unwrap());
        }
        assert_eq!(
            paper
                .activity_group_state(old_group.group_id.key())
                .unwrap()
                .unwrap()
                .disposition,
            "raw_only"
        );
        drop(preparer);
        actor.await.unwrap();
    }
}

/// PASS: current baseline metadata makes a previously raw-only conversion attributable and
/// refuses fenced recovery before tails, retaining its original evidence and empty history.
/// FAIL: a short tail hides the conversion or the baseline commits unsafe recovery early.
#[tokio::test]
async fn reentry_baseline_refuses_previously_raw_only_conversion() {
    let wallet = wallet(0xe5);
    let (dir, paper, _) = fresh(&[wallet]);
    let mut row = activity(wallet, 1, "1", "0xunsafe-baseline", 10);
    row["type"] = json!("CONVERSION");
    let group = aggregate(row.clone(), wallet);
    rollout_record_effect(
        &paper,
        &group,
        &pe_position_ledger::LedgerEffect::RawOnly,
        "raw_only",
    );
    rollout_fence(&dir, wallet, group.group_id.key(), "position_underflow", 10);
    let original = paper.activity_group_state(group.group_id.key()).unwrap();
    let engine = BucketCommitEngine::load(paper.clone(), PositionLedger::new()).unwrap();
    let fetcher = Arc::new(QueueFetcher::new(HashMap::from([(
        activity_url_at(wallet, 4000),
        vec![serde_json::to_vec(&vec![row]).unwrap()],
    )])));
    let (tx, rx) = mpsc::channel(2);
    let actor = spawn_control_actor(rx, engine, paper.clone());
    let preparer = AdmissionPreparer::with_validator(
        tx,
        paper.clone(),
        clocked_validator(fetcher.clone(), vec![4000]),
    );
    let outcome = preparer
        .scenario_prepare_reentry(&[wallet], &HashMap::from([(wallet, 10)]))
        .await
        .unwrap();
    assert!(outcome.admitted.is_empty());
    assert_eq!(outcome.deferred[0].kind, "validation.unsafe_recovery");
    assert!(paper.is_wallet_fenced(&wallet).unwrap());
    assert!(paper.position_anchors(&wallet).unwrap().is_empty());
    assert!(rollout_history(&dir).is_empty());
    assert_eq!(
        paper.activity_group_state(group.group_id.key()).unwrap(),
        original
    );
    assert_eq!(
        fetcher
            .urls()
            .iter()
            .filter(|url| url.contains("/activity?"))
            .count(),
        1
    );
    drop(preparer);
    actor.await.unwrap();
}

fn inactivity_config() -> pe_service::watchlist_maintenance::MaintenanceConfig {
    let config = pe_service::config::ServiceConfig::default();
    pe_service::watchlist_maintenance::MaintenanceConfig {
        interval_secs: config.maintenance_interval_secs,
        inactivity_threshold_secs: config.inactivity_threshold_secs,
        inactivity_hard_cap_secs: config.inactivity_hard_cap_secs,
        demotion_min_trades: config.demotion_min_trades,
        demotion_cb_alpha: config.demotion_cb_alpha.parse().unwrap(),
        demotion_pnl_window_secs: config.demotion_pnl_window_secs,
        membership_mode: pe_service::watchlist_maintenance::MembershipMode::default(),
    }
}

/// PASS: catch-up admission, re-entry with baseline-only activity, and direct boot advance a
/// stale clock from pre-recorded late groups, keep a larger clock, and preserve the delivery cursor.
/// FAIL: cutoff/current-time clocks, an immediate inactivity knockout, or clock updates moving delivery.
#[tokio::test]
async fn accepted_brackets_install_newest_activity_clock_from_pre_recorded_late_groups() {
    use pe_service::watchlist_maintenance::{KnockoutReason, knockout_decision};
    for path in ["catch_up", "reentry", "direct"] {
        for larger in [None, Some(999_999)] {
            let wallet = wallet(0xe6);
            let (_dir, paper, _) = fresh(&[wallet]);
            paper.seed_cursor_if_absent(&wallet, 10).unwrap();
            paper.set_cursor(&wallet, 990_001).unwrap();
            if let Some(epoch) = larger {
                paper.set_activity(&wallet, epoch).unwrap();
            }
            let row = activity(wallet, 1, "1", "0xpre-recorded-active", 990_000);
            let group = aggregate(row.clone(), wallet);
            rollout_record_effect_with_cursor(
                &paper,
                &group,
                &pe_position_ledger::LedgerEffect::RawOnly,
                "reanchor_required_late_group",
                false,
            );
            assert_eq!(paper.activity(&wallet).unwrap(), larger.or(Some(10)));
            let before_cursor = paper.cursor(&wallet).unwrap();
            assert_eq!(
                knockout_decision(Some(10), None, &inactivity_config(), 1_000_000),
                Some(KnockoutReason::Inactivity)
            );
            let positions = vec![position(wallet, 1, "1")];
            let reads = if path == "reentry" {
                reentry_reads(
                    wallet,
                    1_000_000,
                    [1_000_000; 3],
                    &[row],
                    [&[]; 3],
                    &positions,
                )
            } else {
                HashMap::from([
                    (
                        activity_url_at(wallet, 1_000_000),
                        vec![serde_json::to_vec(&vec![row]).unwrap(); 3],
                    ),
                    (
                        position_url(wallet, PositionPartition::NotRedeemable),
                        vec![serde_json::to_vec(&positions).unwrap(); 2],
                    ),
                    (
                        position_url(wallet, PositionPartition::Redeemable),
                        vec![b"[]".to_vec(); 2],
                    ),
                ])
            };
            let fetcher = Arc::new(QueueFetcher::new(reads));
            let validator = clocked_validator(fetcher, vec![1_000_000]);
            let mut engine =
                BucketCommitEngine::load(paper.clone(), PositionLedger::new()).unwrap();
            if path == "direct" {
                let installs = validator
                    .validate_direct(&[wallet], &mut engine, &paper)
                    .await
                    .unwrap();
                assert_eq!(installs[0].newest_activity_unix, Some(990_000));
            } else {
                let (tx, rx) = mpsc::channel(2);
                let actor = spawn_control_actor(rx, engine, paper.clone());
                let preparer = AdmissionPreparer::with_validator(tx, paper.clone(), validator);
                let outcome = if path == "reentry" {
                    preparer
                        .scenario_prepare_reentry(&[wallet], &HashMap::from([(wallet, 10)]))
                        .await
                        .unwrap()
                } else {
                    preparer
                        .scenario_prepare_ranked_until(
                            &[wallet],
                            &HashMap::from([(wallet, 10)]),
                            None,
                        )
                        .await
                        .unwrap()
                };
                assert_eq!(outcome.admitted, vec![wallet]);
                drop(preparer);
                actor.await.unwrap();
            }
            assert_eq!(
                paper.activity(&wallet).unwrap(),
                Some(larger.unwrap_or(990_000)),
                "{path}"
            );
            assert_eq!(paper.cursor(&wallet).unwrap(), before_cursor, "{path}");
            assert_eq!(
                knockout_decision(
                    paper.activity(&wallet).unwrap(),
                    None,
                    &inactivity_config(),
                    1_000_000
                ),
                None
            );
            assert_eq!(
                paper
                    .activity_group_state(group.group_id.key())
                    .unwrap()
                    .unwrap()
                    .disposition,
                "reanchor_required_late_group"
            );
        }
    }
}

/// PASS: catch-up admission, routine refresh and boot retain exactly three full reads and
/// their legacy proof shape, with no baseline field.
/// FAIL: re-entry routing leaks into another path or a full read becomes an incremental tail.
#[tokio::test]
async fn non_reentry_paths_keep_three_full_walks_and_legacy_proofs() {
    for path in ["catch_up", "refresh", "direct"] {
        let wallet = wallet(0xe7);
        let (_dir, paper, mut engine) = fresh(&[wallet]);
        let fetcher = Arc::new(QueueFetcher::new(stable_responses(&[(wallet, 1, "1")])));
        let validator = validator_from_fetcher(fetcher.clone());
        if path == "direct" {
            validator
                .validate_direct(&[wallet], &mut engine, &paper)
                .await
                .unwrap();
        } else {
            let (tx, rx) = mpsc::channel(2);
            let actor = spawn_control_actor(rx, engine, paper.clone());
            let preparer = AdmissionPreparer::with_validator(tx, paper.clone(), validator);
            if path == "catch_up" {
                assert_eq!(
                    preparer.prepare(&[wallet]).await.unwrap().admitted,
                    vec![wallet]
                );
            } else {
                assert_eq!(
                    preparer.prepare_if_due(wallet, END, 1).await.unwrap(),
                    AnchorRefreshOutcome::Anchored
                );
            }
            drop(preparer);
            actor.await.unwrap();
        }
        assert_eq!(
            fetcher
                .urls()
                .into_iter()
                .filter(|url| url.contains("/activity?"))
                .collect::<Vec<_>>(),
            vec![activity_url(wallet); 3],
            "{path}"
        );
        assert_full_history_proof(&paper.position_anchors(&wallet).unwrap()[0].proof_json);
    }
}

/// PASS: a clock rollback between baseline and tails refuses installation using the baseline
/// in the non-monotonic bound check.
/// FAIL: three internally monotonic tails hide a baseline end newer than their first end.
#[tokio::test]
async fn reentry_rejects_non_monotonic_bounds_including_baseline() {
    let wallet = wallet(0xe8);
    let (_dir, paper, engine) = fresh(&[wallet]);
    let fetcher = Arc::new(QueueFetcher::new(reentry_reads(
        wallet,
        4000,
        [3999, 3999, 3999],
        &[],
        [&[]; 3],
        &[],
    )));
    let validator = clocked_validator(fetcher, vec![4000, 3999, 3999, 3999]);
    let (tx, rx) = mpsc::channel(2);
    let actor = spawn_control_actor(rx, engine, paper.clone());
    let outcomes = validator
        .validate_via_control(
            &[wallet],
            &tx,
            &paper,
            pe_service::position_seeder::ValidationPurpose::Reentry,
            None,
        )
        .await;
    assert!(outcomes.accepted.is_empty());
    assert!(matches!(
        outcomes.shared.unwrap(),
        CausalPositionError::NonMonotonicActivityBounds {
            previous: 4000,
            next: 3999,
            ..
        }
    ));
    assert!(paper.position_anchors(&wallet).unwrap().is_empty());
    drop(tx);
    actor.await.unwrap();
}

#[derive(Clone)]
struct BracketLogs(Arc<Mutex<Vec<u8>>>);
impl std::io::Write for BracketLogs {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for BracketLogs {
    type Writer = Self;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}
struct TimedBracketFetcher {
    inner: QueueFetcher,
    calls: AtomicUsize,
}
impl PageFetcher for TimedBracketFetcher {
    async fn fetch_page(&self, url: &str) -> Result<Vec<u8>, SourceError> {
        if url.contains("/activity?") {
            let first = self.calls.fetch_add(1, Ordering::SeqCst) == 0;
            tokio::time::advance(std::time::Duration::from_secs(if first { 20 } else { 1 })).await;
        }
        self.inner.fetch_page(url).await
    }
}

/// PASS: each accepted or failed attempt emits its outcome and separate baseline/stability
/// durations; the first-tail sample starts the window and no tail means a zero window.
/// FAIL: missing attempt logs, lifetime baseline time counted as stability, or failure time omitted.
#[tokio::test(start_paused = true)]
async fn reentry_attempt_logs_separate_baseline_and_stability_timing() {
    use tracing::instrument::WithSubscriber;
    for failure in ["none", "baseline", "tail"] {
        let wallet = wallet(0xe9);
        let (_dir, paper, engine) = fresh(&[wallet]);
        let mut reads = reentry_reads(wallet, 4000, [4000; 3], &[], [&[]; 3], &[]);
        if failure == "baseline" {
            reads.insert(activity_url_at(wallet, 4000), vec![b"{".to_vec()]);
        }
        if failure == "tail" {
            reads.insert(tail_url(wallet, 4000, 4000), vec![b"{".to_vec()]);
        }
        let fetcher = Arc::new(TimedBracketFetcher {
            inner: QueueFetcher::new(reads),
            calls: AtomicUsize::new(0),
        });
        let validator = clocked_validator(fetcher, vec![4000]);
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::fmt()
            .json()
            .without_time()
            .with_writer(BracketLogs(bytes.clone()))
            .finish();
        let (tx, rx) = mpsc::channel(2);
        let actor = spawn_control_actor(rx, engine, paper.clone());
        let outcomes = validator
            .validate_via_control(
                &[wallet],
                &tx,
                &paper,
                pe_service::position_seeder::ValidationPurpose::Reentry,
                None,
            )
            .with_subscriber(subscriber)
            .await;
        if failure == "none" {
            assert_eq!(outcomes.accepted.len(), 1);
        } else {
            assert!(outcomes.shared.is_some());
        }
        let bytes = bytes.lock().unwrap().clone();
        let logs = std::str::from_utf8(&bytes)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .filter(|line| line["fields"]["message"] == "reentry bracket attempt")
            .collect::<Vec<_>>();
        assert_eq!(logs.len(), 1);
        let fields = &logs[0]["fields"];
        assert_eq!(fields["wallet"], wallet.to_string());
        assert_eq!(fields["baseline_ms"], 20_000);
        assert_eq!(
            fields["stability_window_ms"],
            match failure {
                "baseline" => 0,
                "tail" => 1000,
                _ => 3000,
            }
        );
        assert_eq!(
            fields["outcome"],
            if failure == "none" {
                "accepted"
            } else {
                "activity.json"
            }
        );
        drop(bytes);
        drop(tx);
        actor.await.unwrap();
    }
}

/// PASS: contradictory ordinary/combo classifications across baseline and tail fail through
/// the shared mapping helper before the tail commits or a positions read runs.
/// FAIL: independently valid per-read mappings conceal a conflict in their union.
#[tokio::test]
async fn reentry_union_mapping_rejects_baseline_tail_classification_conflict() {
    let wallet = wallet(0xea);
    let (_dir, paper, engine) = fresh(&[wallet]);
    let old = activity(wallet, 1, "1", "0xordinary-baseline", 10);
    let mut tail = activity(wallet, 1, "1", "0xcombo-tail", 3990);
    tail["isCombo"] = json!(true);
    let tails = vec![tail.clone()];
    let fetcher = Arc::new(QueueFetcher::new(reentry_reads(
        wallet,
        4000,
        [4000; 3],
        &[old],
        [&tails; 3],
        &[],
    )));
    let validator = clocked_validator(fetcher.clone(), vec![4000]);
    let (tx, rx) = mpsc::channel(2);
    let actor = spawn_control_actor(rx, engine, paper.clone());
    let outcomes = validator
        .validate_via_control(
            &[wallet],
            &tx,
            &paper,
            pe_service::position_seeder::ValidationPurpose::Reentry,
            None,
        )
        .await;
    assert!(outcomes.accepted.is_empty());
    assert!(outcomes.shared.is_none());
    assert!(matches!(
        &outcomes.deferred[0].1,
        CausalPositionError::Positions {
            source: PositionReadError::MixedActivityClassification { .. },
            ..
        }
    ));
    assert!(
        paper
            .activity_group_state(aggregate(tail, wallet).group_id.key())
            .unwrap()
            .is_none()
    );
    assert!(!fetcher.urls().iter().any(|url| url.contains("/positions?")));
    drop(tx);
    actor.await.unwrap();
}
