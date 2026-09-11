//! Scenario: a late bucket re-anchor does not stop the same complete poll read.

#![cfg(feature = "scenario")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod support;

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use pe_copy_signal_engine::SignalConfig;
use pe_core_types::{
    BasisPoints, MarketId, MarketOutcomeId, OutcomeId, ReceivedAt, ReconstructionQuality,
    ShareAmount, SourceId, SourceTimestamp, VenueMarketId, WalletAddress,
};
use pe_paper_state::{AnchorInstallRecord, PaperStateDb, WalletHistoryStatusRecord};
use pe_position_ledger::LedgerEffect;
use pe_service::activity_ingest::{ActivityIngest, SourceLogHandle};
use pe_service::asset_identity::AssetIdentityResolver;
use pe_service::bucket_commit::{BucketCommitEngine, BucketDecisionContext, FrozenDecisionBasis};
use pe_service::health::new_shared_health_with_ws;
use pe_service::live_watchlist::LiveWatchlist;
use pe_service::orchestrator_control::OrchestratorControl;
use pe_service::paper_recovery::build_leader_ledger;
use pe_service::runtime_config::{LiveRuntimeConfig, RuntimeConfig};
use pe_service::source_event_sink::SourceEventSink;
use pe_service::trade_poller::{ReconciliationObligations, TradePoller, TradePollerConfig};
use pe_source_core::SourceError;
use pe_source_polymarket_public::{
    ActivityAggregate, ActivityParseContext, ActivityTransport, GAMMA_BATCH_SIZE,
    ReconciliationFetcher, parse_activity_response,
};
use pe_trader_index::{Watchlist, WatchlistEntry, WatchlistTier};
use serde_json::{Value, json};
use time::OffsetDateTime;
use tokio::sync::{mpsc, oneshot};

const BASE_URL: &str = "https://data.example.test";
const WALLET_HEX: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const MARKET_A: &str = "0xcondition-a";
const MARKET_B: &str = "0xcondition-b";
const EPOCH: i64 = 1_900_000_000;

struct QueueFetcher {
    pages: Mutex<VecDeque<Vec<u8>>>,
    calls: AtomicUsize,
    urls: Mutex<Vec<String>>,
}

impl QueueFetcher {
    fn new(page: Vec<u8>) -> Self {
        Self::from_pages(vec![page])
    }

    fn from_pages(pages: Vec<Vec<u8>>) -> Self {
        Self {
            pages: Mutex::new(pages.into()),
            calls: AtomicUsize::new(0),
            urls: Mutex::new(Vec::new()),
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl ReconciliationFetcher for QueueFetcher {
    fn fetch<'a>(
        &'a self,
        url: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, SourceError>> + Send + 'a>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.urls.lock().unwrap().push(url.to_owned());
            self.pages
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .pop_front()
                .ok_or_else(|| SourceError::Fatal {
                    message: "unexpected activity page fetch".to_owned(),
                })
        })
    }
}

struct GammaFetcher;

impl ReconciliationFetcher for GammaFetcher {
    fn fetch<'a>(
        &'a self,
        _url: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, SourceError>> + Send + 'a>> {
        Box::pin(async {
            Ok(br#"[{"conditionId":"0xcondition-b","clobTokenIds":["asset-b"]}]"#.to_vec())
        })
    }
}

fn wallet() -> WalletAddress {
    WalletAddress::from_hex(WALLET_HEX).unwrap()
}

fn market(value: &str) -> MarketId {
    MarketId(VenueMarketId(value.to_owned()))
}

fn activity_row(
    activity_type: &str,
    transaction_hash: &str,
    market_id: &str,
    side: &str,
    size: &str,
    asset: &str,
    epoch: i64,
) -> Value {
    json!({
        "proxyWallet": WALLET_HEX,
        "timestamp": epoch,
        "conditionId": market_id,
        "type": activity_type,
        "size": size,
        "usdcSize": size,
        "transactionHash": transaction_hash,
        "price": if activity_type == "TRADE" { "0.5" } else { "1" },
        "asset": asset,
        "side": side,
        "outcomeIndex": 0,
        "outcome": "Yes",
        "isCombo": false,
    })
}

fn aggregate(row: Value) -> ActivityAggregate {
    let context = ActivityParseContext {
        source_id: SourceId("polymarket-data-api".to_owned()),
        observed_at: SourceTimestamp(OffsetDateTime::from_unix_timestamp(EPOCH + 10).unwrap()),
        received_at: ReceivedAt(OffsetDateTime::from_unix_timestamp(EPOCH + 11).unwrap()),
        transport: ActivityTransport::Rest,
    };
    let page =
        parse_activity_response(&serde_json::to_vec(&[row]).unwrap(), wallet(), &context).unwrap();
    let mut aggregates = page.aggregates().unwrap();
    assert_eq!(aggregates.len(), 1);
    aggregates.remove(0)
}

fn context(epoch: i64) -> BucketDecisionContext {
    BucketDecisionContext {
        applied_configuration: RuntimeConfig::from_service_config(
            &pe_service::config::ServiceConfig::default(),
        ),
        decision_inputs_json: "{\"source_window\":\"complete\"}".to_owned(),
        page_occurrences: Vec::new(),
        observed_source_receipts: HashMap::new(),
        reconstruction_quality: ReconstructionQuality::new(100).unwrap(),
        read_commitment: None,

        signal_config: SignalConfig::default(),
        copy_eligible: false,
        bracket_commit: false,
        recorded_at_unix: epoch + 10,
        observation_provenance: HashMap::new(),
        no_copy_dispositions: HashMap::new(),
        identity_overrides: HashMap::new(),
        identity_unresolved: Default::default(),
        history_status: None,
    }
}

fn zero_basis() -> FrozenDecisionBasis {
    FrozenDecisionBasis {
        win_rate_p: pe_core_types::Probability::ZERO,
        bankroll: rust_decimal::Decimal::ZERO,
    }
}

fn watchlist() -> Watchlist {
    Watchlist {
        entries: vec![WatchlistEntry {
            wallet: wallet(),
            tier: WatchlistTier::Active,
            leader_score_bps: BasisPoints(100),
            lcb_5pct_bps: BasisPoints(100),
            win_rate_bps: BasisPoints(7_000),
            closed_trades_in_window: 1,
            reconstruction_quality: ReconstructionQuality::new(100).unwrap(),
        }],
        snapshot_at: SourceTimestamp(OffsetDateTime::UNIX_EPOCH),
        active_count: 1,
        incubator_count: 0,
    }
}

fn long(engine: &BucketCommitEngine, market_id: &str) -> ShareAmount {
    let key = MarketOutcomeId::new(market(market_id), OutcomeId(0));
    engine
        .ledger()
        .position(&wallet())
        .and_then(|snapshot| snapshot.positions.get(&key))
        .map_or(ShareAmount::ZERO, |position| position.long_contracts)
}

#[tokio::test]
async fn late_group_then_strict_decrement_in_one_read_both_become_durable() {
    let dir = tempfile::tempdir().unwrap();
    let source_log_path = dir.path().join("source.log");
    let paper = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
    paper.set_cursor(&wallet(), 0).unwrap();
    paper
        .record_reconciled_history_status(&WalletHistoryStatusRecord {
            wallet: wallet(),
            complete: true,
            proof_json: "{\"scenario\":\"complete\"}".to_owned(),
            updated_at_unix: 1,
        })
        .unwrap();
    paper
        .install_anchors(&[AnchorInstallRecord {
            history_status: None,
            wallet: wallet(),
            balances: vec![
                (
                    market(MARKET_A),
                    OutcomeId(0),
                    ShareAmount::from_whole(5).unwrap(),
                ),
                (
                    market(MARKET_B),
                    OutcomeId(0),
                    ShareAmount::from_whole(5).unwrap(),
                ),
            ],
            activity_cutoff_unix: 0,
            anchored_at_unix: 1,
            ledger_hash_after: "scenario-anchor".to_owned(),
            positions_proof_hash: "scenario-positions".to_owned(),
            activity_bounds_json: "[]".to_owned(),
            source_log_generation: "scenario".to_owned(),
            proof_json: "{}".to_owned(),
            recorded_at_unix: 1,
        }])
        .unwrap();
    let ledger = build_leader_ledger(&paper).unwrap();
    let mut engine = BucketCommitEngine::load(Arc::clone(&paper), ledger).unwrap();

    let committed = aggregate(activity_row(
        "REDEEM",
        "0xcommitted",
        MARKET_A,
        "",
        "5",
        "",
        EPOCH,
    ));
    engine
        .commit(vec![committed], &context(EPOCH), zero_basis())
        .unwrap();
    assert_eq!(long(&engine, MARKET_A), ShareAmount::ZERO);
    assert_eq!(long(&engine, MARKET_B), ShareAmount::from_whole(5).unwrap());
    let ledger_before_read = engine.ledger().snapshots().clone();

    let late_row = activity_row("REDEEM", "0xlate", MARKET_B, "", "5", "", EPOCH);
    let decrement_row = activity_row(
        "TRADE",
        "0xdecrement",
        MARKET_B,
        "SELL",
        "1",
        "asset-b",
        EPOCH + 1,
    );
    let late_id = aggregate(late_row.clone()).group_id.key().clone();
    let decrement_id = aggregate(decrement_row.clone()).group_id.key().clone();
    let activity_page = serde_json::to_vec(&[late_row, decrement_row]).unwrap();
    let fetcher = Arc::new(QueueFetcher::new(activity_page));

    let source_sink = SourceEventSink::open(&source_log_path).unwrap();
    let (source_log, source_rx) = SourceLogHandle::channel(8);
    let (trigger_tx, trigger_rx) = mpsc::channel(4);
    let health = new_shared_health_with_ws(false, true, 90);
    let ingest = tokio::spawn(
        ActivityIngest::poll_only(source_sink, source_rx, trigger_tx, Arc::clone(&health)).run(),
    );
    let asset_identity = Arc::new(AssetIdentityResolver::new_runtime(
        Arc::new(GammaFetcher),
        BASE_URL.to_owned(),
        GAMMA_BATCH_SIZE,
        source_log.clone(),
    ));
    let (control_tx, mut control_rx) = mpsc::channel(4);
    let (engine_tx, engine_rx) = oneshot::channel();
    let control = tokio::spawn(async move {
        while let Some(command) = control_rx.recv().await {
            if let OrchestratorControl::CommitActivityBucket {
                aggregates,
                context,
                committed,
            } = command
            {
                let result = engine
                    .commit(aggregates, context.as_ref(), zero_basis())
                    .map_err(|error| error.to_string());
                let _ = committed.send(result);
            }
        }
        let _ = engine_tx.send(engine);
    });
    let now = OffsetDateTime::from_unix_timestamp(EPOCH + 10).unwrap();
    let (progress_tx, mut progress_rx) = mpsc::channel(64);
    let result = TradePoller::new(
        TradePollerConfig {
            base_url: BASE_URL.to_owned(),
            poll_interval_secs: 30,
            activity_ws_enabled: false,
            copy_latency_budget_secs: 2,
        },
        LiveWatchlist::new(watchlist()),
        fetcher.clone(),
        asset_identity,
        source_log,
        trigger_rx,
        control_tx,
        Arc::clone(&paper),
        health,
        SignalConfig::default(),
        LiveRuntimeConfig::new(RuntimeConfig::from_service_config(
            &pe_service::config::ServiceConfig::default(),
        )),
        ReconciliationObligations::default(),
        None,
    )
    .with_clock(Arc::new(move || now))
    .with_progress(progress_tx)
    .run_until(async move {
        while let Some(progress) = progress_rx.recv().await {
            if matches!(
                progress,
                pe_service::trade_poller::PollerProgress::RoundCompleted
            ) {
                break;
            }
        }
    })
    .await;
    assert!(result.is_ok(), "the complete reconciliation round finishes");
    ingest.await.unwrap();
    control.await.unwrap();
    let engine = engine_rx.await.unwrap();

    assert_eq!(
        fetcher.calls(),
        1,
        "both epochs came from one complete read"
    );
    assert_eq!(engine.ledger().snapshots(), &ledger_before_read);
    assert!(!paper.is_wallet_fenced(&wallet()).unwrap());
    assert!(paper.wallet_coverage(&wallet()).unwrap().reanchor_required);
    assert!(paper.open_decision_pending().unwrap().is_empty());

    let groups = paper.activity_groups_after(&wallet(), EPOCH - 1).unwrap();
    for group_id in [late_id, decrement_id] {
        let group = groups
            .iter()
            .find(|group| group.source_trade_id == group_id)
            .expect("both buckets must become durable before the round exits");
        assert_eq!(group.disposition, "reanchor_required_late_group");
        assert!(matches!(
            LedgerEffect::from_document(&group.proof_json),
            Ok(LedgerEffect::RawOnly)
        ));
    }
}

type RecordedBucket = (
    Vec<ActivityAggregate>,
    Arc<BucketDecisionContext>,
    pe_service::bucket_commit::BucketCommitResult,
);

async fn recorded_poll(
    paper: Arc<PaperStateDb>,
    source_path: &std::path::Path,
    fetcher: Arc<QueueFetcher>,
) -> Vec<RecordedBucket> {
    let ledger = build_leader_ledger(&paper).unwrap();
    let mut engine = BucketCommitEngine::load(Arc::clone(&paper), ledger).unwrap();
    let source_sink = SourceEventSink::open(source_path).unwrap();
    // The coordinator records every acknowledged append in the process index; a repeat round
    // over an existing log must start from that log's verified receipts, as production does.
    let source_receipts = pe_service::risk_inputs::SourceReceiptIndex::replay(source_path).unwrap();
    let (source_log, source_rx) = SourceLogHandle::channel(8);
    let (trigger_tx, trigger_rx) = mpsc::channel(4);
    let health = new_shared_health_with_ws(false, true, 90);
    let ingest = tokio::spawn(
        ActivityIngest::poll_only(source_sink, source_rx, trigger_tx, Arc::clone(&health))
            .with_source_receipt_index(source_receipts)
            .run(),
    );
    let asset_identity = Arc::new(AssetIdentityResolver::new_runtime(
        Arc::new(GammaFetcher),
        BASE_URL.to_owned(),
        GAMMA_BATCH_SIZE,
        source_log.clone(),
    ));
    let (control_tx, mut control_rx) = mpsc::channel(4);
    let control = tokio::spawn(async move {
        let mut commits = Vec::new();
        while let Some(OrchestratorControl::CommitActivityBucket {
            aggregates,
            context,
            committed,
        }) = control_rx.recv().await
        {
            let result = engine
                .commit_with_freshness_policy(
                    aggregates.clone(),
                    &context,
                    zero_basis(),
                    Some(pe_service::bucket_commit::PaperFreshnessPolicy {
                        activity_ws_enabled: false,
                        copy_latency_budget_secs: 2,
                    }),
                )
                .unwrap();
            commits.push((aggregates, Arc::clone(&context), result.clone()));
            let _ = committed.send(Ok(result));
        }
        commits
    });
    let now = OffsetDateTime::from_unix_timestamp(EPOCH + 10).unwrap();
    let (progress_tx, mut progress_rx) = mpsc::channel(64);
    let result = TradePoller::new(
        TradePollerConfig {
            base_url: BASE_URL.to_owned(),
            poll_interval_secs: 30,
            activity_ws_enabled: false,
            copy_latency_budget_secs: 2,
        },
        LiveWatchlist::new(watchlist()),
        fetcher,
        asset_identity,
        source_log,
        trigger_rx,
        control_tx,
        paper,
        health,
        SignalConfig::default(),
        LiveRuntimeConfig::new(RuntimeConfig::from_service_config(
            &pe_service::config::ServiceConfig::default(),
        )),
        ReconciliationObligations::default(),
        None,
    )
    .with_clock(Arc::new(move || now))
    .with_progress(progress_tx)
    .run_until(async move {
        while let Some(progress) = progress_rx.recv().await {
            if matches!(
                progress,
                pe_service::trade_poller::PollerProgress::RoundCompleted
            ) {
                break;
            }
        }
    })
    .await;
    assert!(result.is_ok(), "{result:?}");
    ingest.await.unwrap();
    control.await.unwrap()
}

fn source_frames(path: &std::path::Path) -> Vec<pe_event_log::EventEnvelope> {
    pe_event_log::Reader::replay(path)
        .unwrap()
        .map(|item| item.unwrap().1)
        .collect()
}

/// PASS: a real saturated poll writes schema-3 pages then one commitment and a V4 open row;
/// restart validates all references and reproduces every committed aggregate/effect. An empty
/// read appends no commitment; an already-committed repeat read appends its own commitment but
/// commits no new group and no pending row. FAIL: evidence or idempotency differs.
#[tokio::test]
async fn poller_multipage_commitment_survives_restart() {
    use pe_service::bucket_commit::{ACTIVITY_READ_COMMITMENT_SOURCE_ID, DecisionContinuationV3};
    use pe_service::trade_poller::{ACTIVITY_POLL_PAGE_SCHEMA_VERSION, ACTIVITY_POLL_SOURCE_ID};
    use pe_source_polymarket_public::{
        ACTIVITY_MAX_OFFSET, PolymarketEndpoint, RECONCILIATION_PAGE_LIMIT, fetch_complete_activity,
    };
    let dir = tempfile::tempdir().unwrap();
    let source_path = dir.path().join("source.log");
    let state_path = dir.path().join("paper.db");
    let paper = Arc::new(PaperStateDb::open(&state_path).unwrap());
    support::install_empty_anchor(&paper, wallet(), 0);
    paper
        .record_reconciled_history_status(&WalletHistoryStatusRecord {
            wallet: wallet(),
            complete: true,
            proof_json: "{}".to_owned(),
            updated_at_unix: 1,
        })
        .unwrap();
    let root_page_count = ACTIVITY_MAX_OFFSET / RECONCILIATION_PAGE_LIMIT + 1;
    assert_eq!(root_page_count, 11);
    // The root contains 11 seconds of 500 identical members per group. Its saturated tail
    // forces the real reader to refetch older, terminal-second and newer child windows.
    let full_pages: Vec<Vec<u8>> = (0..root_page_count)
        .map(|index| {
            let epoch = EPOCH + 10 - i64::from(index);
            let row = activity_row(
                "TRADE",
                &format!("0xmulti-{epoch}"),
                MARKET_B,
                "BUY",
                "0.002",
                "asset-b",
                epoch,
            );
            serde_json::to_vec(&vec![
                row;
                usize::try_from(RECONCILIATION_PAGE_LIMIT).unwrap()
            ])
            .unwrap()
        })
        .collect();
    let empty = b"[]".to_vec();
    let mut pages = full_pages.clone();
    pages.push(empty.clone()); // older child (-1, EPOCH-1]
    pages.push(full_pages.last().unwrap().clone()); // terminal second, offset zero
    pages.push(empty.clone()); // terminal second, offset 500
    pages.extend(full_pages.iter().take(10).cloned()); // newer child, offsets 0..4500
    pages.push(empty.clone()); // newer child, offset 5000
    let fetcher = Arc::new(QueueFetcher::from_pages(pages.clone()));
    let commits = recorded_poll(Arc::clone(&paper), &source_path, Arc::clone(&fetcher)).await;
    assert_eq!(fetcher.calls(), pages.len());
    assert_eq!(commits.len(), 11);
    assert_eq!(
        commits
            .iter()
            .map(|(_, _, result)| result.pending.len())
            .sum::<usize>(),
        1
    );
    let mut expected_urls = Vec::new();
    let mut request = |end, start, offset| {
        expected_urls.push(
            PolymarketEndpoint::UserPositionActivityPage {
                user: wallet().to_string(),
                end,
                start,
                offset,
            }
            .url(BASE_URL),
        )
    };
    for offset in
        (0..=ACTIVITY_MAX_OFFSET).step_by(usize::try_from(RECONCILIATION_PAGE_LIMIT).unwrap())
    {
        request(EPOCH + 10, Some(0), offset);
    }
    request(EPOCH - 1, Some(0), 0);
    request(EPOCH, Some(EPOCH), 0);
    request(EPOCH, Some(EPOCH), RECONCILIATION_PAGE_LIMIT);
    for offset in
        (0..=ACTIVITY_MAX_OFFSET).step_by(usize::try_from(RECONCILIATION_PAGE_LIMIT).unwrap())
    {
        request(EPOCH + 10, Some(EPOCH + 1), offset);
    }
    assert_eq!(*fetcher.urls.lock().unwrap(), expected_urls);
    let frames = source_frames(&source_path);
    for frame in frames.iter().take(pages.len()) {
        assert_eq!(frame.source_id.0, ACTIVITY_POLL_SOURCE_ID);
        assert_eq!(frame.schema_version, ACTIVITY_POLL_PAGE_SCHEMA_VERSION);
        assert_eq!(
            frame.parser_version,
            pe_source_polymarket_public::ACTIVITY_PARSER_VERSION
        );
    }
    let commitment_frame = frames
        .iter()
        .find(|frame| frame.source_id.0 == ACTIVITY_READ_COMMITMENT_SOURCE_ID)
        .unwrap();
    assert!(
        frames
            .iter()
            .filter(
                |frame| frame.source_id.0 == pe_source_polymarket_public::GAMMA_MARKETS_SOURCE_ID
            )
            .all(|frame| frame.seq < commitment_frame.seq)
    );
    assert_eq!(
        commitment_frame.source_id.0,
        ACTIVITY_READ_COMMITMENT_SOURCE_ID
    );
    assert_eq!(
        frames
            .iter()
            .filter(|frame| frame.source_id.0 == ACTIVITY_READ_COMMITMENT_SOURCE_ID)
            .count(),
        1
    );
    let commitment = pe_event_log::AppendReceipt {
        sequence: commitment_frame.seq,
        this_hash: commitment_frame.this_hash,
    };
    let rows = paper.open_decision_pending().unwrap();
    assert_eq!(rows.len(), 1);
    let continuation = DecisionContinuationV3::from_durable(&rows[0]).unwrap();
    assert_eq!(continuation.version(), 5);
    assert_eq!(continuation.read_commitment, Some(commitment));
    assert_eq!(continuation.page_occurrences.len(), pages.len());
    for (_, context, _) in &commits {
        assert_eq!(
            context.read_commitment,
            Some(pe_service::bucket_commit::ActivityReadCommitmentReceipt::BindingsV2(commitment))
        );
    }
    let groups_before = paper.activity_groups_after(&wallet(), 0).unwrap();
    let positions_before = paper.leader_positions().unwrap();
    drop(paper);
    let paper = Arc::new(PaperStateDb::open(&state_path).unwrap());
    let index = pe_service::risk_inputs::SourceReceiptIndex::replay(&source_path).unwrap();
    for page in &continuation.page_occurrences {
        assert_eq!(
            index.receipt_at(page.receipt.sequence).unwrap().unwrap().0,
            page.receipt
        );
    }
    assert_eq!(
        index.receipt_at(commitment.sequence).unwrap().unwrap().0,
        commitment
    );
    assert_eq!(
        pe_service::bucket_commit::validate_open_continuations(&paper, &index).unwrap(),
        1
    );
    // Reconstruct through the production complete-reader using only the recorded page payloads.
    let replay_fetcher = QueueFetcher::from_pages(
        frames
            .iter()
            .take(pages.len())
            .map(|frame| frame.payload.clone())
            .collect(),
    );
    let reconstructed =
        fetch_complete_activity(&replay_fetcher, BASE_URL, wallet(), Some(-1), EPOCH + 10)
            .await
            .unwrap();
    let buckets = reconstructed.buckets().unwrap();
    for (rebuilt, (committed, _, _)) in buckets.iter().zip(&commits) {
        assert_eq!(rebuilt, committed);
        for aggregate in rebuilt {
            let durable = paper
                .activity_group_state(aggregate.group_id.key())
                .unwrap()
                .unwrap();
            assert_eq!(
                durable.semantic_revision,
                aggregate.semantic_revision.as_str()
            );
            assert_eq!(
                durable.transaction_hash,
                aggregate.group_id.components().transaction_hash
            );
            assert_eq!(
                pe_position_ledger::AppliedEffect::from_document(&durable.proof_json)
                    .unwrap()
                    .effect,
                pe_position_ledger::LedgerMutation::from_activity(aggregate)
                    .unwrap()
                    .effect
            );
        }
    }
    assert_eq!(buckets.len(), commits.len());
    assert_eq!(
        paper.activity_groups_after(&wallet(), 0).unwrap(),
        groups_before
    );
    assert_eq!(paper.leader_positions().unwrap(), positions_before);
    // An empty next read has no bucket and must not mint a commitment.
    let empty_commits = recorded_poll(
        Arc::clone(&paper),
        &source_path,
        Arc::new(QueueFetcher::new(empty.clone())),
    )
    .await;
    assert!(empty_commits.is_empty());
    assert_eq!(
        source_frames(&source_path)
            .iter()
            .filter(|frame| frame.source_id.0 == ACTIVITY_READ_COMMITMENT_SOURCE_ID)
            .count(),
        1
    );
    // Cursor overlap re-observes the latest second: the engine must report already_committed.
    let repeat = recorded_poll(
        Arc::clone(&paper),
        &source_path,
        Arc::new(QueueFetcher::from_pages(vec![full_pages[0].clone(), empty])),
    )
    .await;
    assert_eq!(repeat.len(), 1);
    assert!(repeat[0].2.already_committed);
    assert!(repeat[0].2.pending.is_empty());
    assert_eq!(
        source_frames(&source_path)
            .iter()
            .filter(|frame| frame.source_id.0 == ACTIVITY_READ_COMMITMENT_SOURCE_ID)
            .count(),
        2,
        "a repeat read with a bucket commits its own read commitment; the already-committed bucket references none"
    );
    assert_eq!(
        paper.activity_groups_after(&wallet(), 0).unwrap(),
        groups_before
    );
    assert_eq!(paper.open_decision_pending().unwrap(), rows);
}

// These barriers connect the actual poller to the durable coordinator and bucket owner. Each
// response and completion is explicitly released; paused time never schedules a wallet read.
struct RequestedPage {
    url: String,
    respond: PageResponse,
}

struct PageResponse(oneshot::Sender<Result<Vec<u8>, SourceError>>);

impl PageResponse {
    fn send(self, payload: Vec<u8>) -> Result<(), Result<Vec<u8>, SourceError>> {
        self.0.send(Ok(payload))
    }

    fn fail(self) {
        self.0
            .send(Err(SourceError::Transient {
                message: "injected retryable read failure".to_owned(),
            }))
            .unwrap();
    }
}

struct GatedFetcher {
    requests: mpsc::Sender<RequestedPage>,
}

impl ReconciliationFetcher for GatedFetcher {
    fn fetch<'a>(
        &'a self,
        url: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, SourceError>> + Send + 'a>> {
        Box::pin(async move {
            let (respond, response) = oneshot::channel();
            self.requests
                .send(RequestedPage {
                    url: url.to_owned(),
                    respond: PageResponse(respond),
                })
                .await
                .map_err(|_| SourceError::Fatal {
                    message: "request barrier closed".to_owned(),
                })?;
            response.await.map_err(|_| SourceError::Fatal {
                message: "response barrier closed".to_owned(),
            })?
        })
    }
}

enum ControlCompletion {
    BucketCommitted,
    Anchored(WalletAddress),
    Boundary(i64),
}

struct RunningPoll {
    append_ack_gate: Arc<tokio::sync::Semaphore>,
    append_ack_arrived: Arc<tokio::sync::Notify>,
    bucket_ack_gate: Arc<tokio::sync::Semaphore>,
    controls: mpsc::Receiver<ControlCompletion>,
    source: SourceLogHandle,
    triggers: mpsc::Sender<pe_service::activity_ingest::ReconciliationTrigger>,
    requests: mpsc::Receiver<RequestedPage>,
    progress: mpsc::Receiver<pe_service::trade_poller::PollerProgress>,
    stop: oneshot::Sender<()>,
    poller: tokio::task::JoinHandle<Result<(), pe_service::trade_poller::TradePollerOwnerError>>,
    ingest: tokio::task::JoinHandle<()>,
    control: tokio::task::JoinHandle<Vec<RecordedBucket>>,
    now: Arc<std::sync::atomic::AtomicI64>,
    active: std::collections::HashSet<WalletAddress>,
    max_active: usize,
}

impl RunningPoll {
    fn track(&mut self, progress: &pe_service::trade_poller::PollerProgress) {
        use pe_service::trade_poller::{PollerProgress, TRADE_RECONCILIATION_CONCURRENCY};
        match progress {
            PollerProgress::Started { wallet, .. } => {
                assert!(self.active.insert(*wallet), "only one operation per wallet");
                self.max_active = self.max_active.max(self.active.len());
                assert!(self.active.len() <= TRADE_RECONCILIATION_CONCURRENCY);
            }
            PollerProgress::Completed { wallet, .. } => assert!(self.active.remove(wallet)),
            PollerProgress::RoundCompleted => {}
        }
    }

    async fn observe(&self, row: Value) -> pe_event_log::AppendReceipt {
        use pe_service::activity_ingest::{ACTIVITY_WS_SOURCE_ID, ReconciliationTrigger};
        let payload = serde_json::to_vec(&row).unwrap();
        let parsed =
            pe_source_polymarket_public::parse_activity_trade_observation(&payload).unwrap();
        let received =
            OffsetDateTime::from_unix_timestamp(self.now.load(Ordering::SeqCst)).unwrap();
        let receipt = self
            .source
            .append(pe_event_log::EnvelopeIn {
                source_id: SourceId(ACTIVITY_WS_SOURCE_ID.to_owned()),
                schema_version: 2,
                parser_version: 2,
                observed_at: parsed.source_time.clone(),
                received_at: ReceivedAt(received),
                content_type: pe_event_log::ContentType::Json,
                payload,
            })
            .await
            .unwrap();
        self.triggers
            .send(ReconciliationTrigger {
                wallet: parsed.wallet,
                source_time: parsed.source_time.0,
                source_trade_id: parsed.group_id.key().clone(),
                provenance: pe_copy_signal_engine::TradeProvenance::ActivityWs,
                received_at: received,
                receipt,
            })
            .await
            .unwrap();
        receipt
    }

    async fn completed(&mut self, target: WalletAddress) -> Vec<pe_event_log::AppendReceipt> {
        while let Some(progress) = self.progress.recv().await {
            self.track(&progress);
            if let pe_service::trade_poller::PollerProgress::Completed { wallet, selected } =
                progress
                && wallet == target
            {
                return selected;
            }
        }
        unreachable!("poller completed channel closed");
    }

    async fn round_completed(&mut self) {
        while let Some(progress) = self.progress.recv().await {
            self.track(&progress);
            if matches!(
                progress,
                pe_service::trade_poller::PollerProgress::RoundCompleted
            ) {
                return;
            }
        }
        unreachable!("round completion channel closed");
    }

    async fn finish(self) -> Vec<RecordedBucket> {
        self.stop.send(()).unwrap();
        self.poller.await.unwrap().unwrap();
        drop(self.source);
        drop(self.triggers);
        self.ingest.await.unwrap();
        self.control.await.unwrap()
    }
}

fn start_recorded_poller(
    dir: &tempfile::TempDir,
    wallets: &[WalletAddress],
) -> (RunningPoll, Arc<PaperStateDb>) {
    start_recorded_poller_with_owner(dir, wallets, false)
}

fn start_recorded_poller_with_owner(
    dir: &tempfile::TempDir,
    wallets: &[WalletAddress],
    real_owner: bool,
) -> (RunningPoll, Arc<PaperStateDb>) {
    start_recorded_poller_with_anchors(dir, wallets, real_owner, false, None)
}

fn start_recorded_poller_with_anchors(
    dir: &tempfile::TempDir,
    wallets: &[WalletAddress],
    real_owner: bool,
    anchors: bool,
    boundary_anchor: Option<i64>,
) -> (RunningPoll, Arc<PaperStateDb>) {
    let paper = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
    for wallet in wallets {
        paper.set_cursor(wallet, EPOCH - 10).unwrap();
        paper
            .record_reconciled_history_status(&WalletHistoryStatusRecord {
                wallet: *wallet,
                complete: true,
                proof_json: "{}".to_owned(),
                updated_at_unix: EPOCH - 10,
            })
            .unwrap();
        paper
            .install_anchors(&[AnchorInstallRecord {
                history_status: None,
                wallet: *wallet,
                balances: Vec::new(),
                activity_cutoff_unix: EPOCH - 10,
                anchored_at_unix: if anchors { EPOCH - 3_601 } else { EPOCH - 10 },
                ledger_hash_after: "scenario-anchor".to_owned(),
                positions_proof_hash: "scenario-positions".to_owned(),
                activity_bounds_json: "[]".to_owned(),
                source_log_generation: "scenario".to_owned(),
                proof_json: "{}".to_owned(),
                recorded_at_unix: EPOCH - 10,
            }])
            .unwrap();
    }
    let source_path = dir.path().join("source.log");
    let sink = SourceEventSink::open(&source_path).unwrap();
    let receipts = pe_service::risk_inputs::SourceReceiptIndex::replay(&source_path).unwrap();
    let (source, source_rx) = SourceLogHandle::channel(8);
    let (trigger_tx, trigger_rx) = mpsc::channel(8);
    let health = new_shared_health_with_ws(false, true, 90);
    let ingest = tokio::spawn(
        ActivityIngest::poll_only(sink, source_rx, trigger_tx.clone(), health.clone())
            .with_source_receipt_index(receipts.clone())
            .run(),
    );
    let identity = Arc::new(AssetIdentityResolver::new_runtime(
        Arc::new(GammaFetcher),
        BASE_URL.to_owned(),
        GAMMA_BATCH_SIZE,
        source.clone(),
    ));
    let append_ack_gate = Arc::new(tokio::sync::Semaphore::new(1));
    let append_ack_arrived = Arc::new(tokio::sync::Notify::new());
    let bucket_ack_gate = Arc::new(tokio::sync::Semaphore::new(1));
    let actor_ack_gate = bucket_ack_gate.clone();
    let (control_events, controls) = mpsc::channel(16);
    let actor_paper = paper.clone();
    let (control_tx, mut control_rx) = mpsc::channel(8);
    let mut engine =
        BucketCommitEngine::load(paper.clone(), build_leader_ledger(&paper).unwrap()).unwrap();
    let (real_tx, real_rx) = mpsc::channel(8);
    let real = real_owner.then(|| {
        let orchestrator = support::continuation_orchestrator(
            paper.clone(),
            &dir.path().join("paper.log"),
            wallet(),
            real_rx,
            support::continuation_hooks(EPOCH),
        );
        tokio::spawn(orchestrator.run(std::future::pending::<()>()))
    });
    let control = tokio::spawn(async move {
        let mut commits = Vec::new();
        while let Some(command) = control_rx.recv().await {
            match command {
                OrchestratorControl::CommitActivityBucket {
                    aggregates,
                    context,
                    committed,
                } => {
                    let result = if real_owner {
                        let (committed, result) = oneshot::channel();
                        real_tx
                            .send(OrchestratorControl::CommitActivityBucket {
                                aggregates: aggregates.clone(),
                                context: context.clone(),
                                committed,
                            })
                            .await
                            .unwrap();
                        result.await.unwrap().unwrap()
                    } else {
                        engine
                            .commit_with_freshness_policy(
                                aggregates.clone(),
                                &context,
                                zero_basis(),
                                Some(pe_service::bucket_commit::PaperFreshnessPolicy {
                                    activity_ws_enabled: true,
                                    copy_latency_budget_secs: 2,
                                }),
                            )
                            .unwrap()
                    };
                    commits.push((aggregates, context, result.clone()));
                    let _ = control_events
                        .send(ControlCompletion::BucketCommitted)
                        .await;
                    let _permit = actor_ack_gate.acquire().await.unwrap();
                    let _ = committed.send(Ok(result));
                }
                OrchestratorControl::DailyBoundary {
                    acknowledged,
                    cutoff_unix,
                    ..
                } => {
                    let _ = control_events
                        .send(ControlCompletion::Boundary(cutoff_unix))
                        .await;
                    let _ = acknowledged.send(Ok(()));
                }
                OrchestratorControl::PrepareAdmissions { acknowledged, .. } => {
                    let _ = acknowledged.send(());
                }
                OrchestratorControl::CaptureAdmissionLedger { wallet, captured } => {
                    let _ = captured.send(
                        pe_service::position_seeder::ledger_capture(
                            engine.ledger(),
                            &actor_paper,
                            wallet,
                        )
                        .map_err(|error| error.to_string()),
                    );
                }
                OrchestratorControl::InstallAnchors {
                    installs,
                    acknowledged,
                } => {
                    let result = engine.install_anchors(&installs);
                    if result.is_ok() {
                        for install in &installs {
                            let _ = control_events
                                .send(ControlCompletion::Anchored(install.wallet))
                                .await;
                        }
                    }
                    let _ = acknowledged.send(result);
                }
                _ => unreachable!("unexpected control"),
            }
        }
        drop(real_tx);
        if let Some(real) = real {
            real.await.unwrap();
        }
        commits
    });
    let (requests_tx, requests) = mpsc::channel(8);
    let (progress_tx, progress) = mpsc::channel(64);
    let (stop, stopped) = oneshot::channel();
    let now = Arc::new(std::sync::atomic::AtomicI64::new(EPOCH));
    let clock = now.clone();
    let mut membership = watchlist();
    membership.entries = wallets
        .iter()
        .map(|wallet| {
            let mut entry = watchlist().entries.remove(0);
            entry.wallet = *wallet;
            entry
        })
        .collect();
    membership.active_count = wallets.len();
    let fetcher: Arc<dyn ReconciliationFetcher> = Arc::new(GatedFetcher {
        requests: requests_tx,
    });
    let preparer = anchors.then(|| {
        pe_service::watchlist_admission::AdmissionPreparer::with_validator(
            control_tx.clone(),
            paper.clone(),
            pe_service::position_seeder::CausalPositionValidator::new(
                fetcher.clone(),
                BASE_URL,
                "scenario",
                identity.clone(),
            )
            .with_clock(Arc::new(|| EPOCH)),
        )
    });
    let mut obligations = ReconciliationObligations::default();
    if let Some(anchor) =
        boundary_anchor.or_else(|| anchors.then_some(EPOCH.div_euclid(86_400) * 86_400 - 86_400))
    {
        obligations.set_boundary_anchor(anchor);
    }
    let poller = TradePoller::new(
        TradePollerConfig {
            base_url: BASE_URL.to_owned(),
            poll_interval_secs: 30,
            activity_ws_enabled: true,
            copy_latency_budget_secs: 2,
        },
        LiveWatchlist::new(membership),
        fetcher,
        identity,
        source
            .clone()
            .with_append_ack_gate(append_ack_gate.clone(), append_ack_arrived.clone()),
        trigger_rx,
        control_tx,
        paper.clone(),
        health,
        SignalConfig::default(),
        LiveRuntimeConfig::new(RuntimeConfig::from_service_config(
            &pe_service::config::ServiceConfig::default(),
        )),
        obligations,
        preparer,
    )
    .with_source_receipt_index(receipts)
    .with_clock(Arc::new(move || {
        OffsetDateTime::from_unix_timestamp(clock.load(Ordering::SeqCst)).unwrap()
    }))
    .with_progress(progress_tx);
    let poller = tokio::spawn(poller.run_until(async move {
        let _ = stopped.await;
    }));
    (
        RunningPoll {
            append_ack_gate,
            append_ack_arrived,
            bucket_ack_gate,
            controls,
            source,
            triggers: trigger_tx,
            requests,
            progress,
            stop,
            poller,
            ingest,
            control,
            now,
            active: Default::default(),
            max_active: 0,
        },
        paper,
    )
}

fn stream_row(wallet: WalletAddress, transaction: &str, epoch: i64) -> Value {
    let mut row = activity_row("TRADE", transaction, MARKET_B, "BUY", "1", "asset-b", epoch);
    row["proxyWallet"] = json!(wallet.to_string());
    row
}

/// PASS: a durable trigger starts a request before the thirty-second cadence advances.
#[tokio::test(start_paused = true)]
async fn urgent_trigger_wakes_idle_poller() {
    let dir = tempfile::tempdir().unwrap();
    let (mut running, paper) = start_recorded_poller(&dir, &[wallet()]);
    running
        .requests
        .recv()
        .await
        .unwrap()
        .respond
        .send(b"[]".to_vec())
        .unwrap();
    running.round_completed().await;
    let ready_at = tokio::time::Instant::now();
    let row = stream_row(wallet(), "idle-wake", EPOCH);
    running.observe(row.clone()).await;
    let request = running.requests.recv().await.unwrap();
    assert_eq!(
        tokio::time::Instant::now(),
        ready_at,
        "an urgent trigger must not wait for a cadence timer"
    );
    assert!(request.url.contains(&format!("end={EPOCH}")));
    request
        .respond
        .send(serde_json::to_vec(std::slice::from_ref(&row)).unwrap())
        .unwrap();
    running.completed(wallet()).await;
    assert!(
        paper
            .activity_group_state(aggregate(row).group_id.key())
            .unwrap()
            .is_some()
    );
    let commits = running.finish().await;
    assert_eq!(commits.len(), 1);
}

/// PASS: B commits while A's backstop response remains held, using at most the two named slots.
#[tokio::test(start_paused = true)]
async fn urgent_wallet_progresses_while_unrelated_read_is_blocked() {
    let dir = tempfile::tempdir().unwrap();
    let other = WalletAddress([0xbb; 20]);
    let (mut running, paper) = start_recorded_poller(&dir, &[wallet(), other]);
    let blocked = running.requests.recv().await.unwrap();
    let row = stream_row(other, "independent", EPOCH);
    running.observe(row.clone()).await;
    let urgent = running.requests.recv().await.unwrap();
    assert!(urgent.url.contains(&other.to_string()));
    urgent
        .respond
        .send(serde_json::to_vec(&[row]).unwrap())
        .unwrap();
    running.completed(other).await;
    assert_eq!(
        paper
            .activity_groups_after(&other, EPOCH - 1)
            .unwrap()
            .len(),
        1
    );
    assert!(running.requests.try_recv().is_err());
    // Stop admission while A is still started; its complete operation must drain.
    running.stop.send(()).unwrap();
    blocked.respond.send(b"[]".to_vec()).unwrap();
    running.poller.await.unwrap().unwrap();
    drop(running.source);
    drop(running.triggers);
    running.ingest.await.unwrap();
    assert_eq!(running.control.await.unwrap().len(), 1);
}

/// PASS: duplicate reader receipts share one frozen attempt and one durable economic group.
#[tokio::test(start_paused = true)]
async fn duplicate_readers_share_one_frozen_attempt() {
    let dir = tempfile::tempdir().unwrap();
    let (mut running, paper) = start_recorded_poller(&dir, &[wallet()]);
    let initial = running.requests.recv().await.unwrap();
    let row = stream_row(wallet(), "three-readers", EPOCH);
    let first = running.observe(row.clone()).await;
    running.observe(row.clone()).await;
    running.observe(row.clone()).await;
    initial.respond.send(b"[]".to_vec()).unwrap();
    running.completed(wallet()).await;
    running
        .requests
        .recv()
        .await
        .unwrap()
        .respond
        .send(serde_json::to_vec(&[row]).unwrap())
        .unwrap();
    assert_eq!(running.completed(wallet()).await, vec![first]);
    let commits = running.finish().await;
    assert_eq!(commits.len(), 1);
    assert_eq!(
        paper
            .activity_groups_after(&wallet(), EPOCH - 1)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        commits[0]
            .1
            .observed_source_receipts
            .values()
            .copied()
            .collect::<Vec<_>>(),
        vec![first]
    );
}

/// PASS: later arrivals cannot enlarge the selected receipts while a read is held or retried.
#[tokio::test(start_paused = true)]
async fn continuous_arrivals_do_not_expand_attempt_frontier() {
    let dir = tempfile::tempdir().unwrap();
    let (mut running, _) = start_recorded_poller(&dir, &[wallet()]);
    running
        .requests
        .recv()
        .await
        .unwrap()
        .respond
        .send(b"[]".to_vec())
        .unwrap();
    running.round_completed().await;
    let original = stream_row(wallet(), "frontier-original", EPOCH);
    let first = running.observe(original.clone()).await;
    let held = running.requests.recv().await.unwrap();
    let later = stream_row(wallet(), "frontier-later", EPOCH);
    let second = running.observe(later.clone()).await;
    held.respond.send(b"[]".to_vec()).unwrap();
    assert_eq!(running.completed(wallet()).await, vec![first]);
    running.now.store(EPOCH + 1, Ordering::SeqCst);
    tokio::time::advance(std::time::Duration::from_secs(1)).await;
    running
        .requests
        .recv()
        .await
        .unwrap()
        .respond
        .send(serde_json::to_vec(&[original]).unwrap())
        .unwrap();
    assert_eq!(running.completed(wallet()).await, vec![first]);
    running
        .requests
        .recv()
        .await
        .unwrap()
        .respond
        .send(serde_json::to_vec(&[later]).unwrap())
        .unwrap();
    assert_eq!(running.completed(wallet()).await, vec![second]);
    running.finish().await;
}

/// PASS: notifications in one second cannot repeat a successful unmatched read; expiry uses backstop.
#[tokio::test(start_paused = true)]
async fn unmatched_retry_requires_advancing_fixed_end() {
    let dir = tempfile::tempdir().unwrap();
    let (mut running, _) = start_recorded_poller(&dir, &[wallet()]);
    running
        .requests
        .recv()
        .await
        .unwrap()
        .respond
        .send(b"[]".to_vec())
        .unwrap();
    running.round_completed().await;
    let row = stream_row(wallet(), "not-indexed", EPOCH);
    let first = running.observe(row.clone()).await;
    running
        .requests
        .recv()
        .await
        .unwrap()
        .respond
        .send(b"[]".to_vec())
        .unwrap();
    running.completed(wallet()).await;
    for _ in 0..3 {
        running.observe(row.clone()).await;
    }
    assert!(running.requests.try_recv().is_err());
    running.now.store(EPOCH + 1, Ordering::SeqCst);
    tokio::time::advance(std::time::Duration::from_secs(1)).await;
    let request = running.requests.recv().await.unwrap();
    assert!(request.url.contains(&format!("end={}", EPOCH + 1)));
    request.respond.send(b"[]".to_vec()).unwrap();
    running.completed(wallet()).await;
    running.now.store(EPOCH + 3, Ordering::SeqCst);
    tokio::time::advance(std::time::Duration::from_secs(2)).await;
    assert!(running.requests.try_recv().is_err());
    running
        .observe(stream_row(wallet(), "arrived-after-expiry", EPOCH + 3))
        .await;
    assert!(running.requests.try_recv().is_err());
    running.now.store(EPOCH + 30, Ordering::SeqCst);
    tokio::time::advance(std::time::Duration::from_secs(27)).await;
    running
        .requests
        .recv()
        .await
        .unwrap()
        .respond
        .send(b"[]".to_vec())
        .unwrap();
    assert_eq!(
        running.completed(wallet()).await,
        vec![first],
        "expiry retains the original frontier for the ordinary backstop"
    );
    running.finish().await;
}

/// PASS: history at either adjacent second keeps its canonical epoch and ages from the older clock.
#[tokio::test(start_paused = true)]
async fn cross_second_binding_preserves_history_order_and_oldest_age() {
    for delta in [-1, 1] {
        let dir = tempfile::tempdir().unwrap();
        let (mut running, paper) = start_recorded_poller(&dir, &[wallet()]);
        running
            .requests
            .recv()
            .await
            .unwrap()
            .respond
            .send(b"[]".to_vec())
            .unwrap();
        running.round_completed().await;
        let stream = stream_row(wallet(), "two-clocks", EPOCH);
        running.observe(stream).await;
        let request = running.requests.recv().await.unwrap();
        // A +1 history row becomes available on the next fixed end, without changing the frontier.
        let request = if delta == 1 {
            request.respond.send(b"[]".to_vec()).unwrap();
            running.completed(wallet()).await;
            running.now.store(EPOCH + 1, Ordering::SeqCst);
            tokio::time::advance(std::time::Duration::from_secs(1)).await;
            running.requests.recv().await.unwrap()
        } else {
            request
        };
        running.now.store(EPOCH + 3, Ordering::SeqCst);
        let history = stream_row(wallet(), "two-clocks", EPOCH + delta);
        let id = aggregate(history.clone()).group_id.key().clone();
        request
            .respond
            .send(serde_json::to_vec(&[history]).unwrap())
            .unwrap();
        running.completed(wallet()).await;
        let commits = running.finish().await;
        assert_eq!(commits.len(), 1);
        assert_eq!(commits[0].2.source_epoch, EPOCH + delta);
        let disposition = commits[0].1.no_copy_dispositions.get(&id).unwrap();
        assert_eq!(disposition.age_secs, 3 - delta.min(0));
        assert_eq!(
            paper
                .activity_group_state(&id)
                .unwrap()
                .unwrap()
                .source_epoch,
            EPOCH + delta
        );
        let commitment = source_frames(&dir.path().join("source.log"))
            .into_iter()
            .find(|frame| {
                frame.source_id.0 == pe_service::bucket_commit::ACTIVITY_READ_COMMITMENT_SOURCE_ID
            })
            .unwrap();
        let commitment: pe_service::bucket_commit::ActivityReadCommitment =
            serde_json::from_slice(&commitment.payload).unwrap();
        assert_eq!(commitment.bindings.unwrap().len(), 1);
    }
}

/// PASS: the real orchestrator freezes and terminalizes a corrected observation; indexed raw
/// history/stream/Gamma reconstruction and restart retain the correction.
#[tokio::test(start_paused = true)]
async fn corrected_identity_binding_replays_from_raw_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let (mut running, paper) = start_recorded_poller_with_owner(&dir, &[wallet()], true);
    running
        .requests
        .recv()
        .await
        .unwrap()
        .respond
        .send(b"[]".to_vec())
        .unwrap();
    running.round_completed().await;
    let mut stream = stream_row(wallet(), "identity-correction", EPOCH);
    stream["conditionId"] = json!(MARKET_A);
    stream["outcomeIndex"] = json!(1);
    let stream_receipt = running.observe(stream).await;
    let history = stream_row(wallet(), "identity-correction", EPOCH);
    running
        .requests
        .recv()
        .await
        .unwrap()
        .respond
        .send(serde_json::to_vec(&[history]).unwrap())
        .unwrap();
    running.completed(wallet()).await;
    running.finish().await;
    let rows = paper.decision_pending_history().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].state,
        pe_paper_state::DecisionPendingState::Terminal
    );
    let continuation =
        pe_service::bucket_commit::DecisionContinuationV3::from_durable(&rows[0]).unwrap();
    assert_eq!(continuation.version(), 5);
    let index = pe_service::risk_inputs::SourceReceiptIndex::replay(&dir.path().join("source.log"))
        .unwrap();
    let observation = continuation
        .observation_from_receipt_index(&index)
        .unwrap()
        .unwrap();
    assert_eq!(observation.source_receipt, stream_receipt);
    assert_eq!(
        continuation.incoming_trade().unwrap().market_id,
        market(MARKET_B)
    );
    pe_service::decision_replay::replay_decision_pending(&rows[0]).unwrap();
    assert!(
        pe_service::trade_poller::rebuild_reconciliation_obligations(
            &dir.path().join("source.log"),
            &paper
        )
        .unwrap()
        .is_empty()
    );
}

/// PASS: source legs stay separate; multiple candidates for corrected stamps create the existing
/// invalid-mapping fence and never manufacture a binding to an arbitrary target.
#[tokio::test(start_paused = true)]
async fn transaction_legs_remain_distinct_during_correlation() {
    for (previous_epoch, verified_mapping) in [
        (None, true),
        (Some(EPOCH), true),
        (Some(EPOCH - 1), true),
        (None, false),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let (mut running, paper) = start_recorded_poller(&dir, &[wallet()]);
        running
            .requests
            .recv()
            .await
            .unwrap()
            .respond
            .send(previous_epoch.map_or_else(
                || b"[]".to_vec(),
                |epoch| serde_json::to_vec(&[stream_row(wallet(), "many-legs", epoch)]).unwrap(),
            ))
            .unwrap();
        running.round_completed().await;
        let mut stream = stream_row(wallet(), "many-legs", EPOCH);
        stream["conditionId"] = json!("incorrect-stamp");
        let mut buy = stream_row(wallet(), "many-legs", EPOCH);
        if !verified_mapping {
            stream["asset"] = json!("unverified-asset");
            buy["asset"] = json!("unverified-asset");
        }
        running.observe(stream).await;
        let mut other_outcome = buy.clone();
        other_outcome["outcomeIndex"] = json!(1);
        let mut sell = buy.clone();
        sell["side"] = json!("SELL");
        let mut asset = buy.clone();
        asset["asset"] = json!("other-asset");
        running
            .requests
            .recv()
            .await
            .unwrap()
            .respond
            .send(serde_json::to_vec(&[buy.clone(), buy, other_outcome, sell, asset]).unwrap())
            .unwrap();
        running.completed(wallet()).await;
        let commits = running.finish().await;
        if !verified_mapping {
            assert!(commits.is_empty());
            assert!(paper.wallet_fences().unwrap().is_empty());
            assert_eq!(
                pe_service::trade_poller::rebuild_reconciliation_obligations(
                    &dir.path().join("source.log"),
                    &paper
                )
                .unwrap()
                .len(),
                1,
                "raw candidates without a verified mapping remain outstanding"
            );
            continue;
        }
        assert_eq!(commits.len(), if previous_epoch.is_some() { 2 } else { 1 });
        let commits = &commits[commits.len() - 1..];
        assert_eq!(commits[0].0.len(), 4);
        assert_eq!(
            commits[0]
                .0
                .iter()
                .map(|aggregate| aggregate.row_count)
                .sum::<u64>(),
            5
        );
        assert_eq!(
            commits[0].2.newly_fenced,
            Some(pe_position_ledger::WalletFenceCause::InvalidMapping)
        );
        assert!(commits[0].2.pending.is_empty());
        let frame = source_frames(&dir.path().join("source.log"))
            .into_iter()
            .rfind(|frame| {
                frame.source_id.0 == pe_service::bucket_commit::ACTIVITY_READ_COMMITMENT_SOURCE_ID
            })
            .unwrap();
        let commitment: pe_service::bucket_commit::ActivityReadCommitment =
            serde_json::from_slice(&frame.payload).unwrap();
        assert_eq!(
            commitment.bindings,
            Some(Vec::new()),
            "ambiguity never fabricates a target binding"
        );
        for target in &commits[0].0 {
            assert!(
                paper
                    .activity_revision_disposed(
                        target.group_id.key(),
                        target.semantic_revision.as_str()
                    )
                    .unwrap()
            );
        }
        if let Some(epoch) = previous_epoch {
            let original = aggregate(stream_row(wallet(), "many-legs", epoch));
            assert_eq!(
                paper
                    .activity_group_state(original.group_id.key())
                    .unwrap()
                    .unwrap()
                    .source_epoch,
                epoch
            );
        }
        let fence = paper.wallet_fences().unwrap().remove(0);
        assert_eq!(fence.cause, "invalid_mapping");
        assert!(
            pe_service::trade_poller::rebuild_reconciliation_obligations(
                &dir.path().join("source.log"),
                &paper
            )
            .unwrap()
            .is_empty()
        );
    }
}

fn write_source_prefix(path: &std::path::Path, frames: &[pe_event_log::EventEnvelope]) {
    let mut writer = pe_event_log::Writer::open(path).unwrap();
    for frame in frames {
        writer
            .append_synced(pe_event_log::EnvelopeIn {
                source_id: frame.source_id.clone(),
                schema_version: frame.schema_version,
                parser_version: frame.parser_version,
                observed_at: frame.observed_at.clone(),
                received_at: frame.received_at.clone(),
                content_type: frame.content_type.clone(),
                payload: frame.payload.clone(),
            })
            .unwrap();
    }
}

/// PASS: pages and a binding commitment retain work until the exact target revision has a
/// durable disposition; a retained predecessor alone cannot acknowledge a revised aggregate.
#[tokio::test(start_paused = true)]
async fn binding_restart_requires_durable_target_revision() {
    for revision_fence in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let (mut running, paper) = start_recorded_poller(&dir, &[wallet()]);
        let mut history = stream_row(wallet(), "restart-revision", EPOCH);
        if revision_fence {
            running
                .requests
                .recv()
                .await
                .unwrap()
                .respond
                .send(serde_json::to_vec(&[history.clone()]).unwrap())
                .unwrap();
        } else {
            running
                .requests
                .recv()
                .await
                .unwrap()
                .respond
                .send(b"[]".to_vec())
                .unwrap();
        }
        running.round_completed().await;
        let mut stream = history.clone();
        stream["conditionId"] = json!("old-stream-stamp");
        running.observe(stream).await;
        if revision_fence {
            history["size"] = json!("2");
        }
        let request = running.requests.recv().await.unwrap();
        let before = dir.path().join("before.db");
        // Existing SQLite backup owner preserves the pre-attempt durable state.
        rusqlite::Connection::open(dir.path().join("paper.db"))
            .unwrap()
            .execute("VACUUM INTO ?1", [before.to_str().unwrap()])
            .unwrap();
        request
            .respond
            .send(serde_json::to_vec(&[history.clone()]).unwrap())
            .unwrap();
        running.completed(wallet()).await;
        running.finish().await;
        let frames = source_frames(&dir.path().join("source.log"));
        let commitment_index = frames
            .iter()
            .rposition(|frame| {
                frame.source_id.0 == pe_service::bucket_commit::ACTIVITY_READ_COMMITMENT_SOURCE_ID
            })
            .unwrap();
        for (name, count) in [
            ("pages", commitment_index),
            ("commitment", commitment_index + 1),
        ] {
            let path = dir.path().join(format!("{name}.log"));
            write_source_prefix(&path, &frames[..count]);
            let before = PaperStateDb::open(&before).unwrap();
            assert_eq!(
                pe_service::trade_poller::rebuild_reconciliation_obligations(&path, &before)
                    .unwrap()
                    .len(),
                1,
                "{name}"
            );
        }
        let target = aggregate(history);
        assert!(
            paper
                .activity_revision_disposed(
                    target.group_id.key(),
                    target.semantic_revision.as_str()
                )
                .unwrap()
        );
        if revision_fence {
            assert_ne!(
                paper
                    .activity_group_state(target.group_id.key())
                    .unwrap()
                    .unwrap()
                    .semantic_revision,
                target.semantic_revision.as_str()
            );
        }
        assert!(
            pe_service::trade_poller::rebuild_reconciliation_obligations(
                &dir.path().join("source.log"),
                &paper
            )
            .unwrap()
            .is_empty()
        );
    }
}

/// PASS: authentic enclosing receipts and recomputed digests reach raw binding validation;
/// boot and continuation reconstruction reject semantic mutations and missing nonempty read proofs.
#[tokio::test(start_paused = true)]
async fn binding_tamper_and_generation_substitution_are_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let (mut running, paper) = start_recorded_poller_with_owner(&dir, &[wallet()], true);
    running
        .requests
        .recv()
        .await
        .unwrap()
        .respond
        .send(b"[]".to_vec())
        .unwrap();
    running.round_completed().await;
    let mut stream = stream_row(wallet(), "tamper-binding", EPOCH);
    stream["conditionId"] = json!("stream-stamp");
    running.observe(stream).await;
    running
        .requests
        .recv()
        .await
        .unwrap()
        .respond
        .send(serde_json::to_vec(&[stream_row(wallet(), "tamper-binding", EPOCH)]).unwrap())
        .unwrap();
    running.completed(wallet()).await;
    running.finish().await;
    let original = source_frames(&dir.path().join("source.log"));
    let pending = paper.decision_pending_history().unwrap().remove(0);
    let continuation =
        pe_service::bucket_commit::DecisionContinuationV3::from_durable(&pending).unwrap();
    let original_index =
        pe_service::risk_inputs::SourceReceiptIndex::replay(&dir.path().join("source.log"))
            .unwrap();
    assert!(
        continuation
            .observation_from_receipt_index(&original_index)
            .unwrap()
            .is_some()
    );
    pe_service::decision_replay::replay_decision_pending(&pending).unwrap();
    let commitment_frame = original
        .iter()
        .find(|frame| {
            frame.source_id.0 == pe_service::bucket_commit::ACTIVITY_READ_COMMITMENT_SOURCE_ID
        })
        .unwrap();
    let original_commitment: pe_service::bucket_commit::ActivityReadCommitment =
        serde_json::from_slice(&commitment_frame.payload).unwrap();
    let proof = original_commitment.read_proof.as_ref().unwrap();
    for (change, expected) in [
        (
            "bindings",
            "websocket correction has no verified observation binding",
        ),
        ("revision", "binding target revision differs"),
        ("group", "binding stream group differs from its receipt"),
        (
            "stream_receipt",
            "binding stream has the wrong source contract",
        ),
        ("occurrence", "binding target page occurrence differs"),
        ("metadata", "binding metadata provenance differs"),
        ("digest", "commitment differs from its frozen proof"),
        ("schema", "commitment has the wrong source contract"),
        ("parser", "commitment has the wrong source contract"),
        ("version", "commitment differs from its frozen proof"),
        ("read_proof", "binding commitment read proof is absent"),
    ] {
        let mut frames = original.clone();
        let frame = frames
            .iter_mut()
            .find(|frame| frame.seq == commitment_frame.seq)
            .unwrap();
        let mut value: Value = serde_json::from_slice(&frame.payload).unwrap();
        match change {
            "bindings" => value["bindings"] = json!([]),
            "revision" => value["bindings"][0]["semantic_revision"] = json!("changed"),
            "group" => value["bindings"][0]["stream_group_id"] = json!("g2:changed"),
            "stream_receipt" => {
                value["bindings"][0]["stream_receipt"] = json!(proof.page_occurrences[0].receipt)
            }
            "occurrence" => value["bindings"][0]["page_occurrence_index"] = json!(99),
            "metadata" => {
                value["bindings"][0]["identity_provenance"]["source_log_sequence"] = json!(0)
            }
            "digest" => value["digest"] = json!("00".repeat(32)),
            "schema" => frame.schema_version = 1,
            "parser" => frame.parser_version = 2,
            "version" => value["version"] = json!(1),
            "read_proof" => {
                value.as_object_mut().unwrap().remove("read_proof");
            }
            _ => unreachable!(),
        }
        if change != "digest" {
            let bindings = serde_json::from_value::<
                Vec<pe_service::bucket_commit::ObservationBinding>,
            >(value["bindings"].clone())
            .unwrap();
            let recomputed: Value = serde_json::from_slice(
                &pe_service::bucket_commit::activity_read_commitment_payload_v2(
                    original_commitment.wallet,
                    original_commitment.fixed_end,
                    &proof.page_occurrences,
                    &proof.pages,
                    &bindings,
                )
                .unwrap(),
            )
            .unwrap();
            value["digest"] = recomputed["digest"].clone();
        }
        frame.payload = serde_json::to_vec(&value).unwrap();
        let path = dir.path().join(format!("{change}.log"));
        write_source_prefix(&path, &frames);
        let rebuilt = pe_service::trade_poller::rebuild_reconciliation_obligations(&path, &paper);
        if change == "bindings" {
            assert_eq!(
                rebuilt.unwrap().len(),
                1,
                "an empty commitment cannot discharge a corrected observation"
            );
        } else {
            let error = rebuilt.unwrap_err().to_string();
            if !matches!(change, "schema" | "parser" | "version") {
                assert!(error.contains(expected), "{change}: {error}");
            }
        }
        let index = pe_service::risk_inputs::SourceReceiptIndex::replay(&path).unwrap();
        let receipt = index.receipt_at(commitment_frame.seq).unwrap().unwrap().0;
        // Rebind to the actual synchronized replacement, so receipt authentication succeeds.
        let mut changed = continuation.clone();
        changed.read_commitment = Some(receipt);
        for receipt in changed
            .page_occurrences
            .iter()
            .map(|page| page.receipt)
            .chain(changed.observed_source_receipt)
            .chain(changed.read_commitment)
        {
            assert_eq!(
                index.receipt_at(receipt.sequence).unwrap().unwrap().0,
                receipt
            );
        }
        assert!(
            changed.observation_from_receipt_index(&index).is_err(),
            "{change}"
        );
        rusqlite::Connection::open(dir.path().join("paper.db"))
            .unwrap()
            .execute(
                "UPDATE decision_pending SET frozen_inputs_json = ?1 WHERE source_trade_id = ?2",
                rusqlite::params![
                    serde_json::to_string(&changed).unwrap(),
                    pending.source_trade_id.0
                ],
            )
            .unwrap();
        let report = support::qualify_source_census(
            &dir.path().join(format!("qualify-{change}")),
            &path,
            &dir.path().join("paper.db"),
            EPOCH,
        )
        .await;
        assert_eq!(
            report.verdict,
            pe_service::qualification::QualificationVerdict::InsufficientEvidence
        );
        let reason = report.reasons.join("; ");
        assert!(reason.contains(expected), "{change}: {reason}");
    }
}

/// PASS: shutdown leaves a held wallet operation alive through its page append and bucket ack;
/// queued work never starts after shutdown, and every task joins.
#[tokio::test(start_paused = true)]
async fn shutdown_drains_started_wallet_operations() {
    let dir = tempfile::tempdir().unwrap();
    let (mut running, paper) = start_recorded_poller(&dir, &[wallet()]);
    let request = running.requests.recv().await.unwrap();
    running
        .observe(stream_row(wallet(), "queued-at-stop", EPOCH))
        .await;
    let held_append = running
        .append_ack_gate
        .clone()
        .acquire_owned()
        .await
        .unwrap();
    let held_bucket = running
        .bucket_ack_gate
        .clone()
        .acquire_owned()
        .await
        .unwrap();
    running.stop.send(()).unwrap();
    assert!(!running.poller.is_finished());
    let history = stream_row(wallet(), "started-before-stop", EPOCH);
    request
        .respond
        .send(serde_json::to_vec(&[history]).unwrap())
        .unwrap();
    running.append_ack_arrived.notified().await;
    assert!(
        !running.poller.is_finished(),
        "the durable page acknowledgement is still held"
    );
    assert!(
        paper
            .activity_groups_after(&wallet(), EPOCH - 1)
            .unwrap()
            .is_empty()
    );
    drop(held_append);
    loop {
        if matches!(
            running.controls.recv().await.unwrap(),
            ControlCompletion::BucketCommitted
        ) {
            break;
        }
    }
    assert!(
        !running.poller.is_finished(),
        "the durable bucket acknowledgement is still held"
    );
    drop(held_bucket);
    running.poller.await.unwrap().unwrap();
    assert!(running.requests.try_recv().is_err());
    drop(running.source);
    drop(running.triggers);
    running.ingest.await.unwrap();
    assert_eq!(running.control.await.unwrap().len(), 1);
    assert_eq!(
        paper
            .activity_groups_after(&wallet(), EPOCH - 1)
            .unwrap()
            .len(),
        1
    );
}

/// PASS: held urgent reads coexist with finite backstop revisits, both anchor classes, and the
/// oldest ready boundary. A source receipt outside the boundary receive window cannot starve it.
#[tokio::test(start_paused = true)]
async fn urgent_load_preserves_backstop_anchor_and_boundary_progress() {
    let dir = tempfile::tempdir().unwrap();
    let age_due = WalletAddress([0xbb; 20]);
    let (mut running, paper) =
        start_recorded_poller_with_anchors(&dir, &[wallet(), age_due], false, true, None);
    rusqlite::Connection::open(dir.path().join("paper.db"))
        .unwrap()
        .execute(
            "UPDATE poll_cursors SET reanchor_required = 1 WHERE wallet_hex = ?1",
            [wallet().to_string()],
        )
        .unwrap();
    let first = running.requests.recv().await.unwrap();
    let urgent_wallet = WalletAddress([0xcc; 20]);
    running
        .observe(stream_row(urgent_wallet, "urgent-during-reanchor", EPOCH))
        .await;
    let urgent = running.requests.recv().await.unwrap();
    assert!(urgent.url.contains(&urgent_wallet.to_string()));
    first.respond.send(b"[]".to_vec()).unwrap();
    // Keep urgent HTTP held while the independent slot completes its background round and bracket.
    let anchored = loop {
        tokio::select! {
            request = running.requests.recv() => request.unwrap().respond.send(b"[]".to_vec()).unwrap(),
            event = running.controls.recv() => if let ControlCompletion::Anchored(wallet) = event.unwrap() { break wallet; },
        }
    };
    assert_eq!(anchored, wallet());
    assert!(!paper.wallet_coverage(&wallet()).unwrap().reanchor_required);
    let cutoff = loop {
        if let ControlCompletion::Boundary(cutoff) = running.controls.recv().await.unwrap() {
            break cutoff;
        }
    };
    assert_eq!(cutoff, EPOCH.div_euclid(86_400) * 86_400);
    urgent
        .respond
        .send(
            serde_json::to_vec(&[stream_row(urgent_wallet, "urgent-during-reanchor", EPOCH)])
                .unwrap(),
        )
        .unwrap();
    running.completed(urgent_wallet).await;
    let second_urgent = WalletAddress([0xdd; 20]);
    running
        .observe(stream_row(
            second_urgent,
            "urgent-during-age-refresh",
            EPOCH,
        ))
        .await;
    let urgent = running.requests.recv().await.unwrap();
    assert!(urgent.url.contains(&second_urgent.to_string()));
    tokio::time::advance(std::time::Duration::from_secs(30)).await;
    let anchored = loop {
        tokio::select! {
            request = running.requests.recv() => request.unwrap().respond.send(b"[]".to_vec()).unwrap(),
            event = running.controls.recv() => if let ControlCompletion::Anchored(wallet) = event.unwrap() { break wallet; },
        }
    };
    assert_eq!(anchored, age_due);
    urgent
        .respond
        .send(
            serde_json::to_vec(&[stream_row(
                second_urgent,
                "urgent-during-age-refresh",
                EPOCH,
            )])
            .unwrap(),
        )
        .unwrap();
    running.completed(second_urgent).await;
    running.finish().await;
}

/// PASS: a failed empty backstop releases ownership before a fresh urgent trigger, with no tick.
#[tokio::test(start_paused = true)]
async fn retryable_empty_backstop_does_not_delay_fresh_trigger() {
    let dir = tempfile::tempdir().unwrap();
    let (mut running, paper) = start_recorded_poller(&dir, &[wallet()]);
    let clock = tokio::time::Instant::now();
    running.requests.recv().await.unwrap().respond.fail();
    assert!(running.completed(wallet()).await.is_empty());
    running.round_completed().await;
    let row = stream_row(wallet(), "after-empty-failure", EPOCH);
    let receipt = running.observe(row.clone()).await;
    let request = running.requests.recv().await.unwrap();
    assert_eq!(tokio::time::Instant::now(), clock);
    request
        .respond
        .send(serde_json::to_vec(&[row]).unwrap())
        .unwrap();
    assert_eq!(running.completed(wallet()).await, vec![receipt]);
    running.finish().await;
    assert!(
        pe_service::trade_poller::rebuild_reconciliation_obligations(
            &dir.path().join("source.log"),
            &paper
        )
        .unwrap()
        .is_empty()
    );
}

/// PASS: arrivals during read failure and bucket acknowledgment stay outside the frozen retry;
/// an unrelated operation occupies the other slot throughout, with no third or duplicate wallet.
#[tokio::test(start_paused = true)]
async fn arrivals_during_failure_and_ack_delay_preserve_frontier_and_two_slots() {
    let dir = tempfile::tempdir().unwrap();
    let other = WalletAddress([0xbb; 20]);
    let (mut running, _) = start_recorded_poller(&dir, &[wallet(), other]);
    let blocked = running.requests.recv().await.unwrap();
    assert!(blocked.url.contains(&wallet().to_string()));
    let first_row = stream_row(other, "frozen-before-failure", EPOCH);
    let first = running.observe(first_row.clone()).await;
    let failing = running.requests.recv().await.unwrap();
    let later_row = stream_row(other, "during-failure", EPOCH);
    let later = running.observe(later_row.clone()).await;
    failing.respond.fail();
    assert_eq!(running.completed(other).await, vec![first]);
    assert!(running.requests.try_recv().is_err());
    running.now.store(EPOCH + 1, Ordering::SeqCst);
    tokio::time::advance(std::time::Duration::from_secs(1)).await;
    let retry = running.requests.recv().await.unwrap();
    let gate = running.bucket_ack_gate.clone();
    let held = gate.acquire().await.unwrap();
    retry
        .respond
        .send(serde_json::to_vec(&[first_row]).unwrap())
        .unwrap();
    assert!(matches!(
        running.controls.recv().await.unwrap(),
        ControlCompletion::BucketCommitted
    ));
    let newest_row = stream_row(other, "during-ack-delay", EPOCH + 1);
    let newest = running.observe(newest_row.clone()).await;
    assert!(running.requests.try_recv().is_err());
    drop(held);
    assert_eq!(running.completed(other).await, vec![first]);
    running
        .requests
        .recv()
        .await
        .unwrap()
        .respond
        .send(serde_json::to_vec(&[later_row, newest_row]).unwrap())
        .unwrap();
    assert_eq!(running.completed(other).await, vec![later, newest]);
    assert_eq!(running.max_active, 2);
    assert!(running.requests.try_recv().is_err());
    running.stop.send(()).unwrap();
    blocked.respond.send(b"[]".to_vec()).unwrap();
    running.poller.await.unwrap().unwrap();
    drop(running.source);
    drop(running.triggers);
    running.ingest.await.unwrap();
    running.control.await.unwrap();
}

/// PASS: a pre-boundary ambiguity after a permanent fence holds publication until its operation
/// acknowledges, then releases the boundary, frontier and old read start without replacing the fence.
#[tokio::test(start_paused = true)]
async fn existing_fence_discharges_new_ambiguity_and_releases_oldest_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let cutoff = (EPOCH.div_euclid(86_400) + 1) * 86_400;
    let (mut running, paper) =
        start_recorded_poller_with_anchors(&dir, &[wallet()], false, false, Some(cutoff - 86_400));
    running
        .requests
        .recv()
        .await
        .unwrap()
        .respond
        .send(b"[]".to_vec())
        .unwrap();
    running.round_completed().await;
    let mut first = stream_row(wallet(), "first-ambiguity", EPOCH);
    first["conditionId"] = json!("incorrect-stamp");
    running.observe(first).await;
    let left = stream_row(wallet(), "first-ambiguity", EPOCH);
    let mut right = left.clone();
    right["outcomeIndex"] = json!(1);
    running
        .requests
        .recv()
        .await
        .unwrap()
        .respond
        .send(serde_json::to_vec(&[left, right]).unwrap())
        .unwrap();
    running.completed(wallet()).await;
    let fence = paper.wallet_fences().unwrap();
    assert_eq!(fence.len(), 1);
    assert!(matches!(
        running.controls.recv().await.unwrap(),
        ControlCompletion::BucketCommitted
    ));

    running.now.store(cutoff - 1, Ordering::SeqCst);
    let mut second = stream_row(wallet(), "new-ambiguity", cutoff - 1);
    second["conditionId"] = json!("another-incorrect-stamp");
    let second_receipt = running.observe(second).await;
    let held = running.requests.recv().await.unwrap();
    running.now.store(cutoff, Ordering::SeqCst);
    tokio::time::advance(std::time::Duration::from_secs(30)).await;
    running.round_completed().await;
    assert!(
        running.controls.try_recv().is_err(),
        "qualifying observation holds the boundary"
    );
    let frames = source_frames(&dir.path().join("source.log"));
    let boundary = frames
        .iter()
        .find(|frame| frame.source_id.0 == pe_service::trade_poller::DAILY_BOUNDARY_SOURCE_ID)
        .unwrap();
    assert!(second_receipt.sequence < boundary.seq);
    // A restart can already use the existing permanent fence, even before another commitment.
    assert!(
        pe_service::trade_poller::rebuild_reconciliation_obligations(
            &dir.path().join("source.log"),
            &paper
        )
        .unwrap()
        .is_empty()
    );

    let gate = running.bucket_ack_gate.clone();
    let held_ack = gate.acquire().await.unwrap();
    let left = stream_row(wallet(), "new-ambiguity", cutoff - 1);
    let mut right = left.clone();
    right["outcomeIndex"] = json!(1);
    held.respond
        .send(serde_json::to_vec(&[left, right]).unwrap())
        .unwrap();
    assert!(matches!(
        running.controls.recv().await.unwrap(),
        ControlCompletion::BucketCommitted
    ));
    assert!(
        running.controls.try_recv().is_err(),
        "publication also waits for acknowledgment"
    );
    drop(held_ack);
    assert_eq!(running.completed(wallet()).await, vec![second_receipt]);
    assert!(
        matches!(running.controls.recv().await.unwrap(), ControlCompletion::Boundary(value) if value == cutoff)
    );
    assert_eq!(paper.wallet_fences().unwrap(), fence);
    assert!(
        pe_service::trade_poller::rebuild_reconciliation_obligations(
            &dir.path().join("source.log"),
            &paper
        )
        .unwrap()
        .is_empty()
    );

    // No old frontier owns a later trigger, even while the cadence remains paused.
    let fresh = stream_row(wallet(), "after-discharge", cutoff);
    let fresh_receipt = running.observe(fresh.clone()).await;
    let request = running.requests.recv().await.unwrap();
    assert!(
        request.url.contains(&format!("start={}", cutoff - 1)),
        "{}",
        request.url
    );
    request
        .respond
        .send(serde_json::to_vec(&[fresh]).unwrap())
        .unwrap();
    assert_eq!(running.completed(wallet()).await, vec![fresh_receipt]);
    let absent = running
        .observe(stream_row(
            wallet(),
            "fenced-with-no-history-target",
            cutoff,
        ))
        .await;
    running
        .requests
        .recv()
        .await
        .unwrap()
        .respond
        .send(b"[]".to_vec())
        .unwrap();
    assert_eq!(running.completed(wallet()).await, vec![absent]);
    assert!(
        pe_service::trade_poller::rebuild_reconciliation_obligations(
            &dir.path().join("source.log"),
            &paper,
        )
        .unwrap()
        .is_empty()
    );
    tokio::time::advance(std::time::Duration::from_secs(30)).await;
    let backstop = running.requests.recv().await.unwrap();
    assert!(
        backstop.url.contains(&format!("start={cutoff}")),
        "{}",
        backstop.url
    );
    backstop.respond.send(b"[]".to_vec()).unwrap();
    assert!(running.completed(wallet()).await.is_empty());
    let commits = running.finish().await;
    assert!(
        commits
            .iter()
            .all(|(_, _, result)| result.pending.is_empty())
    );
    assert_eq!(paper.wallet_fences().unwrap(), fence);
}

/// PASS: changing any required correlation field alone leaves the original receipt outstanding;
/// the different-wallet response is refused by the complete reader before correlation.
#[tokio::test(start_paused = true)]
async fn differing_correlation_fields_never_bind_an_original() {
    for field in ["transactionHash", "proxyWallet", "type", "asset", "side"] {
        let dir = tempfile::tempdir().unwrap();
        let (mut running, paper) = start_recorded_poller(&dir, &[wallet()]);
        running
            .requests
            .recv()
            .await
            .unwrap()
            .respond
            .send(b"[]".to_vec())
            .unwrap();
        running.round_completed().await;
        let mut stream = stream_row(wallet(), "negative-correlation", EPOCH);
        stream["conditionId"] = json!("incorrect-stream-stamp");
        let receipt = running.observe(stream).await;
        let mut history = stream_row(wallet(), "negative-correlation", EPOCH);
        history[field] = match field {
            "transactionHash" => json!("different-transaction"),
            "proxyWallet" => json!(WalletAddress([0xbb; 20]).to_string()),
            "type" => json!("SPLIT"),
            "asset" => json!("different-asset"),
            "side" => json!("SELL"),
            _ => unreachable!(),
        };
        running
            .requests
            .recv()
            .await
            .unwrap()
            .respond
            .send(serde_json::to_vec(&[history]).unwrap())
            .unwrap();
        assert_eq!(running.completed(wallet()).await, vec![receipt], "{field}");
        let commits = running.finish().await;
        assert!(commits.is_empty(), "{field}");
        assert!(paper.wallet_fences().unwrap().is_empty(), "{field}");
        let path = dir.path().join("source.log");
        let obligations =
            pe_service::trade_poller::rebuild_reconciliation_obligations(&path, &paper).unwrap();
        assert_eq!(obligations.len(), 1, "{field}");
        assert_eq!(
            obligations.migration_evidence()[0]["receipt"],
            json!(receipt)
        );
        for frame in source_frames(&path).into_iter().filter(|frame| {
            frame.source_id.0 == pe_service::bucket_commit::ACTIVITY_READ_COMMITMENT_SOURCE_ID
        }) {
            let value: pe_service::bucket_commit::ActivityReadCommitment =
                serde_json::from_slice(&frame.payload).unwrap();
            assert_eq!(value.bindings, Some(Vec::new()), "{field}");
        }
    }
}

/// PASS: an exact leg and a corrected leg in one transaction retain separate raw groups,
/// revisions, receipts and metadata requirements, with aggregate multiplicity unchanged.
#[tokio::test(start_paused = true)]
async fn mixed_exact_and_corrected_legs_keep_individual_bindings() {
    let dir = tempfile::tempdir().unwrap();
    let (mut running, paper) = start_recorded_poller(&dir, &[wallet()]);
    let held = running.requests.recv().await.unwrap();
    let exact = stream_row(wallet(), "mixed-legs", EPOCH);
    let exact_receipt = running.observe(exact.clone()).await;
    let mut corrected = exact.clone();
    corrected["side"] = json!("SELL");
    corrected["conditionId"] = json!("incorrect-stamp");
    let corrected_id = aggregate(corrected.clone()).group_id.key().clone();
    let corrected_receipt = running.observe(corrected).await;
    held.respond.send(b"[]".to_vec()).unwrap();
    running.completed(wallet()).await;
    let mut sell = exact.clone();
    sell["side"] = json!("SELL");
    let exact_target = aggregate(exact.clone());
    let corrected_target = aggregate(sell.clone());
    running
        .requests
        .recv()
        .await
        .unwrap()
        .respond
        .send(serde_json::to_vec(&[exact, sell]).unwrap())
        .unwrap();
    assert_eq!(
        running.completed(wallet()).await,
        vec![exact_receipt, corrected_receipt]
    );
    let commits = running.finish().await;
    assert_eq!(commits.len(), 1);
    assert_eq!(commits[0].0.len(), 2);
    let path = dir.path().join("source.log");
    let frame = source_frames(&path)
        .into_iter()
        .find(|frame| {
            frame.source_id.0 == pe_service::bucket_commit::ACTIVITY_READ_COMMITMENT_SOURCE_ID
        })
        .unwrap();
    let value: pe_service::bucket_commit::ActivityReadCommitment =
        serde_json::from_slice(&frame.payload).unwrap();
    let bindings = value.bindings.unwrap();
    assert_eq!(bindings.len(), 2);
    let exact_binding = bindings
        .iter()
        .find(|binding| binding.stream_receipt == exact_receipt)
        .unwrap();
    assert_eq!(&exact_binding.stream_group_id, exact_target.group_id.key());
    assert_eq!(&exact_binding.history_group_id, exact_target.group_id.key());
    assert_eq!(
        exact_binding.semantic_revision,
        exact_target.semantic_revision.as_str()
    );
    assert!(exact_binding.identity_receipt.is_none());
    assert!(exact_binding.identity_provenance.is_none());
    let corrected_binding = bindings
        .iter()
        .find(|binding| binding.stream_receipt == corrected_receipt)
        .unwrap();
    assert_eq!(corrected_binding.stream_group_id, corrected_id);
    assert_eq!(
        &corrected_binding.history_group_id,
        corrected_target.group_id.key()
    );
    assert_eq!(
        corrected_binding.semantic_revision,
        corrected_target.semantic_revision.as_str()
    );
    assert!(corrected_binding.identity_receipt.is_some());
    assert!(corrected_binding.identity_provenance.is_some());
    assert_eq!(exact_binding.page_raw_hash, corrected_binding.page_raw_hash);
    assert_eq!(
        exact_binding.page_occurrence_index,
        corrected_binding.page_occurrence_index
    );
    assert!(
        pe_service::trade_poller::rebuild_reconciliation_obligations(&path, &paper)
            .unwrap()
            .is_empty()
    );
}

/// PASS: a crash after the fence witness but before the revision transaction keeps the fence
/// and discharges the ambiguous original on reopen; retry preserves the original economic effect.
#[tokio::test(start_paused = true)]
async fn restart_between_fence_witness_and_revision_keeps_ambiguity_discharged() {
    let dir = tempfile::tempdir().unwrap();
    let (mut running, _) = start_recorded_poller(&dir, &[wallet()]);
    let original = stream_row(wallet(), "witness-revision", EPOCH);
    let original_aggregate = aggregate(original.clone());
    running
        .requests
        .recv()
        .await
        .unwrap()
        .respond
        .send(serde_json::to_vec(std::slice::from_ref(&original)).unwrap())
        .unwrap();
    running.round_completed().await;
    let before = dir.path().join("before-revision.db");
    rusqlite::Connection::open(dir.path().join("paper.db"))
        .unwrap()
        .execute("VACUUM INTO ?1", [before.to_str().unwrap()])
        .unwrap();
    let mut stream = original.clone();
    stream["conditionId"] = json!("incorrect-stamp");
    running.observe(stream).await;
    let mut revision = original;
    revision["size"] = json!("2");
    let mut other_leg = revision.clone();
    other_leg["outcomeIndex"] = json!(1);
    running
        .requests
        .recv()
        .await
        .unwrap()
        .respond
        .send(serde_json::to_vec(&[revision, other_leg]).unwrap())
        .unwrap();
    running.completed(wallet()).await;
    let commits = running.finish().await;
    let (aggregates, context, _) = commits.last().unwrap();
    let paper = Arc::new(PaperStateDb::open(&before).unwrap());
    let original_state = paper
        .activity_group_state(original_aggregate.group_id.key())
        .unwrap()
        .unwrap();
    let conn = rusqlite::Connection::open(&before).unwrap();
    conn.execute_batch("CREATE TRIGGER fail_revision BEFORE INSERT ON activity_group_revisions BEGIN SELECT RAISE(FAIL, 'injected revision transaction failure'); END;").unwrap();
    let mut engine =
        BucketCommitEngine::load(paper.clone(), build_leader_ledger(&paper).unwrap()).unwrap();
    let error = engine
        .commit(aggregates.clone(), context, zero_basis())
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("injected revision transaction failure"),
        "{error}"
    );
    assert_eq!(
        paper.wallet_fences().unwrap().len(),
        1,
        "the first transaction committed"
    );
    let changed = aggregates
        .iter()
        .find(|item| item.group_id == original_aggregate.group_id)
        .unwrap();
    assert!(
        !paper
            .activity_revision_disposed(changed.group_id.key(), changed.semantic_revision.as_str())
            .unwrap()
    );
    drop(engine);
    drop(paper);
    let paper = Arc::new(PaperStateDb::open(&before).unwrap());
    let fence = paper.wallet_fences().unwrap();
    assert!(
        pe_service::trade_poller::rebuild_reconciliation_obligations(
            &dir.path().join("source.log"),
            &paper
        )
        .unwrap()
        .is_empty()
    );
    assert_eq!(
        paper
            .activity_group_state(original_aggregate.group_id.key())
            .unwrap()
            .unwrap(),
        original_state
    );
    conn.execute_batch("DROP TRIGGER fail_revision;").unwrap();
    let mut engine =
        BucketCommitEngine::load(paper.clone(), build_leader_ledger(&paper).unwrap()).unwrap();
    engine
        .commit(aggregates.clone(), context, zero_basis())
        .unwrap();
    assert_eq!(paper.wallet_fences().unwrap(), fence);
    assert!(
        paper
            .activity_revision_disposed(changed.group_id.key(), changed.semantic_revision.as_str())
            .unwrap()
    );
    assert_eq!(
        paper
            .activity_group_state(original_aggregate.group_id.key())
            .unwrap()
            .unwrap(),
        original_state
    );
}

/// PASS: boot keeps all authenticated candidate revisions for one original group/receipt;
/// a later undisposed revision cannot overwrite an earlier durably disposed candidate.
#[tokio::test(start_paused = true)]
async fn boot_binding_index_preserves_every_target_revision() {
    let dir = tempfile::tempdir().unwrap();
    let (mut running, paper) = start_recorded_poller(&dir, &[wallet()]);
    running
        .requests
        .recv()
        .await
        .unwrap()
        .respond
        .send(b"[]".to_vec())
        .unwrap();
    running.round_completed().await;
    let mut stream = stream_row(wallet(), "indexed-revisions", EPOCH);
    stream["conditionId"] = json!("incorrect-stamp");
    running.observe(stream).await;
    let mut history = stream_row(wallet(), "indexed-revisions", EPOCH);
    running
        .requests
        .recv()
        .await
        .unwrap()
        .respond
        .send(serde_json::to_vec(std::slice::from_ref(&history)).unwrap())
        .unwrap();
    running.completed(wallet()).await;
    running.finish().await;
    let path = dir.path().join("source.log");
    let frame = source_frames(&path)
        .into_iter()
        .find(|frame| {
            frame.source_id.0 == pe_service::bucket_commit::ACTIVITY_READ_COMMITMENT_SOURCE_ID
        })
        .unwrap();
    let first: pe_service::bucket_commit::ActivityReadCommitment =
        serde_json::from_slice(&frame.payload).unwrap();
    let mut binding = first.bindings.unwrap().remove(0);
    history["size"] = json!("2");
    let payload = serde_json::to_vec(&[history]).unwrap();
    let now = OffsetDateTime::from_unix_timestamp(EPOCH + 1).unwrap();
    let mut writer = pe_event_log::Writer::open(&path).unwrap();
    let receipt = writer
        .append_synced(pe_event_log::EnvelopeIn {
            source_id: SourceId(pe_service::trade_poller::ACTIVITY_POLL_SOURCE_ID.to_owned()),
            schema_version: pe_service::trade_poller::ACTIVITY_POLL_PAGE_SCHEMA_VERSION,
            parser_version: pe_source_polymarket_public::ACTIVITY_PARSER_VERSION,
            observed_at: SourceTimestamp(now),
            received_at: ReceivedAt(now),
            content_type: pe_event_log::ContentType::Json,
            payload: payload.clone(),
        })
        .unwrap();
    let read = support::producer_shaped_read_v2(wallet(), &payload, EPOCH + 1, EPOCH + 1, receipt);
    binding.semantic_revision = read.aggregates[0].semantic_revision.as_str().to_owned();
    binding.page_occurrence_index = 0;
    binding.page_raw_hash = read.page.raw_hash.clone();
    assert!(
        !paper
            .activity_revision_disposed(&binding.history_group_id, &binding.semantic_revision)
            .unwrap()
    );
    let inputs: Value = serde_json::from_str(&read.decision_inputs_json).unwrap();
    let pages: Vec<pe_source_polymarket_public::ReconciliationPageEvidence> =
        serde_json::from_value(inputs["pages"].clone()).unwrap();
    let payload = pe_service::bucket_commit::activity_read_commitment_payload_v2(
        wallet(),
        EPOCH + 1,
        &[read.page],
        &pages,
        &[binding],
    )
    .unwrap();
    writer
        .append_synced(pe_event_log::EnvelopeIn {
            source_id: SourceId(
                pe_service::bucket_commit::ACTIVITY_READ_COMMITMENT_SOURCE_ID.to_owned(),
            ),
            schema_version: pe_service::bucket_commit::ACTIVITY_READ_COMMITMENT_SCHEMA_VERSION,
            parser_version: pe_service::bucket_commit::ACTIVITY_READ_COMMITMENT_PARSER_VERSION,
            observed_at: SourceTimestamp(now),
            received_at: ReceivedAt(now),
            content_type: pe_event_log::ContentType::Json,
            payload,
        })
        .unwrap();
    drop(writer);
    assert!(
        pe_service::trade_poller::rebuild_reconciliation_obligations(&path, &paper)
            .unwrap()
            .is_empty()
    );
}

/// PASS: a qualifying pre-midnight observation blocks the oldest boundary until its durable
/// bucket acknowledgment, while the backstop round completes independently.
#[tokio::test(start_paused = true)]
async fn qualifying_obligation_holds_boundary_until_durable_acknowledgment() {
    let dir = tempfile::tempdir().unwrap();
    let cutoff = (EPOCH.div_euclid(86_400) + 1) * 86_400;
    let (mut running, paper) =
        start_recorded_poller_with_anchors(&dir, &[wallet()], false, false, Some(cutoff - 86_400));
    running
        .requests
        .recv()
        .await
        .unwrap()
        .respond
        .send(b"[]".to_vec())
        .unwrap();
    running.round_completed().await;
    running.now.store(cutoff - 1, Ordering::SeqCst);
    let row = stream_row(wallet(), "qualifying-boundary", cutoff - 1);
    let receipt = running.observe(row.clone()).await;
    let request = running.requests.recv().await.unwrap();
    running.now.store(cutoff, Ordering::SeqCst);
    tokio::time::advance(std::time::Duration::from_secs(30)).await;
    running.round_completed().await;
    assert!(running.controls.try_recv().is_err());
    let obligations = pe_service::trade_poller::rebuild_reconciliation_obligations(
        &dir.path().join("source.log"),
        &paper,
    )
    .unwrap();
    assert_eq!(obligations.len(), 1);
    let boundary = source_frames(&dir.path().join("source.log"))
        .into_iter()
        .find(|frame| frame.source_id.0 == pe_service::trade_poller::DAILY_BOUNDARY_SOURCE_ID)
        .unwrap();
    assert!(boundary.seq > receipt.sequence);
    assert!(
        obligations.migration_evidence()[0]["received_at_unix"]
            .as_i64()
            .unwrap()
            < cutoff
    );
    let gate = running.bucket_ack_gate.clone();
    let held = gate.acquire().await.unwrap();
    request
        .respond
        .send(serde_json::to_vec(&[row]).unwrap())
        .unwrap();
    assert!(matches!(
        running.controls.recv().await.unwrap(),
        ControlCompletion::BucketCommitted
    ));
    assert!(running.controls.try_recv().is_err());
    drop(held);
    assert_eq!(running.completed(wallet()).await, vec![receipt]);
    assert!(
        matches!(running.controls.recv().await.unwrap(), ControlCompletion::Boundary(value) if value == cutoff)
    );
    assert!(paper.wallet_fences().unwrap().is_empty());
    running.finish().await;
}
