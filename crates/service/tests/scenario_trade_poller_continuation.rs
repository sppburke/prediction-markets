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
    .run_until(async {})
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
    .run_until(async {})
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
    let commitment_frame = &frames[pages.len()];
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
