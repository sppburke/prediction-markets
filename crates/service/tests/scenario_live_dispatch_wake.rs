//! AC9: committed paper outcomes wake the real sequential fanout before its next tick.
//! Orchestrator unit tests separately prove notifications follow successful owner commits.
#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;

use pe_copy_signal_engine::LeaderSignal;
use pe_core_types::{
    CollateralAmount, EventSeq, LeaderAction, MarketId, OutcomeId, Price, ProbabilityPpm,
    ReconstructionQuality, ShareAmount, SourceTimestamp, SourceTradeId, TraderId, VenueId,
    VenueMarketId, WalletAddress,
};
use pe_event_log::AppendReceipt;
use pe_execution_core::{LiveJournal, ObservationEvidence};
use pe_paper_state::{
    DispatchFlip, DispatchSeedRecord, DispatchTargetSeed, FillRecord, LeaderPositionRow,
    NoCopyDisposition, PaperStateDb,
};
use pe_service::activity_ingest::SourceLogHandle;
use pe_service::clob_book::ReqwestClobBookFetcher;
use pe_service::config::ServiceConfig;
use pe_service::live_accounts::{LiveAccounts, LiveAccountsSnapshot};
use pe_service::live_fanout::{LiveFanoutConfig, run_live_fanout_until};
use pe_service::live_projections::LiveProjectionWriter;
use pe_service::live_watchlist::LiveWatchlist;
use pe_service::mid_price_cache::MidPriceCache;
use pe_service::paper_recovery::PaperLog;
use pe_service::risk_inputs::SourceReceiptIndex;
use pe_service::runtime_config::{LiveRuntimeConfig, RuntimeConfig};
use pe_service::source_event_sink::SourceEventSink;
use pe_service::supervisor::ShutdownController;
use pe_trader_index::Watchlist;
use rust_decimal_macros::dec;
use time::OffsetDateTime;
use tokio::sync::{Notify, mpsc, oneshot};

#[tokio::test(start_paused = true)]
async fn scenario_ac9_fill_no_copy_and_no_fill_are_processed_before_the_tick() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
    db.set_bankroll(dec!(100)).unwrap();
    let journal_path = dir.path().join("live.log");
    let journal = Arc::new(LiveJournal::open(&journal_path).unwrap());
    let paper_log = PaperLog::open(dir.path().join("paper.log")).unwrap();
    let source_path = dir.path().join("source.log");
    let _sink = SourceEventSink::open(&source_path).unwrap();
    let source_receipts = SourceReceiptIndex::replay(&source_path).unwrap();
    let (source_log, _source_rx) = SourceLogHandle::channel(1);
    let (control, _control_rx) = mpsc::channel(1);
    let ready = Arc::new(Notify::new());
    let http = reqwest::Client::new();
    let now = OffsetDateTime::now_utc();
    let accounts = LiveAccounts::new(LiveAccountsSnapshot {
        fetched_at_unix: Some(now.unix_timestamp()),
        control_available: true,
        credential_metadata_available: true,
        ..LiveAccountsSnapshot::default()
    });
    let (stop, shutdown) = oneshot::channel();
    let owner = run_live_fanout_until(
        LiveFanoutConfig {
            paper_state: db.clone(),
            dispatch_ready: ready.clone(),
            live_accounts: accounts,
            live_watchlist: LiveWatchlist::new(Watchlist {
                entries: Vec::new(),
                snapshot_at: SourceTimestamp(now),
                active_count: 0,
                incubator_count: 0,
            }),
            runtime_config: LiveRuntimeConfig::new(RuntimeConfig::from_service_config(
                &ServiceConfig::default(),
            )),
            identity: None,
            journal,
            journal_path,
            era_live_prefix: None,
            projection: LiveProjectionWriter::new(http.clone(), "", "", ""),
            book_fetcher: Arc::new(ReqwestClobBookFetcher::new(http.clone())),
            mid_price_cache: MidPriceCache::new(String::new()),
            source_log,
            source_receipts,
            paper_log,
            orchestrator_control: control.downgrade(),
            http,
            polygon_receipt_rpc_url: String::new(),
            supabase_url: String::new(),
            supabase_anon_key: String::new(),
            supabase_secret_key: String::new(),
            gamma_base_url: String::new(),
            clob_base_url: String::new(),
            data_base_url: String::new(),
            projection_reconcile_interval_secs: 3600,
            shutdown: ShutdownController::new().0,
        },
        async {
            let _ = shutdown.await;
        },
    );
    tokio::pin!(owner);
    // Consume the immediate first tick before making any seed ready.
    assert!(futures::poll!(&mut owner).is_pending());
    let start = tokio::time::Instant::now();
    for (ordinal, outcome) in ["fill", "no_copy", "no_fill"].into_iter().enumerate() {
        let id = SourceTradeId(format!("g2:{}", ordinal.to_string().repeat(64)));
        let market = MarketId(VenueMarketId(format!("market-{ordinal}")));
        let signal = LeaderSignal {
            leader: TraderId(WalletAddress([0xaa; 20])),
            venue: VenueId::polymarket(),
            market_id: market.clone(),
            outcome_id: OutcomeId(0),
            action: LeaderAction::Entry,
            leader_side: pe_core_types::Side::Buy,
            leader_price: Price::new(dec!(0.5)).unwrap(),
            leader_size: ShareAmount::from_whole(1).unwrap(),
            observed_at: now,
            received_at: now,
            reconstruction_quality: ReconstructionQuality::new(100).unwrap(),
            source_trade_id: id.clone(),
            action_confidence_ppm: ProbabilityPpm(1_000_000),
        };
        let receipt = AppendReceipt {
            sequence: EventSeq(1),
            this_hash: blake3::hash(b"receipt"),
        };
        db.stage_dispatch_seed(&DispatchSeedRecord {
            dispatch_id: outcome.to_owned(),
            signal_json: serde_json::to_string(&serde_json::json!({
                "schema_version": 1,
                "signal": signal,
                "observation": ObservationEvidence {
                    source_receipt: receipt,
                    complete_bound_receipt: receipt,
                    observed_unix_ms: 0,
                    provenance: "activity_ws".to_owned(),
                },
            }))
            .unwrap(),
            source_trade_id: id.0.clone(),
            created_at_unix: now.unix_timestamp(),
            targets: vec![DispatchTargetSeed {
                account_id: "disabled".to_owned(),
                credential_bundle_version: 1,
                credential_key_id: "key".to_owned(),
            }],
        })
        .unwrap();
        let leader = LeaderPositionRow {
            wallet: signal.leader.0,
            market_id: market.clone(),
            outcome_id: OutcomeId(0),
            long_contracts: ShareAmount::ZERO,
            short_contracts: ShareAmount::ZERO,
        };
        let paper_outcome = if outcome == "fill" {
            "fill"
        } else {
            "no_fill:declined"
        };
        let flip = Some(DispatchFlip {
            dispatch_id: outcome,
            paper_outcome,
        });
        match outcome {
            "fill" => {
                db.commit_fill_with_flip(
                    &id,
                    &leader,
                    &FillRecord {
                        idempotency_key: format!("fill-{ordinal}"),
                        market_id: market,
                        outcome_id: OutcomeId(0),
                        side: pe_core_types::Side::Buy,
                        quantity: ShareAmount::from_whole(1).unwrap(),
                        fill_price: Price::new(dec!(0.5)).unwrap(),
                        principal: CollateralAmount::from_decimal_exact(dec!(0.5)).unwrap(),
                        fee: CollateralAmount::ZERO,
                    },
                    EventSeq(1),
                    flip,
                )
                .unwrap();
            }
            "no_copy" => db
                .commit_seen_no_copy_with_flip_pending(
                    &id,
                    &leader,
                    &NoCopyDisposition {
                        provenance: "rest_poll".to_owned(),
                        age_secs: 121,
                        reason: "declined".to_owned(),
                        recorded_at_unix: now.unix_timestamp(),
                    },
                    flip,
                    None,
                )
                .unwrap(),
            _ => db
                .commit_seen_no_fill_with_flip(&id, &leader, flip)
                .unwrap(),
        }
        assert!(futures::poll!(&mut owner).is_pending());
        assert_eq!(db.dispatch_seed(outcome).unwrap().unwrap().state, "ready");
        ready.notify_one();
        assert!(futures::poll!(&mut owner).is_pending());
        let seed = db.dispatch_seed(outcome).unwrap().unwrap();
        assert!(seed.finalized_at_unix.is_some(), "{outcome}");
        let target = db.dispatch_targets(outcome).unwrap().remove(0);
        assert_eq!(target.state, "terminal");
        assert_eq!(target.terminal_reason.as_deref(), Some("not_armed"));
        assert_eq!(tokio::time::Instant::now(), start);
    }
    stop.send(()).unwrap();
    owner.await.unwrap();
}
