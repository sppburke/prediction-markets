#![forbid(unsafe_code)]

use std::env;
use std::path::PathBuf;
use std::str::FromStr as _;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use axum::{Router, routing::get};
use pe_config::ServiceConfig;
use pe_core_types::SourceId;
use pe_event_log::Writer;
use pe_execution_core::{ExecutionDispatcher, LiveExecutor};
use pe_funding_graph::FundingGraphAccumulator;
use pe_operator_graph::ClusteringConfig;
use pe_source_onchain_polygon::{LivePolygonConnector, PolygonConnectorConfig};
use pe_source_polymarket_public::ReqwestFetcher;
use pe_strategy_winner_follow::{
    ExecutionMode, PaperExecutor, WinnerFollowConfig, WinnerFollowStrategy,
};
use pe_trader_index::fetcher::{WatchlistFetchConfig, WatchlistFetcher};
use pe_venue_polymarket::{PolymarketCredentials, PolymarketVenueAdapter, ReqwestCLOBClient};
use rust_decimal::Decimal;
use tokio::sync::mpsc;
use tracing::info;

use pe_service::health::{SharedHealth, new_shared_health};
use pe_service::operator_graph_scheduler::OperatorGraphScheduler;
use pe_service::orchestrator::{Orchestrator, OrchestratorConfig};
use pe_service::seed;
use pe_service::trade_poller::{TradePoller, TradePollerConfig};

#[tokio::main]
async fn main() -> Result<()> {
    let cfg = load_config()?;

    pe_service::logging::setup(&cfg.jsonl_log_path, "info")?;
    info!("pe-service starting");

    let bankroll = Decimal::from_str(&cfg.bankroll_usd)
        .with_context(|| format!("parse bankroll_usd '{}'", cfg.bankroll_usd))?;
    let mode = parse_mode(&cfg.mode)?;

    // Bootstrap watchlist from live Polymarket leaderboard.
    let fetch_config = WatchlistFetchConfig {
        base_url: cfg.polymarket_base_url.clone(),
        watchlist_size: cfg.watchlist_size,
    };
    let mut watchlist_fetcher =
        WatchlistFetcher::new(fetch_config, ReqwestFetcher::new(reqwest::Client::new()));
    let watchlist = watchlist_fetcher
        .fetch_watchlist()
        .await
        .context("bootstrap watchlist")?;
    info!(active = watchlist.active_count, "watchlist bootstrapped");

    // Paper event-log writer.
    let paper_writer = Writer::open(&cfg.event_log_path)
        .with_context(|| format!("open event log {}", cfg.event_log_path.display()))?;
    let paper_executor = PaperExecutor::new(paper_writer, SourceId("pe-service.paper".into()));

    // Live event-log writer (separate file so paper and live fills are distinct streams).
    let live_log_path = cfg.event_log_path.with_extension("live.log");
    let live_writer = Writer::open(&live_log_path)
        .with_context(|| format!("open live event log {}", live_log_path.display()))?;

    // Polymarket CLOB adapter.
    let clob_creds = PolymarketCredentials::mainnet(
        cfg.polymarket_funder_address.clone(),
        cfg.polymarket_private_key.clone(),
        cfg.polymarket_clob_api_key.clone(),
        cfg.polymarket_clob_api_secret.clone(),
        cfg.polymarket_clob_api_passphrase.clone(),
    );
    // Log funder address but never the key material.
    info!(funder = %cfg.polymarket_funder_address, "polymarket clob credentials loaded");

    let clob_client = ReqwestCLOBClient::new(reqwest::Client::new());
    let adapter = PolymarketVenueAdapter::new(clob_client, clob_creds)
        .with_base_url(&cfg.polymarket_clob_base_url);
    let live_executor = LiveExecutor::new(adapter, live_writer, SourceId("pe-service.live".into()));
    let dispatcher = ExecutionDispatcher::new(paper_executor, live_executor);

    let health = new_shared_health();

    // Bounded channels per _GLOSSARY.md defaults.
    let (polygon_tx, polygon_rx) = mpsc::channel(cfg.polygon_channel_capacity);
    let (trade_tx, trade_rx) = mpsc::channel(cfg.polymarket_channel_capacity);

    // Shared accumulator: orchestrator ingests polygon events; scheduler reads snapshots.
    let accumulator = Arc::new(Mutex::new(FundingGraphAccumulator::new()));

    // Merge leaderboard with optional bootstrap seed. The merged Watchlist
    // carries full WatchlistEntry metadata for every wallet so the Orchestrator
    // can look up tier and scores for seed-only wallets.
    let seed_wl = seed::load_seed_watchlist(&cfg.seed_watchlist_path)?;
    let watchlist = seed::merge_watchlist(&watchlist, seed_wl.as_ref());
    if seed_wl.is_some() {
        info!(
            total = watchlist.entries.len(),
            active = watchlist.active_count,
            incubator = watchlist.incubator_count,
            "seed watchlist merged with leaderboard"
        );
        if watchlist.entries.len() > cfg.watchlist_size {
            tracing::warn!(
                merged = watchlist.entries.len(),
                watchlist_size = cfg.watchlist_size,
                "merged wallet count exceeds watchlist_size; trade poller rate-limit budget was sized for watchlist_size"
            );
        }
    }
    // Wallets feed both the Polygon WS topic[2] filter (via funder discovery)
    // and the Polymarket trade poller.
    let wallets: Vec<_> = watchlist.entries.iter().map(|e| e.wallet).collect();
    // `funding_max_hops` is sourced from ServiceConfig so the value flows
    // through the config-hash; other ClusteringConfig fields stay at default
    // until they're surfaced in their own follow-up.
    let clustering_config = ClusteringConfig {
        funding_max_hops: cfg.funding_max_hops,
        ..ClusteringConfig::default()
    };

    // Operator-graph scheduler — rebuilds clusters every 60s and publishes via watch.
    let (scheduler, operator_identities_rx) = OperatorGraphScheduler::new(
        accumulator.clone(),
        clustering_config.clone(),
        cfg.operator_graph_rebuild_cadence_secs,
    );
    let scheduler_task = tokio::spawn(scheduler.run());

    // Polygon source task.
    let polygon_task = spawn_polygon_task(
        polygon_config_from(&cfg, wallets.clone(), clustering_config.funding_max_hops),
        polygon_tx,
        health.clone(),
    );

    // Polymarket trade poller task.
    let trade_task = tokio::spawn(
        TradePoller::new(
            TradePollerConfig {
                base_url: cfg.polymarket_base_url.clone(),
                poll_interval_secs: cfg.trade_poll_interval_secs,
            },
            wallets,
            ReqwestFetcher::new(reqwest::Client::new()),
            trade_tx,
        )
        .run(),
    );

    // Orchestrator.
    let orch = Orchestrator::new(
        polygon_rx,
        trade_rx,
        accumulator,
        operator_identities_rx,
        watchlist,
        OrchestratorConfig {
            bankroll,
            mode,
            signal_config: Default::default(),
            cluster_observation_window_secs: 300,
        },
        WinnerFollowStrategy::new(WinnerFollowConfig::default()),
        dispatcher,
        health.clone(),
    )
    .context("build orchestrator")?;

    // HTTP health server.
    let app = Router::new()
        .route("/health/live", get(pe_service::health::live))
        .route("/health/ready", get(pe_service::health::ready))
        .with_state(health);
    let listener = tokio::net::TcpListener::bind(&cfg.bind)
        .await
        .with_context(|| format!("bind {}", cfg.bind))?;
    info!(bind = %cfg.bind, "pe-service listening");
    let http_task = tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            tracing::error!(error = %e, "http server error");
        }
    });

    // Run orchestrator until ctrl_c.
    orch.run(async {
        tokio::signal::ctrl_c().await.ok();
        info!("shutdown signal received");
    })
    .await;

    info!("orchestrator stopped; cleaning up");
    polygon_task.abort();
    trade_task.abort();
    http_task.abort();
    scheduler_task.abort();
    info!("pe-service stopped");
    Ok(())
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn load_config() -> Result<ServiceConfig> {
    match env::args().nth(1).map(PathBuf::from) {
        Some(path) => pe_config::load(&path)
            .with_context(|| format!("loading config from {}", path.display())),
        None => Ok(ServiceConfig::default()),
    }
}

fn parse_mode(s: &str) -> Result<ExecutionMode> {
    match s.to_lowercase().replace('-', "_").as_str() {
        "shadow" => Ok(ExecutionMode::Shadow),
        "paper" => Ok(ExecutionMode::Paper),
        "live_tiny" | "livetiny" => Ok(ExecutionMode::LiveTiny),
        "promoted" => Ok(ExecutionMode::Promoted),
        other => Err(anyhow::anyhow!(
            "unknown mode '{}'; expected shadow|paper|live_tiny|promoted",
            other
        )),
    }
}

fn polygon_config_from(
    cfg: &ServiceConfig,
    seed_wallets: Vec<pe_core_types::WalletAddress>,
    funding_max_hops: u8,
) -> PolygonConnectorConfig {
    PolygonConnectorConfig {
        http_url: cfg.polygon_http_url.clone(),
        ws_url: cfg.polygon_ws_url.clone(),
        backfill_blocks: cfg.backfill_blocks,
        backfill_page_size: cfg.polygon_backfill_page_size,
        checkpoint_path: cfg.polygon_checkpoint_path.clone(),
        channel_capacity: cfg.polygon_channel_capacity,
        seed_wallets,
        funding_max_hops,
        funder_source: cfg.funder_source.clone(),
        etherscan_api_key: cfg.etherscan_api_key.clone(),
    }
}

fn spawn_polygon_task(
    config: PolygonConnectorConfig,
    tx: mpsc::Sender<pe_source_core::SourceEvent>,
    health: SharedHealth,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        match LivePolygonConnector::connect(SourceId("polygon".into()), config).await {
            Err(e) => {
                tracing::error!(error = %e, "polygon connector failed to connect");
                let mut h = health
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                h.polygon_status = pe_source_core::SourceStatus::Dead;
            }
            Ok(mut connector) => {
                use pe_source_core::SourceConnector as _;
                loop {
                    match connector.next_event().await {
                        Err(e) => {
                            {
                                let mut h = health
                                    .lock()
                                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                                h.polygon_status = connector.health().status;
                            }
                            tracing::warn!(error = %e, "polygon source error");
                            if matches!(e, pe_source_core::SourceError::Fatal { .. }) {
                                break;
                            }
                        }
                        Ok(event) => {
                            {
                                let mut h = health
                                    .lock()
                                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                                h.polygon_status = connector.health().status;
                            }
                            if tx.send(event).await.is_err() {
                                break; // orchestrator dropped its receiver
                            }
                        }
                    }
                }
            }
        }
    })
}
