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
use pe_funding_graph::FundingGraphAccumulator;
use pe_operator_graph::ClusteringConfig;
use pe_source_onchain_polygon::{LivePolygonConnector, PolygonConnectorConfig};
use pe_source_polymarket_public::ReqwestFetcher;
use pe_strategy_winner_follow::{
    ExecutionMode, PaperExecutor, WinnerFollowConfig, WinnerFollowStrategy,
};
use pe_trader_index::fetcher::{WatchlistFetchConfig, WatchlistFetcher};
use rust_decimal::Decimal;
use tokio::sync::mpsc;
use tracing::info;

use pe_service::health::{SharedHealth, new_shared_health};
use pe_service::operator_graph_scheduler::OperatorGraphScheduler;
use pe_service::orchestrator::{Orchestrator, OrchestratorConfig};
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

    // Event-log writer for PaperExecutor.
    let writer = Writer::open(&cfg.event_log_path)
        .with_context(|| format!("open event log {}", cfg.event_log_path.display()))?;
    let paper_executor = PaperExecutor::new(writer, SourceId("pe-service.paper".into()));

    let health = new_shared_health();

    // Bounded channels per _GLOSSARY.md defaults.
    let (polygon_tx, polygon_rx) = mpsc::channel(cfg.polygon_channel_capacity);
    let (trade_tx, trade_rx) = mpsc::channel(cfg.polymarket_channel_capacity);

    // Shared accumulator: orchestrator ingests polygon events; scheduler reads snapshots.
    let accumulator = Arc::new(Mutex::new(FundingGraphAccumulator::new()));

    // Operator-graph scheduler — rebuilds clusters every 60s and publishes via watch.
    let (scheduler, operator_identities_rx) = OperatorGraphScheduler::new(
        accumulator.clone(),
        ClusteringConfig::default(),
        cfg.operator_graph_rebuild_cadence_secs,
    );
    let scheduler_task = tokio::spawn(scheduler.run());

    // Polygon source task.
    let polygon_task = spawn_polygon_task(polygon_config_from(&cfg), polygon_tx, health.clone());

    // Polymarket trade poller task.
    let wallets: Vec<_> = watchlist.entries.iter().map(|e| e.wallet).collect();
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
        paper_executor,
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

fn polygon_config_from(cfg: &ServiceConfig) -> PolygonConnectorConfig {
    PolygonConnectorConfig {
        http_url: cfg.polygon_http_url.clone(),
        ws_url: cfg.polygon_ws_url.clone(),
        backfill_blocks: cfg.backfill_blocks,
        checkpoint_path: cfg.polygon_checkpoint_path.clone(),
        channel_capacity: cfg.polygon_channel_capacity,
    }
}

fn spawn_polygon_task(
    config: PolygonConnectorConfig,
    tx: mpsc::Sender<pe_source_core::SourceEvent>,
    health: SharedHealth,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        match LivePolygonConnector::connect(SourceId("polygon".into()), config).await {
            Err(e) => tracing::error!(error = %e, "polygon connector failed to connect"),
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
