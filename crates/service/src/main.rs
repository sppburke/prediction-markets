#![forbid(unsafe_code)]

use std::env;
use std::path::PathBuf;
use std::str::FromStr as _;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use axum::{Router, routing::get};
use pe_core_types::SourceId;
use pe_event_log::Writer;
use pe_execution_core::{ExecutionDispatcher, LiveExecutor};
use pe_funding_graph::FundingGraphAccumulator;
use pe_operator_graph::ClusteringConfig;
use pe_paper_state::PaperStateDb;
use pe_service::config::{self as service_config, ServiceConfig};
use pe_service::paper_recovery::{build_leader_ledger, reconcile_paper_state};
use pe_source_onchain_polygon::{LivePolygonConnector, PolygonConnectorConfig};
use pe_source_polymarket_public::ReqwestFetcher;
use pe_strategy_winner_follow::{ExecutionMode, PaperExecutor, WinnerFollowStrategy};
use pe_trader_index::fetcher::{WatchlistFetchConfig, WatchlistFetcher};
use pe_venue_polymarket::{PolymarketCredentials, PolymarketVenueAdapter, ReqwestCLOBClient};
use rust_decimal::Decimal;
use tokio::sync::mpsc;
use tracing::info;

use pe_core_types::Price;
use pe_paper_pnl::{GammaResolutionFetcher, PnlLedger, ResolutionStore};
use pe_service::entry_gate::CopyEntryGateConfig;
use pe_service::health::{SharedHealth, new_shared_health};
use pe_service::market_end_cache::MarketEndCache;
use pe_service::operator_graph_scheduler::OperatorGraphScheduler;
use pe_service::orchestrator::{Orchestrator, OrchestratorConfig};
use pe_service::paper_api::PaperApiState;
use pe_service::position_seeder::{run_reseed_loop, seed_all};
use pe_service::seed;
use pe_service::trade_poller::{TradePoller, TradePollerConfig};
use pe_service::wallet_history::WalletHistoryLoader;
use time::OffsetDateTime;

#[tokio::main]
async fn main() -> Result<()> {
    // --report: load config, compute P&L snapshot, print, exit.
    if env::args().any(|a| a == "--report") {
        return run_report();
    }
    // --rebuild-state: wipe paper-state DB (after backup) and replay event log, exit.
    if env::args().any(|a| a == "--rebuild-state") {
        return run_rebuild_state();
    }

    let cfg = load_config()?;

    pe_service::logging::setup(&cfg.jsonl_log_path, "info")?;
    info!("pe-service starting");

    let configured_bankroll = Decimal::from_str(&cfg.bankroll_usd)
        .with_context(|| format!("parse bankroll_usd '{}'", cfg.bankroll_usd))?;
    let mode = parse_mode(&cfg.mode)?;

    // Fail fast: a kelly_fraction_override above the mode default requires the approval flag.
    if let Some(kf) = &cfg.strategy.kelly_fraction_override {
        let mode_default = match mode {
            ExecutionMode::Shadow | ExecutionMode::Paper | ExecutionMode::LiveTiny => {
                Decimal::new(25, 2)
            }
            ExecutionMode::Promoted => Decimal::new(50, 2),
        };
        anyhow::ensure!(
            kf.0 <= mode_default || cfg.strategy.kelly_fraction_above_default_human_approved,
            "kelly_fraction_override ({}) exceeds mode '{}' default ({}); \
             set kelly_fraction_above_default_human_approved = true to allow this",
            kf.0,
            cfg.mode,
            mode_default
        );
    }

    // Parse the copy-entry price band eagerly so a malformed/inverted band fails
    // fast before any I/O (mirrors the parse_mode / kelly validation pattern).
    let band_lo = Price::new(
        Decimal::from_str(&cfg.entry_gate_price_band_lo).with_context(|| {
            format!(
                "parse entry_gate_price_band_lo '{}'",
                cfg.entry_gate_price_band_lo
            )
        })?,
    )
    .with_context(|| {
        format!(
            "entry_gate_price_band_lo '{}' out of [0,1]",
            cfg.entry_gate_price_band_lo
        )
    })?;
    let band_hi = Price::new(
        Decimal::from_str(&cfg.entry_gate_price_band_hi).with_context(|| {
            format!(
                "parse entry_gate_price_band_hi '{}'",
                cfg.entry_gate_price_band_hi
            )
        })?,
    )
    .with_context(|| {
        format!(
            "entry_gate_price_band_hi '{}' out of [0,1]",
            cfg.entry_gate_price_band_hi
        )
    })?;
    anyhow::ensure!(
        band_lo < band_hi,
        "entry_gate_price_band_lo ({}) must be < entry_gate_price_band_hi ({})",
        band_lo.0,
        band_hi.0
    );
    let entry_gate_config = CopyEntryGateConfig {
        price_band_lo: band_lo,
        price_band_hi: band_hi,
        fail_closed: cfg.entry_gate_fail_closed,
    };

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

    // Crash-safe paper-state mirror. Open, initialise bankroll (idempotent), then
    // reconcile any event-log fills whose SQLite commit was lost to a crash, and
    // rehydrate the leader position ledger — all before the orchestrator runs.
    let paper_state = Arc::new(
        PaperStateDb::open(&cfg.paper_state_db_path)
            .with_context(|| format!("open paper-state {}", cfg.paper_state_db_path.display()))?,
    );
    paper_state
        .init_bankroll(configured_bankroll)
        .context("initialise paper-state bankroll")?;
    let reconciled = reconcile_paper_state(&cfg.event_log_path, &paper_state)
        .context("reconcile paper-state from event log")?;
    if reconciled > 0 {
        info!(
            reconciled,
            "replayed uncommitted fills from event log on startup"
        );
    }
    let bankroll = paper_state
        .bankroll()
        .context("read paper-state bankroll")?
        .unwrap_or(configured_bankroll);
    let mut leader_ledger =
        build_leader_ledger(&paper_state).context("rehydrate leader position ledger")?;
    info!(bankroll = %bankroll, "paper-state opened");

    // Paper event-log writer (opened after reconciliation reads the existing log).
    let paper_writer = Writer::open(&cfg.event_log_path)
        .with_context(|| format!("open event log {}", cfg.event_log_path.display()))?;
    let paper_executor = PaperExecutor::new(
        paper_writer,
        SourceId("pe-service.paper".into()),
        cfg.paper_fill_haircut_bps,
        cfg.paper_fill_slippage_bps,
    );

    // Live event-log writer (separate file so paper and live fills are distinct streams).
    let live_log_path = cfg.event_log_path.with_extension("live.log");
    let live_writer = Writer::open(&live_log_path)
        .with_context(|| format!("open live event log {}", live_log_path.display()))?;

    // Fail fast: live modes require all five CLOB credential fields to be non-empty.
    // Mirrors the parse_mode pattern of validating config eagerly before any I/O.
    if matches!(mode, ExecutionMode::LiveTiny | ExecutionMode::Promoted) {
        anyhow::ensure!(
            !cfg.polymarket_funder_address.is_empty(),
            "PE_POLYMARKET_FUNDER_ADDRESS is required for mode '{}'",
            cfg.mode
        );
        anyhow::ensure!(
            !cfg.polymarket_private_key.is_empty(),
            "PE_POLYMARKET_PRIVATE_KEY is required for mode '{}'",
            cfg.mode
        );
        anyhow::ensure!(
            !cfg.polymarket_clob_api_key.is_empty(),
            "PE_POLYMARKET_CLOB_API_KEY is required for mode '{}'",
            cfg.mode
        );
        anyhow::ensure!(
            !cfg.polymarket_clob_api_secret.is_empty(),
            "PE_POLYMARKET_CLOB_API_SECRET is required for mode '{}'",
            cfg.mode
        );
        anyhow::ensure!(
            !cfg.polymarket_clob_api_passphrase.is_empty(),
            "PE_POLYMARKET_CLOB_API_PASSPHRASE is required for mode '{}'",
            cfg.mode
        );
    }

    // Polymarket CLOB adapter.
    let clob_creds = PolymarketCredentials::mainnet(
        cfg.polymarket_funder_address.clone(),
        cfg.polymarket_private_key.clone(),
        cfg.polymarket_clob_api_key.clone(),
        cfg.polymarket_clob_api_secret.clone(),
        cfg.polymarket_clob_api_passphrase.clone(),
    );
    // Log funder address only when credentials are present (live mode only).
    if !cfg.polymarket_funder_address.is_empty() {
        info!(funder = %cfg.polymarket_funder_address, "polymarket clob credentials loaded");
    }

    let clob_client = ReqwestCLOBClient::new(reqwest::Client::new());
    let adapter = PolymarketVenueAdapter::new(clob_client, clob_creds)
        .with_base_url(&cfg.polymarket_clob_base_url);
    let live_executor = LiveExecutor::new(adapter, live_writer, SourceId("pe-service.live".into()));
    let dispatcher = ExecutionDispatcher::new(paper_executor, live_executor);

    // Polygon liveness is only meaningful when the WS source is configured;
    // an empty ws_url means "etherscan-only mode" (no live on-chain feed).
    let health = new_shared_health(!cfg.polygon_ws_url.is_empty());

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
    // Wallets feed both the Polygon WS topic[2] filter (via funder discovery),
    // the Polymarket trade poller, and the position seeder.
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

    // Startup seed: fetch current positions from the API for each watchlisted wallet,
    // overlay onto the leader ledger, then advance each wallet's poll cursor to now so
    // the trade poller skips the downtime backlog. Must run before wallets is moved into
    // TradePoller::new so the borrow of wallets is valid, and before the cursor is
    // advanced after the poller has already started consuming it.
    {
        let seed_fetcher = ReqwestFetcher::new(reqwest::Client::new());
        let snapshot_map = seed_all(
            &wallets,
            &cfg.polymarket_base_url,
            cfg.position_page_limit,
            cfg.position_size_threshold,
            &seed_fetcher,
        )
        .await;
        let seeded = snapshot_map.len();
        leader_ledger.overlay(snapshot_map.clone());
        let now_unix = OffsetDateTime::now_utc().unix_timestamp();
        for wallet in snapshot_map.keys() {
            if let Err(e) = paper_state.set_cursor(wallet, now_unix) {
                tracing::warn!(wallet = %wallet, error = %e, "failed to advance startup cursor");
            }
        }
        info!(
            seeded,
            total = wallets.len(),
            "startup position seed complete"
        );
    }

    // Startup wallet-history backfill: each watchlisted wallet's complete set of
    // previously-entered markets, so the copy-entry gate admits only first-ever
    // entries. Borrows `wallets` (must run before it is moved into TradePoller).
    let history_map = {
        let history_fetcher = ReqwestFetcher::new(reqwest::Client::new());
        let map = WalletHistoryLoader::load(
            &wallets,
            &cfg.polymarket_base_url,
            &cfg.wallet_market_history_path,
            &history_fetcher,
        )
        .await;
        info!(
            wallets_with_history = map.len(),
            total = wallets.len(),
            "wallet market-history backfill complete"
        );
        map
    };

    // Reseed channel: carries periodic snapshots into the orchestrator (cap 1 = back-pressure).
    let (reseed_tx, reseed_rx) = mpsc::channel(1);
    let reseed_task = if cfg.position_reseed_interval_secs > 0 {
        let reseed_wallets = wallets.clone();
        let reseed_base_url = cfg.polymarket_base_url.clone();
        let reseed_fetcher = ReqwestFetcher::new(reqwest::Client::new());
        Some(tokio::spawn(run_reseed_loop(
            reseed_wallets,
            reseed_base_url,
            cfg.position_page_limit,
            cfg.position_size_threshold,
            cfg.position_reseed_interval_secs,
            reseed_fetcher,
            reseed_tx,
        )))
    } else {
        drop(reseed_tx);
        None
    };

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
            paper_state.clone(),
            health.clone(),
        )
        .run(),
    );

    // Orchestrator.
    let market_end_cache = MarketEndCache::new(cfg.gamma_base_url.clone());
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
            max_resolution_horizon_secs: cfg.max_resolution_horizon_secs,
            entry_gate_config,
        },
        history_map,
        WinnerFollowStrategy::new(cfg.strategy.clone()),
        dispatcher,
        paper_state.clone(),
        leader_ledger,
        health.clone(),
        market_end_cache.clone(),
        reseed_rx,
    )
    .context("build orchestrator")?;

    // Resolution polling task: periodically fetch Gamma for closed markets.
    let resolution_task = spawn_resolution_task(
        paper_state.clone(),
        cfg.gamma_base_url.clone(),
        cfg.gamma_resolution_poll_interval_secs,
        cfg.paper_resolutions_path.clone(),
    );

    // Shared state for paper API handlers.
    let paper_api_state = Arc::new(PaperApiState {
        paper_state: paper_state.clone(),
        resolutions_path: cfg.paper_resolutions_path.clone(),
        initial_bankroll: configured_bankroll,
        market_end_cache,
    });

    // HTTP server: health + paper API.
    let app = Router::new()
        .route("/health/live", get(pe_service::health::live))
        .route("/health/ready", get(pe_service::health::ready))
        .route("/paper/pnl", get(pe_service::paper_api::pnl))
        .route("/paper/positions", get(pe_service::paper_api::positions))
        .route("/paper/fills", get(pe_service::paper_api::fills))
        .route("/paper/status", get(pe_service::paper_api::status))
        .route("/dashboard", get(pe_service::paper_api::dashboard))
        .with_state(health)
        .layer(axum::Extension(paper_api_state));
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
    resolution_task.abort();
    if let Some(t) = reseed_task {
        t.abort();
    }
    info!("pe-service stopped");
    Ok(())
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Wipe the paper-state DB (after backup) and replay all event-log fills from scratch.
///
/// Reconstructs: `fills`, `positions`, `bankroll`, `last_applied_event_seq`.
/// NOT reconstructed: `seen_trades` (no-fill trades never logged), `leader_positions`
/// (no leader wallet in `PaperFill`/`OrderIntent`), `poll_cursors` (same reason).
/// The idempotency-key PK backstop and organic cursor re-warm cover the gaps on restart.
fn run_rebuild_state() -> Result<()> {
    let cfg = load_config()?;
    let db_path = &cfg.paper_state_db_path;

    // Back up before wiping so the user can recover from an accidental rebuild.
    let backup_path = db_path.with_extension("db.bak");
    if db_path.exists() {
        std::fs::rename(db_path, &backup_path)
            .with_context(|| format!("backup {} → {}", db_path.display(), backup_path.display()))?;
        println!(
            "Backed up {} → {}",
            db_path.display(),
            backup_path.display()
        );
    }

    let paper_state = PaperStateDb::open(db_path)
        .with_context(|| format!("create paper-state {}", db_path.display()))?;
    let configured_bankroll = Decimal::from_str(&cfg.bankroll_usd)
        .with_context(|| format!("parse bankroll_usd '{}'", cfg.bankroll_usd))?;
    paper_state
        .init_bankroll(configured_bankroll)
        .context("init bankroll")?;

    // last_applied_event_seq = None on a fresh DB → reconcile_paper_state replays all frames.
    let applied =
        reconcile_paper_state(&cfg.event_log_path, &paper_state).context("replay event log")?;

    let bankroll = paper_state
        .bankroll()
        .context("read bankroll")?
        .unwrap_or(configured_bankroll);

    println!("Rebuilt paper-state from event log.");
    println!("  fills replayed:  {applied}");
    println!("  bankroll:        {bankroll}");
    println!("  NOT rebuilt:     seen_trades, leader_positions, poll_cursors (not in event log)");
    if cfg.paper_resolutions_path.exists() {
        println!(
            "  resolutions:     {} (untouched — re-apply via resolution poller on next start)",
            cfg.paper_resolutions_path.display()
        );
    }
    std::process::exit(0);
}

/// Print P&L report from the existing paper-state DB and exit.
fn run_report() -> Result<()> {
    let cfg = load_config()?;
    let paper_state = PaperStateDb::open(&cfg.paper_state_db_path)
        .with_context(|| format!("open paper-state {}", cfg.paper_state_db_path.display()))?;
    let configured_bankroll = Decimal::from_str(&cfg.bankroll_usd)
        .with_context(|| format!("parse bankroll_usd '{}'", cfg.bankroll_usd))?;
    let store = ResolutionStore::load(&cfg.paper_resolutions_path)
        .with_context(|| format!("load resolutions {}", cfg.paper_resolutions_path.display()))?;
    let snapshot = PnlLedger::snapshot(&paper_state, &store, configured_bankroll)
        .context("compute P&L snapshot")?;
    let json = serde_json::to_string_pretty(&snapshot).context("serialize snapshot")?;
    println!("{json}");
    std::process::exit(0);
}

fn load_config() -> Result<ServiceConfig> {
    let args: Vec<String> = env::args().skip(1).collect();

    if args.iter().any(|a| a == "--print-config") {
        match toml::to_string_pretty(&ServiceConfig::default()) {
            Ok(s) => {
                print!("{s}");
                std::process::exit(0);
            }
            Err(e) => anyhow::bail!("--print-config failed: {e}"),
        }
    }

    // First non-flag argument is the optional TOML config path.
    let config_path = args
        .iter()
        .find(|a| !a.starts_with("--"))
        .map(PathBuf::from);
    service_config::load(config_path.as_deref()).with_context(|| match &config_path {
        Some(p) => format!("loading config from {}", p.display()),
        None => "loading config from environment".to_owned(),
    })
}

/// Periodically fetch Gamma resolutions for all open positions and credit the bankroll.
fn spawn_resolution_task(
    paper_state: Arc<PaperStateDb>,
    gamma_base_url: String,
    poll_interval_secs: u64,
    resolutions_path: std::path::PathBuf,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let fetcher = GammaResolutionFetcher::new(
            gamma_base_url,
            ReqwestFetcher::new(reqwest::Client::new()).with_min_interval_ms(50),
        );
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(poll_interval_secs)).await;
            if let Err(e) = tick_resolution(&paper_state, &fetcher, &resolutions_path).await {
                tracing::warn!(error = %e, "resolution poll error");
            }
        }
    })
}

async fn tick_resolution(
    paper_state: &PaperStateDb,
    fetcher: &GammaResolutionFetcher<ReqwestFetcher>,
    resolutions_path: &std::path::Path,
) -> Result<()> {
    let mut store = ResolutionStore::load(resolutions_path).context("load resolution store")?;

    let positions = paper_state.paper_positions().context("read positions")?;

    // Collect market IDs with open positions that have not been settled yet.
    let pending: Vec<pe_core_types::MarketId> = positions
        .iter()
        .map(|p| p.market_id.clone())
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .filter(|mid| !store.is_settled(mid))
        .collect();

    if pending.is_empty() {
        return Ok(());
    }

    let resolved = fetcher
        .fetch_closed(&pending)
        .await
        .context("fetch resolutions")?;

    let now_unix = OffsetDateTime::now_utc().unix_timestamp();
    for res in resolved {
        let market_positions: Vec<_> = positions
            .iter()
            .filter(|p| p.market_id == res.market_id)
            .cloned()
            .collect();
        let credit = PnlLedger::resolution_credit(&market_positions, &res.outcome_prices);
        // Write sidecar before crediting bankroll: a crash after sidecar but before SQLite
        // means the market is already marked settled, so the next poll skips it (under-credit,
        // not over-credit). The reverse order would double-credit on restart.
        store
            .mark_settled(
                res.market_id.clone(),
                res.outcome_prices.clone(),
                credit,
                now_unix,
            )
            .context("mark settled")?;
        if credit > rust_decimal::Decimal::ZERO {
            paper_state
                .credit_bankroll(credit)
                .context("credit bankroll")?;
        }
        tracing::info!(market = %res.market_id, %credit, "resolution applied");
    }
    Ok(())
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
