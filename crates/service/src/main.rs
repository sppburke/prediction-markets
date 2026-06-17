#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::str::FromStr as _;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use axum::{Router, routing::get};
use pe_core_types::SourceId;
use pe_event_log::Writer;
use pe_execution_core::{ExecutionDispatcher, LiveExecutor};
use pe_paper_state::PaperStateDb;
use pe_service::config::{self as service_config, ServiceConfig};
use pe_service::paper_recovery::{build_leader_ledger, reconcile_paper_state};
use pe_source_polymarket_public::ReqwestFetcher;
use pe_strategy_winner_follow::{ExecutionMode, PaperExecutor, WinnerFollowStrategy};
use pe_trader_index::Watchlist;
use pe_trader_index::fetcher::{WatchlistFetchConfig, WatchlistFetcher};
use pe_venue_polymarket::{PolymarketCredentials, PolymarketVenueAdapter, ReqwestCLOBClient};
use rust_decimal::Decimal;
use tokio::sync::mpsc;
use tracing::info;

use pe_paper_pnl::{GammaResolutionFetcher, PnlLedger, ResolutionStore};
use pe_service::clob_book::ReqwestClobBookFetcher;
use pe_service::entry_gate::CopyEntryGateConfig;
use pe_service::health::new_shared_health;
use pe_service::live_watchlist::LiveWatchlist;
use pe_service::market_end_cache::MarketEndCache;
use pe_service::mid_price_cache::MidPriceCache;
use pe_service::orchestrator::{Orchestrator, OrchestratorConfig};
use pe_service::paper_api::PaperApiState;
use pe_service::position_seeder::{run_reseed_loop, seed_all};
use pe_service::seed;
use pe_service::snapshot_worker::{SnapshotHandle, run_snapshot_worker};
use pe_service::supabase_reader;
use pe_service::supabase_refresh::{
    HttpWatchlistPublisher, WatchlistSizePublisher, run_supabase_refresh_loop,
};
use pe_service::supabase_sink::{SinkHandle, SupabaseWriter, run_sink};
use pe_service::trade_poller::{TradePoller, TradePollerConfig};
use pe_service::wallet_history::WalletHistoryLoader;
use pe_service::watchlist_maintenance::{MaintenanceConfig, run_maintenance_loop};
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

    // Copy-entry gate posture. The leader-price band was removed in #339 — live sizing
    // is re-based on the current market price instead (see the orchestrator copy path).
    let entry_gate_config = CopyEntryGateConfig {
        fail_closed: cfg.entry_gate_fail_closed,
    };

    // Parse the max-fill price cap eagerly so a malformed value fails fast before any I/O.
    let max_fill_price = Decimal::from_str(&cfg.max_fill_price)
        .with_context(|| format!("parse max_fill_price '{}'", cfg.max_fill_price))?;

    // Bootstrap the initial wallet set. The Supabase live ranking handoff (#339) is primary
    // when `supabase_url` is configured; otherwise fall back to the live leaderboard merged
    // with the on-disk bootstrap seed. An empty/failed Supabase fetch also falls back.
    //
    // The Supabase path also returns the last-trade side-map (#357) — each wallet's real last
    // on-chain trade time — used below to seed the poll cursor (the inactivity clock). The
    // leaderboard fallback has no such data, so its map is empty and those wallets keep their
    // persisted cursor (the poller re-warms it organically).
    let (initial_watchlist, bootstrap_last_trade): (Watchlist, HashMap<_, _>) = if cfg
        .supabase_url
        .is_empty()
    {
        (
            bootstrap_from_leaderboard_and_seed(&cfg).await?,
            HashMap::new(),
        )
    } else {
        match supabase_reader::fetch(
            &reqwest::Client::new(),
            &cfg.supabase_url,
            &cfg.supabase_anon_key,
            &cfg.supabase_secret_key,
            supabase_reader::SUPABASE_FETCH_LIMIT,
        )
        .await
        {
            Ok((wl, last_trade)) if !wl.entries.is_empty() => {
                info!(
                    active = wl.active_count,
                    total = wl.entries.len(),
                    "watchlist bootstrapped from supabase"
                );
                (wl, last_trade)
            }
            Ok(_) => {
                tracing::warn!(
                    "supabase returned an empty ranking; falling back to leaderboard + seed"
                );
                (
                    bootstrap_from_leaderboard_and_seed(&cfg).await?,
                    HashMap::new(),
                )
            }
            Err(e) => {
                tracing::warn!(error = %e, "supabase bootstrap failed; falling back to leaderboard + seed");
                (
                    bootstrap_from_leaderboard_and_seed(&cfg).await?,
                    HashMap::new(),
                )
            }
        }
    };

    // Fail fast if there is nothing to copy (no Supabase rows, no seed, no leaderboard).
    anyhow::ensure!(
        !initial_watchlist.entries.is_empty(),
        "no wallets to copy: set `supabase_url` (live ranking) or `seed_watchlist_path` (bootstrap seed)"
    );

    let live_watchlist = LiveWatchlist::new(initial_watchlist);

    // Shared writer mutex (#350 WS1 PR-D): serializes the score-update refresh loop and the
    // maintenance tick's evict+backfill on the live watchlist's ArcSwap (readers stay lock-free).
    let watchlist_writer_lock = Arc::new(tokio::sync::Mutex::new(()));

    // Publish the initial watched-count so the analytics site reflects it within seconds of
    // start (the refresh loop's first publish is one interval away). Best-effort; needs the
    // service-role secret (anon is read-only under RLS).
    if !cfg.supabase_url.is_empty() && !cfg.supabase_secret_key.is_empty() {
        let publisher = HttpWatchlistPublisher::new(
            reqwest::Client::new(),
            &cfg.supabase_url,
            &cfg.supabase_anon_key,
            &cfg.supabase_secret_key,
        );
        let size = live_watchlist.snapshot().entries.len();
        if let Err(e) = publisher.publish(size).await {
            tracing::warn!(error = %e, "initial watchlist-size publish failed");
        }
    }

    // One-time snapshot driving the startup position seed and wallet-history backfill.
    // Refresh-admitted wallets are not retro-seeded/backfilled (acceptable for v1; #3).
    let wallets: Vec<_> = live_watchlist
        .snapshot()
        .entries
        .iter()
        .map(|e| e.wallet)
        .collect();

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

    let health = new_shared_health(false);

    // Bounded channel per _GLOSSARY.md defaults.
    let (trade_tx, trade_rx) = mpsc::channel(cfg.polymarket_channel_capacity);

    // Startup seed: fetch current positions from the API for each watchlisted wallet and overlay
    // them onto the leader ledger, then seed each wallet's poll cursor from its real last-trade
    // time (#357) so the inactivity clock and the poller's first forward sweep both start at the
    // real last trade — reconstructing any trades that happened while the service was down rather
    // than skipping them. Must run before `wallets` is moved into TradePoller::new so the borrow
    // stays valid, and before the poller starts consuming cursors.
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
        // Seed the poll cursor from each bootstrapped wallet's real last-trade time (#357),
        // UNCONDITIONALLY: this establishes the inactivity clock at the real last trade
        // (idle = now − cursor) and repairs any corrupted `now`-seed left by a prior build. A
        // wallet absent from the side-map (leaderboard fallback, or a NULL `last_trade_unix`
        // column) keeps its persisted cursor; a never-seeded `None` cursor self-heals via the
        // poller's first unbounded fetch and is never reset to `now` (no admission grace).
        let mut cursor_seeded = 0usize;
        for (wallet, last_trade_unix) in &bootstrap_last_trade {
            if let Err(e) = paper_state.set_cursor(wallet, *last_trade_unix) {
                tracing::warn!(wallet = %wallet, error = %e, "failed to seed startup cursor from last_trade_unix");
            } else {
                cursor_seeded += 1;
            }
        }
        info!(
            seeded,
            cursor_seeded,
            total = wallets.len(),
            "startup position seed complete"
        );
    }

    // Startup wallet-history backfill: each watchlisted wallet's complete set of
    // previously-entered markets, so the copy-entry gate admits only first-ever
    // entries. Borrows `wallets` (must run before it is moved into TradePoller).
    let history_map = {
        // Per-request timeout so a stalled Polymarket connection can't hang startup
        // (the loader falls back to the cached sidecar for any wallet that times out).
        let history_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(20))
            .build()
            .context("build wallet-history http client")?;
        let history_fetcher = ReqwestFetcher::new(history_client);
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

    // Polymarket trade poller task. Reads the live wallet set per poll round (#339).
    let trade_task = tokio::spawn(
        TradePoller::new(
            TradePollerConfig {
                base_url: cfg.polymarket_base_url.clone(),
                poll_interval_secs: cfg.trade_poll_interval_secs,
            },
            live_watchlist.clone(),
            ReqwestFetcher::new(reqwest::Client::new()),
            trade_tx,
            paper_state.clone(),
            health.clone(),
        )
        .run(),
    );

    // Orchestrator.
    let market_end_cache = MarketEndCache::new(cfg.gamma_base_url.clone());
    // Mid-price cache for marking open dashboard positions to market (own rate gate).
    let mid_price_cache = MidPriceCache::new(cfg.gamma_base_url.clone());

    // Supabase analytics sink (issue #343): best-effort dual-write of fills + settlements.
    // Spawned only when enabled and a Supabase URL is configured; otherwise `None` (no-op).
    let (sink_handle, sink_task) = if cfg.supabase_sink_enabled && !cfg.supabase_url.is_empty() {
        if cfg.supabase_secret_key.is_empty() {
            tracing::warn!(
                "supabase_sink_enabled but supabase_secret_key is empty; the sink writes with \
                 the anon key, which RLS allows only to read — all upserts will 403. Set \
                 PE_SUPABASE_SECRET_KEY to enable sink writes."
            );
        }
        let (handle, rx) = SinkHandle::channel(cfg.supabase_sink_channel_capacity);
        let writer = SupabaseWriter::new(
            reqwest::Client::new(),
            &cfg.supabase_url,
            &cfg.supabase_anon_key,
            &cfg.supabase_secret_key,
        );
        let dropped = handle.dropped_counter();
        let task = tokio::spawn(run_sink(
            writer,
            paper_state.clone(),
            rx,
            Duration::from_secs(cfg.supabase_sink_reconcile_interval_secs),
            dropped,
        ));
        (Some(handle), Some(task))
    } else {
        (None, None)
    };

    // Liquidity-at-fill capture (issue #350 WS2 PR-H): off-hot-path Gamma + CLOB /book snapshot
    // per BUY fill, written to SQLite (canonical) + a best-effort Supabase mirror. Spawned under
    // the same gate as the sink (the mirror reuses the Supabase writer); the worker shares the
    // orchestrator's mid-price cache so a fill rarely incurs an extra Gamma fetch. The
    // empty-secret-key warning is emitted once by the sink block above (identical gate) and
    // covers this writer too — keep the blocks ordered so it is not duplicated.
    let (snapshot_handle, snapshot_task) =
        if cfg.supabase_sink_enabled && !cfg.supabase_url.is_empty() {
            let (handle, rx) = SnapshotHandle::channel(cfg.snapshot_channel_capacity);
            let writer = SupabaseWriter::new(
                reqwest::Client::new(),
                &cfg.supabase_url,
                &cfg.supabase_anon_key,
                &cfg.supabase_secret_key,
            );
            let dropped = handle.dropped_counter();
            let task = tokio::spawn(run_snapshot_worker(
                rx,
                mid_price_cache.clone(),
                ReqwestClobBookFetcher::new(reqwest::Client::new()),
                paper_state.clone(),
                Some(writer),
                dropped,
            ));
            (Some(handle), Some(task))
        } else {
            (None, None)
        };

    let orch = Orchestrator::new(
        trade_rx,
        live_watchlist.clone(),
        OrchestratorConfig {
            bankroll,
            mode,
            signal_config: Default::default(),
            max_resolution_horizon_secs: cfg.max_resolution_horizon_secs,
            min_resolution_horizon_secs: cfg.min_resolution_horizon_secs,
            max_fill_price,
            entry_gate_config,
        },
        history_map,
        WinnerFollowStrategy::new(cfg.strategy.clone()),
        dispatcher,
        paper_state.clone(),
        leader_ledger,
        health.clone(),
        market_end_cache.clone(),
        mid_price_cache.clone(),
        reseed_rx,
        sink_handle.clone(),
        snapshot_handle,
    )
    .context("build orchestrator")?;

    // Resolution polling task: periodically fetch Gamma for closed markets.
    let resolution_task = spawn_resolution_task(
        paper_state.clone(),
        cfg.gamma_base_url.clone(),
        cfg.gamma_resolution_poll_interval_secs,
        sink_handle,
    );

    // Live-watchlist refresh task (#339): poll Supabase on the configured interval and refresh
    // the scores of the live set (score-update-only, #350 WS1). Spawned only when configured.
    let supabase_task = if !cfg.supabase_url.is_empty() && cfg.supabase_refresh_interval_secs > 0 {
        Some(tokio::spawn(run_supabase_refresh_loop(
            live_watchlist.clone(),
            reqwest::Client::new(),
            cfg.supabase_url.clone(),
            cfg.supabase_anon_key.clone(),
            cfg.supabase_secret_key.clone(),
            supabase_reader::SUPABASE_FETCH_LIMIT,
            cfg.supabase_refresh_interval_secs,
            watchlist_writer_lock.clone(),
        )))
    } else {
        None
    };

    // Watchlist maintenance tick (#350 WS1 PR-D): inactivity + underperformance knockout +
    // atomic backfill. Spawned only when Supabase is configured and the interval is non-zero.
    let maintenance_task = if !cfg.supabase_url.is_empty() && cfg.maintenance_interval_secs > 0 {
        let demotion_cb_alpha = Decimal::from_str(&cfg.demotion_cb_alpha)
            .with_context(|| format!("parse demotion_cb_alpha '{}'", cfg.demotion_cb_alpha))?;
        let maint_cfg = MaintenanceConfig {
            interval_secs: cfg.maintenance_interval_secs,
            inactivity_threshold_secs: cfg.inactivity_threshold_secs,
            inactivity_hard_cap_secs: cfg.inactivity_hard_cap_secs,
            demotion_min_trades: cfg.demotion_min_trades,
            demotion_cb_alpha,
            bench_overfetch: cfg.bench_overfetch,
            cap: supabase_reader::MAINTAINED_SET_SIZE,
        };
        Some(tokio::spawn(run_maintenance_loop(
            live_watchlist.clone(),
            paper_state.clone(),
            reqwest::Client::new(),
            cfg.supabase_url.clone(),
            cfg.supabase_anon_key.clone(),
            cfg.supabase_secret_key.clone(),
            watchlist_writer_lock.clone(),
            maint_cfg,
        )))
    } else {
        None
    };

    // Shared state for paper API handlers.
    let paper_api_state = Arc::new(PaperApiState {
        paper_state: paper_state.clone(),
        initial_bankroll: configured_bankroll,
        market_end_cache,
        mid_price_cache,
    });

    // HTTP server: health + paper API.
    let app = Router::new()
        .route("/health/live", get(pe_service::health::live))
        .route("/health/ready", get(pe_service::health::ready))
        .route("/paper/pnl", get(pe_service::paper_api::pnl))
        .route("/paper/positions", get(pe_service::paper_api::positions))
        .route("/paper/fills", get(pe_service::paper_api::fills))
        .route("/paper/status", get(pe_service::paper_api::status))
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
    trade_task.abort();
    http_task.abort();
    resolution_task.abort();
    if let Some(t) = reseed_task {
        t.abort();
    }
    if let Some(t) = supabase_task {
        t.abort();
    }
    if let Some(t) = maintenance_task {
        t.abort();
    }
    if let Some(t) = sink_task {
        t.abort();
    }
    if let Some(t) = snapshot_task {
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
    std::process::exit(0);
}

/// Print P&L report from the existing paper-state DB and exit.
fn run_report() -> Result<()> {
    let cfg = load_config()?;
    let paper_state = Arc::new(
        PaperStateDb::open(&cfg.paper_state_db_path)
            .with_context(|| format!("open paper-state {}", cfg.paper_state_db_path.display()))?,
    );
    let configured_bankroll = Decimal::from_str(&cfg.bankroll_usd)
        .with_context(|| format!("parse bankroll_usd '{}'", cfg.bankroll_usd))?;
    let store = ResolutionStore::load(Arc::clone(&paper_state)).context("load resolutions")?;
    // `--report` is offline (no live mids): value open positions at $0, matching
    // the prior report semantics.
    let snapshot = PnlLedger::snapshot(
        &paper_state,
        &store,
        configured_bankroll,
        &std::collections::HashMap::new(),
    )
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
    sink: Option<SinkHandle>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let fetcher = GammaResolutionFetcher::new(
            gamma_base_url,
            ReqwestFetcher::new(reqwest::Client::new()).with_min_interval_ms(50),
        );
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(poll_interval_secs)).await;
            if let Err(e) = tick_resolution(&paper_state, &fetcher, sink.as_ref()).await {
                tracing::warn!(error = %e, "resolution poll error");
            }
        }
    })
}

async fn tick_resolution(
    paper_state: &Arc<PaperStateDb>,
    fetcher: &GammaResolutionFetcher<ReqwestFetcher>,
    sink: Option<&SinkHandle>,
) -> Result<()> {
    let mut store =
        ResolutionStore::load(Arc::clone(paper_state)).context("load resolution store")?;

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
    let any_settled = !resolved.is_empty();
    for res in resolved {
        let market_positions: Vec<_> = positions
            .iter()
            .filter(|p| p.market_id == res.market_id)
            .cloned()
            .collect();
        let credit = PnlLedger::resolution_credit(&market_positions, &res.outcome_prices);
        // Mark settled (SQLite-authoritative) before crediting the bankroll:
        // a crash after `mark_settled` but before `credit_bankroll` leaves the market marked
        // settled, so the next poll skips it (under-credit, not over-credit). The reverse order
        // would double-credit on restart.
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
    // Nudge the Supabase sink once per tick to re-upsert the settled set (canonical JSON).
    if any_settled && let Some(sink) = sink {
        sink.send_resolution();
    }
    Ok(())
}

/// Bootstrap the initial watchlist from the live Polymarket leaderboard merged with the
/// optional on-disk bootstrap seed (the pre-#339 path; the Supabase live source is primary
/// when configured).
async fn bootstrap_from_leaderboard_and_seed(cfg: &ServiceConfig) -> Result<Watchlist> {
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
    info!(
        active = watchlist.active_count,
        "watchlist bootstrapped from leaderboard"
    );

    // Merge leaderboard with the optional bootstrap seed. The merged Watchlist carries
    // full WatchlistEntry metadata for every wallet so the orchestrator can look up tier
    // and scores for seed-only wallets.
    let seed_wl = seed::load_seed_watchlist(&cfg.seed_watchlist_path)?;
    let merged = seed::merge_watchlist(&watchlist, seed_wl.as_ref());
    if seed_wl.is_some() {
        info!(
            total = merged.entries.len(),
            active = merged.active_count,
            incubator = merged.incubator_count,
            "seed watchlist merged with leaderboard"
        );
        if merged.entries.len() > cfg.watchlist_size {
            tracing::warn!(
                merged = merged.entries.len(),
                watchlist_size = cfg.watchlist_size,
                "merged wallet count exceeds watchlist_size; trade poller rate-limit budget was sized for watchlist_size"
            );
        }
    }
    Ok(merged)
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
