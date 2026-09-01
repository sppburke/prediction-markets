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
use pe_execution_core::{ExecutionDispatcher, LiveJournal};
use pe_paper_state::PaperStateDb;
use pe_service::config::{self as service_config, ServiceConfig};
use pe_service::paper_recovery::{build_leader_ledger, reconcile_paper_state};
use pe_source_polymarket_public::ReqwestFetcher;
use pe_strategy_winner_follow::{ExecutionMode, PaperExecutor, WinnerFollowStrategy};
use pe_trader_index::Watchlist;
use rust_decimal::Decimal;
use tokio::sync::{mpsc, watch};
use tracing::info;

use pe_paper_pnl::{GammaResolutionFetcher, PnlLedger, ResolutionStore};
use pe_service::clob_book::ReqwestClobBookFetcher;
use pe_service::config_poller::{
    CONFIG_POLL_INTERVAL_SECS, SupabaseConfigFetcher, capacity_request_channel,
    fetch_service_config, run_capacity_worker, run_config_poll_loop,
};
use pe_service::entry_gate::CopyEntryGateConfig;
use pe_service::health::new_shared_health_with_ws;
use pe_service::live_watchlist::LiveWatchlist;
use pe_service::market_end_cache::MarketEndCache;
use pe_service::mid_price_cache::MidPriceCache;
use pe_service::orchestrator::{Orchestrator, OrchestratorConfig};
use pe_service::paper_api::PaperApiState;
use pe_service::runtime_config::{
    AppliedWatchlistCapacity, FillMode, LiveRuntimeConfig, load_initial_runtime_config,
};
use pe_service::snapshot_worker::{SnapshotHandle, run_snapshot_worker};
use pe_service::supabase_backfill::backfill_supabase;
use pe_service::supabase_reader;
use pe_service::supabase_refresh::{
    HttpWatchlistPublisher, WatchlistSizePublisher, run_supabase_refresh_loop,
};
use pe_service::supabase_sink::{SinkHandle, SupabaseWriter, run_sink};
use pe_service::supabase_state::{
    SupabaseStateClient, apply_resolution_authoritative, supabase_authoritative_boot,
};
use pe_service::trade_poller::{TradePoller, TradePollerConfig};
use pe_service::watchlist_admission::AdmissionPreparer;
use pe_service::watchlist_capacity::SupabaseWatchlistCapacity;
use pe_service::watchlist_maintenance::{MaintenanceConfig, MembershipMode, run_maintenance_loop};
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
    // --backfill-supabase: one-time SQLite → Supabase push for the authoritative cutover
    // (issue #397). Run once with the service stopped, before flipping the flag. Exits.
    if env::args().any(|a| a == "--backfill-supabase") {
        return run_backfill_supabase().await;
    }

    let cfg = load_config()?;

    // Hold the rolling-log worker guards for the whole process; dropping them flushes the
    // non-blocking writers (losing buffered lines), so keep `_log_guards` alive until exit.
    let _log_guards =
        pe_service::logging::setup(&cfg.jsonl_log_path, "info", cfg.log_retention_days)?;
    info!("pe-service starting");

    let configured_bankroll = Decimal::from_str(&cfg.bankroll_usd)
        .with_context(|| format!("parse bankroll_usd '{}'", cfg.bankroll_usd))?;
    let mode = parse_mode(&cfg.mode)?;

    // (#398 step 8) The boot-time Kelly approval guard was removed: its invariant now lives in
    // `runtime_config::parse_config` and is re-enforced on every poll (and at boot via
    // `load_initial_runtime_config`), so a runtime override change is governed too — not just the
    // boot value. An above-ceiling override without the approval flag is cleared, not a hard-fail.

    // Copy-entry gate posture. The leader-price band was removed in #339 — live sizing
    // is re-based on the current market price instead (see the orchestrator copy path).
    let entry_gate_config = CopyEntryGateConfig;

    // Parse the fill-price band eagerly so a malformed value fails fast before any I/O.
    let max_fill_price = Decimal::from_str(&cfg.max_fill_price)
        .with_context(|| format!("parse max_fill_price '{}'", cfg.max_fill_price))?;
    let min_fill_price = Decimal::from_str(&cfg.min_fill_price)
        .with_context(|| format!("parse min_fill_price '{}'", cfg.min_fill_price))?;

    // #398 WS1: Supabase `service_config` is authoritative for the non-secret runtime knobs.
    // Fetch it once at boot (best-effort; fall back to env/compiled on any error) and seed the
    // `LiveRuntimeConfig` ArcSwap that the config poller refreshes and the orchestrator reads per
    // event. `clob_creds_present` gates live-mode transitions (paper stays paper without creds).
    // The ordinary paper service has no credentialed construction path. Supabase therefore cannot
    // promote it into live execution; the isolated canary binary owns separate credentials/state.
    let clob_creds_present = false;
    let initial_config_rows = if cfg.supabase_url.is_empty() {
        Vec::new()
    } else {
        fetch_service_config(
            &reqwest::Client::new(),
            &cfg.supabase_url,
            &cfg.supabase_anon_key,
            &cfg.supabase_secret_key,
        )
        .await
        .unwrap_or_else(|e| {
            tracing::warn!(error = %e, "initial service_config fetch failed; using env/compiled defaults");
            Vec::new()
        })
    };
    let live_runtime_config = LiveRuntimeConfig::new(load_initial_runtime_config(
        &initial_config_rows,
        &cfg,
        clob_creds_present,
    ));

    // Bootstrap the initial wallet set from Supabase `latest_ranking` — the sole wallet
    // source (#339, #370). The fetch also returns the last-trade side-map (#357): each
    // wallet's real last on-chain trade time, used below to seed the poll cursor (the
    // inactivity clock). There is no leaderboard/seed fallback — the service hard-fails fast
    // if Supabase is empty or unreachable at boot, chosen over running an unvalidated set.
    // Deploy precondition: the authoritative `rank_and_push` cron must already be populating
    // `latest_ranking`.
    // Record the ranking batch observed at boot BEFORE fetching the watchlist, so a batch
    // landing in between reads as a transition on the first maintenance tick (full_rerank
    // then swaps immediately) rather than being pinned as already-seen. Best-effort: `None`
    // makes the first full-rerank tick apply whatever batch it observes (#542).
    let boot_batch_marker = supabase_reader::fetch_latest_batch_id(
        &reqwest::Client::new(),
        &cfg.supabase_url,
        &cfg.supabase_anon_key,
        &cfg.supabase_secret_key,
    )
    .await
    .unwrap_or_default();

    let initial_watchlist_size = live_runtime_config.snapshot().active_watchlist_size;
    let (initial_watchlist, bootstrap_last_trade): (Watchlist, HashMap<_, _>) =
        supabase_reader::fetch(
            &reqwest::Client::new(),
            &cfg.supabase_url,
            &cfg.supabase_anon_key,
            &cfg.supabase_secret_key,
            initial_watchlist_size,
        )
        .await
        .context("bootstrap watchlist from Supabase (the sole wallet source)")?;
    info!(
        active = initial_watchlist.active_count,
        total = initial_watchlist.entries.len(),
        "watchlist bootstrapped from supabase"
    );

    // Fail fast if Supabase returned no wallets — there is no fallback source (#370). The read is
    // survivor-filtered (#518), so "empty" now has two causes: `latest_ranking` itself is empty
    // (the authoritative `rank_and_push` cron has not populated it), or the newest batch carries
    // no surviving rows (no verdict recorded, or the ranker endorsed nobody). Both fail closed:
    // refuse to boot rather than run with an empty watchlist.
    anyhow::ensure!(
        !initial_watchlist.entries.is_empty(),
        "no wallets to copy: Supabase `latest_ranking` returned no SURVIVING rows \
         (is rank_and_push populating it, and does the newest batch carry `survives` verdicts?)"
    );

    let live_watchlist = LiveWatchlist::new(initial_watchlist);

    // Shared writer mutex (#350 WS1 PR-D): serializes the score-update refresh loop and the
    // maintenance tick's evict+backfill on the live watchlist's ArcSwap (readers stay lock-free).
    let watchlist_writer_lock = Arc::new(tokio::sync::Mutex::new(()));
    let applied_watchlist_capacity = AppliedWatchlistCapacity::new(initial_watchlist_size);

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
    // #511: LEGACY-ONLY blind frame replay. In authoritative mode the boot frame-walk
    // below owns local application — every unresolved frame is decided by the authority
    // (`commit_fill_v2`), so a refused frame can never resurrect locally. The blind
    // replay would apply such frames unconditionally.
    if !cfg.supabase_authoritative {
        let reconciled = reconcile_paper_state(&cfg.event_log_path, &paper_state)
            .context("reconcile paper-state from event log")?;
        if reconciled > 0 {
            info!(
                reconciled,
                "replayed uncommitted fills from event log on startup"
            );
        }
    }

    // Supabase authoritative client (issue #397): built only when the flag is set. It is the
    // sole writer of `paper_fills`/`settled_markets` (the best-effort `run_sink` is not spawned
    // below), and runs the boot frame-walk-then-pull so the bankroll/positions read just after
    // reflect the Supabase source of truth. Fail-closed: requires the service-role secret, and
    // a walk/pull error aborts boot (refuse to run authoritative without the authority).
    let supabase_state = if cfg.supabase_authoritative {
        anyhow::ensure!(
            !cfg.supabase_url.is_empty(),
            "PE_SUPABASE_AUTHORITATIVE=1 requires PE_SUPABASE_URL"
        );
        anyhow::ensure!(
            !cfg.supabase_secret_key.is_empty(),
            "PE_SUPABASE_AUTHORITATIVE=1 requires the service-role PE_SUPABASE_SECRET_KEY \
             (RLS blocks anon writes)"
        );
        let client = SupabaseStateClient::new(
            reqwest::Client::new(),
            &cfg.supabase_url,
            &cfg.supabase_anon_key,
            &cfg.supabase_secret_key,
        );
        supabase_authoritative_boot(&client, &paper_state, &cfg.event_log_path)
            .await
            .context("supabase authoritative boot (frame-walk then pull)")?;
        info!(
            "supabase authoritative mode active: paper-state writes through to Supabase (system of record)"
        );
        Some(client)
    } else {
        None
    };

    // #508 Decision 10 (#511: AFTER frame dispositions exist in either mode): resume staged
    // dispatch aggregates — flip seeds whose fill frame reached a disposition, finalize
    // stuck seeds, leave redeliverable seeds pending. Never reconstructs targets.
    pe_service::dispatch_recovery::resume_dispatch_seeds(&cfg.event_log_path, &paper_state)
        .context("resume dispatch seeds")?;

    let bankroll = paper_state
        .bankroll()
        .context("read paper-state bankroll")?
        .unwrap_or(configured_bankroll);
    let leader_ledger =
        build_leader_ledger(&paper_state).context("rehydrate leader position ledger")?;

    // The version-two activity/gate records are the sole membership authority (#544).
    // Fenced wallets and wallets without proven-complete reconciled history are removed
    // before any producer or maintenance reader can observe the boot generation.
    let complete_history = paper_state
        .complete_history_wallets()
        .context("load complete durable wallet history set")?;
    let mut unavailable: std::collections::HashSet<_> = paper_state
        .wallet_fences()
        .context("load durable wallet fences")?
        .into_iter()
        .map(|fence| fence.wallet)
        .collect();
    unavailable.extend(
        live_watchlist
            .snapshot()
            .entries
            .iter()
            .filter(|entry| !complete_history.contains(&entry.wallet))
            .map(|entry| entry.wallet),
    );
    live_watchlist.remove_fenced(&unavailable);
    anyhow::ensure!(
        !live_watchlist.snapshot().entries.is_empty(),
        "no wallets eligible after durable fence/history filtering"
    );

    // Publish only the durable-authority-filtered initial watched count.
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

    let dispatcher = ExecutionDispatcher::paper_only(paper_executor);

    let health = new_shared_health_with_ws(
        false,
        cfg.polymarket_activity_ws_enabled,
        i64::try_from(cfg.trade_poll_interval_secs.saturating_mul(3)).unwrap_or(i64::MAX),
    );

    // Bounded channel per _GLOSSARY.md defaults.
    let (trade_tx, trade_rx) = mpsc::channel(cfg.polymarket_channel_capacity);

    // Seed the delivery cursor without mutating the ordered activity ledger. The positions API
    // is no longer an overwrite authority (#544).
    let mut cursor_seeded = 0usize;
    for (wallet, last_trade_unix) in &bootstrap_last_trade {
        if let Err(e) = paper_state.seed_cursor_if_absent(wallet, *last_trade_unix) {
            tracing::warn!(wallet = %wallet, error = %e, "failed to seed startup cursor from last_trade_unix");
        } else {
            cursor_seeded += 1;
        }
    }
    info!(cursor_seeded, "startup delivery cursors seeded");

    // Bounded service-side activity-bucket/admission control channel (#544).
    let (control_tx, control_rx) = mpsc::channel(2);
    let (producer_start_tx, producer_start_rx) = watch::channel(false);

    // Runtime admissions prove durable history and recheck the fence under the shared
    // watchlist-writer lock before publication. Lane D adds the causal positions bracket.
    let admission_preparer = AdmissionPreparer::new(control_tx.clone(), paper_state.clone());

    // #530: websocket-primary ingest task (flag-gated). Owns the source event
    // log; a boot-time open failure fails boot — enabled mode must satisfy the
    // raw-evidence invariant. Disabled mode spawns nothing (poll-only,
    // byte-identical to pre-#530 behavior — the rollback posture).
    let activity_ingest_task = if cfg.polymarket_activity_ws_enabled {
        let sink = pe_service::source_event_sink::SourceEventSink::open(&cfg.source_event_log_path)
            .with_context(|| {
                format!(
                    "open source event log {}",
                    cfg.source_event_log_path.display()
                )
            })?;
        let mut start = producer_start_rx.clone();
        let activity_watchlist = live_watchlist.clone();
        let activity_trade_tx = trade_tx.clone();
        let activity_health = health.clone();
        Some(tokio::spawn(async move {
            if start.wait_for(|started| *started).await.is_err() {
                return;
            }
            pe_service::activity_ingest::ActivityIngest::new(
                activity_watchlist,
                sink,
                activity_trade_tx,
                activity_health,
            )
            .run()
            .await;
        }))
    } else {
        None
    };

    // Polymarket trade poller task. Reads the live wallet set per poll round (#339).
    let mut poller_start = producer_start_rx;
    let poller_watchlist = live_watchlist.clone();
    let poller_paper_state = paper_state.clone();
    let poller_health = health.clone();
    let poller_base_url = cfg.polymarket_base_url.clone();
    let trade_task = tokio::spawn(async move {
        if poller_start.wait_for(|started| *started).await.is_err() {
            return;
        }
        TradePoller::new(
            TradePollerConfig {
                base_url: poller_base_url,
                poll_interval_secs: cfg.trade_poll_interval_secs,
            },
            poller_watchlist,
            ReqwestFetcher::new(reqwest::Client::new()),
            trade_tx,
            poller_paper_state,
            poller_health,
        )
        .run()
        .await;
    });

    // Orchestrator.
    let market_end_cache = MarketEndCache::new(cfg.gamma_base_url.clone());
    // Mid-price cache for marking open dashboard positions to market (own rate gate).
    let mid_price_cache = MidPriceCache::new(cfg.gamma_base_url.clone());

    // Supabase analytics sink (issue #343): best-effort dual-write of fills + settlements.
    // Spawned only when enabled and a Supabase URL is configured; otherwise `None` (no-op).
    // NOT spawned in authoritative mode (issue #397): the `commit_fill`/`apply_resolution`
    // RPCs are the sole writer of `paper_fills`/`settled_markets`, so a best-effort
    // `merge-duplicates` upsert from `run_sink` must not race them. With `sink_handle = None`
    // the orchestrator's `send_fill` and `tick_resolution`'s `send_resolution` are suppressed
    // automatically; the liquidity-snapshot worker (#350) keeps its own gate and stays alive.
    let (sink_handle, sink_task) =
        if cfg.supabase_sink_enabled && !cfg.supabase_url.is_empty() && !cfg.supabase_authoritative
        {
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
    // One shared CLOB /book fetcher (#398 WS2, now also the #486 paper best-ask hot path): its 5
    // rps rate gate is global across the snapshot worker, the price-impact gate, and the best-ask
    // fill basis. Built unconditionally so the orchestrator always has it; the worker clones it
    // only when the snapshot block runs. `with_base_url` is override-only parity with the order
    // adapter (no prod change at the default) — the book is now on the paper fill path (#486).
    let book_http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .context("build bounded CLOB book HTTP client")?;
    let book_fetcher = Arc::new(
        ReqwestClobBookFetcher::new(book_http_client)
            .with_base_url(cfg.polymarket_clob_base_url.clone()),
    );

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
                book_fetcher.clone(),
                paper_state.clone(),
                Some(writer),
                dropped,
            ));
            (Some(handle), Some(task))
        } else {
            (None, None)
        };

    // #508: live account contexts — one boot fetch (best-effort; empty on failure) plus a
    // 30 s refresh loop, mirroring the config poller's last-known-good posture. With no
    // Supabase (or no armed accounts) the snapshot is empty and the copy path is the
    // Phase-A baseline (no dispatch seeds are staged).
    let live_accounts = if cfg.supabase_url.is_empty() {
        None
    } else {
        let accounts_http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(20))
            .build()
            .context("build live-accounts HTTP client")?;
        let initial = match pe_service::live_accounts::fetch_live_accounts(
            &accounts_http_client,
            &cfg.supabase_url,
            &cfg.supabase_anon_key,
            &cfg.supabase_secret_key,
        )
        .await
        {
            Ok(snapshot) => snapshot,
            Err(e) => {
                tracing::warn!(error = %e, "live accounts boot fetch failed; starting with none armed");
                pe_service::live_accounts::LiveAccountsSnapshot::default()
            }
        };
        let live = pe_service::live_accounts::LiveAccounts::new(initial);
        tokio::spawn(pe_service::live_accounts::run_live_accounts_poller(
            live.clone(),
            accounts_http_client,
            cfg.supabase_url.clone(),
            cfg.supabase_anon_key.clone(),
            cfg.supabase_secret_key.clone(),
            CONFIG_POLL_INTERVAL_SECS,
        ));
        Some(live)
    };

    // Ordinary #508 live execution: one task owns strict account/seed ordering, mode probes,
    // redemption posture, and retention. A missing age identity is warned exactly once and
    // passed as `None`; the mode machine then cannot arm, while the paper orchestrator remains
    // fully operational. The account-tagged journal is a mode-0600 sibling of the paper log.
    let live_fanout_task = if let Some(live_accounts) = live_accounts.clone() {
        let identity = match pe_service::live_credentials::load_identity_from_credentials_dir() {
            Ok(identity) => Some(identity),
            Err(error) => {
                tracing::warn!(error = %error, "ordinary live age identity unavailable; live arming disabled");
                None
            }
        };
        let journal_path = live_journal_path(&cfg.event_log_path);
        match LiveJournal::open(&journal_path) {
            Ok(journal) => {
                let live_http_client = reqwest::Client::builder()
                    .timeout(Duration::from_secs(20))
                    .build()
                    .context("build bounded ordinary-live HTTP client")?;
                let projection = pe_service::live_projections::LiveProjectionWriter::new(
                    live_http_client.clone(),
                    &cfg.supabase_url,
                    &cfg.supabase_anon_key,
                    &cfg.supabase_secret_key,
                );
                Some(tokio::spawn(pe_service::live_fanout::run_live_fanout(
                    pe_service::live_fanout::LiveFanoutConfig {
                        paper_state: paper_state.clone(),
                        live_accounts,
                        live_watchlist: live_watchlist.clone(),
                        runtime_config: live_runtime_config.clone(),
                        identity,
                        journal: Arc::new(journal),
                        journal_path,
                        projection,
                        book_fetcher: book_fetcher.clone(),
                        http: live_http_client,
                        supabase_url: cfg.supabase_url.clone(),
                        supabase_anon_key: cfg.supabase_anon_key.clone(),
                        supabase_secret_key: cfg.supabase_secret_key.clone(),
                        gamma_base_url: cfg.gamma_base_url.clone(),
                        clob_base_url: cfg.polymarket_clob_base_url.clone(),
                        data_base_url: cfg.polymarket_base_url.clone(),
                        projection_reconcile_interval_secs: cfg
                            .supabase_sink_reconcile_interval_secs,
                    },
                )))
            }
            Err(error) => {
                tracing::warn!(path = %journal_path.display(), error = %error, "ordinary live journal unavailable; live fan-out disabled");
                None
            }
        }
    } else {
        None
    };

    let mut orch = Orchestrator::new(
        trade_rx,
        live_watchlist.clone(),
        OrchestratorConfig {
            bankroll,
            mode,
            signal_config: Default::default(),
            max_resolution_horizon_secs: cfg.max_resolution_horizon_secs,
            min_resolution_horizon_secs: cfg.min_resolution_horizon_secs,
            activity_ws_enabled: cfg.polymarket_activity_ws_enabled,
            copy_latency_budget_secs: cfg.copy_latency_budget_secs,
            watchlist_writer_lock: Some(watchlist_writer_lock.clone()),
            max_fill_price,
            min_fill_price,
            paper_fill_haircut_bps: cfg.paper_fill_haircut_bps,
            paper_fill_slippage_bps: cfg.paper_fill_slippage_bps,
            // Boot value; production wires `runtime_config: Some(..)` so it is refreshed per event
            // from the snapshot. An unknown env/TOML override defaults to `ClobBestAsk` (already
            // warned when `from_service_config` built the runtime snapshot above).
            fill_mode: FillMode::parse(&cfg.fill_mode).unwrap_or_default(),
            clob_best_ask_fallback_haircut_bps: cfg.clob_best_ask_fallback_haircut_bps,
            entry_gate_config,
            runtime_config: Some(live_runtime_config.clone()),
            live_accounts: live_accounts.clone(),
        },
        WinnerFollowStrategy::new(cfg.strategy.clone()),
        dispatcher,
        paper_state.clone(),
        leader_ledger,
        health.clone(),
        market_end_cache.clone(),
        mid_price_cache.clone(),
        control_rx,
        sink_handle.clone(),
        snapshot_handle,
        supabase_state.clone(),
        book_fetcher,
    )
    .context("build orchestrator")?;
    orch.resume_pending_before_producers()
        .await
        .context("resume decision_pending before source producers")?;
    producer_start_tx
        .send(true)
        .map_err(|_| anyhow::anyhow!("source producer start gate closed"))?;

    // Cumulative authoritative-RPC counter for status.json — grabbed before `supabase_state`
    // is moved into the resolution task below; `None` when not in authoritative mode.
    let supabase_rpc_calls = supabase_state.as_ref().map(|c| c.call_counter());

    // Resolution polling task: periodically fetch Gamma for closed markets. In authoritative
    // mode (issue #397) it credits via the `apply_resolution` RPC; `sink_handle` is `None`,
    // so the best-effort `send_resolution` nudge is suppressed.
    let resolution_task = spawn_resolution_task(
        paper_state.clone(),
        cfg.gamma_base_url.clone(),
        cfg.gamma_resolution_poll_interval_secs,
        sink_handle,
        supabase_state,
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
            applied_watchlist_capacity.clone(),
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
        let membership_mode =
            MembershipMode::parse(&cfg.watchlist_membership_mode).with_context(|| {
                format!(
                    "invalid watchlist_membership_mode '{}' (knockout | full_rerank)",
                    cfg.watchlist_membership_mode
                )
            })?;
        let maint_cfg = MaintenanceConfig {
            interval_secs: cfg.maintenance_interval_secs,
            inactivity_threshold_secs: cfg.inactivity_threshold_secs,
            inactivity_hard_cap_secs: cfg.inactivity_hard_cap_secs,
            demotion_min_trades: cfg.demotion_min_trades,
            demotion_cb_alpha,
            demotion_pnl_window_secs: cfg.demotion_pnl_window_secs,
            bench_overfetch: cfg.bench_overfetch,
            membership_mode,
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
            applied_watchlist_capacity.clone(),
            admission_preparer.clone(),
            boot_batch_marker,
        )))
    } else {
        None
    };

    // Service-config polling remains fixed at 30 seconds while a separate latest-only worker
    // performs slow admission preparation. The poller stays the sole RuntimeConfig writer; the
    // capacity worker commits membership + its applied epoch under the structural mutex and sends
    // a small completion back to the poller.
    let (config_poll_task, capacity_task) = if cfg.supabase_url.is_empty() {
        (None, None)
    } else {
        let (capacity_requests, capacity_request_rx) =
            capacity_request_channel(initial_watchlist_size, watchlist_writer_lock.clone());
        let (capacity_result_tx, capacity_result_rx) = mpsc::channel(4);
        let capacity_http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(20))
            .build()
            .context("build bounded watchlist-capacity HTTP client")?;
        let capacity_applier = SupabaseWatchlistCapacity::new(
            live_watchlist.clone(),
            paper_state.clone(),
            watchlist_writer_lock.clone(),
            applied_watchlist_capacity.clone(),
            capacity_request_rx.clone(),
            admission_preparer,
            capacity_http_client,
            cfg.supabase_url.clone(),
            cfg.supabase_anon_key.clone(),
            cfg.supabase_secret_key.clone(),
        );
        let worker = tokio::spawn(run_capacity_worker(
            capacity_applier,
            applied_watchlist_capacity.clone(),
            capacity_request_rx,
            capacity_result_tx,
        ));
        let config_http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(20))
            .build()
            .context("build bounded service-config HTTP client")?;
        let poller = tokio::spawn(run_config_poll_loop(
            live_runtime_config.clone(),
            SupabaseConfigFetcher::new(
                config_http_client,
                cfg.supabase_url.clone(),
                cfg.supabase_anon_key.clone(),
                cfg.supabase_secret_key.clone(),
            ),
            capacity_requests,
            applied_watchlist_capacity.clone(),
            capacity_result_rx,
            CONFIG_POLL_INTERVAL_SECS,
            clob_creds_present,
        ));
        (Some(poller), Some(worker))
    };

    // Status snapshot task (issue #184 follow-up): every `status_interval_secs`, atomically
    // rewrite `status.json` with current health (bankroll, counts, watchlist size, Supabase RPC
    // count, uptime) — the agent-friendly "how is it doing?" file. `0` disables it.
    let status_task = if cfg.status_interval_secs > 0 {
        Some(tokio::spawn(pe_service::status_writer::run_status_writer(
            cfg.status_path.clone(),
            Duration::from_secs(cfg.status_interval_secs),
            paper_state.clone(),
            live_watchlist.clone(),
            applied_watchlist_capacity.clone(),
            cfg.mode.clone(),
            cfg.supabase_authoritative,
            supabase_rpc_calls,
            live_accounts.clone(),
            Some(health.clone()),
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
    if let Some(t) = activity_ingest_task {
        t.abort();
    }
    http_task.abort();
    resolution_task.abort();
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
    if let Some(t) = status_task {
        t.abort();
    }
    if let Some(t) = config_poll_task {
        t.abort();
    }
    if let Some(t) = capacity_task {
        t.abort();
    }
    if let Some(t) = live_fanout_task {
        t.abort();
    }
    info!("pe-service stopped");
    Ok(())
}

fn live_journal_path(event_log_path: &std::path::Path) -> PathBuf {
    event_log_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| std::path::Path::new("."))
        .join("live_journal.log")
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
    // #511: frame-only reconstruction cannot know authority dispositions (a refused or
    // ambiguously-failed frame would resurrect locally and diverge from Supabase).
    // Rebuild is a LEGACY-mode tool.
    anyhow::ensure!(
        !cfg.supabase_authoritative,
        "--rebuild-state is refused in authoritative mode (PE_SUPABASE_AUTHORITATIVE=true): \
         local frame replay cannot know authority dispositions. Restore locally by restarting \
         the service — the boot frame-walk converges SQLite on the Supabase system of record."
    );
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

    // #511: restore the settled-markets authority from the backup BEFORE replaying, so
    // replay refuses fills into already-settled markets instead of resurrecting
    // never-creditable positions.
    if backup_path.exists() {
        let restored = paper_state
            .restore_settled_markets_from(&backup_path)
            .context("restore settled_markets from backup")?;
        println!("  settled markets restored: {restored}");
    }

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

/// One-time SQLite → Supabase backfill for the authoritative cutover (issue #397), then exit.
///
/// Reconciles the local paper-state from the event log (so its scalars are complete), then
/// pushes `paper_bankroll` + `paper_positions`, completes the `paper_fills` tail, and seeds
/// the catch-up watermark to the event-log head. Run with the service stopped, after the
/// schema is applied and before flipping `PE_SUPABASE_AUTHORITATIVE`.
async fn run_backfill_supabase() -> Result<()> {
    let cfg = load_config()?;
    anyhow::ensure!(
        !cfg.supabase_url.is_empty(),
        "--backfill-supabase requires PE_SUPABASE_URL"
    );
    anyhow::ensure!(
        !cfg.supabase_secret_key.is_empty(),
        "--backfill-supabase requires the service-role PE_SUPABASE_SECRET_KEY (RLS blocks anon writes)"
    );

    let paper_state = PaperStateDb::open(&cfg.paper_state_db_path)
        .with_context(|| format!("open paper-state {}", cfg.paper_state_db_path.display()))?;
    let configured_bankroll = Decimal::from_str(&cfg.bankroll_usd)
        .with_context(|| format!("parse bankroll_usd '{}'", cfg.bankroll_usd))?;
    paper_state
        .init_bankroll(configured_bankroll)
        .context("init bankroll")?;
    let reconciled = reconcile_paper_state(&cfg.event_log_path, &paper_state)
        .context("reconcile paper-state from event log")?;

    let counts = backfill_supabase(
        &paper_state,
        &cfg.supabase_url,
        &cfg.supabase_anon_key,
        &cfg.supabase_secret_key,
    )
    .await
    .context("backfill supabase")?;

    println!("Supabase backfill complete (SQLite → Supabase):");
    println!("  event-log fills reconciled: {reconciled}");
    println!("  paper_bankroll set:         {}", counts.bankroll_set);
    println!("  paper_positions upserted:   {}", counts.positions);
    println!("  paper_fills HWM:            {}", counts.fills_hwm);
    println!("  supabase watermark (head):  {}", counts.watermark);
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
    supabase_state: Option<SupabaseStateClient>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let fetcher = GammaResolutionFetcher::new(
            gamma_base_url,
            ReqwestFetcher::new(reqwest::Client::new()).with_min_interval_ms(50),
        );
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(poll_interval_secs)).await;
            if let Err(e) = tick_resolution(
                &paper_state,
                &fetcher,
                sink.as_ref(),
                supabase_state.as_ref(),
            )
            .await
            {
                tracing::warn!(error = %e, "resolution poll error");
            }
        }
    })
}

async fn tick_resolution(
    paper_state: &Arc<PaperStateDb>,
    fetcher: &GammaResolutionFetcher<ReqwestFetcher>,
    sink: Option<&SinkHandle>,
    supabase_state: Option<&SupabaseStateClient>,
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
        let credit;
        if let Some(sup) = supabase_state {
            // Authoritative (#397/#511): `apply_resolution_v2` FIRST — the credit is
            // computed INSIDE the RPC from `paper_positions` under the bankroll lock
            // (closing the read-then-resolve TOCTOU with fills), then mirrored to SQLite
            // with the RETURNED canonical values. On RPC error, leave the market
            // unsettled locally so the next tick retries (fail-closed).
            let _ = &market_positions; // authoritative credit is server-computed (#511)
            match apply_resolution_authoritative(
                sup,
                &mut store,
                &res.market_id,
                &res.outcome_prices,
                now_unix,
            )
            .await
            {
                Ok(_bankroll) => {
                    credit = store
                        .settled_credit(&res.market_id)
                        .unwrap_or(rust_decimal::Decimal::ZERO);
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        market = %res.market_id,
                        "authoritative apply_resolution failed; leaving unsettled for next-tick retry"
                    );
                    continue;
                }
            }
        } else {
            // Legacy (#511): settle + credit in ONE SQLite transaction, the credit
            // computed inside it from freshly-read positions — a fill committing between
            // an outside read and the settle can no longer be silently uncredited.
            let (applied_credit, _bankroll) = paper_state
                .settle_and_credit_from_positions(
                    &res.market_id,
                    &serde_json::to_string(
                        &res.outcome_prices
                            .iter()
                            .map(ToString::to_string)
                            .collect::<Vec<_>>(),
                    )
                    .context("encode outcome prices")?,
                    now_unix,
                    |positions| PnlLedger::resolution_credit(positions, &res.outcome_prices),
                )
                .context("settle and credit")?;
            credit = applied_credit;
            store
                .note_settled(
                    res.market_id.clone(),
                    res.outcome_prices.clone(),
                    applied_credit,
                    now_unix,
                )
                .context("note settled")?;
        }
        tracing::info!(market = %res.market_id, %credit, "resolution applied");
    }
    // Nudge the Supabase sink once per tick to re-upsert the settled set (canonical JSON).
    if any_settled && let Some(sink) = sink {
        sink.send_resolution();
    }
    Ok(())
}

fn parse_mode(s: &str) -> Result<ExecutionMode> {
    match s.to_lowercase().replace('-', "_").as_str() {
        "shadow" => Ok(ExecutionMode::Shadow),
        "paper" => Ok(ExecutionMode::Paper),
        "live_tiny" | "livetiny" | "promoted" => Err(anyhow::anyhow!(
            "ordinary live modes are retired; use the isolated inactive canary role"
        )),
        other => Err(anyhow::anyhow!(
            "unknown mode '{}'; expected shadow|paper",
            other
        )),
    }
}
