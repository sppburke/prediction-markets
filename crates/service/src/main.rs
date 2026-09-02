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
use pe_event_log::{Scanner, Writer};
use pe_execution_core::{ExecutionDispatcher, LiveJournal};
use pe_paper_state::{MigrationMetadata, MigrationPhase, PaperStateDb};
use pe_service::bucket_commit::BucketCommitEngine;
use pe_service::config::{self as service_config, ServiceConfig};
use pe_service::paper_migration::{
    PaperMigrationBoot, PaperMigrationPaths, append_remote_authority_snapshot,
    record_activation_facts, rollback_version_one, validate_initial_configuration,
    validate_migration_authority,
};
use pe_service::paper_recovery::{build_leader_ledger, reconcile_paper_state};
use pe_service::position_seeder::CausalPositionValidator;
use pe_source_polymarket_public::{ReconciliationFetcher, ReqwestFetcher};
use pe_strategy_winner_follow::{ExecutionMode, PaperExecutor, WinnerFollowStrategy};
use pe_trader_index::Watchlist;
use rust_decimal::Decimal;
use tokio::sync::{mpsc, watch};
use tracing::{error, info, warn};

use pe_paper_pnl::{GammaResolutionFetcher, PnlLedger, ResolutionStore};
use pe_service::clob_book::ReqwestClobBookFetcher;
use pe_service::config_poller::{
    CONFIG_POLL_INTERVAL_SECS, SupabaseConfigFetcher, capacity_request_channel,
    fetch_service_config, run_capacity_worker, run_config_poll_loop,
};
use pe_service::entry_gate::CopyEntryGateConfig;
use pe_service::health::new_shared_health_with_ws;
use pe_service::live_watchlist::{LiveWatchlist, projection_dirty_channel};
use pe_service::market_end_cache::MarketEndCache;
use pe_service::mid_price_cache::MidPriceCache;
use pe_service::orchestrator::{Orchestrator, OrchestratorConfig};
use pe_service::paper_api::PaperApiState;
use pe_service::runtime_config::{
    AppliedWatchlistCapacity, LiveRuntimeConfig, RuntimeConfigStatus, load_initial_runtime_config,
};
use pe_service::snapshot_worker::{SnapshotHandle, run_snapshot_worker};
use pe_service::supabase_backfill::backfill_supabase;
use pe_service::supabase_reader;
use pe_service::supabase_refresh::{WatchlistProjectionStatus, run_supabase_refresh_loop};
use pe_service::supabase_sink::{SinkHandle, SupabaseWriter, run_sink};
use pe_service::supabase_state::{
    SupabaseStateClient, apply_resolution_authoritative, supabase_authoritative_boot,
    supabase_authoritative_boot_observed,
};
use pe_service::supervisor::{
    SHUTDOWN_DEADLINE, ShutdownController, ShutdownPhase, TaskEvent, TaskExit, TaskFailure,
    TaskName, TaskResult, TaskSupervisor, cancel_at, cancel_result_at,
};
use pe_service::trade_poller::{
    TradePoller, TradePollerConfig, rebuild_reconciliation_obligations,
};
use pe_service::watchlist_admission::AdmissionPreparer;
use pe_service::watchlist_capacity::SupabaseWatchlistCapacity;
use pe_service::watchlist_maintenance::{MaintenanceConfig, MembershipMode, run_maintenance_loop};
use time::OffsetDateTime;

/// Longest HTTP 429 `Retry-After` the reconciliation fetcher waits out in-line (issue #555;
/// `docs/_GLOSSARY.md`): the venue answers a boot-bracket page burst with `Retry-After: 1`.
const RECONCILIATION_RATE_LIMIT_RETRY_SECS: u32 = 1;

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().skip(1).collect();
    if args.iter().any(|argument| argument == "--version") {
        println!("{}", pe_service::build_info::version_line());
        return Ok(());
    }
    if let Some(position) = args
        .iter()
        .position(|argument| argument == "--verify-staged-revision")
    {
        let expected = args
            .get(position + 1)
            .context("--verify-staged-revision requires a full Git object identity")?;
        pe_service::build_info::verify_staged_revision(expected)
            .context("verify staged binary identity")?;
        println!("{}", pe_service::build_info::version_line());
        return Ok(());
    }
    if let Some(position) = args
        .iter()
        .position(|argument| argument == "--verify-staged-identity")
    {
        let expected_revision = args.get(position + 1).context(
            "--verify-staged-identity requires a full Git object identity and BLAKE3 digest",
        )?;
        let expected_hash = args.get(position + 2).context(
            "--verify-staged-identity requires a full Git object identity and BLAKE3 digest",
        )?;
        let actual_hash =
            pe_service::build_info::verify_staged_identity(expected_revision, expected_hash)
                .context("verify staged binary identity and bytes")?;
        println!(
            "{} artifact_blake3={actual_hash}",
            pe_service::build_info::version_line()
        );
        return Ok(());
    }
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
    if env::args().any(|a| a == "--rollback-paper-v1") {
        return run_rollback_paper_v1();
    }

    let cfg = load_config()?;

    // Hold the rolling-log worker guards for the whole process; dropping them flushes the
    // non-blocking writers (losing buffered lines), so keep `log_guards` alive until exit.
    let log_guards =
        pe_service::logging::setup(&cfg.jsonl_log_path, "info", cfg.log_retention_days)?;
    info!("pe-service starting");

    let configured_bankroll = Decimal::from_str(&cfg.bankroll_usd)
        .with_context(|| format!("parse bankroll_usd '{}'", cfg.bankroll_usd))?;
    // (#398 step 8) The boot-time Kelly approval guard was removed: its invariant now lives in
    // `runtime_config::parse_config` and is re-enforced on every poll (and at boot via
    // `load_initial_runtime_config`), so a runtime override change is governed too — not just the
    // boot value. An above-ceiling override without the approval flag rejects the whole proposal.

    // Copy-entry gate posture. The leader-price band was removed in #339 — live sizing
    // is re-based on the current market price instead (see the orchestrator copy path).
    let entry_gate_config = CopyEntryGateConfig;

    // #398 WS1: Supabase `service_config` is authoritative for the non-secret runtime knobs.
    // Fetch and validate it once at boot inside the existing twenty-second request envelope.
    // No listener or producer starts without one complete valid hot snapshot (#544).
    // The ordinary paper service has no credentialed construction path. Supabase therefore cannot
    // promote it into live execution; the isolated canary binary owns separate credentials/state.
    let clob_creds_present = false;
    anyhow::ensure!(
        !cfg.supabase_url.is_empty(),
        "PE_SUPABASE_URL is required for the authoritative runtime snapshot"
    );
    let config_http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .context("build bounded initial service-config HTTP client")?;
    let initial_config_rows = fetch_service_config(
        &config_http_client,
        &cfg.supabase_url,
        &cfg.supabase_anon_key,
        &cfg.supabase_secret_key,
    )
    .await
    .context("required initial service_config fetch")?;
    let initial_runtime_config =
        load_initial_runtime_config(&initial_config_rows, &cfg, clob_creds_present)
            .context("validate required initial service_config snapshot")?;
    let mode = parse_mode(&initial_runtime_config.mode)?;
    let max_fill_price = initial_runtime_config.max_fill_price;
    let min_fill_price = initial_runtime_config.min_fill_price;
    let runtime_config_status = RuntimeConfigStatus::new(&initial_runtime_config);
    let live_runtime_config = LiveRuntimeConfig::new(initial_runtime_config.clone());

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

    let (projection_dirty, projection_dirty_rx) = projection_dirty_channel();
    let live_watchlist =
        LiveWatchlist::new_with_projection(initial_watchlist, projection_dirty.clone());
    let projection_status = WatchlistProjectionStatus::default();

    // Shared writer mutex (#350 WS1 PR-D): serializes the score-update refresh loop and the
    // maintenance tick's evict+backfill on the live watchlist's ArcSwap (readers stay lock-free).
    let watchlist_writer_lock = Arc::new(tokio::sync::Mutex::new(()));
    let applied_watchlist_capacity = AppliedWatchlistCapacity::new(initial_watchlist_size);

    let paper_schema_version = MigrationMetadata::schema_version(&cfg.paper_state_db_path)
        .context("inspect paper-state schema before migration boot")?;
    validate_migration_authority(paper_schema_version, cfg.supabase_authoritative)
        .context("validate paper migration authority mode")?;
    if paper_schema_version == 1 {
        // #544 activation prerequisites apply only to the one-time v1->v2 boot:
        // a later ordinary boot must accept any VALID applied snapshot (cap edits
        // in 1..=10_000 are legitimate after activation).
        validate_initial_configuration(&live_runtime_config.snapshot())
            .context("validate #544 activation configuration before migration")?;
    }
    let mut migration_boot = PaperMigrationBoot::prepare(
        PaperMigrationPaths {
            fixed_main: cfg.paper_state_db_path.clone(),
            source_log: cfg.source_event_log_path.clone(),
            paper_log: cfg.event_log_path.clone(),
            live_journal: live_journal_path(&cfg.event_log_path),
            legacy_history: cfg.legacy_wallet_history_path.clone(),
            binary_identity: build_identity().to_owned(),
        },
        OffsetDateTime::now_utc().unix_timestamp(),
    )
    .context("prepare or resume paper-state v2 migration")?;

    // Crash-safe paper-state mirror. During the one-time migration every boot
    // write targets the recorded side main until the bracket completes.
    // reconcile any event-log fills whose SQLite commit was lost to a crash, and
    // rehydrate the leader position ledger — all before the orchestrator runs.
    let mut paper_state = Arc::new(
        PaperStateDb::open(&migration_boot.active_main).with_context(|| {
            format!("open paper-state {}", migration_boot.active_main.display())
        })?,
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
        if migration_boot.session.is_some() {
            supabase_authoritative_boot_observed(
                &client,
                &paper_state,
                &cfg.event_log_path,
                |bankroll, positions| {
                    append_remote_authority_snapshot(
                        &cfg.source_event_log_path,
                        bankroll,
                        positions,
                    )
                },
            )
            .await
            .context("supabase authoritative migration boot (logged frame-walk then pull)")?;
        } else {
            supabase_authoritative_boot(&client, &paper_state, &cfg.event_log_path)
                .await
                .context("supabase authoritative boot (frame-walk then pull)")?;
        }
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

    // Durable fences are decision boundaries and filter BEFORE the bracket;
    // history completeness filters AFTER it — the bracket's complete fixed-end
    // activity catch-up is the reconciliation that promotes the one-time
    // conservative sidecar seed, so at the first v2 boot nothing is complete
    // until the bracket has run (#544 activation fix: the previous pre-bracket
    // non-empty assertion made the first migration boot fail closed forever).
    let fenced: std::collections::HashSet<_> = paper_state
        .wallet_fences()
        .context("load durable wallet fences")?
        .into_iter()
        .map(|fence| fence.wallet)
        .collect();
    live_watchlist.remove_fenced(&fenced);

    // Validate the initial evaluation universe before any producer can observe it.
    // The installed migration record names the immutable version-two source-log
    // generation to which every accepted bracket is bound.
    let source_binding = Scanner::verify(&cfg.source_event_log_path)
        .context("verify source-log generation before position bracket")?;
    let source_log_generation = serde_json::to_string(&serde_json::json!({
        "path": source_binding.path,
        "physical_tail": source_binding.physical_tail,
        "last_sequence": source_binding.last_sequence.map(|sequence| sequence.0),
        "last_hash": source_binding.last_hash.to_hex().to_string(),
    }))
    .context("encode source-log generation for position validation")?;
    // One fetcher owns the documented public-API rate gate for boot brackets
    // and runtime reconciliation (#544).
    let position_fetcher: Arc<dyn ReconciliationFetcher> = Arc::new(
        ReqwestFetcher::new(reqwest::Client::new())
            .with_rate_limit_retry_max_secs(RECONCILIATION_RATE_LIMIT_RETRY_SECS),
    );
    let boot_position_validator = if migration_boot.session.is_some() {
        CausalPositionValidator::new_recording(
            position_fetcher.clone(),
            cfg.polymarket_base_url.clone(),
            source_log_generation,
            &cfg.source_event_log_path,
        )
        .context("open migration source-log recorder")?
    } else {
        CausalPositionValidator::new(
            position_fetcher.clone(),
            cfg.polymarket_base_url.clone(),
            source_log_generation,
        )
    };
    let mut boot_engine = BucketCommitEngine::load(paper_state.clone(), leader_ledger)
        .context("load boot activity ledger owner")?;
    let boot_wallets = live_watchlist
        .snapshot()
        .entries
        .iter()
        .map(|entry| entry.wallet)
        .collect::<Vec<_>>();
    boot_position_validator
        .validate_direct(&boot_wallets, &mut boot_engine, &paper_state)
        .await
        .context("causal current-position validation for boot universe")?;
    let leader_ledger = boot_engine.into_ledger();
    drop(boot_position_validator);

    let complete_history = paper_state
        .complete_history_wallets()
        .context("reload durable history completeness after boot brackets")?;
    let history_incomplete: std::collections::HashSet<_> = live_watchlist
        .snapshot()
        .entries
        .iter()
        .filter(|entry| !complete_history.contains(&entry.wallet))
        .map(|entry| entry.wallet)
        .collect();
    live_watchlist.remove_fenced(&history_incomplete);
    anyhow::ensure!(
        !live_watchlist.snapshot().entries.is_empty(),
        "no wallets eligible after durable fence/history filtering"
    );

    if migration_boot.session.is_some() {
        let activation_obligations =
            rebuild_reconciliation_obligations(&cfg.source_event_log_path, &paper_state)
                .context("capture migration reconciliation obligations")?;
        record_activation_facts(
            &paper_state,
            &boot_wallets,
            &activation_obligations,
            build_identity(),
        )?;
    }

    if let Some(session) = migration_boot.session.take() {
        let state = Arc::try_unwrap(paper_state).map_err(|_| {
            anyhow::anyhow!("paper migration retained a database handle at activation")
        })?;
        drop(state);
        session
            .finish()
            .context("activate version-two paper main")?;
        paper_state = Arc::new(PaperStateDb::open(&cfg.paper_state_db_path).with_context(
            || {
                format!(
                    "reopen installed paper-state {}",
                    cfg.paper_state_db_path.display()
                )
            },
        )?);
    }
    let installed_migration = MigrationMetadata::read(&cfg.paper_state_db_path)
        .context("read installed migration metadata after position validation")?
        .context("installed migration metadata missing after position validation")?;
    anyhow::ensure!(
        installed_migration.phase == MigrationPhase::Installed,
        "position validation requires installed migration metadata, found {}",
        installed_migration.phase
    );
    let _activation = installed_migration
        .activation_tails
        .as_ref()
        .context("installed migration metadata omitted activation tails")?;
    let runtime_source_binding = Scanner::verify(&cfg.source_event_log_path)
        .context("verify current source-log generation after paper activation")?;
    let runtime_source_generation = serde_json::to_string(&serde_json::json!({
        "path": runtime_source_binding.path,
        "physical_tail": runtime_source_binding.physical_tail,
        "last_sequence": runtime_source_binding.last_sequence.map(|sequence| sequence.0),
        "last_hash": runtime_source_binding.last_hash.to_hex().to_string(),
    }))
    .context("encode installed source-log generation")?;
    let position_validator = CausalPositionValidator::new(
        position_fetcher.clone(),
        cfg.polymarket_base_url.clone(),
        runtime_source_generation,
    );

    projection_dirty.mark();
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
    let task_status = health
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .task_status
        .clone();
    let (shutdown, _) = ShutdownController::new();
    let mut supervisor = TaskSupervisor::new(task_status.clone());
    supervisor.register_external(TaskName::JsonTracingFullAppender);
    supervisor.register_external(TaskName::JsonTracingErrorAppender);
    let listener = tokio::net::TcpListener::bind(&cfg.bind)
        .await
        .with_context(|| format!("bind {}", cfg.bind))?;

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
    let admission_preparer = AdmissionPreparer::with_validator(
        control_tx.clone(),
        paper_state.clone(),
        position_validator,
    );

    // Rebuild durable reader obligations before either source producer starts.
    // The existing source log plus aggregate records are sufficient, so #544
    // adds no second database or obligation table.
    let obligations = rebuild_reconciliation_obligations(&cfg.source_event_log_path, &paper_state)
        .context("rebuild durable activity reconciliation obligations")?;
    info!(
        obligations = obligations.len(),
        "activity obligations rebuilt"
    );

    // One bounded single-writer coordinator owns both websocket rows and every
    // fixed-end public page. Append acknowledgement precedes all triggers/apply.
    let sink = pe_service::source_event_sink::SourceEventSink::open(&cfg.source_event_log_path)
        .with_context(|| {
            format!(
                "open source event log {}",
                cfg.source_event_log_path.display()
            )
        })?;
    let (source_log, source_rx) =
        pe_service::activity_ingest::SourceLogHandle::channel(cfg.polymarket_channel_capacity);
    let (trigger_tx, trigger_rx) = mpsc::channel(cfg.polymarket_channel_capacity);
    let mut start = producer_start_rx.clone();
    let activity_watchlist = live_watchlist.clone();
    let activity_health = health.clone();
    let activity_ws_enabled = cfg.polymarket_activity_ws_enabled;
    let activity_shutdown = shutdown.subscribe();
    supervisor.spawn(TaskName::ActivityIngest, async move {
        if start.wait_for(|started| *started).await.is_err() {
            return Ok(TaskExit::ChannelClosed("producer_start"));
        }
        let ingest = if activity_ws_enabled {
            pe_service::activity_ingest::ActivityIngest::new(
                activity_watchlist,
                sink,
                source_rx,
                trigger_tx,
                activity_health,
            )
        } else {
            pe_service::activity_ingest::ActivityIngest::poll_only(
                sink,
                source_rx,
                trigger_tx,
                activity_health,
            )
        };
        ingest
            .run_until(activity_shutdown.wait_for(ShutdownPhase::StopProducers))
            .await
            .map(|()| TaskExit::CleanShutdown)
            .map_err(TaskFailure::typed)
    });

    // Polymarket trade poller task. Reads the live wallet set per poll round (#339).
    let mut poller_start = producer_start_rx;
    let poller_watchlist = live_watchlist.clone();
    let poller_paper_state = paper_state.clone();
    let poller_health = health.clone();
    let poller_base_url = cfg.polymarket_base_url.clone();
    let poller_ws_enabled = cfg.polymarket_activity_ws_enabled;
    let poller_copy_latency_budget_secs = cfg.copy_latency_budget_secs;
    let poller_control_tx = control_tx.clone();
    let poller_runtime_config = live_runtime_config.clone();
    let poller_admission_preparer = admission_preparer.clone();
    let public_poll_shutdown = shutdown.subscribe();
    supervisor.spawn(TaskName::PublicActivityPoll, async move {
        if poller_start.wait_for(|started| *started).await.is_err() {
            return Ok(TaskExit::ChannelClosed("producer_start"));
        }
        TradePoller::new(
            TradePollerConfig {
                base_url: poller_base_url,
                poll_interval_secs: cfg.trade_poll_interval_secs,
                activity_ws_enabled: poller_ws_enabled,
                copy_latency_budget_secs: poller_copy_latency_budget_secs,
            },
            poller_watchlist,
            position_fetcher,
            source_log,
            trigger_rx,
            poller_control_tx,
            poller_paper_state,
            poller_health,
            Default::default(),
            poller_runtime_config,
            obligations,
            Some(poller_admission_preparer),
        )
        .run_until(public_poll_shutdown.wait_for(ShutdownPhase::StopProducers))
        .await
        .map(|()| TaskExit::CleanShutdown)
        .map_err(TaskFailure::typed)
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
    let sink_handle =
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
            let sink_state = paper_state.clone();
            let reconcile_interval = Duration::from_secs(cfg.supabase_sink_reconcile_interval_secs);
            supervisor.spawn(TaskName::SupabaseAnalyticsSink, async move {
                run_sink(writer, sink_state, rx, reconcile_interval, dropped).await;
                Ok(TaskExit::ChannelClosed("supabase_sink_events"))
            });
            Some(handle)
        } else {
            None
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

    let snapshot_handle = if cfg.supabase_sink_enabled && !cfg.supabase_url.is_empty() {
        let (handle, rx) = SnapshotHandle::channel(cfg.snapshot_channel_capacity);
        let writer = SupabaseWriter::new(
            reqwest::Client::new(),
            &cfg.supabase_url,
            &cfg.supabase_anon_key,
            &cfg.supabase_secret_key,
        );
        let dropped = handle.dropped_counter();
        let snapshot_mid_cache = mid_price_cache.clone();
        let snapshot_book_fetcher = book_fetcher.clone();
        let snapshot_state = paper_state.clone();
        supervisor.spawn(TaskName::LiquiditySnapshotWorker, async move {
            run_snapshot_worker(
                rx,
                snapshot_mid_cache,
                snapshot_book_fetcher,
                snapshot_state,
                Some(writer),
                dropped,
            )
            .await;
            Ok(TaskExit::ChannelClosed("liquidity_snapshot_requests"))
        });
        Some(handle)
    } else {
        None
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
        let accounts_poller = pe_service::live_accounts::run_live_accounts_poller(
            live.clone(),
            accounts_http_client,
            cfg.supabase_url.clone(),
            cfg.supabase_anon_key.clone(),
            cfg.supabase_secret_key.clone(),
            CONFIG_POLL_INTERVAL_SECS,
        );
        supervisor.spawn(
            TaskName::LiveAccountsPoller,
            cancel_at(
                accounts_poller,
                shutdown.subscribe(),
                ShutdownPhase::StopProducers,
            ),
        );
        Some(live)
    };

    // Ordinary #508 live execution: one task owns strict account/seed ordering, mode probes,
    // redemption posture, and retention. A missing age identity is warned exactly once and
    // passed as `None`; the mode machine then cannot arm, while the paper orchestrator remains
    // fully operational. The account-tagged journal is a mode-0600 sibling of the paper log.
    if let Some(live_accounts) = live_accounts.clone() {
        let identity = match pe_service::live_credentials::load_identity_from_credentials_dir() {
            Ok(identity) => Some(identity),
            Err(error) => {
                tracing::warn!(error = %error, "ordinary live age identity unavailable; live arming disabled");
                None
            }
        };
        let journal_path = live_journal_path(&cfg.event_log_path);
        let journal = LiveJournal::open(&journal_path).with_context(|| {
            format!("open and validate live journal {}", journal_path.display())
        })?;
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
        let fanout_config = pe_service::live_fanout::LiveFanoutConfig {
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
            projection_reconcile_interval_secs: cfg.supabase_sink_reconcile_interval_secs,
        };
        let fanout_health = health.clone();
        let fanout_shutdown = shutdown.subscribe();
        supervisor.spawn(TaskName::LiveFanout, async move {
            let result = pe_service::live_fanout::run_live_fanout_until(
                fanout_config,
                fanout_shutdown.wait_for(ShutdownPhase::StopSinks),
            )
            .await;
            if result.is_err() {
                let mut health = fanout_health
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                health.live_durability_uncertain = true;
                health.refresh_event_log_writable();
            }
            result
                .map(|()| TaskExit::CleanShutdown)
                .map_err(TaskFailure::typed)
        });
    }

    let mut orch = Orchestrator::new(
        trade_rx,
        live_watchlist.clone(),
        OrchestratorConfig {
            bankroll,
            mode,
            signal_config: Default::default(),
            max_resolution_horizon_secs: initial_runtime_config.max_resolution_horizon_secs,
            min_resolution_horizon_secs: initial_runtime_config.min_resolution_horizon_secs,
            activity_ws_enabled: cfg.polymarket_activity_ws_enabled,
            copy_latency_budget_secs: cfg.copy_latency_budget_secs,
            watchlist_writer_lock: Some(watchlist_writer_lock.clone()),
            max_fill_price,
            min_fill_price,
            paper_fill_haircut_bps: cfg.paper_fill_haircut_bps,
            paper_fill_slippage_bps: cfg.paper_fill_slippage_bps,
            fill_mode: initial_runtime_config.fill_mode,
            price_impact_cap_bps: initial_runtime_config.price_impact_cap_bps,
            entry_gate_config,
            runtime_config: Some(live_runtime_config.clone()),
            live_accounts: live_accounts.clone(),
        },
        WinnerFollowStrategy::new(initial_runtime_config.winner_follow_config()),
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
    let orchestrator_shutdown = shutdown.subscribe();
    supervisor.spawn(TaskName::Orchestrator, async move {
        orch.run_coordinated(orchestrator_shutdown.wait_for(ShutdownPhase::DrainOrchestrator))
            .await
            .map(|()| TaskExit::CleanShutdown)
            .map_err(TaskFailure::typed)
    });
    producer_start_tx
        .send(true)
        .map_err(|_| anyhow::anyhow!("source producer start gate closed"))?;

    // Cumulative authoritative-RPC counter for status.json — grabbed before `supabase_state`
    // is moved into the resolution task below; `None` when not in authoritative mode.
    let supabase_rpc_calls = supabase_state.as_ref().map(|c| c.call_counter());

    // Resolution polling task: periodically fetch Gamma for closed markets. In authoritative
    // mode (issue #397) it credits via the `apply_resolution` RPC; `sink_handle` is `None`,
    // so the best-effort `send_resolution` nudge is suppressed.
    let resolution_poller = run_resolution_poller(
        paper_state.clone(),
        cfg.gamma_base_url.clone(),
        cfg.gamma_resolution_poll_interval_secs,
        sink_handle.clone(),
        supabase_state,
        shutdown.subscribe().wait_for(ShutdownPhase::StopProducers),
    );
    supervisor.spawn(TaskName::ResolutionPoller, resolution_poller);

    // Live-watchlist refresh task (#339): poll Supabase on the configured interval and refresh
    // the scores of the live set (score-update-only, #350 WS1). Spawned only when configured.
    let refresh = run_supabase_refresh_loop(
        live_watchlist.clone(),
        paper_state.clone(),
        reqwest::Client::new(),
        cfg.supabase_url.clone(),
        cfg.supabase_anon_key.clone(),
        cfg.supabase_secret_key.clone(),
        applied_watchlist_capacity.clone(),
        cfg.supabase_refresh_interval_secs,
        watchlist_writer_lock.clone(),
        projection_dirty_rx,
        projection_status.clone(),
    );
    supervisor.spawn(
        TaskName::WatchlistRefresh,
        cancel_at(refresh, shutdown.subscribe(), ShutdownPhase::StopProducers),
    );

    // Watchlist maintenance tick (#350 WS1 PR-D): inactivity + underperformance knockout +
    // atomic backfill. Spawned only when Supabase is configured and the interval is non-zero.
    if !cfg.supabase_url.is_empty() && cfg.maintenance_interval_secs > 0 {
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
        let maintenance = run_maintenance_loop(
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
        );
        supervisor.spawn(
            TaskName::WatchlistMaintenance,
            cancel_at(
                maintenance,
                shutdown.subscribe(),
                ShutdownPhase::StopProducers,
            ),
        );
    }

    // Service-config polling remains fixed at 30 seconds while a separate latest-only worker
    // performs slow admission preparation. The poller stays the sole RuntimeConfig writer; the
    // capacity worker commits membership + its applied epoch under the structural mutex and sends
    // a small completion back to the poller.
    if !cfg.supabase_url.is_empty() {
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
            admission_preparer.clone(),
            capacity_http_client,
            cfg.supabase_url.clone(),
            cfg.supabase_anon_key.clone(),
            cfg.supabase_secret_key.clone(),
        );
        let worker = run_capacity_worker(
            capacity_applier,
            applied_watchlist_capacity.clone(),
            capacity_request_rx,
            capacity_result_tx,
        );
        supervisor.spawn(
            TaskName::CapacityWorker,
            cancel_result_at(worker, shutdown.subscribe(), ShutdownPhase::StopProducers),
        );
        let config_http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(20))
            .build()
            .context("build bounded service-config HTTP client")?;
        let poller = run_config_poll_loop(
            live_runtime_config.clone(),
            runtime_config_status.clone(),
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
            Some(health.clone()),
        );
        supervisor.spawn(
            TaskName::RuntimeConfigPoller,
            cancel_result_at(poller, shutdown.subscribe(), ShutdownPhase::StopProducers),
        );
    }

    // A zero interval disables periodic writes but retains the critical owner so coordinated
    // shutdown still publishes one final identity/task snapshot (#544).
    let status_interval =
        (cfg.status_interval_secs > 0).then(|| Duration::from_secs(cfg.status_interval_secs));
    // Register the final two owners before the status writer's immediate first tick.
    task_status.register(TaskName::HttpServer);
    let status_writer = pe_service::status_writer::run_status_writer(
        cfg.status_path.clone(),
        status_interval,
        paper_state.clone(),
        live_watchlist.clone(),
        applied_watchlist_capacity.clone(),
        live_runtime_config.clone(),
        runtime_config_status.clone(),
        projection_status.clone(),
        cfg.supabase_authoritative,
        supabase_rpc_calls,
        live_accounts.clone(),
        Some(health.clone()),
        task_status.clone(),
        shutdown.subscribe().wait_for(ShutdownPhase::FinalStatus),
    );
    supervisor.spawn(TaskName::StatusWriter, async move {
        status_writer
            .await
            .map(|()| TaskExit::CleanShutdown)
            .map_err(TaskFailure::typed)
    });

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
        .with_state(health.clone())
        .layer(axum::Extension(paper_api_state));
    info!(bind = %cfg.bind, "pe-service listening");
    let http_shutdown = shutdown.subscribe();
    supervisor.spawn(TaskName::HttpServer, async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(http_shutdown.wait_for(ShutdownPhase::StopHttp))
            .await
            .map(|()| TaskExit::CleanShutdown)
            .map_err(TaskFailure::typed)
    });

    let initial_failure = loop {
        tokio::select! {
            signal = tokio::signal::ctrl_c() => {
                signal.context("wait for shutdown signal")?;
                info!("shutdown signal received");
                break None;
            }
            event = supervisor.observe_next() => {
                let Some(event) = event else {
                    break Some("all production owners exited".to_owned());
                };
                log_task_event(&event);
                if event.initiates_shutdown() {
                    break Some(format_task_failure(&event));
                }
            }
        }
    };

    // One application-wide deadline bounds every drain and join. A timeout aborts only the named
    // stragglers and immediately joins those abort completions; no task is detached (#544).
    let deadline = tokio::time::Instant::now() + SHUTDOWN_DEADLINE;
    advance_shutdown(&shutdown, &task_status, ShutdownPhase::StopProducers);
    let producers = [
        TaskName::ActivityIngest,
        TaskName::PublicActivityPoll,
        TaskName::ResolutionPoller,
        TaskName::LiveAccountsPoller,
        TaskName::WatchlistRefresh,
        TaskName::WatchlistMaintenance,
        TaskName::CapacityWorker,
        TaskName::RuntimeConfigPoller,
    ];
    let mut shutdown_timed_out = !join_named_until(&mut supervisor, &producers, deadline).await;
    drop(producer_start_tx);
    drop(admission_preparer);
    drop(control_tx);
    drop(trade_tx);

    advance_shutdown(&shutdown, &task_status, ShutdownPhase::DrainOrchestrator);
    shutdown_timed_out |=
        !join_named_until(&mut supervisor, &[TaskName::Orchestrator], deadline).await;
    drop(sink_handle);

    advance_shutdown(&shutdown, &task_status, ShutdownPhase::StopSinks);
    let sinks = [
        TaskName::LiveFanout,
        TaskName::SupabaseAnalyticsSink,
        TaskName::LiquiditySnapshotWorker,
    ];
    shutdown_timed_out |= !join_named_until(&mut supervisor, &sinks, deadline).await;

    advance_shutdown(&shutdown, &task_status, ShutdownPhase::StopHttp);
    shutdown_timed_out |=
        !join_named_until(&mut supervisor, &[TaskName::HttpServer], deadline).await;

    // Mark appenders stopping before the final snapshot; their guards flush and join last.
    task_status.advance_phase(ShutdownPhase::Complete);
    shutdown.advance(ShutdownPhase::FinalStatus);
    // The final status write gets its own minimum budget even when earlier
    // phases exhausted the shared deadline (#544 review round 3): the last
    // snapshot is the restart operator's primary evidence and must not be
    // aborted just because a producer overspent.
    let final_status_deadline =
        deadline.max(tokio::time::Instant::now() + pe_service::supervisor::POST_ABORT_JOIN_BOUND);
    shutdown_timed_out |= !join_named_until(
        &mut supervisor,
        &[TaskName::StatusWriter],
        final_status_deadline,
    )
    .await;
    drop(log_guards);
    task_status.mark_stopped(TaskName::JsonTracingFullAppender);
    task_status.mark_stopped(TaskName::JsonTracingErrorAppender);
    shutdown.advance(ShutdownPhase::Complete);
    if !supervisor.join_all_bounded().await {
        // A pinned non-yielding task never observes abort, and dropping the
        // runtime would wait on it forever: force the bounded exit the plan
        // promises — durable state recovers on the next start (#544 review).
        eprintln!("pe-service: final join bound expired with unjoined owners; forcing exit");
        std::process::exit(70);
    }

    info!("pe-service stopped");
    if shutdown_timed_out {
        anyhow::bail!("coordinated shutdown exceeded {SHUTDOWN_DEADLINE:?}");
    }
    if let Some(failure) = initial_failure {
        anyhow::bail!("critical production owner failed: {failure}");
    }
    Ok(())
}

fn advance_shutdown(
    controller: &ShutdownController,
    task_status: &pe_service::supervisor::TaskStatus,
    phase: ShutdownPhase,
) {
    task_status.advance_phase(phase);
    controller.advance(phase);
}

fn format_task_failure(event: &TaskEvent) -> String {
    event.failure.as_ref().map_or_else(
        || format!("{} exited", event.name),
        |failure| format!("{}: {:?}: {}", event.name, failure.kind, failure.message),
    )
}

fn log_task_event(event: &TaskEvent) {
    if event.failure.is_some() {
        match event.class {
            pe_service::supervisor::TaskClass::Critical => {
                error!(task = %event.name, failure = %format_task_failure(event), "critical owner exited")
            }
            pe_service::supervisor::TaskClass::BestEffortAnalytics
            | pe_service::supervisor::TaskClass::BestEffortObservability => {
                warn!(task = %event.name, failure = %format_task_failure(event), "best-effort owner degraded")
            }
        }
    } else {
        info!(task = %event.name, "owner joined after coordinated shutdown");
    }
}

async fn join_named_until(
    supervisor: &mut TaskSupervisor,
    names: &[TaskName],
    deadline: tokio::time::Instant,
) -> bool {
    loop {
        if !names.iter().any(|name| supervisor.is_running(*name)) {
            return true;
        }
        match tokio::time::timeout_at(deadline, supervisor.observe_next()).await {
            Ok(Some(event)) => log_task_event(&event),
            Ok(None) => return true,
            Err(_) => {
                supervisor.abort_and_join(names).await;
                return false;
            }
        }
    }
}

fn live_journal_path(event_log_path: &std::path::Path) -> PathBuf {
    event_log_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| std::path::Path::new("."))
        .join("live_journal.log")
}

fn build_identity() -> &'static str {
    pe_service::build_info::embedded().source_revision
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

fn run_rollback_paper_v1() -> Result<()> {
    let cfg = load_config()?;
    let version_one_backup = env::var_os("PE_PAPER_V1_BACKUP_PATH")
        .map(PathBuf::from)
        .context("--rollback-paper-v1 requires PE_PAPER_V1_BACKUP_PATH")?;
    let failed_side = env::var_os("PE_PAPER_FAILED_SIDE_PATH")
        .map(PathBuf::from)
        .context("--rollback-paper-v1 requires PE_PAPER_FAILED_SIDE_PATH")?;
    rollback_version_one(
        &PaperMigrationPaths {
            fixed_main: cfg.paper_state_db_path,
            source_log: cfg.source_event_log_path,
            paper_log: cfg.event_log_path.clone(),
            live_journal: live_journal_path(&cfg.event_log_path),
            legacy_history: cfg.legacy_wallet_history_path,
            binary_identity: build_identity().to_owned(),
        },
        &version_one_backup,
        &failed_side,
    )?;
    println!("paper-state version-one main restored; WAL/SHM sidecars were not restored");
    Ok(())
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
async fn run_resolution_poller(
    paper_state: Arc<PaperStateDb>,
    gamma_base_url: String,
    poll_interval_secs: u64,
    sink: Option<SinkHandle>,
    supabase_state: Option<SupabaseStateClient>,
    shutdown: impl std::future::Future<Output = ()>,
) -> TaskResult {
    let fetcher = GammaResolutionFetcher::new(
        gamma_base_url,
        ReqwestFetcher::new(reqwest::Client::new()).with_min_interval_ms(50),
    );
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            () = &mut shutdown => return Ok(TaskExit::CleanShutdown),
            () = tokio::time::sleep(Duration::from_secs(poll_interval_secs.max(1))) => {}
        }
        if let Err(error) = tick_resolution(
            &paper_state,
            &fetcher,
            sink.as_ref(),
            supabase_state.as_ref(),
        )
        .await
        {
            warn!(error = %error, "resolution poll error");
        }
    }
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
