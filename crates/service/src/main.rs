#![forbid(unsafe_code)]

use std::collections::{HashMap, HashSet};
use std::env;
use std::path::PathBuf;
use std::str::FromStr as _;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use axum::{Router, routing::get};
use pe_core_types::{PolymarketConditionId, ReceivedAt, SourceId, SourceTimestamp, WalletAddress};
use pe_event_log::{ContentType, EnvelopeIn, Scanner, Writer};
use pe_execution_core::LiveJournal;
use pe_paper_state::{MigrationMetadata, MigrationPhase, PaperStateDb};
use pe_service::asset_identity::AssetIdentityResolver;
use pe_service::bucket_commit::BucketCommitEngine;
use pe_service::config::{self as service_config, ServiceConfig};
use pe_service::paper_migration::{
    PaperMigrationBoot, PaperMigrationPaths, append_remote_authority_snapshot,
    record_activation_facts, rollback_version_one, update_installed_log_paths,
    validate_initial_configuration, validate_migration_authority,
};
use pe_service::paper_recovery::{
    active_risk_halts, build_leader_ledger, paper_era, reconcile_paper_state, replay_membership,
    scan_paper_log,
};
use pe_service::position_seeder::CausalPositionValidator;
use pe_source_polymarket_public::{
    CLOB_RESOLUTION_PARSER_VERSION, CLOB_RESOLUTION_SCHEMA_VERSION, ClobPayoutResolution,
    GAMMA_BATCH_SIZE, PageFetcher, ReconciliationFetcher, ReqwestFetcher, parse_clob_market,
};
use pe_strategy_winner_follow::{ExecutionMode, WinnerFollowStrategy};
use pe_trader_index::Watchlist;
use rust_decimal::Decimal;
use tokio::sync::{mpsc, watch};
use tracing::{error, info, warn};

use pe_paper_pnl::{PnlLedger, ResolutionStore};
use pe_service::clob_book::ReqwestClobBookFetcher;
use pe_service::config_poller::{
    CONFIG_POLL_INTERVAL_SECS, QualificationSealHandle, RiskHaltReleaseHandle,
    SupabaseConfigFetcher, capacity_request_channel, fetch_service_config,
    partition_risk_halt_release_hash, run_capacity_worker, run_config_poll_loop,
};
use pe_service::entry_gate::CopyEntryGateConfig;
use pe_service::health::new_shared_health_with_ws;
use pe_service::live_watchlist::{LiveWatchlist, projection_dirty_channel};
use pe_service::market_end_cache::MarketEndCache;
use pe_service::mid_price_cache::MidPriceCache;
use pe_service::orchestrator::{Orchestrator, OrchestratorConfig};
use pe_service::paper_api::PaperApiState;
use pe_service::runtime_config::{
    AppliedWatchlistCapacity, ConfigEra, LiveRuntimeConfig, MAX_ACTIVE_WATCHLIST_SIZE,
    RuntimeConfigStatus, load_initial_runtime_config,
};
use pe_service::snapshot_worker::{SnapshotHandle, run_snapshot_worker};
use pe_service::supabase_backfill::backfill_supabase;
use pe_service::supabase_reader;
use pe_service::supabase_refresh::{WatchlistProjectionStatus, run_supabase_refresh_loop};
use pe_service::supabase_sink::{SinkHandle, SupabaseWriter, run_sink};
use pe_service::supabase_state::{
    SourceEvidence, SupabaseStateClient, reconcile_active_financial_frames,
    supabase_authoritative_boot, supabase_authoritative_boot_observed,
};
use pe_service::supervisor::{
    SHUTDOWN_DEADLINE, ShutdownController, ShutdownPhase, TaskEvent, TaskExit, TaskFailure,
    TaskName, TaskResult, TaskSupervisor, cancel_at, cancel_result_at,
};
use pe_service::trade_poller::{
    TradePoller, TradePollerConfig, rebuild_reconciliation_obligations, recover_daily_boundary,
};
use pe_service::watchlist_admission::{AdmissionPreparer, anchor_refresh_due};
use pe_service::watchlist_capacity::SupabaseWatchlistCapacity;
use pe_service::watchlist_maintenance::{MaintenanceConfig, MembershipMode, run_maintenance_loop};
use time::OffsetDateTime;

/// Longest HTTP 429 `Retry-After` the reconciliation fetcher waits out in-line (issue #555;
/// `docs/_GLOSSARY.md`): the venue answers a boot-bracket page burst with `Retry-After: 1`.
const RECONCILIATION_RATE_LIMIT_RETRY_SECS: u32 = 1;

#[derive(Debug, PartialEq, Eq)]
struct BootAnchorSelection {
    reused: Vec<WalletAddress>,
    walked: Vec<WalletAddress>,
}

fn select_boot_anchor_wallets(
    paper_state: &PaperStateDb,
    wallets: &[WalletAddress],
    first_migration_boot: bool,
    now_unix: i64,
) -> Result<BootAnchorSelection, pe_paper_state::PaperStateError> {
    if first_migration_boot {
        return Ok(BootAnchorSelection {
            reused: Vec::new(),
            walked: wallets.to_vec(),
        });
    }
    let mut selection = BootAnchorSelection {
        reused: Vec::new(),
        walked: Vec::new(),
    };
    for wallet in wallets {
        let coverage = paper_state.wallet_coverage(wallet)?;
        let reusable = !paper_state.is_wallet_fenced(wallet)?
            && paper_state.wallet_history_complete(wallet)?
            && paper_state.cursor(wallet)?.is_some()
            && coverage.activity_cutoff_unix.is_some()
            && coverage.anchor_seq.is_some()
            && !anchor_refresh_due(
                &coverage,
                now_unix,
                pe_service::trade_poller::ANCHOR_REFRESH_SECS,
            );
        if reusable {
            selection.reused.push(*wallet);
        } else {
            selection.walked.push(*wallet);
        }
    }
    Ok(selection)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().skip(1).collect();
    let exit_after_anchors = args
        .iter()
        .any(|argument| argument == "--exit-after-anchors");
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
        let derived_revision = pe_service::build_info::embedded().source_revision;
        let derived_hash = {
            let executable = std::env::current_exe().context("resolve staged executable")?;
            let bytes = std::fs::read(&executable)
                .with_context(|| format!("read staged executable {}", executable.display()))?;
            blake3::hash(&bytes).to_hex().to_string()
        };
        let (expected_revision, expected_hash) = match (
            args.get(position + 1),
            args.get(position + 2),
        ) {
            (None, None) => (derived_revision, derived_hash.as_str()),
            (Some(revision), Some(hash)) => (revision.as_str(), hash.as_str()),
            _ => anyhow::bail!(
                "--verify-staged-identity accepts either no values or a full Git object identity and BLAKE3 digest"
            ),
        };
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
    if env::args().any(|a| a == "--update-paper-migration-paths") {
        return run_update_paper_migration_paths();
    }
    if args
        .iter()
        .any(|argument| argument == "--validate-open-continuations")
    {
        let paper_state = PaperStateDb::open_read_only(&PathBuf::from(required_arg_value(
            &args,
            "--paper-state",
        )?))?;
        let index = pe_service::risk_inputs::SourceReceiptIndex::replay(&PathBuf::from(
            required_arg_value(&args, "--source-log")?,
        ))
        .context("build verified source receipt index for open-continuation census")?;
        let validated =
            pe_service::bucket_commit::validate_open_continuations(&paper_state, &index)?;
        println!("open_rows={validated} validated={validated}");
        return Ok(());
    }
    if args.iter().any(|argument| argument == "--qualify") {
        let options = pe_service::qualification::QualifyOptions {
            paper_log: PathBuf::from(required_arg_value(&args, "--paper-log")?),
            source_log: PathBuf::from(required_arg_value(&args, "--source-log")?),
            live_journal: Some(PathBuf::from(required_arg_value(&args, "--live-journal")?)),
            paper_state: PathBuf::from(required_arg_value(&args, "--paper-state")?),
            seal_hash: required_arg_value(&args, "--seal-hash")?,
            output: PathBuf::from(required_arg_value(&args, "--output")?),
        };
        let (verdict, report_hash) = pe_service::qualification::run_qualify(&options)
            .await
            .context("run network-free sealed qualification")?;
        println!("verdict={verdict:?} report_blake3={report_hash}");
        return Ok(());
    }
    if let Some(raw_command) = optional_arg_value(&args, "--financial-era") {
        let command = match raw_command.as_str() {
            "prepare" => pe_service::qualification::FinancialEraCommand::Prepare,
            "start" => pe_service::qualification::FinancialEraCommand::Start,
            "rollback-check" => pe_service::qualification::FinancialEraCommand::RollbackCheck,
            value => anyhow::bail!(
                "--financial-era must be prepare, start, or rollback-check; got {value}"
            ),
        };
        let manifest = PathBuf::from(required_arg_value(&args, "--activation-manifest")?);
        let financial_config_rows = match command {
            pe_service::qualification::FinancialEraCommand::Prepare
            | pe_service::qualification::FinancialEraCommand::Start => Some(PathBuf::from(
                required_arg_value(&args, "--financial-config-rows")?,
            )),
            pe_service::qualification::FinancialEraCommand::RollbackCheck => None,
        };
        let config_path = args
            .first()
            .filter(|argument| !argument.starts_with("--"))
            .map(PathBuf::from);
        let offline_config = service_config::load(config_path.as_deref()).with_context(|| {
            config_path.as_ref().map_or_else(
                || "load financial-era config from environment".to_owned(),
                |path| format!("load financial-era config from {}", path.display()),
            )
        })?;
        let result = pe_service::qualification::run_financial_era(
            command,
            &manifest,
            &offline_config,
            financial_config_rows.as_deref(),
        )
        .context("run network-free financial-era command")?;
        println!("{result}");
        return Ok(());
    }

    let cfg = load_config()?;

    // Derive the financial era exactly once, from the verified paper log, before constructing
    // any HTTP client. A Start has no local-authority interpretation: credentials and the
    // Start-bound Supabase protocol are mandatory for every subsequent boot.
    let financial_era = if cfg.event_log_path.exists() {
        Some(paper_era(
            scan_paper_log(&cfg.event_log_path)
                .context("verify paper log and derive financial era")?,
        ))
    } else {
        None
    };
    let financial_start_record = financial_era.as_ref().and_then(|era| era.start.clone());
    let active_risk_halt_count = financial_era
        .as_ref()
        .map_or(0, |era| active_risk_halts(era).len());
    let financial_start = financial_start_record.as_ref().map(|(receipt, _)| *receipt);
    if financial_start.is_some() {
        anyhow::ensure!(
            cfg.supabase_authoritative,
            "QualificationStarted requires PE_SUPABASE_AUTHORITATIVE=1; blind local replay is forbidden"
        );
        anyhow::ensure!(
            !cfg.supabase_url.is_empty(),
            "QualificationStarted requires PE_SUPABASE_URL"
        );
        anyhow::ensure!(
            !cfg.supabase_secret_key.is_empty(),
            "QualificationStarted requires the service-role PE_SUPABASE_SECRET_KEY"
        );
    }

    // Hold the rolling-log worker guards for the whole process; dropping them flushes the
    // non-blocking writers (losing buffered lines), so keep `log_guards` alive until exit.
    let log_guards =
        pe_service::logging::setup(&cfg.jsonl_log_path, "info", cfg.log_retention_days)?;
    info!("pe-service starting");
    info!(
        active_risk_halt_count,
        "active paper risk causes rebuilt from the verified prefix"
    );

    // #572: an installed generation's source log is verified once, under the writer lock, and
    // every boot projection below is built from that walk. Any other migration phase keeps the
    // established per-owner scans.
    let migration_paths = PaperMigrationPaths {
        fixed_main: cfg.paper_state_db_path.clone(),
        source_log: cfg.source_event_log_path.clone(),
        paper_log: cfg.event_log_path.clone(),
        live_journal: live_journal_path(&cfg.event_log_path),
        legacy_history: cfg.legacy_wallet_history_path.clone(),
        binary_identity: build_identity().to_owned(),
    };
    let (mut source_log_boot, mut boot_sink, walked_source_binding) =
        match pe_service::source_log_boot::SourceLogBoot::open(
            &migration_paths,
            financial_start.is_some(),
        )
        .context("walk the installed source event log")?
        {
            Some(opened) => (Some(opened.boot), Some(opened.sink), Some(opened.binding)),
            None => (None, None, None),
        };

    let configured_bankroll = Decimal::from_str(&cfg.bankroll_usd)
        .with_context(|| format!("parse bankroll_usd '{}'", cfg.bankroll_usd))?;
    let starting_bankroll = financial_start_record
        .as_ref()
        .map_or(configured_bankroll, |(_, start)| {
            start.starting_bankroll.to_decimal()
        });
    if financial_start.is_some() {
        anyhow::ensure!(
            configured_bankroll == starting_bankroll,
            "configured bankroll {configured_bankroll} differs from QualificationStarted baseline {starting_bankroll}"
        );
    }
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
    // The one injected CLOB-resolution transport shares the boot-owned HTTP client, uses the
    // canonical 200 ms CLOB gate, and performs no private retry/backoff.
    let clob_resolution_fetcher = Arc::new(
        ReqwestFetcher::new(config_http_client.clone())
            .with_min_interval_ms(200)
            .with_max_retries(0),
    );
    let initial_config_rows = fetch_service_config(
        &config_http_client,
        &cfg.supabase_url,
        &cfg.supabase_anon_key,
        &cfg.supabase_secret_key,
    )
    .await
    .context("required initial service_config fetch")?;
    let config_era = if financial_start.is_some() {
        ConfigEra::Financial15
    } else {
        ConfigEra::Legacy17
    };
    let initial_partitioned = partition_risk_halt_release_hash(&initial_config_rows);
    if let Some(warning) = initial_partitioned.warning {
        warn!(?warning, "boot risk halt release row ignored");
    }
    let initial_release_hash = initial_partitioned.risk_halt_release_hash.clone();
    let initial_runtime_config = load_initial_runtime_config(
        &initial_partitioned.economic_rows,
        &cfg,
        clob_creds_present,
        config_era,
    )
    .context("validate required initial service_config snapshot")?;
    let mode = parse_mode(&initial_runtime_config.mode)?;
    let max_fill_price = initial_runtime_config.max_fill_price;
    let min_fill_price = initial_runtime_config.min_fill_price;
    let runtime_config_status = RuntimeConfigStatus::new(&initial_runtime_config);
    let live_runtime_config = LiveRuntimeConfig::new(initial_runtime_config.clone());

    // A financial Start makes the synchronized paper prefix the structural membership owner.
    // Its initial entries come from the exact ranking batch named by Start; subsequent replacement
    // vectors are verified and reconstructed from the source-log artifacts named by each durable
    // MembershipChanged record. Therefore a newer published batch with no synchronized membership
    // record remains a transition for the first maintenance tick instead of changing (or
    // invalidating) the boot generation.
    let initial_watchlist_size = live_runtime_config.snapshot().active_watchlist_size;
    let ranking_client = reqwest::Client::new();
    let (initial_watchlist, bootstrap_last_trade, boot_batch_marker): (
        Watchlist,
        HashMap<_, _>,
        Option<i64>,
    ) = if let Some((_, start)) = &financial_start_record {
        let (start_batch, mut start_last_trade) = supabase_reader::fetch_batch(
            &ranking_client,
            &cfg.supabase_url,
            &cfg.supabase_anon_key,
            &cfg.supabase_secret_key,
            start.ranking_batch_id,
            MAX_ACTIVE_WATCHLIST_SIZE,
        )
        .await
        .context("fetch QualificationStarted ranking batch")?;
        let era = financial_era
            .as_ref()
            .context("QualificationStarted is missing its financial era")?;
        let replayed = match source_log_boot.as_ref() {
            Some(boot) => boot.replay_membership(era, start_batch),
            None => replay_membership(era, start_batch, &cfg.source_event_log_path),
        }
        .context("replay Start-bound structural membership")?
        .context("QualificationStarted is missing from its financial era")?;
        let restored = replayed
            .watchlist
            .entries
            .iter()
            .map(|entry| entry.wallet)
            .collect::<HashSet<_>>();
        start_last_trade.retain(|wallet, _| restored.contains(wallet));
        (
            replayed.watchlist,
            start_last_trade,
            Some(replayed.last_ranking_batch_id),
        )
    } else {
        // Before a financial Start, `latest_ranking` remains the boot owner. Read its marker first
        // so a batch landing between the two reads is applied on the first maintenance tick. A
        // failed marker read becomes `None`, which also forces the first full-rerank tick (#542).
        let marker = supabase_reader::fetch_latest_batch_id(
            &ranking_client,
            &cfg.supabase_url,
            &cfg.supabase_anon_key,
            &cfg.supabase_secret_key,
        )
        .await
        .unwrap_or_default();
        let (watchlist, last_trade) = supabase_reader::fetch(
            &ranking_client,
            &cfg.supabase_url,
            &cfg.supabase_anon_key,
            &cfg.supabase_secret_key,
            initial_watchlist_size,
        )
        .await
        .context("bootstrap watchlist from Supabase (the sole pre-Start wallet source)")?;
        (watchlist, last_trade, marker)
    };
    info!(
        active = initial_watchlist.active_count,
        total = initial_watchlist.entries.len(),
        batch_id = ?boot_batch_marker,
        durable = financial_start.is_some(),
        "watchlist membership rebuilt"
    );

    // Fail fast if the selected Supabase batch returned no durable members — there is no fallback
    // source (#370). Both the pre-Start moving read and the Start-pinned read are survivor-filtered
    // (#518), so a batch with no surviving rows fails closed rather than running an empty set.
    anyhow::ensure!(
        !initial_watchlist.entries.is_empty(),
        "no wallets to copy: the boot membership generation contains no SURVIVING rows"
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
    let mut migration_boot = match source_log_boot.as_ref() {
        Some(boot) => boot.prepare_installed(
            migration_paths.clone(),
            OffsetDateTime::now_utc().unix_timestamp(),
        ),
        None => PaperMigrationBoot::prepare(
            migration_paths.clone(),
            OffsetDateTime::now_utc().unix_timestamp(),
        ),
    }
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
        .init_bankroll(starting_bankroll)
        .context("initialise paper-state bankroll")?;
    if financial_start.is_some() {
        anyhow::ensure!(
            paper_state
                .bankroll()
                .context("read Start-bound local bankroll")?
                == Some(starting_bankroll),
            "local bankroll differs from QualificationStarted before authority mutation"
        );
    }
    // #511: LEGACY-ONLY blind frame replay. In authoritative mode the boot frame-walk
    // below owns local application — every unresolved frame is decided by the authority
    // (`commit_fill_v2`), so a refused frame can never resurrect locally. The blind
    // replay would apply such frames unconditionally.
    if !cfg.supabase_authoritative && financial_start.is_none() {
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
        if let Some(start) = financial_start {
            let authoritative_bankroll = client
                .fetch_bankroll()
                .await
                .context("read Start-bound authoritative bankroll")?;
            anyhow::ensure!(
                authoritative_bankroll == Some(starting_bankroll),
                "authoritative bankroll {:?} differs from QualificationStarted baseline {}",
                authoritative_bankroll,
                starting_bankroll
            );
            paper_state
                .seed_financial_start(start)
                .context("seed local financial Start")?;
            client
                .seed_financial_start(start)
                .await
                .context("seed authoritative financial Start")?;
        } else if migration_boot.session.is_some() {
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

    // Open the sole paper writer and converge the active financial prefix before reading any
    // bankroll used for sizing or API state.
    let mut paper_writer = Writer::open(&cfg.event_log_path)
        .with_context(|| format!("open event log {}", cfg.event_log_path.display()))?;
    if financial_start.is_some() {
        let authority = supabase_state.as_ref().context(
            "active financial era requires the authoritative client before paper writer boot",
        )?;
        let boot_receipts = source_log_boot.as_ref().map(|boot| boot.receipt_index());
        let source_evidence = match boot_receipts.as_ref() {
            Some(index) => SourceEvidence::Index(index),
            None => SourceEvidence::Log(&cfg.source_event_log_path),
        };
        let recovered = reconcile_active_financial_frames(
            authority,
            &paper_state,
            &cfg.event_log_path,
            source_evidence,
            &mut paper_writer,
        )
        .await
        .context("recover active paper financial protocol")?;
        if recovered > 0 {
            info!(recovered, "completed unmatched paper Prepared records");
        }
    }

    // #508 Decision 10 (#511: AFTER frame dispositions exist in either mode): resume staged
    // dispatch aggregates — flip seeds whose fill frame reached a disposition, finalize
    // stuck seeds, leave redeliverable seeds pending. Never reconstructs targets.
    if financial_start.is_none() {
        pe_service::dispatch_recovery::resume_dispatch_seeds(&cfg.event_log_path, &paper_state)
            .context("resume dispatch seeds")?;
    }

    let bankroll = if financial_start.is_some() {
        paper_state
            .financial_snapshot(OffsetDateTime::now_utc().unix_timestamp())
            .context("read recovered active financial snapshot")?
            .cash
    } else {
        paper_state
            .bankroll()
            .context("read paper-state bankroll")?
            .unwrap_or(configured_bankroll)
    };
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
    let source_binding = match walked_source_binding {
        Some(binding) => binding,
        None => Scanner::verify(&cfg.source_event_log_path)
            .context("verify source-log generation before position bracket")?,
    };
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
    let boot_source_log = Arc::new(tokio::sync::Mutex::new(match boot_sink.take() {
        Some(sink) => sink,
        None => pe_service::source_event_sink::SourceEventSink::open(&cfg.source_event_log_path)
            .context("open boot source-log recorder")?,
    }));
    let asset_identity = Arc::new(AssetIdentityResolver::new(
        position_fetcher.clone(),
        cfg.gamma_base_url.clone(),
        GAMMA_BATCH_SIZE,
        Arc::clone(&boot_source_log),
    ));
    let boot_position_validator = if migration_boot.session.is_some() {
        CausalPositionValidator::new_recording(
            position_fetcher.clone(),
            cfg.polymarket_base_url.clone(),
            source_log_generation,
            Arc::clone(&boot_source_log),
            Arc::clone(&asset_identity),
        )
    } else {
        CausalPositionValidator::new(
            position_fetcher.clone(),
            cfg.polymarket_base_url.clone(),
            source_log_generation,
            Arc::clone(&asset_identity),
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
    let boot_anchor_selection = select_boot_anchor_wallets(
        &paper_state,
        &boot_wallets,
        migration_boot.session.is_some(),
        OffsetDateTime::now_utc().unix_timestamp(),
    )
    .context("select reusable boot anchors")?;
    info!(
        reused = boot_anchor_selection.reused.len(),
        walked = boot_anchor_selection.walked.len(),
        first_migration_boot = migration_boot.session.is_some(),
        "boot anchor selection census"
    );
    let anchored = boot_position_validator
        .validate_direct(
            &boot_anchor_selection.walked,
            &mut boot_engine,
            &paper_state,
        )
        .await
        .context("causal current-position validation for boot universe")?;
    let leader_ledger = boot_engine.into_ledger();
    drop(boot_position_validator);
    let (source_log, source_rx) =
        pe_service::activity_ingest::SourceLogHandle::channel(cfg.polymarket_channel_capacity);
    let resolution_source_log = source_log.clone();
    let orchestrator_source_log = source_log.clone();
    asset_identity.activate_runtime(source_log.clone()).await;
    // The boot recorder is unique again: walk the frames this boot appended under the same lock.
    let mut boot_sink = Some(
        Arc::try_unwrap(boot_source_log)
            .map_err(|_| {
                anyhow::anyhow!("boot source-log recorder retained a handle after the anchor walk")
            })?
            .into_inner(),
    );
    let walked_runtime_binding = match (source_log_boot.as_mut(), boot_sink.as_mut()) {
        (Some(boot), Some(sink)) => Some(
            boot.extend(sink)
                .context("walk the source-log frames appended during boot")?,
        ),
        _ => {
            boot_sink = None;
            None
        }
    };

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
    // Live at boot means accepted by this boot's bracket. Deferred wallets
    // re-enter only through the runtime admission preparer; fenced wallets stay
    // excluded. On a resumed side main an earlier bracket's promoted history
    // would otherwise keep a now-deferred wallet live.
    let anchored_wallets: Vec<_> = anchored.iter().map(|install| install.wallet).collect();
    let accepted_wallets: std::collections::HashSet<_> = anchored_wallets
        .iter()
        .chain(&boot_anchor_selection.reused)
        .copied()
        .collect();
    let not_accepted: std::collections::HashSet<_> = boot_wallets
        .iter()
        .filter(|wallet| !accepted_wallets.contains(wallet))
        .copied()
        .collect();
    live_watchlist.remove_fenced(&not_accepted);
    anyhow::ensure!(
        !live_watchlist.snapshot().entries.is_empty(),
        "no wallets eligible after durable fence/history/acceptance filtering"
    );

    if migration_boot.session.is_some() {
        let activation_obligations =
            rebuild_reconciliation_obligations(&cfg.source_event_log_path, &paper_state)
                .context("capture migration reconciliation obligations")?;
        record_activation_facts(
            &paper_state,
            &anchored_wallets,
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
    if exit_after_anchors {
        info!("boot anchors prepared; exiting before runtime writers and listeners");
        return Ok(());
    }
    let runtime_source_binding = match walked_runtime_binding {
        Some(binding) => binding,
        None => Scanner::verify(&cfg.source_event_log_path)
            .context("verify current source-log generation after paper activation")?,
    };
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
        Arc::clone(&asset_identity),
    );

    projection_dirty.mark();
    info!(bankroll = %bankroll, "paper-state opened");

    if financial_start.is_some() {
        pe_service::dispatch_recovery::resume_dispatch_seeds(&cfg.event_log_path, &paper_state)
            .context("resume active-era dispatch seeds after financial recovery")?;
    }
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
    let source_receipts = match source_log_boot.as_ref() {
        Some(boot) => boot.receipt_index(),
        None => pe_service::risk_inputs::SourceReceiptIndex::replay(&cfg.source_event_log_path)
            .context("build verified source receipt index")?,
    };
    let open_rows =
        pe_service::bucket_commit::validate_open_continuations(&paper_state, &source_receipts)
            .context("validate open decision continuations before resume")?;
    info!(open_rows, "open decision continuations validated");
    let risk_halt_release = financial_start.is_some().then(|| {
        RiskHaltReleaseHandle::new(
            cfg.event_log_path.clone(),
            source_receipts.clone(),
            control_tx.clone(),
        )
    });
    let qualification_seal = financial_start
        .is_some()
        .then(|| QualificationSealHandle::new(control_tx.clone()));
    let (producer_start_tx, producer_start_rx) = watch::channel(false);

    // Runtime admissions prove durable history and recheck the fence under the shared
    // watchlist-writer lock before publication. Lane D adds the causal positions bracket.
    let admission_preparer = AdmissionPreparer::with_validator(
        control_tx.clone(),
        paper_state.clone(),
        position_validator,
    )
    .with_source_log(orchestrator_source_log.clone());

    // Rebuild durable reader obligations before either source producer starts.
    // The existing source log plus aggregate records are sufficient, so #544
    // adds no second database or obligation table.
    let obligations = match source_log_boot.as_mut() {
        Some(boot) => boot
            .obligations(&paper_state, &cfg.event_log_path)
            .context("rebuild activity obligations from the boot walk")?,
        None => {
            let mut obligations =
                rebuild_reconciliation_obligations(&cfg.source_event_log_path, &paper_state)
                    .context("rebuild durable activity reconciliation obligations")?;
            if financial_start.is_some() {
                recover_daily_boundary(
                    &cfg.source_event_log_path,
                    &cfg.event_log_path,
                    &mut obligations,
                )
                .context("recover causal daily boundary")?;
            }
            obligations
        }
    };
    info!(
        obligations = obligations.len(),
        "activity obligations rebuilt"
    );

    // One bounded single-writer coordinator owns both websocket rows and every
    // fixed-end public page. Append acknowledgement precedes all triggers/apply.
    let sink = match (source_log_boot.as_ref(), boot_sink.take()) {
        (Some(boot), Some(mut sink)) => {
            boot.verify_handoff(&mut sink)
                .context("hand the walked source log to the runtime coordinator")?;
            sink
        }
        (None, None) => {
            pe_service::source_event_sink::SourceEventSink::open(&cfg.source_event_log_path)
                .with_context(|| {
                    format!(
                        "open source event log {}",
                        cfg.source_event_log_path.display()
                    )
                })?
        }
        _ => anyhow::bail!("boot source-log recorder and walk state disagree at the handoff"),
    };
    let (trigger_tx, trigger_rx) = mpsc::channel(cfg.polymarket_channel_capacity);
    let mut start = producer_start_rx.clone();
    let activity_watchlist = live_watchlist.clone();
    let activity_health = health.clone();
    let activity_ws_enabled = cfg.polymarket_activity_ws_enabled;
    let activity_shutdown = shutdown.subscribe();
    let activity_ingest = if activity_ws_enabled {
        pe_service::activity_ingest::ActivityIngest::new(
            activity_watchlist,
            sink,
            source_rx,
            trigger_tx,
            activity_health,
        )
        .with_source_receipt_index(source_receipts.clone())
    } else {
        pe_service::activity_ingest::ActivityIngest::poll_only(
            sink,
            source_rx,
            trigger_tx,
            activity_health,
        )
        .with_source_receipt_index(source_receipts.clone())
    };
    let reconciliation_obligations_dropped =
        activity_ingest.reconciliation_triggers_dropped_counter();
    supervisor.spawn(TaskName::ActivityIngest, async move {
        if start.wait_for(|started| *started).await.is_err() {
            return Ok(TaskExit::ChannelClosed("producer_start"));
        }
        activity_ingest
            .run_until(activity_shutdown.wait_for(ShutdownPhase::StopSinks))
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
    let poller_asset_identity = Arc::clone(&asset_identity);
    let poller_source_receipts = source_receipts.clone();
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
            poller_asset_identity,
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
        .with_source_receipt_index(poller_source_receipts)
        .run_until(public_poll_shutdown.wait_for(ShutdownPhase::StopProducers))
        .await
        .map(|()| TaskExit::CleanShutdown)
        .map_err(TaskFailure::typed)
    });

    // Orchestrator.
    // Dashboard-only end-time projection; active economics uses admission evidence directly.
    let market_end_cache = MarketEndCache::new(cfg.gamma_base_url.clone());
    // Mid-price cache for marking open dashboard positions to market (own rate gate).
    let mid_price_cache = MidPriceCache::new(cfg.gamma_base_url.clone())
        .with_source_log(orchestrator_source_log.clone());

    // Supabase analytics sink (issue #343): best-effort dual-write of fills + settlements.
    // Spawned only when enabled and a Supabase URL is configured; otherwise `None` (no-op).
    // Not spawned in authoritative mode (issue #397): the Prepared-sequenced
    // `commit_fill_v2`/`apply_resolution_v2` paths own `paper_fills`/`settled_markets`, so a
    // best-effort `merge-duplicates` upsert from `run_sink` must not race them. With
    // `sink_handle = None`
    // the active financial protocol remains the only paper-state writer; the liquidity-snapshot
    // worker (#350) keeps its own gate and stays alive.
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
            .with_source_log(orchestrator_source_log.clone())
            .with_base_url(cfg.polymarket_clob_base_url.clone()),
    );
    let admission_http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .context("build bounded market-admission HTTP client")?;
    let boundary_mark_fetcher = Arc::new(pe_service::mark_prices::HistoricalMarkAdapter::new(
        admission_http_client.clone(),
        cfg.polymarket_clob_base_url.clone(),
        orchestrator_source_log.clone(),
    ));
    let admission_builder = pe_service::live_venue_adapter::LiveAdmissionBuilder::new(
        admission_http_client,
        cfg.gamma_base_url.clone(),
        cfg.polymarket_clob_base_url.clone(),
        orchestrator_source_log,
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
        let qualification = match optional_arg_value(&args, "--qualification-report") {
            Some(path) => {
                match pe_service::live_mode::load_qualification_facts(&PathBuf::from(&path)) {
                    Ok(report) => Some(report),
                    Err(error) => {
                        tracing::warn!(
                            path,
                            error = %error,
                            "qualification report unavailable; live arming disabled"
                        );
                        None
                    }
                }
            }
            None => {
                tracing::warn!("--qualification-report was not supplied; live arming disabled");
                None
            }
        };
        let identity = match pe_service::live_credentials::load_identity_from_credentials_dir() {
            Ok(identity) => Some(identity),
            Err(error) => {
                tracing::warn!(error = %error, "ordinary live age identity unavailable; live arming disabled");
                None
            }
        };
        let journal_path = live_journal_path(&cfg.event_log_path);
        let era_live_prefix = if cfg.event_log_path.exists() {
            paper_era(
                scan_paper_log(&cfg.event_log_path)
                    .context("verify paper era for the live-journal prefix")?,
            )
            .start
            .map(|(_, start)| {
                Ok::<_, anyhow::Error>(pe_event_log::LogTailBinding {
                    path: journal_path.clone(),
                    physical_tail: start.live_prefix.physical_tail,
                    last_sequence: start.live_prefix.last_sequence,
                    last_hash: blake3::Hash::from_hex(&start.live_prefix.last_hash)
                        .context("decode QualificationStarted live-prefix hash")?,
                })
            })
            .transpose()?
        } else {
            None
        };
        if let Some(prefix) = &era_live_prefix {
            pe_event_log::Scanner::verify_prefix(prefix)
                .context("verify QualificationStarted live-journal prefix")?;
        }
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
        let live_book_fetcher = Arc::new(
            ReqwestClobBookFetcher::new(live_http_client.clone())
                .with_base_url(cfg.polymarket_clob_base_url.clone())
                .with_source_log(resolution_source_log.clone()),
        );
        let fanout_config = pe_service::live_fanout::LiveFanoutConfig {
            paper_state: paper_state.clone(),
            live_accounts,
            live_watchlist: live_watchlist.clone(),
            runtime_config: live_runtime_config.clone(),
            qualification,
            identity,
            journal: Arc::new(journal),
            journal_path,
            era_live_prefix,
            projection,
            book_fetcher: live_book_fetcher,
            mid_price_cache: mid_price_cache
                .clone()
                .with_source_log(resolution_source_log.clone()),
            source_log: resolution_source_log.clone(),
            source_receipts: source_receipts.clone(),
            paper_log_path: cfg.event_log_path.clone(),
            orchestrator_control: control_tx.clone(),
            http: live_http_client,
            polygon_receipt_rpc_url: cfg.polygon_receipt_rpc_url.clone(),
            supabase_url: cfg.supabase_url.clone(),
            supabase_anon_key: cfg.supabase_anon_key.clone(),
            supabase_secret_key: cfg.supabase_secret_key.clone(),
            gamma_base_url: cfg.gamma_base_url.clone(),
            clob_base_url: cfg.polymarket_clob_base_url.clone(),
            data_base_url: cfg.polymarket_base_url.clone(),
            projection_reconcile_interval_secs: cfg.supabase_sink_reconcile_interval_secs,
            shutdown: shutdown.clone(),
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
            price_impact_cap_bps: initial_runtime_config.price_impact_cap_bps,
            entry_gate_config,
            runtime_config: Some(live_runtime_config.clone()),
            live_accounts: live_accounts.clone(),
        },
        WinnerFollowStrategy::new(initial_runtime_config.winner_follow_config()),
        paper_writer,
        paper_state.clone(),
        leader_ledger,
        health.clone(),
        mid_price_cache.clone(),
        control_rx,
        sink_handle.clone(),
        snapshot_handle,
        supabase_state.clone(),
        book_fetcher,
    )
    .context("build orchestrator")?;
    if financial_start.is_some() {
        orch.configure_financial_log_paths(
            cfg.event_log_path.clone(),
            cfg.source_event_log_path.clone(),
            admission_builder,
            boundary_mark_fetcher,
            source_receipts,
        )
        .context("configure active financial protocol")?;
    }
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
    if let Some(handle) = qualification_seal.as_ref() {
        handle
            .apply(
                initial_runtime_config.canonical_hash(),
                pe_service::paper_recovery::FINANCIAL_SEMANTIC_VERSION,
            )
            .await
            .map_err(|error| {
                anyhow::anyhow!("synchronize boot qualification seal check: {error}")
            })?;
    }
    if let (Some(handle), Some(release_hash)) =
        (risk_halt_release.as_ref(), initial_release_hash.as_deref())
    {
        handle
            .apply(release_hash)
            .await
            .map_err(anyhow::Error::msg)
            .context("synchronize boot risk halt release")?;
    }
    producer_start_tx
        .send(true)
        .map_err(|_| anyhow::anyhow!("source producer start gate closed"))?;

    // Cumulative authoritative-RPC counter for status.json — grabbed before `supabase_state`
    // is moved into the resolution task below; `None` when not in authoritative mode.
    let supabase_rpc_calls = supabase_state.as_ref().map(|c| c.call_counter());

    if financial_start.is_some() {
        let resolution_poller = run_financial_resolution_poller(
            paper_state.clone(),
            cfg.polymarket_clob_base_url.clone(),
            cfg.gamma_resolution_poll_interval_secs,
            clob_resolution_fetcher.clone(),
            resolution_source_log,
            control_tx.clone(),
            shutdown.subscribe().wait_for(ShutdownPhase::StopProducers),
        );
        supervisor.spawn(TaskName::ResolutionPoller, resolution_poller);
    }

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
            config_era,
            Some(health.clone()),
            risk_halt_release.clone(),
            qualification_seal.clone(),
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
        Some(reconciliation_obligations_dropped),
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
        initial_bankroll: starting_bankroll,
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

    advance_shutdown(&shutdown, &task_status, ShutdownPhase::DrainOrchestrator);
    shutdown_timed_out |=
        !join_named_until(&mut supervisor, &[TaskName::Orchestrator], deadline).await;
    drop(sink_handle);

    advance_shutdown(&shutdown, &task_status, ShutdownPhase::StopSinks);
    let sinks = [
        // Source acknowledgements remain available while wallet operations and the
        // serialized control owner drain.
        TaskName::ActivityIngest,
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
    ensure_pre_start_paper_log(&cfg.event_log_path, "--rebuild-state")?;
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

/// Rebind the installed migration record of a generation that was moved as a whole (the
/// rehearsal's private copy) to the configured paths, then exit (#570). A no-op for a moved copy
/// whose recorded paths already match; refused for a main still in its recorded origin directory
/// (the production generation), whether or not its paths match.
fn run_update_paper_migration_paths() -> Result<()> {
    let cfg = load_config()?;
    let updated = update_installed_log_paths(&PaperMigrationPaths {
        fixed_main: cfg.paper_state_db_path,
        source_log: cfg.source_event_log_path,
        paper_log: cfg.event_log_path.clone(),
        live_journal: live_journal_path(&cfg.event_log_path),
        legacy_history: cfg.legacy_wallet_history_path,
        binary_identity: build_identity().to_owned(),
    })?;
    println!(
        "paper migration paths {}",
        if updated { "updated" } else { "unchanged" }
    );
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
    ensure_pre_start_paper_log(&cfg.event_log_path, "--backfill-supabase")?;
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

fn ensure_pre_start_paper_log(path: &std::path::Path, operation: &str) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }
    let era = paper_era(
        scan_paper_log(path).with_context(|| format!("verify paper log before {operation}"))?,
    );
    anyhow::ensure!(
        era.start.is_none(),
        "{operation} is refused after QualificationStarted; preserve the era and roll forward"
    );
    Ok(())
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

fn optional_arg_value(args: &[String], name: &str) -> Option<String> {
    let with_equals = format!("{name}=");
    args.iter().enumerate().find_map(|(index, argument)| {
        argument
            .strip_prefix(&with_equals)
            .map(str::to_owned)
            .or_else(|| {
                (argument == name)
                    .then(|| args.get(index.saturating_add(1)).cloned())
                    .flatten()
            })
    })
}

fn required_arg_value(args: &[String], name: &str) -> Result<String> {
    optional_arg_value(args, name).with_context(|| format!("{name} requires a value"))
}

/// Poll the same CLOB per-condition evidence used by live resolution, append each successful
/// response exactly once, and hand only resolved canonical vectors to the paper serializer.
async fn run_financial_resolution_poller(
    paper_state: Arc<PaperStateDb>,
    clob_base_url: String,
    poll_interval_secs: u64,
    fetcher: Arc<ReqwestFetcher>,
    source_log: pe_service::activity_ingest::SourceLogHandle,
    control: mpsc::Sender<pe_service::orchestrator_control::OrchestratorControl>,
    shutdown: impl std::future::Future<Output = ()>,
) -> TaskResult {
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            () = &mut shutdown => return Ok(TaskExit::CleanShutdown),
            () = tokio::time::sleep(Duration::from_secs(poll_interval_secs.max(1))) => {}
        }
        if let Err(error) = tick_financial_resolution(
            &paper_state,
            &clob_base_url,
            fetcher.as_ref(),
            &source_log,
            &control,
        )
        .await
        {
            warn!(error = %error, "financial resolution poll error");
        }
    }
}

async fn tick_financial_resolution(
    paper_state: &PaperStateDb,
    clob_base_url: &str,
    fetcher: &ReqwestFetcher,
    source_log: &pe_service::activity_ingest::SourceLogHandle,
    control: &mpsc::Sender<pe_service::orchestrator_control::OrchestratorControl>,
) -> Result<()> {
    let now = OffsetDateTime::now_utc();
    let snapshot = paper_state
        .financial_snapshot(now.unix_timestamp())
        .context("read financial resolution snapshot")?;
    let conditions = snapshot
        .positions
        .into_iter()
        .map(|position| position.market_id.0.0)
        .collect::<std::collections::BTreeSet<_>>();

    for condition in conditions.into_iter().map(PolymarketConditionId) {
        if let Err(error) = resolve_financial_condition(
            clob_base_url,
            fetcher,
            source_log,
            control,
            condition.clone(),
        )
        .await
        {
            warn!(condition = %condition.0, error = %error, "financial resolution condition failed; continuing");
        }
    }
    Ok(())
}

async fn resolve_financial_condition(
    clob_base_url: &str,
    fetcher: &ReqwestFetcher,
    source_log: &pe_service::activity_ingest::SourceLogHandle,
    control: &mpsc::Sender<pe_service::orchestrator_control::OrchestratorControl>,
    condition: PolymarketConditionId,
) -> Result<()> {
    let observed_at = OffsetDateTime::now_utc();
    let url = format!(
        "{}/markets/{}",
        clob_base_url.trim_end_matches('/'),
        condition.0
    );
    let body = fetcher
        .fetch_page(&url)
        .await
        .with_context(|| format!("fetch CLOB resolution {}", condition.0))?;
    let received_at = OffsetDateTime::now_utc();
    let receipt = source_log
        .append(EnvelopeIn {
            source_id: SourceId("polymarket.clob.market".to_owned()),
            schema_version: CLOB_RESOLUTION_SCHEMA_VERSION,
            parser_version: CLOB_RESOLUTION_PARSER_VERSION,
            observed_at: SourceTimestamp(observed_at),
            received_at: ReceivedAt(received_at),
            content_type: ContentType::Json,
            payload: body.clone(),
        })
        .await
        .context("append CLOB resolution response")?;
    let parsed = parse_clob_market(&body).context("parse CLOB resolution response")?;
    anyhow::ensure!(
        parsed.condition_id.as_deref() == Some(condition.0.as_str()),
        "CLOB resolution condition differs from request"
    );
    let ClobPayoutResolution::Resolved(payout) = parsed.resolution_evidence().payout else {
        return Ok(());
    };
    let (acknowledged, acknowledgement) = tokio::sync::oneshot::channel();
    control
        .send(
            pe_service::orchestrator_control::OrchestratorControl::ResolutionCandidate {
                condition,
                payout_by_outcome_index_json: payout.canonical_json(),
                receipt,
                acknowledged,
            },
        )
        .await
        .context("send paper resolution candidate")?;
    acknowledgement
        .await
        .context("paper resolution acknowledgement dropped")?
        .map_err(anyhow::Error::msg)?;
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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use pe_paper_state::{AnchorInstallRecord, WalletHistoryStatusRecord};
    use rusqlite::params;

    const NOW: i64 = 10_000;

    fn wallet(id: u64) -> WalletAddress {
        WalletAddress::from_hex(&format!("0x{id:040x}")).unwrap()
    }

    fn install_reusable_facts(
        paper_state: &PaperStateDb,
        wallet: WalletAddress,
        anchored_at_unix: i64,
    ) {
        paper_state
            .record_reconciled_history_status(&WalletHistoryStatusRecord {
                wallet,
                complete: true,
                proof_json: "{}".to_owned(),
                updated_at_unix: NOW,
            })
            .unwrap();
        paper_state.set_cursor(&wallet, 9_000).unwrap();
        paper_state
            .install_anchors(&[AnchorInstallRecord {
                wallet,
                balances: Vec::new(),
                activity_cutoff_unix: 9_000,
                anchored_at_unix,
                ledger_hash_after: "ledger".to_owned(),
                positions_proof_hash: "positions".to_owned(),
                activity_bounds_json: "{}".to_owned(),
                source_log_generation: "generation".to_owned(),
                proof_json: "{}".to_owned(),
                recorded_at_unix: NOW,
            }])
            .unwrap();
    }

    #[test]
    fn ordinary_boot_reuses_only_wallets_matching_runtime_anchor_rule() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("paper.db");
        let paper_state = PaperStateDb::open(&path).unwrap();
        let with_validation = wallet(1);
        let without_validation = wallet(2);
        let aged = wallet(3);
        let reanchor_required = wallet(4);
        let fenced = wallet(5);
        let history_incomplete = wallet(6);
        let without_cursor = wallet(7);
        let without_cutoff = wallet(8);
        let refresh_secs = i64::try_from(pe_service::trade_poller::ANCHOR_REFRESH_SECS).unwrap();
        install_reusable_facts(&paper_state, with_validation, NOW - refresh_secs);
        for candidate in [
            without_validation,
            reanchor_required,
            fenced,
            history_incomplete,
            without_cutoff,
        ] {
            install_reusable_facts(&paper_state, candidate, NOW);
        }
        install_reusable_facts(&paper_state, aged, NOW - refresh_secs - 1);
        paper_state
            .record_reconciled_history_status(&WalletHistoryStatusRecord {
                wallet: without_cursor,
                complete: true,
                proof_json: "{}".to_owned(),
                updated_at_unix: NOW,
            })
            .unwrap();

        let connection = rusqlite::Connection::open(&path).unwrap();
        connection
            .execute(
                "DELETE FROM position_validations WHERE wallet_hex = ?1",
                params![without_validation.to_string()],
            )
            .unwrap();
        connection
            .execute(
                "UPDATE poll_cursors SET reanchor_required = 1 WHERE wallet_hex = ?1",
                params![reanchor_required.to_string()],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO wallet_fences \
                 (wallet_hex, source_trade_id, cause, proof_json, fenced_at_unix) \
                 VALUES (?1, 'group', 'test', '{}', ?2)",
                params![fenced.to_string(), NOW],
            )
            .unwrap();
        connection
            .execute(
                "UPDATE wallet_history_status_v2 SET complete = 0 WHERE wallet_hex = ?1",
                params![history_incomplete.to_string()],
            )
            .unwrap();
        connection
            .execute(
                "UPDATE poll_cursors SET activity_cutoff_unix = NULL WHERE wallet_hex = ?1",
                params![without_cutoff.to_string()],
            )
            .unwrap();

        let wallets = [
            with_validation,
            without_validation,
            aged,
            reanchor_required,
            fenced,
            history_incomplete,
            without_cursor,
            without_cutoff,
        ];
        let selection = select_boot_anchor_wallets(&paper_state, &wallets, false, NOW).unwrap();
        assert_eq!(selection.reused, vec![with_validation, without_validation]);
        assert_eq!(
            selection.walked,
            vec![
                aged,
                reanchor_required,
                fenced,
                history_incomplete,
                without_cursor,
                without_cutoff,
            ]
        );

        let migration =
            select_boot_anchor_wallets(&paper_state, &[with_validation], true, NOW).unwrap();
        assert!(migration.reused.is_empty());
        assert_eq!(migration.walked, vec![with_validation]);
    }
}
