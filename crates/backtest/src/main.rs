//! `pe-backtest` binary entry point.

use std::collections::HashSet;
use std::path::PathBuf;

use pe_backtest::FunderGraphTimeline;
use pe_backtest::config::{BacktestConfig, load};
use pe_backtest::error::BacktestError;
use pe_backtest::report::{KellySweepReport, KellySweepRun};
use pe_backtest::simulation;
use pe_bootstrap::cache::WalletCache;
use pe_bootstrap::dune::DuneClient;
use pe_strategy_winner_follow::WinnerFollowStrategy;
use pe_trader_index::{LedgerConfig, RankerConfig};
use rayon::prelude::*;
use time::OffsetDateTime;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), BacktestError> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    // First argument: optional TOML config path, or `--print-config`.
    let first_arg = std::env::args().nth(1);

    if first_arg.as_deref() == Some("--print-config") {
        let default_toml = toml::to_string_pretty(&BacktestConfig::default())
            .map_err(|e| BacktestError::Internal(format!("serialize default config: {e}")))?;
        print!("{default_toml}");
        return Ok(());
    }

    let config_path = first_arg.map(PathBuf::from);
    let config = load(config_path.as_deref())?;

    // Load wallet trade cache (mutable so Dune resolutions can be written).
    let mut cache = WalletCache::open(&config.bootstrap_cache_path)?;
    let all_wallet_addresses = cache.all_wallet_addresses();
    let mut all_trades = cache.all_trades();
    let snapshots = cache.load_all_snapshots()?;

    // Fetch on-chain resolutions via Dune before running the simulation so that
    // financial/quantitative markets (absent from the Gamma API) are resolved.
    if let Some(api_key) = &config.dune_api_key {
        let all_market_ids: HashSet<String> = cache.all_market_ids().into_iter().collect();
        let already_resolved = cache.resolved_market_ids();
        let unresolved: HashSet<String> = all_market_ids
            .difference(&already_resolved)
            .cloned()
            .collect();
        if unresolved.is_empty() {
            info!("backtest: all markets already resolved — skipping Dune fetch");
        } else {
            // Scope the Dune scan to our actual trade history window, not all of
            // history from epoch 0. A 30-day buffer before the earliest trade
            // captures resolutions for markets entered near our data horizon.
            const THIRTY_DAYS_SECS: i64 = 30 * 86_400;
            let min_trade_ts = cache.min_trade_unix()?.saturating_sub(THIRTY_DAYS_SECS);
            info!(
                unresolved = unresolved.len(),
                from_unix = min_trade_ts,
                "backtest: fetching Dune resolutions"
            );
            let dune = DuneClient::new(api_key.clone());
            match dune
                .fetch_resolutions(&unresolved, min_trade_ts, config.dune_namespace.as_deref())
                .await
            {
                Ok(rows) => {
                    let fetched_at = OffsetDateTime::now_utc().unix_timestamp();
                    let mut inserted = 0usize;
                    for (market_id, winner, resolved_at_unix) in rows {
                        cache.insert_resolution(
                            &market_id,
                            winner,
                            resolved_at_unix,
                            fetched_at,
                        )?;
                        inserted += 1;
                    }
                    info!(
                        inserted,
                        unresolved = unresolved.len(),
                        "backtest: Dune resolutions fetched"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "backtest: Dune resolution fetch failed — continuing without on-chain resolutions"
                    );
                }
            }
        }
    }

    let resolutions = pe_bootstrap::gamma::load_resolutions(&cache)?;
    let schedules = pe_bootstrap::gamma::load_schedules(&cache)?;
    let liq_index = pe_bootstrap::gamma::load_liquidity(&cache)?;

    info!(
        wallets = all_wallet_addresses.len(),
        trades = all_trades.len(),
        snapshots = snapshots.len(),
        resolutions = resolutions.len(),
        schedules = schedules.len(),
        liquidity = liq_index.len(),
        "cache loaded"
    );

    if all_trades.is_empty() {
        tracing::warn!(
            cache_path = %config.bootstrap_cache_path.display(),
            "no trades in cache — populate with pe-bootstrap first"
        );
        return Ok(());
    }

    // Pre-sort trades once before the sweep/non-sweep branch. `run_simulation`
    // documents this as a precondition (see simulation.rs) and guards it with a
    // `debug_assert`. Single sort here covers both the parallel Kelly-fraction
    // sweep (which shares the slice across rayon workers via SweepContext) and
    // the single-config branch — avoids the previous per-thread `Vec` clone in
    // `run_one_kelly_fraction` that drove the post-#137 memory regression
    // (issue #156). Stable `sort_by_key` preserves byte-for-byte ordering for
    // trades with identical `timestamp.0` (millisecond ties are real in
    // batch/MEV-bundle fills) and matches the original in-place sort that lived
    // inside `run_simulation`.
    all_trades.sort_by_key(|t| t.timestamp.0);
    info!(
        count = all_trades.len(),
        "backtest: trades pre-sorted for Kelly sweep"
    );

    // Phase 0: build temporal funder graph from cached edges (populated by pe-bootstrap).
    let funder_timeline = FunderGraphTimeline::from_cache(&cache)?;

    // Phase 1: walk-forward simulation.
    std::fs::create_dir_all(&config.output_dir)?;

    let ranker_config = RankerConfig {
        min_reconstruction_quality: config.ranker_min_quality,
        active_min_closed_trades: config.ranker_active_min_closed,
        active_min_distinct_markets: config.ranker_active_min_markets,
        incubator_min_closed_trades: config.ranker_incubator_min_closed,
        incubator_min_distinct_markets: config.ranker_incubator_min_markets,
        ..RankerConfig::default()
    };
    info!(
        min_quality = ranker_config.min_reconstruction_quality,
        active_min_closed = ranker_config.active_min_closed_trades,
        active_min_markets = ranker_config.active_min_distinct_markets,
        incubator_min_closed = ranker_config.incubator_min_closed_trades,
        incubator_min_markets = ranker_config.incubator_min_distinct_markets,
        "ranker config (backtest-adjusted)"
    );

    // Derive output filename prefix from config file stem (when provided).
    let file_stem = config_path
        .as_ref()
        .and_then(|p| p.file_stem())
        .and_then(|s| s.to_str())
        .map(str::to_owned);

    // Flat-USD sizing (issue #134) bypasses Kelly entirely, so sweeping
    // Kelly fractions while the flag is set would produce N identical
    // reports. Suppress the sweep with one warning when both are set.
    let sweep = config.kelly_sweep_fractions.as_ref().filter(|_| {
        if let Some(flat_usd) = config.flat_usd {
            tracing::warn!(
                flat_usd = %flat_usd,
                sweep_fractions_count = config.kelly_sweep_fractions.as_ref().map_or(0, Vec::len),
                "PE_BACKTEST_FLAT_USD set; ignoring kelly_sweep_fractions"
            );
            false
        } else {
            true
        }
    });

    if let Some(fractions) = sweep {
        // ── Sweep mode ──────────────────────────────────────────────────────────
        //
        // All fractions execute in parallel via rayon. The simulation kernel is pure
        // and per-fraction state is stack-local, so each run is fully independent.
        // Cap thread count via `RAYON_NUM_THREADS` env var (no plumbing in TOML).
        // Log lines from concurrent threads interleave by arrival order — query by
        // structured `kelly_fraction` and `thread` fields, not line position.
        info!(
            fractions = fractions.len(),
            "backtest: Kelly sweep mode — starting parallel runs"
        );
        let ledger_config = LedgerConfig::default();
        let ctx = simulation::SweepContext {
            config: &config,
            all_trades: &all_trades,
            funder_timeline: &funder_timeline,
            snapshots: &snapshots,
            resolutions: &resolutions,
            schedules: &schedules,
            liq_index: &liq_index,
            ranker_config: &ranker_config,
            ledger_config: &ledger_config,
        };
        let mut runs: Vec<KellySweepRun> = fractions
            .par_iter()
            .map(|&kf| simulation::run_one_kelly_fraction(kf, &ctx))
            .collect::<Result<Vec<_>, BacktestError>>()?;
        // Sort by kelly_fraction so output is field-equal across runs regardless
        // of rayon scheduling. KellyFraction derives Ord (Decimal is Ord; the
        // [0, 1] constructor invariant rules out any NaN-equivalent), so a
        // total-order sort is well-defined.
        runs.sort_unstable_by_key(|run| run.kelly_fraction);
        let mut sweep_report = KellySweepReport {
            runs,
            cache_path: config.bootstrap_cache_path.clone(),
            executed_at: OffsetDateTime::now_utc(),
            resolved_config: None,
        };
        sweep_report.resolved_config = Some(config.clone());

        // Write {stem-}kelly-sweep-{ISO8601}.json.
        let ts = sweep_report
            .executed_at
            .format(&time::format_description::well_known::Rfc3339)
            .map_err(|e| BacktestError::Internal(format!("timestamp format: {e}")))?
            .replace(':', "-");
        let filename = match &file_stem {
            Some(stem) => format!("{stem}-kelly-sweep-{ts}.json"),
            None => format!("kelly-sweep-{ts}.json"),
        };
        let sweep_path = config.output_dir.join(filename);
        let json = serde_json::to_vec_pretty(&sweep_report)?;
        std::fs::write(&sweep_path, &json)?;
        info!(path = ?sweep_path, "backtest: Kelly sweep report written");

        // Print markdown table to stdout.
        print!("{}", sweep_report.to_markdown_table());
    } else {
        // ── Single-run mode (default) ────────────────────────────────────────
        let strategy = WinnerFollowStrategy::new(config.strategy.clone());
        let mut report = simulation::run_simulation(
            &config,
            &all_trades,
            &funder_timeline,
            &snapshots,
            &resolutions,
            &schedules,
            &liq_index,
            &ranker_config,
            &LedgerConfig::default(),
            &strategy,
            true, // write report.json + trades.ndjson
        )?;
        report.resolved_config = Some(config.clone());

        // Re-write report.json with the resolved_config embedded.
        let report_path = config.output_dir.join("report.json");
        let json = serde_json::to_vec_pretty(&report)?;
        std::fs::write(&report_path, &json)?;

        info!(
            total_pnl_usd = %report.total_pnl_usd,
            sharpe_ratio = %report.sharpe_ratio,
            max_drawdown_pct = %report.max_drawdown_pct,
            total_copies = report.total_copies,
            win_rate_pct = %report.win_rate_pct,
            "backtest complete — results in {:?}",
            config.output_dir,
        );
    }

    Ok(())
}
