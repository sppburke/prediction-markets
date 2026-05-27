//! `pe-skill-select` CLI (issue #212).
//!
//! Subcommands:
//!   extract [config.toml]   — Phase A: stream the active-tradeable universe and
//!                             write `wallet_features` at the configured cutoff.
//!   select  [config.toml]   — load `wallet_features` for the cutoff, run BHq +
//!                             deflated-Sharpe selection, print the watchlist.
//!   forward-test [config.toml] — select, then hold each selected wallet's
//!                             post-cutoff buys to resolution; print flat-$1 +
//!                             Kelly-f PnL (GROSS of fees).
//!   export-watchlist [config.toml] — convert a `.txt` watchlist of wallet hex
//!                             addresses into a `pe_trader_index::Watchlist` JSON
//!                             file consumable by `pe-service seed_watchlist_path`.
//!
//! Config is `PE_SKILL_*` env overlaid on an optional TOML path. Exit codes:
//! 0 = success, 1 = fatal, 2 = usage error.

use pe_skill_select::{
    ExportWatchlistConfig, ForwardSource, SelectionInput, SkillCache, SkillConfig,
    rank_by_composite, run_export_watchlist, run_extract, run_forward_test, select_wallets,
};
use rust_decimal::Decimal;
use time::OffsetDateTime;
use tracing_subscriber::EnvFilter;

fn main() {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let args: Vec<String> = std::env::args().collect();
    let sub = args.get(1).map(String::as_str);
    let toml_path = args.get(2).map(std::path::PathBuf::from);

    let exit = match sub {
        Some("extract") => run_extract_cmd(toml_path.as_deref()),
        Some("select") => run_select_cmd(toml_path.as_deref()),
        Some("composite") => run_composite_cmd(toml_path.as_deref()),
        Some("forward-test") => run_forward_cmd(toml_path.as_deref()),
        Some("export-watchlist") => run_export_watchlist_cmd(toml_path.as_deref()),
        other => {
            eprintln!(
                "usage: pe-skill-select <extract|select|composite|forward-test|export-watchlist> [config.toml]   (got {other:?})"
            );
            2
        }
    };
    std::process::exit(exit);
}

/// Load `wallet_features` at the cutoff and run BHq + deflated-Sharpe selection,
/// returning the selected wallet hexes. Shared by `select` and `forward-test`.
fn selected_wallets(cfg: &SkillConfig) -> Result<Vec<SelectionInput>, i32> {
    let cache = SkillCache::open_read_only(&cfg.cache_path).map_err(|e| {
        tracing::error!(error = %e, "skill-select: cache open failed");
        1
    })?;
    let rows = cache
        .load_features_for_cutoff(cfg.cutoff_unix)
        .map_err(|e| {
            tracing::error!(error = %e, "skill-select: load failed");
            1
        })?;
    Ok(rows
        .iter()
        .map(|w| SelectionInput {
            wallet_hex: w.features.wallet_hex.clone(),
            skill_pvalue_bps: w.skill_pvalue_bps,
            sharpe_bps: w.features.sharpe_bps,
            trading_days: w.features.trading_days,
        })
        .collect())
}

/// Build the selected-wallet hex list for `forward-test` by delegating to
/// whichever ranker `cfg.forward_source` names. Both branches honour the same
/// `bhq_q_bps` / `top_n` / `min_trading_days` gates; only the rank function
/// differs.
fn selected_wallets_for_source(cfg: &SkillConfig) -> Result<Vec<String>, i32> {
    let cache = SkillCache::open_read_only(&cfg.cache_path).map_err(|e| {
        tracing::error!(error = %e, "skill-select: cache open failed");
        1
    })?;
    let rows = cache
        .load_features_for_cutoff(cfg.cutoff_unix)
        .map_err(|e| {
            tracing::error!(error = %e, "skill-select: load failed");
            1
        })?;
    let selected: Vec<String> = match cfg.forward_source {
        ForwardSource::Select => {
            let inputs: Vec<SelectionInput> = rows
                .iter()
                .map(|w| SelectionInput {
                    wallet_hex: w.features.wallet_hex.clone(),
                    skill_pvalue_bps: w.skill_pvalue_bps,
                    sharpe_bps: w.features.sharpe_bps,
                    trading_days: w.features.trading_days,
                })
                .collect();
            select_wallets(&inputs, cfg.bhq_q_bps, cfg.top_n, cfg.min_trading_days)
                .iter()
                .filter(|r| r.selected)
                .map(|r| r.wallet_hex.clone())
                .collect()
        }
        ForwardSource::Composite => rank_by_composite(
            &rows,
            &cfg.composite_weights(),
            cfg.bhq_q_bps,
            cfg.top_n,
            cfg.min_trading_days,
        )
        .iter()
        .filter(|r| r.selected)
        .map(|r| r.wallet_hex.clone())
        .collect(),
    };
    Ok(selected)
}

/// Load config; on failure log and return the fatal exit code.
fn load(toml_path: Option<&std::path::Path>) -> Result<SkillConfig, i32> {
    SkillConfig::load(toml_path).map_err(|e| {
        tracing::error!(error = %e, "skill-select: config error");
        1
    })
}

fn run_extract_cmd(toml_path: Option<&std::path::Path>) -> i32 {
    let cfg = match load(toml_path) {
        Ok(c) => c,
        Err(code) => return code,
    };
    let extracted_at = OffsetDateTime::now_utc().unix_timestamp();
    match run_extract(
        &cfg.cache_path,
        cfg.cutoff_unix,
        cfg.min_closed_trades,
        cfg.min_distinct_events,
        cfg.beta_binomial_alpha,
        cfg.beta_binomial_beta,
        cfg.permutations,
        cfg.rng_seed,
        extracted_at,
        cfg.extract_threads,
        cfg.extract_clean_prior,
    ) {
        Ok(report) => {
            println!(
                "extract: scanned={} written={} skipped={} (cutoff_unix={})",
                report.wallets_scanned,
                report.wallets_written,
                report.wallets_skipped,
                cfg.cutoff_unix,
            );
            0
        }
        Err(e) => {
            tracing::error!(error = %e, "skill-select extract: fatal");
            1
        }
    }
}

fn run_select_cmd(toml_path: Option<&std::path::Path>) -> i32 {
    let cfg = match load(toml_path) {
        Ok(c) => c,
        Err(code) => return code,
    };
    let inputs = match selected_wallets(&cfg) {
        Ok(i) => i,
        Err(code) => return code,
    };
    let results = select_wallets(&inputs, cfg.bhq_q_bps, cfg.top_n, cfg.min_trading_days);
    let selected = results.iter().filter(|r| r.selected).count();
    println!(
        "select: candidates={} selected={} (cutoff_unix={}, bhq_q_bps={}, top_n={}, min_trading_days={})",
        inputs.len(),
        selected,
        cfg.cutoff_unix,
        cfg.bhq_q_bps,
        cfg.top_n,
        cfg.min_trading_days,
    );
    for r in results.iter().filter(|r| r.selected) {
        println!(
            "{}\tpvalue_bps={}\tdeflated_sharpe_bps={}",
            r.wallet_hex, r.skill_pvalue_bps, r.deflated_sharpe_bps
        );
    }
    0
}

/// Stage-2 composite ranker (docs/24- §2 PR-4 MVP — hand-weighted z-score
/// linear combo over the 12 features). BHq+min_trading_days gate identical to
/// `select`; the only difference is the rank function.
fn run_composite_cmd(toml_path: Option<&std::path::Path>) -> i32 {
    let cfg = match load(toml_path) {
        Ok(c) => c,
        Err(code) => return code,
    };
    let cache = match SkillCache::open_read_only(&cfg.cache_path) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "skill-select composite: cache open failed");
            return 1;
        }
    };
    let rows = match cache.load_features_for_cutoff(cfg.cutoff_unix) {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, "skill-select composite: load failed");
            return 1;
        }
    };
    let weights = cfg.composite_weights();
    let results = rank_by_composite(
        &rows,
        &weights,
        cfg.bhq_q_bps,
        cfg.top_n,
        cfg.min_trading_days,
    );
    let selected = results.iter().filter(|r| r.selected).count();
    println!(
        "composite: candidates={} selected={} (cutoff_unix={}, bhq_q_bps={}, top_n={}, min_trading_days={})",
        rows.len(),
        selected,
        cfg.cutoff_unix,
        cfg.bhq_q_bps,
        cfg.top_n,
        cfg.min_trading_days,
    );
    for r in results.iter().filter(|r| r.selected) {
        println!(
            "{}\tcomposite_bps={}\tpvalue_bps={}\tsharpe_bps={}",
            r.wallet_hex, r.composite_score_bps, r.skill_pvalue_bps, r.sharpe_bps
        );
    }
    0
}

fn run_export_watchlist_cmd(toml_path: Option<&std::path::Path>) -> i32 {
    let cfg = match load(toml_path) {
        Ok(c) => c,
        Err(code) => return code,
    };
    let export_cfg = ExportWatchlistConfig {
        cache_path: cfg.cache_path.clone(),
        watchlist_txt_path: cfg.export_watchlist_input_path.clone(),
        cutoff_unix: cfg.cutoff_unix,
        output_path: cfg.export_watchlist_output_path.clone(),
    };
    match run_export_watchlist(&export_cfg) {
        Ok(stats) => {
            println!(
                "export-watchlist: requested={} written={} missing={} (cutoff_unix={}, output={:?})",
                stats.wallets_requested,
                stats.wallets_written,
                stats.wallets_missing,
                cfg.cutoff_unix,
                cfg.export_watchlist_output_path,
            );
            0
        }
        Err(e) => {
            tracing::error!(error = %e, "skill-select export-watchlist: fatal");
            1
        }
    }
}

fn run_forward_cmd(toml_path: Option<&std::path::Path>) -> i32 {
    let cfg = match load(toml_path) {
        Ok(c) => c,
        Err(code) => return code,
    };
    let selected = match selected_wallets_for_source(&cfg) {
        Ok(s) => s,
        Err(code) => return code,
    };

    let bps = |n: u32| Decimal::from(n) / Decimal::from(10_000u32);
    match run_forward_test(
        &cfg.cache_path,
        &selected,
        cfg.cutoff_unix,
        bps(cfg.kelly_fraction_bps),
        bps(cfg.forward_price_bucket_width_bps),
        cfg.forward_min_bucket_trades,
    ) {
        Ok(r) => {
            println!(
                "forward-test (GROSS of fees): source={} wallets={} resolved={} excluded={} \
                 kelly_fallback={} flat_pnl_usd={} kelly_pnl_usd={} (cutoff_unix={}, f={}, top_n={})",
                cfg.forward_source,
                r.wallets,
                r.resolved_positions,
                r.excluded_positions,
                r.kelly_fallback_positions,
                r.flat_pnl_usd,
                r.kelly_pnl_usd,
                cfg.cutoff_unix,
                bps(cfg.kelly_fraction_bps),
                cfg.top_n,
            );
            0
        }
        Err(e) => {
            tracing::error!(error = %e, "skill-select forward-test: fatal");
            1
        }
    }
}
