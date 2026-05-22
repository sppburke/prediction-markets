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
//!
//! Config is `PE_SKILL_*` env overlaid on an optional TOML path. Exit codes:
//! 0 = success, 1 = fatal, 2 = usage error.

use pe_skill_select::{
    SelectionInput, SkillCache, SkillConfig, run_extract, run_forward_test, select_wallets,
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
        Some("forward-test") => run_forward_cmd(toml_path.as_deref()),
        other => {
            eprintln!(
                "usage: pe-skill-select <extract|select|forward-test> [config.toml]   (got {other:?})"
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
        })
        .collect())
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
        cfg.permutations,
        cfg.rng_seed,
        extracted_at,
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
    let results = select_wallets(&inputs, cfg.bhq_q_bps, cfg.top_n);
    let selected = results.iter().filter(|r| r.selected).count();
    println!(
        "select: candidates={} selected={} (cutoff_unix={}, bhq_q_bps={}, top_n={})",
        inputs.len(),
        selected,
        cfg.cutoff_unix,
        cfg.bhq_q_bps,
        cfg.top_n,
    );
    for r in results.iter().filter(|r| r.selected) {
        println!(
            "{}\tpvalue_bps={}\tdeflated_sharpe_bps={}",
            r.wallet_hex, r.skill_pvalue_bps, r.deflated_sharpe_bps
        );
    }
    0
}

fn run_forward_cmd(toml_path: Option<&std::path::Path>) -> i32 {
    let cfg = match load(toml_path) {
        Ok(c) => c,
        Err(code) => return code,
    };
    let inputs = match selected_wallets(&cfg) {
        Ok(i) => i,
        Err(code) => return code,
    };
    let results = select_wallets(&inputs, cfg.bhq_q_bps, cfg.top_n);
    let selected: Vec<String> = results
        .iter()
        .filter(|r| r.selected)
        .map(|r| r.wallet_hex.clone())
        .collect();

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
                "forward-test (GROSS of fees): wallets={} resolved={} excluded={} kelly_fallback={} \
                 flat_pnl_usd={} kelly_pnl_usd={} (cutoff_unix={}, f={})",
                r.wallets,
                r.resolved_positions,
                r.excluded_positions,
                r.kelly_fallback_positions,
                r.flat_pnl_usd,
                r.kelly_pnl_usd,
                cfg.cutoff_unix,
                bps(cfg.kelly_fraction_bps),
            );
            0
        }
        Err(e) => {
            tracing::error!(error = %e, "skill-select forward-test: fatal");
            1
        }
    }
}
