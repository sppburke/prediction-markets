//! `pe-skill-select` CLI (issue #212).
//!
//! Subcommands:
//!   extract [config.toml]   — Phase A: stream the active-tradeable universe and
//!                             write `wallet_features` at the configured cutoff.
//!   select  [config.toml]   — load `wallet_features` for the cutoff, run BHq +
//!                             deflated-Sharpe selection, print the watchlist.
//!
//! Config is `PE_SKILL_*` env overlaid on an optional TOML path. Exit codes:
//! 0 = success, 1 = fatal, 2 = usage error.

use pe_skill_select::{SelectionInput, SkillCache, SkillConfig, run_extract, select_wallets};
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
        other => {
            eprintln!("usage: pe-skill-select <extract|select> [config.toml]   (got {other:?})");
            2
        }
    };
    std::process::exit(exit);
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
    let cache = match SkillCache::open_read_only(&cfg.cache_path) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "skill-select select: cache open failed");
            return 1;
        }
    };
    let rows = match cache.load_features_for_cutoff(cfg.cutoff_unix) {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, "skill-select select: load failed");
            return 1;
        }
    };
    let inputs: Vec<SelectionInput> = rows
        .iter()
        .map(|w| SelectionInput {
            wallet_hex: w.features.wallet_hex.clone(),
            skill_pvalue_bps: w.skill_pvalue_bps,
            sharpe_bps: w.features.sharpe_bps,
        })
        .collect();
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
