//! Read-only backtest-readiness probe (issue #208).
//!
//! The `coverage` subcommand opens `data/wallet_cache.db` **read-only** and
//! reports three gap counts — `fetch_incomplete`, `missing_resolution`,
//! `missing_schedule` — so an operator can gate the expensive backfill/backtest
//! steps on a cheap probe (every count `0` means
//! the cache is ready). It never mutates the cache, never creates or migrates
//! it, and never takes the `CacheMutationLock`; see
//! [`crate::cache::WalletCache::open_read_only`].
//!
//! The probe reports raw counts only. Interpreting a non-zero `missing_schedule`
//! (a still-open market with a future `end_date_unix` is an expected non-gap,
//! not a genuine gap) is the operator's job in the Phase 2 runbook, not this
//! tool's.

use std::path::Path;

use tracing::info;

use crate::cache::{CoverageReport, WalletCache};
use crate::error::BootstrapError;

/// Open the cache read-only and compute the coverage gap counts.
///
/// Emits both a structured log line (AC1) and a human-readable stdout summary,
/// then returns the [`CoverageReport`]. The caller maps the report to the
/// process exit code: `0` when [`CoverageReport::is_clean`], `2` when any gap
/// is non-zero; an `Err` here is the only `1` (fatal) path.
pub fn run_coverage(cache_path: &Path) -> Result<CoverageReport, BootstrapError> {
    let cache = WalletCache::open_read_only(cache_path)?;
    let report = cache.coverage_counts()?;

    info!(
        fetch_incomplete = report.fetch_incomplete,
        missing_resolution = report.missing_resolution,
        missing_schedule = report.missing_schedule,
        clean = report.is_clean(),
        "coverage: backtest-readiness probe complete"
    );
    println!(
        "coverage: fetch_incomplete={} missing_resolution={} missing_schedule={} -> {}",
        report.fetch_incomplete,
        report.missing_resolution,
        report.missing_schedule,
        if report.is_clean() { "CLEAN" } else { "GAPS" },
    );

    Ok(report)
}
