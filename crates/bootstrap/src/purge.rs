//! `pe-bootstrap purge` — delete proven-loser & dead-weight backfilled wallets
//! from the local SQLite cache after a rank-and-push run (issue #385).
//!
//! Two deletion rules, both restricted to `is_active = 1`:
//!
//! - **Rule A (proven loser)** — eligible per the ranker CSV but a money-loser
//!   (`tstat_net <= purge_loser_tstat_max && mean_net < 0 && n_eff >=
//!   purge_loser_neff_min`). Deleted **and tombstoned** ([`crate::cache::WalletCache::purge_wallets`]
//!   writes a `purged_wallets` row) so a non-override discovery source cannot
//!   silently re-ingest it. Re-discovery via the Polymarket leaderboard or Radion
//!   lifts the tombstone (handled in `upsert_wallets_bulk`).
//! - **Rule B (dead weight)** — `is_active = 1`, not eligible, refreshed this run
//!   (`last_polymarket_fetch_at` within `BACKFILL_STALENESS_SECS`), newest trade
//!   older than `purge_inactivity_secs`. Deleted with **no tombstone** — discovery
//!   may re-find it.
//!
//! The guard keeps the rule-B set off every eligible (hence pushed-cohort) wallet:
//! the pushed cohort ⊆ `latency_shift_rerank.py load_candidates` (`eligible &
//! tstat_net>=floor & mean_net>0`) ⊆ the CSV's `eligible=True` set, so excluding
//! eligible from rule B fully covers it. Rule A is exempt — it intentionally
//! deletes eligible losers, a set disjoint from the cohort (`mean_net<0` vs `>0`).
//!
//! **No refresh here.** `is_active`/`trade_count` are already current from the
//! Step-0 `backfill` stage (`backfill.rs` runs `refresh_trade_counts` +
//! `apply_activation_rules` at its tail) earlier in the same pipeline run; nothing
//! between backfill and purge mutates them. An armed purge on a globally stale
//! cache (no fresh backfill) is refused.
//!
//! **Bulk-delete index dance (issue #401).** An armed run drops the two
//! non-lookup `trades` secondary indexes around the chunked delete and rebuilds
//! them once on the VACUUM-compacted table (`drop → delete → VACUUM → recreate`),
//! so per-row index churn becomes a single bulk build. The recreate runs even on
//! a mid-run delete/VACUUM error (recreate-then-propagate). The lookup index
//! `idx_trades_wallet_ts` and the PK are kept. Dry runs touch no indexes.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use time::OffsetDateTime;

use crate::BootstrapConfig;
use crate::cache::{PurgeReason, PurgeReport, PurgeRow, WalletCache};
use crate::error::BootstrapError;
use crate::pile;

/// `data/eval-results` run-directory root and the per-run decision CSV name.
const EVAL_RESULTS_DIR: &str = "data/eval-results";
const RANKED_CSV_NAME: &str = "ranked_72hr_buyandhold.csv";
const CRON_DIR_PREFIX: &str = "cron-";

/// Parsed decision CSV: the full eligible set + the rule-A (proven-loser) subset.
struct Decisions {
    eligible: HashSet<String>,
    rule_a: Vec<String>,
}

/// Run the purge stage (issue #385). `dry_run` (the CLI `--dry-run`) forces a
/// report-only pass; an armed delete additionally requires `purge_enabled`.
/// Returns the [`PurgeReport`] for the caller to log.
pub fn run_purge(
    config: &BootstrapConfig,
    cache: &mut WalletCache,
    dry_run: bool,
) -> Result<PurgeReport, BootstrapError> {
    let now_unix = OffsetDateTime::now_utc().unix_timestamp();
    let armed = config.purge_enabled && !dry_run;

    // (a) No refresh — rely on the Step-0 backfill. Refuse an armed purge on a
    // globally stale cache (mirrors push_ranking_to_supabase.py's CacheStaleError).
    let newest = cache.newest_trade_unix()?;
    let stale = newest.is_none_or(|ts| now_unix - ts > pile::BACKFILL_STALENESS_SECS);
    if stale {
        if armed {
            return Err(BootstrapError::Purge {
                message: format!(
                    "cache globally stale (newest trade {}s old > {}s) — run `pe-bootstrap backfill` before an armed purge",
                    newest.map_or(-1, |ts| now_unix - ts),
                    pile::BACKFILL_STALENESS_SECS,
                ),
            });
        }
        tracing::warn!(
            "purge: cache globally stale — report is advisory; an armed run would refuse until a fresh backfill"
        );
    }

    // (b) Resolve + parse the decision CSV.
    let csv_path = resolve_decision_csv(config)?;
    let decisions = parse_decision_csv(
        &csv_path,
        config.purge_loser_tstat_max,
        config.purge_loser_neff_min,
    )?;

    // Restrict rule A to is_active = 1 (strict v1 boundary).
    let active: HashSet<String> = cache.active_tradeable_wallet_hexes()?.into_iter().collect();
    let rule_a: Vec<String> = decisions
        .rule_a
        .into_iter()
        .filter(|w| active.contains(w))
        .collect();

    // Rule B: active + refreshed-this-run + dormant, minus eligible (the guard).
    let rule_a_set: HashSet<&String> = rule_a.iter().collect();
    let rule_b: Vec<String> = cache
        .select_dead_weight_candidates(
            now_unix,
            config.purge_inactivity_secs,
            pile::BACKFILL_STALENESS_SECS,
        )?
        .into_iter()
        .filter(|w| !decisions.eligible.contains(w) && !rule_a_set.contains(w))
        .collect();

    let mut rows: Vec<PurgeRow> = Vec::with_capacity(rule_a.len() + rule_b.len());
    for w in &rule_a {
        rows.push(PurgeRow {
            wallet_hex: w.clone(),
            reason: PurgeReason::ProvenLoser,
        });
    }
    for w in rule_b {
        rows.push(PurgeRow {
            wallet_hex: w,
            reason: PurgeReason::DeadWeight,
        });
    }

    let report = if armed {
        // Fast bulk delete (issue #401): drop the two non-lookup `trades`
        // secondary indexes so the per-wallet delete only churns the
        // `wallet_hex` lookup index + PK, then rebuild them once on the
        // compacted table. Ordering: drop → delete → VACUUM → recreate.
        // Recreate-then-propagate: bind the delete/VACUUM results, ALWAYS
        // rebuild the indexes in this same invocation, *then* surface the
        // first error — so a mid-run failure never leaves a slow-query DB
        // waiting on the next `open`'s SCHEMA backstop. Nothing is deleted
        // before the drop, so the drop's own `?` early-return is safe.
        cache.drop_trades_bulk_delete_indexes()?;
        let purge_res = cache.purge_wallets(&rows, now_unix, false);
        let vacuum_res = if purge_res.is_ok() {
            cache.vacuum()
        } else {
            Ok(())
        };
        cache.create_trades_bulk_delete_indexes()?;
        let report = purge_res?;
        vacuum_res?;
        tracing::info!("purge: VACUUM complete; dropped + rebuilt 2 trades indexes");
        report
    } else {
        // Dry-run / disabled: report-only, no index ops (deletes nothing).
        cache.purge_wallets(&rows, now_unix, true)?
    };
    tracing::info!(
        csv = %csv_path.display(),
        armed,
        eligible = decisions.eligible.len(),
        proven_losers = report.proven_losers_deleted,
        dead_weight = report.dead_weight_deleted,
        trades = report.trades_deleted,
        snapshots = report.snapshots_deleted,
        tombstones = report.tombstones_written,
        dry_run = report.dry_run,
        "purge: report"
    );
    Ok(report)
}

/// Resolve the decision CSV: explicit `purge_decision_csv` if set, else the
/// newest `data/eval-results/cron-<UTC>/` run that has a `ranked_72hr_buyandhold.csv`
/// (the `cron-<UTC>` timestamp sorts chronologically). Errors if none is found —
/// never silently no-ops on a missing verdict.
fn resolve_decision_csv(config: &BootstrapConfig) -> Result<PathBuf, BootstrapError> {
    if let Some(p) = &config.purge_decision_csv {
        let path = PathBuf::from(p);
        if !path.is_file() {
            return Err(BootstrapError::Purge {
                message: format!("decision CSV not found: {}", path.display()),
            });
        }
        return Ok(path);
    }

    let base = Path::new(EVAL_RESULTS_DIR);
    let entries = std::fs::read_dir(base).map_err(|e| BootstrapError::Purge {
        message: format!("cannot read {}: {e}", base.display()),
    })?;
    let mut best: Option<(String, PathBuf)> = None;
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if !name.starts_with(CRON_DIR_PREFIX) {
            continue;
        }
        let csv = entry.path().join(RANKED_CSV_NAME);
        if csv.is_file() && best.as_ref().is_none_or(|(b, _)| name > *b) {
            best = Some((name, csv));
        }
    }
    best.map(|(_, p)| p).ok_or_else(|| BootstrapError::Purge {
        message: format!(
            "no decision CSV found: set PE_BOOTSTRAP_PURGE_DECISION_CSV or run rank_and_push.sh first (no {EVAL_RESULTS_DIR}/{CRON_DIR_PREFIX}*/{RANKED_CSV_NAME})"
        ),
    })
}

/// Parse the ranker decision CSV into the eligible set + the rule-A subset.
///
/// Header-mapped manual parse (matches the `backtest::config::parse_csv_fractions`
/// precedent): the pandas-emitted CSV has simple `wallet`/float columns with no
/// embedded commas. Required columns: `wallet`, `eligible`, `tstat_net`,
/// `mean_net`, `n_eff`. A wallet absent from the CSV is "not eligible" by
/// construction (rank_72hr only emits wallets with qualifying positions).
fn parse_decision_csv(
    path: &Path,
    tstat_max: f64,
    neff_min: f64,
) -> Result<Decisions, BootstrapError> {
    let content = std::fs::read_to_string(path)?;
    let mut lines = content.lines();
    let header = lines.next().ok_or_else(|| BootstrapError::Purge {
        message: format!("empty decision CSV: {}", path.display()),
    })?;
    let cols: Vec<&str> = header.split(',').map(str::trim).collect();
    let col = |name: &str| -> Result<usize, BootstrapError> {
        cols.iter()
            .position(|c| *c == name)
            .ok_or_else(|| BootstrapError::Purge {
                message: format!("decision CSV missing '{name}' column: {}", path.display()),
            })
    };
    let i_wallet = col("wallet")?;
    let i_elig = col("eligible")?;
    let i_tstat = col("tstat_net")?;
    let i_mean = col("mean_net")?;
    let i_neff = col("n_eff")?;
    let max_idx = i_wallet.max(i_elig).max(i_tstat).max(i_mean).max(i_neff);

    let mut eligible = HashSet::new();
    let mut rule_a = Vec::new();
    for line in lines {
        if line.trim().is_empty() {
            continue;
        }
        let f: Vec<&str> = line.split(',').collect();
        if f.len() <= max_idx {
            continue; // malformed / short row
        }
        let wallet = f[i_wallet].trim();
        if wallet.is_empty() {
            continue;
        }
        let elig_raw = f[i_elig].trim();
        let is_eligible = elig_raw.eq_ignore_ascii_case("true") || elig_raw == "1";
        if !is_eligible {
            continue; // not eligible → never rule A; absent from the eligible set
        }
        eligible.insert(wallet.to_owned());

        // Rule A: eligible AND tstat_net <= max AND mean_net < 0 AND n_eff >= min.
        let tstat = f[i_tstat].trim().parse::<f64>();
        let mean = f[i_mean].trim().parse::<f64>();
        let neff = f[i_neff].trim().parse::<f64>();
        if let (Ok(t), Ok(m), Ok(n)) = (tstat, mean, neff)
            && t <= tstat_max
            && m < 0.0
            && n >= neff_min
        {
            rule_a.push(wallet.to_owned());
        }
    }
    Ok(Decisions { eligible, rule_a })
}
