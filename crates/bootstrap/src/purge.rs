//! `pe-bootstrap purge` — delete proven-loser & dead-weight backfilled wallets
//! from the local SQLite cache after a rank-and-push run (issue #385).
//!
//! Two deletion rules, both restricted to `is_active = 1`:
//!
//! - **Rule A (proven loser)** — eligible per the ranker CSV but a money-loser
//!   (`tstat_net <= purge_loser_tstat_max && mean_net < 0 && n_eff >=
//!   purge_loser_neff_min`). Deleted **and tombstoned** ([`crate::cache::WalletCache::purge_wallets`]
//!   writes a `purged_wallets` row) so a non-override discovery source cannot
//!   silently re-ingest it. Re-discovery via the Polymarket leaderboard
//!   lifts the tombstone (handled in `upsert_wallets_bulk`).
//! - **Rule B (dead weight)** — `is_active = 1`, not eligible, refreshed this run
//!   (`last_polymarket_fetch_at` within `BACKFILL_STALENESS_SECS`), newest trade
//!   older than `purge_inactivity_secs`. Deleted with **no tombstone** — discovery
//!   may re-find it.
//!
//! [`run_infra_purge`] is the direct infrastructure path: it archives and
//! removes every `is_infra = 1` wallet regardless of active state and writes a
//! durable, non-liftable `infra` tombstone. Like the ordinary purge, it is
//! report-only unless `purge_enabled` is true and `--dry-run` is absent (#544).
//!
//! The guard keeps the rule-B set off every eligible (hence pushed-cohort) wallet:
//! the pushed cohort ⊆ `latency_shift_rerank.py load_candidates` (`eligible &
//! tstat_net>=floor & mean_net>0`) ⊆ the CSV's `eligible=True` set, so excluding
//! eligible from rule B fully covers it. Rule A is exempt — it intentionally
//! deletes eligible losers, a set disjoint from the cohort (`mean_net<0` vs `>0`).
//!
//! **No refresh here.** `is_active`/`trade_count` are already current from the
//! Step-0 `backfill` stage (which refreshes counts) earlier in the same pipeline
//! run. The pre-ranking infra purge does not alter the ordinary purge's non-infra
//! candidate state. An armed purge on a globally stale
//! cache (no fresh backfill) is refused.
//!
//! **Bulk-delete index dance (issue #401) + reclamation contract (#538).** An
//! armed BULK run (delete-set ≥ threshold) drops the two non-lookup `trades`
//! secondary indexes around the chunked delete, reclaims free pages, and
//! rebuilds the indexes once (`drop → delete → reclaim → recreate`), so per-row
//! index churn becomes a single bulk build. Maintenance vocabulary is distinct
//! from delete classification: reclamation is `incremental_vacuum` on a
//! converted db, or the one-time conversion `VACUUM` on a legacy mode-0 db.
//! The recreate runs even on a mid-run delete/reclaim error
//! (recreate-then-propagate). A `reclamation_pending` marker commits BEFORE the
//! index drop and clears only after reclaim AND recreate succeed. Recovery is
//! serviced only by a direct ordinary purge: a subthreshold/empty run recovers
//! via reclaim + idempotent index creation with NO index drop
//! (schema-on-open already healed any absence), while an above-threshold run
//! subsumes recovery in its normal bulk maintenance. Direct `purge-infra` never
//! services recovery. The lookup index
//! `idx_trades_wallet_ts` and the PK are kept. Dry runs touch neither indexes
//! nor the marker.

use std::collections::HashSet;
use std::io::Write;
use std::path::{Path, PathBuf};

use time::OffsetDateTime;

use crate::BootstrapConfig;
use crate::cache::{PurgeReason, PurgeReport, PurgeRow, ReclamationReport, WalletCache};
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
/// The bulk index-drop + free-page reclamation (issue #401/#538) run only when
/// the delete-set is at least `purge_bulk_min_wallets`; a smaller armed purge
/// deletes with the indexes live and no reclamation — unless a prior
/// `reclamation_pending` marker forces the drop-free recovery path. Returns the
/// [`PurgeReport`] to log.
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

    let (report, bulk) = execute_purge_rows(config, cache, &rows, now_unix, armed, "purge")?;
    tracing::info!(
        csv = %csv_path.display(),
        armed,
        bulk,
        bulk_threshold = config.purge_bulk_min_wallets,
        delete_set = rows.len(),
        eligible = decisions.eligible.len(),
        proven_losers = report.proven_losers_deleted,
        dead_weight = report.dead_weight_deleted,
        infrastructure = report.infrastructure_deleted,
        trades = report.trades_deleted,
        snapshots = report.snapshots_deleted,
        wallet_features = report.wallet_features_deleted,
        tombstones = report.tombstones_written,
        dry_run = report.dry_run,
        "purge: report"
    );
    Ok(report)
}

/// Report or delete every live infrastructure wallet and its wallet-keyed cache
/// data. The existing `purge_enabled` switch disarms both purge commands (#544).
pub fn run_infra_purge(
    config: &BootstrapConfig,
    cache: &mut WalletCache,
    dry_run: bool,
) -> Result<PurgeReport, BootstrapError> {
    let now_unix = OffsetDateTime::now_utc().unix_timestamp();
    let rows: Vec<PurgeRow> = cache
        .infra_wallet_hexes()?
        .into_iter()
        .map(|wallet_hex| PurgeRow {
            wallet_hex,
            reason: PurgeReason::Infrastructure,
        })
        .collect();
    let armed = config.purge_enabled && !dry_run;
    let (report, bulk) = execute_purge_rows(config, cache, &rows, now_unix, armed, "purge-infra")?;
    tracing::info!(
        armed,
        bulk,
        delete_set = rows.len(),
        infrastructure = report.infrastructure_deleted,
        trades = report.trades_deleted,
        snapshots = report.snapshots_deleted,
        wallet_features = report.wallet_features_deleted,
        tombstones = report.tombstones_written,
        dry_run = report.dry_run,
        "purge-infra: report"
    );
    Ok(report)
}

/// Shared archive/delete/index lifecycle for ordinary and infrastructure purge.
fn execute_purge_rows(
    config: &BootstrapConfig,
    cache: &mut WalletCache,
    rows: &[PurgeRow],
    now_unix: i64,
    armed: bool,
    label: &str,
) -> Result<(PurgeReport, bool), BootstrapError> {
    if armed && !rows.is_empty() && config.purge_archive_enabled {
        let archive_path = config
            .purge_archive_path
            .as_deref()
            .map(PathBuf::from)
            .unwrap_or_else(|| config.cache_path.with_extension("purge-archive.db"));
        let archived = cache.archive_wallets(rows, &archive_path, now_unix)?;
        tracing::info!(
            stage = label,
            archive = %archive_path.display(),
            trades_archived = archived.trades_archived,
            wallets_archived = archived.wallets_archived,
            snapshots_archived = archived.snapshots_archived,
            manifest_written = archived.manifest_written,
            "purge: archive-before-DELETE complete"
        );
    }

    // #538 fail-closed marker read: an error here aborts — a pending recovery
    // must never be silently skipped because the marker could not be read.
    // Recovery is serviced ONLY by the direct ordinary purge ("purge"):
    // purge-infra must not silently turn an infrastructure-only command into
    // unrelated multi-hour maintenance. An
    // above-threshold run needs no recovery arm — its normal bulk maintenance
    // drains the entire freelist (backlog included) and clears the marker.
    let bulk =
        armed && u64::try_from(rows.len()).unwrap_or(u64::MAX) >= config.purge_bulk_min_wallets;
    let report = if bulk {
        // Marker commits BEFORE the index drop so an interrupted run is retried
        // by the next purge invocation (#538).
        cache.set_reclamation_pending()?;
        cache.drop_trades_bulk_delete_indexes()?;
        // Ordering contract (#538): capture all three results; recreation is
        // attempted unconditionally after RETURNED delete/reclaim errors (never
        // an early `?`); propagate delete → reclaim → recreate. Schema-on-open
        // remains the process-death backstop.
        let purge_res = cache.purge_wallets(rows, now_unix, false);
        let reclaim_res = if purge_res.is_ok() {
            cache.reclaim_free_pages()
        } else {
            Err(BootstrapError::Invalid {
                message: "reclamation skipped: bulk delete failed".to_owned(),
            })
        };
        let recreate_res = cache.create_trades_bulk_delete_indexes();
        let report = purge_res?;
        let reclamation = reclaim_res?;
        recreate_res?;
        cache.clear_reclamation_pending()?;
        tracing::info!(
            stage = label,
            auto_vacuum_before = reclamation.auto_vacuum_before,
            path = ?reclamation.path,
            freelist_before = reclamation.freelist_before,
            freelist_after = reclamation.freelist_after,
            page_count_before = reclamation.page_count_before,
            page_count_after = reclamation.page_count_after,
            "purge: bulk mode complete"
        );
        report
    } else if armed {
        let report = cache.purge_wallets(rows, now_unix, false)?;
        if let Some(reclamation) = recover_pending_reclamation(cache)? {
            tracing::info!(
                stage = label,
                auto_vacuum_before = reclamation.auto_vacuum_before,
                path = ?reclamation.path,
                freelist_before = reclamation.freelist_before,
                freelist_after = reclamation.freelist_after,
                page_count_before = reclamation.page_count_before,
                page_count_after = reclamation.page_count_after,
                "purge: pending reclamation recovered"
            );
        }
        tracing::info!(stage = label, "purge: subthreshold mode complete");
        report
    } else {
        cache.purge_wallets(rows, now_unix, true)?
    };
    Ok((report, bulk))
}

/// Complete only a previously marked reclamation recovery (#544).
///
/// This path cannot select, archive, tombstone, or delete wallets. It factors
/// the ordinary purge's existing drop-free recovery sequence so activation can
/// explicitly run `reclaim -> idempotent index create -> marker clear` without
/// invoking either purge selector.
pub fn recover_pending_reclamation(
    cache: &mut WalletCache,
) -> Result<Option<ReclamationReport>, BootstrapError> {
    if !cache.reclamation_pending()? {
        return Ok(None);
    }
    let reclamation = cache.reclaim_free_pages()?;
    cache.create_trades_bulk_delete_indexes()?;
    cache.clear_reclamation_pending()?;
    Ok(Some(reclamation))
}

/// Append one outcome from an explicit purge command to its durable ledger.
///
/// Automatic purge stages no longer exist; this records only direct operator
/// invocations and never changes the command's already-determined exit code.
pub fn append_direct_purge_status(
    eval_results_dir: &Path,
    stage: &str,
    exit_code: i32,
) -> Result<(), BootstrapError> {
    std::fs::create_dir_all(eval_results_dir)?;
    let path = eval_results_dir.join("purge_status.jsonl");
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    serde_json::to_writer(
        &mut file,
        &serde_json::json!({
            "ts_unix": OffsetDateTime::now_utc().unix_timestamp(),
            "level": if exit_code == 0 { "info" } else { "warn" },
            "message": "direct purge command exit",
            "stage": stage,
            "exit_code": exit_code,
        }),
    )?;
    writeln!(file)?;
    file.flush()?;
    Ok(())
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    static RECOVERY_TRACE: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());
    fn recovery_trace_collect(sql: &str) {
        RECOVERY_TRACE.lock().unwrap().push(sql.to_owned());
    }

    static INTERRUPT_ON_RECLAIM: std::sync::Mutex<Option<rusqlite::InterruptHandle>> =
        std::sync::Mutex::new(None);
    fn interrupt_on_reclaim(sql: &str) {
        if sql.contains("incremental_vacuum")
            && let Some(h) = INTERRUPT_ON_RECLAIM.lock().unwrap().as_ref()
        {
            h.interrupt();
        }
    }

    #[test]
    fn bulk_reclaim_failure_recreates_and_retains_marker() {
        // #538 ordering precedence, reclaim half: the connection's own interrupt
        // handle fires from the SQL trace the moment `incremental_vacuum` starts
        // — a deterministic OperationInterrupted with no new fault machinery.
        // The bulk run must propagate the RECLAIM error, still attempt index
        // recreation, and leave `reclamation_pending` set.
        let dir = tempfile::TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("c.db")).unwrap();
        // Non-empty freelist so the pragma has real steps to be interrupted in.
        cache
            .raw_conn_mut_for_test()
            .execute_batch(
                "CREATE TABLE junk (x BLOB); \
                 INSERT INTO junk SELECT randomblob(4096) FROM \
                   (WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i < 200) \
                    SELECT i FROM n); \
                 DROP TABLE junk;",
            )
            .unwrap();
        *INTERRUPT_ON_RECLAIM.lock().unwrap() =
            Some(cache.raw_conn_mut_for_test().get_interrupt_handle());
        cache.install_sql_trace(Some(interrupt_on_reclaim));

        let config = BootstrapConfig {
            purge_enabled: true,
            purge_archive_enabled: false,
            purge_bulk_min_wallets: 1,
            cache_path: dir.path().join("c.db"),
            ..BootstrapConfig::default()
        };
        let rows = [PurgeRow {
            wallet_hex: "0x00000000000000000000000000000000000000dd".to_owned(),
            reason: PurgeReason::DeadWeight,
        }];
        let err = execute_purge_rows(&config, &mut cache, &rows, 1_700_000_000, true, "purge")
            .unwrap_err();
        cache.install_sql_trace(None);
        *INTERRUPT_ON_RECLAIM.lock().unwrap() = None;

        assert!(
            format!("{err}").to_lowercase().contains("interrupt"),
            "the RECLAIM stage's error wins after a successful delete (got: {err})"
        );
        assert!(
            cache.reclamation_pending().unwrap(),
            "marker retained after a failed reclamation — recovery owed"
        );
        // Recreate-then-propagate: both indexes exist despite the reclaim error.
        let names: Vec<String> = {
            let conn = cache.raw_conn_mut_for_test();
            let mut stmt = conn
                .prepare("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='trades'")
                .unwrap();
            let names = stmt
                .query_map([], |r| r.get::<_, String>(0))
                .unwrap()
                .map(Result::unwrap)
                .collect::<Vec<String>>();
            drop(stmt);
            names
        };
        assert!(names.iter().any(|n| n == "idx_trades_market_id"));
        assert!(
            names
                .iter()
                .any(|n| n == "idx_trades_buy_market_outcome_wallet_ts")
        );
    }

    #[test]
    fn reclamation_pending_recovers_without_index_drop() {
        // #538 recovery contract: with the marker set and an empty delete set,
        // the armed subthreshold path reclaims, ensures indexes idempotently,
        // and clears the marker — WITHOUT ever issuing `DROP INDEX` (schema-on-
        // open already healed any absence; a second drop/rebuild would repeat
        // the multi-hour bulk cost for nothing).
        let dir = tempfile::TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("c.db")).unwrap();
        cache.set_reclamation_pending().unwrap();

        let config = BootstrapConfig {
            purge_enabled: true,
            purge_archive_enabled: false,
            cache_path: dir.path().join("c.db"),
            ..BootstrapConfig::default()
        };
        RECOVERY_TRACE.lock().unwrap().clear();
        cache.install_sql_trace(Some(recovery_trace_collect));
        let (report, bulk) =
            execute_purge_rows(&config, &mut cache, &[], 1_700_000_000, true, "purge").unwrap();
        cache.install_sql_trace(None);

        assert!(!bulk, "empty delete set is subthreshold");
        assert_eq!(report.tombstones_written, 0);
        assert!(
            !cache.reclamation_pending().unwrap(),
            "recovery cleared the marker"
        );
        let stmts = RECOVERY_TRACE.lock().unwrap().clone();
        assert!(
            stmts.iter().any(|q| q.contains("incremental_vacuum")),
            "recovery reclaimed: {stmts:?}"
        );
        assert!(
            !stmts
                .iter()
                .any(|q| q.to_uppercase().contains("DROP INDEX")),
            "recovery must never drop indexes: {stmts:?}"
        );
    }
}
