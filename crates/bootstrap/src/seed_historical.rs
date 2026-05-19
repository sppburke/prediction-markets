//! Historical-snapshot seeding phase — `pe-bootstrap seed-historical`.
//!
//! Runs the parameterised Dune `discover_wallets` query at each requested UTC
//! midnight and inserts the resulting wallet set into `leaderboard_snapshots`.
//! Idempotent: re-running with overlapping dates silently skips already-seeded rows.

use time::OffsetDateTime;

use crate::cache::WalletCache;
use crate::config::BootstrapConfig;
use crate::dune::DuneClient;
use crate::error::BootstrapError;

/// Result of the historical-snapshot seeding phase.
#[derive(Debug, Default, Clone, Copy)]
pub struct SeedHistoricalReport {
    /// Number of as-of dates requested.
    pub dates_attempted: usize,
    /// Dates skipped because a snapshot row already existed for that date.
    pub dates_skipped: usize,
    /// Total wallet rows inserted across all new snapshots.
    pub rows_inserted: usize,
}

/// Seed historical leaderboard snapshots for `as_of_dates`.
///
/// Requires `config.dune_api_key`. The Dune filter parameters mirror the live
/// path so historical snapshots use identical quality filters.
///
/// # Precondition
/// `cache` must be open. `as_of_dates` should contain UTC midnights; non-midnight
/// times are accepted but may produce unexpected `snapshot_at_unix` values.
pub async fn run_seed_historical(
    config: &BootstrapConfig,
    cache: &mut WalletCache,
    as_of_dates: &[OffsetDateTime],
) -> Result<SeedHistoricalReport, BootstrapError> {
    let api_key = config
        .dune_api_key
        .clone()
        .ok_or_else(|| BootstrapError::MissingEnv("PE_DUNE_API_KEY".to_owned()))?;
    let dune = DuneClient::new(api_key);

    let already_seeded: std::collections::HashSet<i64> =
        cache.all_snapshot_dates()?.into_iter().collect();

    let mut rows_inserted: usize = 0;
    let mut dates_skipped: usize = 0;

    for as_of in as_of_dates {
        let unix = as_of.unix_timestamp();
        if already_seeded.contains(&unix) {
            tracing::info!(
                as_of_unix = unix,
                as_of = %as_of,
                "seed-historical: snapshot already present — skipping"
            );
            dates_skipped += 1;
            continue;
        }
        tracing::info!(
            as_of_unix = unix,
            as_of = %as_of,
            "seed-historical: seeding historical snapshot via dune"
        );
        let wallets = dune
            .discover_wallets(
                *as_of,
                config.dune_min_closed_markets,
                config.dune_min_win_rate_pct,
                config.dune_active_window_days,
                config.dune_max_avg_hours_to_resolution,
            )
            .await?;
        cache.insert_snapshot(unix, &wallets)?;
        rows_inserted += wallets.len();
        tracing::info!(
            as_of_unix = unix,
            wallets = wallets.len(),
            "seed-historical: snapshot inserted"
        );
    }

    Ok(SeedHistoricalReport {
        dates_attempted: as_of_dates.len(),
        dates_skipped,
        rows_inserted,
    })
}

/// Parse `PE_SEED_AS_OF_DATES`: comma-separated `YYYY-MM-DD` UTC dates.
///
/// Returns `Ok(Vec::new())` when the variable is unset or empty (caller treats this
/// as "no seeding requested"). Whitespace around each entry is trimmed; empty
/// entries (e.g. trailing comma) are skipped.
pub fn parse_seed_as_of_env(value: &str) -> Result<Vec<OffsetDateTime>, BootstrapError> {
    let mut out = Vec::new();
    for raw in value.split(',') {
        let s = raw.trim();
        if s.is_empty() {
            continue;
        }
        let date = time::Date::parse(s, &time::format_description::well_known::Iso8601::DATE)
            .map_err(|e| BootstrapError::Parse {
                message: format!("PE_SEED_AS_OF_DATES `{s}`: {e}"),
            })?;
        out.push(date.midnight().assume_utc());
    }
    Ok(out)
}
