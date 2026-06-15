//! Supabase (PostgREST) reader for the copy-trade wallet-ranking handoff (issue #339).
//!
//! The local ranker pushes append-only ranking batches to Supabase
//! (`scripts/push_ranking_to_supabase.py`); `pe-service` reads the `latest_ranking`
//! view and maps each row to a [`WatchlistEntry`]. The mapping is pure and unit-tested;
//! [`fetch`] is the only I/O.
//!
//! Numeric columns are decoded losslessly: PostgREST may serialize `numeric` as either a
//! JSON number or a string, so they are parsed through [`serde_json::Value`] →
//! [`Decimal`] (via the exact decimal literal), never through `f64`.

use std::str::FromStr as _;

use pe_core_types::{BasisPoints, ReconstructionQuality, SourceTimestamp, WalletAddress};
use pe_trader_index::{Watchlist, WatchlistEntry, WatchlistTier};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive as _;
use serde::Deserialize;
use time::OffsetDateTime;

/// t-stat → basis-points scale for `leader_score_bps`. The score only orders entries
/// within the live set (`watchlist.rs`: entries sorted descending by `leader_score_bps`);
/// it is not a gate. A t-stat of 2.5 maps to 2500 bps. See `docs/_GLOSSARY.md`.
const LS_TSTAT_BPS_SCALE: i64 = 1_000;

/// Number of top-ranked wallets fetched per refresh — the live copy set (`?limit=`).
/// The ranker pushes a deeper top-200 batch; #3 (online policy) widens the fetch.
/// See `docs/_GLOSSARY.md`: `supabase_fetch_limit`.
pub const SUPABASE_FETCH_LIMIT: usize = 25;

/// Upper bound on the accumulated (additive, never-evicted) live set across refreshes;
/// matches the ranker's top-200 push. Eviction/demotion is deferred to the online policy
/// (#3). See `docs/_GLOSSARY.md`: `supabase_live_cap`.
pub const SUPABASE_LIVE_CAP: usize = 200;

/// Reconstruction quality assigned to Supabase-sourced wallets. The ranker has already
/// applied its own data-quality gates, so these wallets are treated as fully reconstructed
/// (`100`) for the copy path's `LeaderAction` classification.
const SUPABASE_RECONSTRUCTION_QUALITY: u8 = 100;

/// Error surface for a Supabase fetch. The refresh loop logs and continues on any of these.
#[derive(Debug, thiserror::Error)]
pub enum SupabaseError {
    /// The HTTP request itself failed (DNS, connect, timeout, …).
    #[error("supabase request failed: {0}")]
    Transport(reqwest::Error),
    /// The endpoint returned a non-2xx status.
    #[error("supabase returned HTTP {0}")]
    Status(u16),
    /// The 2xx body could not be decoded as the expected JSON rows.
    #[error("supabase response decode failed: {0}")]
    Decode(reqwest::Error),
}

/// One row of the `latest_ranking` view. Unused columns (`batch_id`, `rank`, `ls_edge`,
/// `fill_rate`, `avg_price`) are ignored by serde.
#[derive(Debug, Deserialize)]
struct RankingRow {
    wallet_hex: String,
    /// Mean payoff among filled positions ∈ [0,1] = Kelly `p`. → `win_rate_bps`.
    #[serde(default)]
    hit_rate: Option<serde_json::Value>,
    /// Latency-shifted net-edge t-stat. → `leader_score_bps` (ordering only).
    #[serde(default)]
    ls_tstat: Option<serde_json::Value>,
    /// Number of filled positions in the eligibility window. → `closed_trades_in_window`.
    #[serde(default)]
    n_trades: Option<i64>,
}

/// Parse a PostgREST numeric cell (JSON number or string) into a [`Decimal`] without f64.
fn cell_to_decimal(v: Option<&serde_json::Value>) -> Option<Decimal> {
    match v {
        // `Number::to_string` emits the exact decimal literal (e.g. "0.63"), which
        // `Decimal::from_str` parses precisely — no float round-trip.
        Some(serde_json::Value::Number(n)) => Decimal::from_str(&n.to_string()).ok(),
        Some(serde_json::Value::String(s)) => Decimal::from_str(s.trim()).ok(),
        _ => None,
    }
}

/// Map one ranking row to a [`WatchlistEntry`]. Returns `None` only when `wallet_hex` is
/// not a valid address (the row is skipped); all other fields fall back to honest zeros.
fn map_row(row: &RankingRow) -> Option<WatchlistEntry> {
    // Reuse the address serde impl (validates `0x` + 40 hex). Bad hex → skip the row.
    let wallet: WalletAddress =
        serde_json::from_value(serde_json::Value::String(row.wallet_hex.clone())).ok()?;

    let win_rate_bps = cell_to_decimal(row.hit_rate.as_ref())
        .map(|hr| (hr * Decimal::from(10_000)).round())
        .and_then(|d| d.to_i32())
        .unwrap_or(0)
        .clamp(0, 10_000);

    let leader_score_bps = cell_to_decimal(row.ls_tstat.as_ref())
        .map(|t| (t * Decimal::from(LS_TSTAT_BPS_SCALE)).round())
        .and_then(|d| d.to_i32())
        .unwrap_or(0);

    let closed_trades_in_window = row
        .n_trades
        .filter(|&n| n >= 0)
        .and_then(|n| u32::try_from(n).ok())
        .unwrap_or(0);

    // `100` is statically in range, so `new` cannot fail here; `.ok()?` keeps the lint
    // and the proof-of-validity together without an `unwrap`.
    let reconstruction_quality =
        ReconstructionQuality::new(SUPABASE_RECONSTRUCTION_QUALITY).ok()?;

    Some(WatchlistEntry {
        wallet,
        tier: WatchlistTier::Active,
        leader_score_bps: BasisPoints(leader_score_bps),
        lcb_5pct_bps: BasisPoints(0),
        win_rate_bps: BasisPoints(win_rate_bps),
        closed_trades_in_window,
        reconstruction_quality,
    })
}

/// Assemble fetched rows into a [`Watchlist`] (all tier `Active`, snapshot stamped now).
fn to_watchlist(rows: &[RankingRow]) -> Watchlist {
    let entries: Vec<WatchlistEntry> = rows.iter().filter_map(map_row).collect();
    let active_count = entries
        .iter()
        .filter(|e| e.tier == WatchlistTier::Active)
        .count();
    let total = entries.len();
    Watchlist {
        entries,
        snapshot_at: SourceTimestamp(OffsetDateTime::now_utc()),
        active_count,
        incubator_count: total - active_count,
    }
}

/// Fetch the latest ranking from Supabase and map it to a [`Watchlist`].
///
/// `GET {base_url}/rest/v1/latest_ranking?order=rank&limit={limit}` with `apikey`
/// (publishable/anon) and `Authorization: Bearer` (service-role secret) headers; either
/// key falls back to the other if one is empty.
pub async fn fetch(
    client: &reqwest::Client,
    base_url: &str,
    anon_key: &str,
    secret_key: &str,
    limit: usize,
) -> Result<Watchlist, SupabaseError> {
    let url = format!(
        "{}/rest/v1/latest_ranking?order=rank&limit={}",
        base_url.trim_end_matches('/'),
        limit
    );
    let apikey = if anon_key.is_empty() {
        secret_key
    } else {
        anon_key
    };
    let bearer = if secret_key.is_empty() {
        anon_key
    } else {
        secret_key
    };

    let resp = client
        .get(&url)
        .header("apikey", apikey)
        .header(reqwest::header::AUTHORIZATION, format!("Bearer {bearer}"))
        .send()
        .await
        .map_err(SupabaseError::Transport)?;

    let status = resp.status();
    if !status.is_success() {
        return Err(SupabaseError::Status(status.as_u16()));
    }
    let rows: Vec<RankingRow> = resp.json().await.map_err(SupabaseError::Decode)?;
    Ok(to_watchlist(&rows))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use serde_json::json;

    const HEX_A: &str = "0x0000000000000000000000000000000000000001";

    fn row(
        wallet_hex: &str,
        hit: serde_json::Value,
        tstat: serde_json::Value,
        n: Option<i64>,
    ) -> RankingRow {
        RankingRow {
            wallet_hex: wallet_hex.to_string(),
            hit_rate: Some(hit),
            ls_tstat: Some(tstat),
            n_trades: n,
        }
    }

    #[test]
    fn hit_rate_number_maps_to_win_rate_bps() {
        let e = map_row(&row(HEX_A, json!(0.63), json!(2.5), Some(42))).unwrap();
        assert_eq!(e.win_rate_bps.0, 6300);
        assert_eq!(e.leader_score_bps.0, 2500);
        assert_eq!(e.closed_trades_in_window, 42);
        assert_eq!(e.tier, WatchlistTier::Active);
        assert_eq!(e.lcb_5pct_bps.0, 0);
    }

    #[test]
    fn hit_rate_string_maps_identically() {
        // PostgREST may quote numerics; the string path must match the number path.
        let e = map_row(&row(HEX_A, json!("0.63"), json!("2.5"), Some(42))).unwrap();
        assert_eq!(e.win_rate_bps.0, 6300);
        assert_eq!(e.leader_score_bps.0, 2500);
    }

    #[test]
    fn null_or_zero_numerics_fall_back_to_zero() {
        let mut r = row(HEX_A, json!(null), json!(null), None);
        r.hit_rate = None;
        r.ls_tstat = None;
        let e = map_row(&r).unwrap();
        assert_eq!(e.win_rate_bps.0, 0);
        assert_eq!(e.leader_score_bps.0, 0);
        assert_eq!(e.closed_trades_in_window, 0);

        let e0 = map_row(&row(HEX_A, json!(0), json!(0), Some(0))).unwrap();
        assert_eq!(e0.win_rate_bps.0, 0);
        assert_eq!(e0.leader_score_bps.0, 0);
    }

    #[test]
    fn win_rate_is_clamped_to_basis_point_range() {
        // A degenerate hit_rate above 1.0 must not exceed 10_000 bps (Kelly p ≤ 1).
        let e = map_row(&row(HEX_A, json!(1.5), json!(0), Some(1))).unwrap();
        assert_eq!(e.win_rate_bps.0, 10_000);
    }

    #[test]
    fn bad_hex_row_is_skipped() {
        assert!(map_row(&row("not-a-wallet", json!(0.5), json!(1.0), Some(1))).is_none());
        assert!(map_row(&row("0x123", json!(0.5), json!(1.0), Some(1))).is_none());
    }

    #[test]
    fn to_watchlist_collects_valid_rows_only() {
        let rows = vec![
            row(HEX_A, json!(0.6), json!(2.0), Some(10)),
            row("bad", json!(0.6), json!(2.0), Some(10)),
        ];
        let wl = to_watchlist(&rows);
        assert_eq!(wl.entries.len(), 1);
        assert_eq!(wl.active_count, 1);
        assert_eq!(wl.incubator_count, 0);
    }
}
