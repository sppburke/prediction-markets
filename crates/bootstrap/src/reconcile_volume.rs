//! §5 PnL-inflation reconciliation report (issue #207 Slice 1c).
//!
//! Compares per-market trade volume reported by the Polymarket Data API
//! (the `trades` table) against the USDC volume of on-chain `OrderFilled`
//! legs (the `counterparty_edges` table, populated by `counterparty-edges`
//! subcommand in Slice 1b). The headline output is the **aggregate inflation
//! ratio** `data_api_usd / on_chain_usd` across markets where both sides have
//! data — Yang et al. (SSRN 6556613) report a ~42% inflation prior on a
//! similar dataset.
//!
//! ## Method
//!
//! For each `counterparty_edges` row, we identify the USDC leg and resolve
//! the market `condition_id` via the `token_conditions` map (Slice 0).
//!
//! - **V2**: `maker_asset_id_dec` is always the position token id (and resolves
//!   to a `condition_id` in `token_conditions`); the other leg is implicit
//!   USDC. The maker's direction is in `side`:
//!     - `side = 0` (maker BUY): maker pays USDC = `maker_amount_raw`
//!     - `side = 1` (maker SELL): maker receives USDC = `taker_amount_raw`
//! - **V1**: both `maker_asset_id_dec` and `taker_asset_id_dec` are explicit
//!   ERC-1155 ids. Whichever resolves in `token_conditions` is the position
//!   token; the other is the USDC leg whose amount counts toward USDC volume.
//!   If neither resolves the row is skipped (no condition attribution); if
//!   both resolve the row is skipped (anomalous — two position-token legs
//!   in a single fill).
//!
//! Raw amounts are uint256-decimal strings; USDC has 6 decimals
//! ([`COLLATERAL_DECIMALS`]) so the USD value is `raw / 10^6`. Per-market
//! sums are decimal-pure (`rust_decimal`).
//!
//! ## Output
//!
//! [`ReconcileReport`] reports the reconciled-market count, the markets
//! present on only one side (gap diagnostics), and the aggregate +
//! per-market ratio distribution (median, p25, p75, count above 1.5×).
//!
//! ## Preconditions
//!
//! - `counterparty_edges` is populated (run `pe-bootstrap counterparty-edges`).
//! - `token_conditions` is populated (run `pe-bootstrap events`).
//! - `trades` is populated by the normal Polymarket Data-API bootstrap.
//!
//! If `counterparty_edges` is empty the report returns cleanly with
//! `markets_reconciled = 0` (not an error — operationally, the user has just
//! not run the on-chain scan yet).

use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;

use rust_decimal::Decimal;
use tracing::info;

use pe_source_onchain_polygon::contracts::COLLATERAL_DECIMALS;

use crate::cache::{CounterpartyEdgeResolvedRow, WalletCache};
use crate::error::BootstrapError;

/// One USDC = `10^6` raw uint256 units (USDC.e on Polygon).
fn collateral_divisor() -> Decimal {
    Decimal::from(10u64.pow(COLLATERAL_DECIMALS))
}

/// Markets with a per-market inflation ratio above this threshold are counted
/// in the `markets_above_threshold` field of the report. `1.5` flags 50%+
/// inflation — well past Yang et al.'s ~42% prior, the "very inflated" tail.
const INFLATION_THRESHOLD: &str = "1.5";

/// Outcome of one `run_reconcile_volume` invocation.
///
/// `PartialEq`/`Eq` enable the read-only-vs-read-write scenario test (see
/// `tests/scenario_reconcile_volume_read_only.rs`) to assert byte-identical
/// reports; all fields are integer/decimal/option-thereof, no floats, so
/// `Eq` is sound.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReconcileReport {
    /// Markets with both Data-API trades and on-chain USDC fills (the only
    /// markets where a ratio is computable).
    pub markets_reconciled: usize,
    /// Markets in `trades` with no resolved on-chain USDC volume (either no
    /// `counterparty_edges` rows for this market or its token isn't in
    /// `token_conditions`).
    pub markets_only_data_api: usize,
    /// Markets with on-chain USDC fills but no Data-API trades (rare — flags
    /// either a Polymarket Data API completeness gap or, conversely, a
    /// market that traded on-chain via the CTF Exchange but doesn't surface
    /// through the public API).
    pub markets_only_on_chain: usize,
    /// Sum of Data-API USD volume across the reconciled markets.
    pub data_api_volume_usd: Decimal,
    /// Sum of on-chain USDC volume across the reconciled markets.
    pub on_chain_volume_usd: Decimal,
    /// Aggregate ratio: `data_api_volume_usd / on_chain_volume_usd`. `1.0`
    /// means perfect agreement; `>1.0` means Data-API is inflated; `<1.0`
    /// means Data-API is undercounting (rare).
    pub aggregate_inflation_ratio: Option<Decimal>,
    /// Median of the per-market ratio distribution.
    pub median_inflation_ratio: Option<Decimal>,
    /// 25th percentile of the per-market ratio distribution.
    pub p25_inflation_ratio: Option<Decimal>,
    /// 75th percentile of the per-market ratio distribution.
    pub p75_inflation_ratio: Option<Decimal>,
    /// Count of markets where the per-market ratio exceeds [`INFLATION_THRESHOLD`].
    pub markets_above_threshold: usize,
    /// On-chain legs that could not be attributed to any market (neither
    /// `maker_asset_id_dec` nor `taker_asset_id_dec` resolved in
    /// `token_conditions`). High counts here indicate either an unswept
    /// Gamma `/events` set or a V1 token id format that doesn't match.
    pub on_chain_legs_unattributed: usize,
}

/// Run the reconciliation and return a report. Read-only (no cache writes).
pub fn run_reconcile_volume(cache: &WalletCache) -> Result<ReconcileReport, BootstrapError> {
    info!("reconcile-volume: starting");

    let mut on_chain_by_market = HashMap::<String, Decimal>::new();
    let mut on_chain_legs_unattributed = 0usize;

    aggregate_on_chain_usdc(
        cache,
        &mut on_chain_by_market,
        &mut on_chain_legs_unattributed,
    )?;
    let data_api_by_market = aggregate_data_api_usd(cache)?;

    let (reconciled, only_data_api, only_on_chain) =
        partition_markets(&data_api_by_market, &on_chain_by_market);

    let mut data_api_total = Decimal::ZERO;
    let mut on_chain_total = Decimal::ZERO;
    let mut per_market_ratios: Vec<Decimal> = Vec::with_capacity(reconciled.len());
    let threshold = Decimal::from_str(INFLATION_THRESHOLD).map_err(|_| BootstrapError::Internal)?;
    let mut markets_above = 0usize;
    for market in &reconciled {
        let d = data_api_by_market[market];
        let o = on_chain_by_market[market];
        data_api_total += d;
        on_chain_total += o;
        if !o.is_zero() {
            let r = d / o;
            per_market_ratios.push(r);
            if r > threshold {
                markets_above += 1;
            }
        }
    }
    let aggregate_inflation_ratio = if on_chain_total.is_zero() {
        None
    } else {
        Some(data_api_total / on_chain_total)
    };
    let (median, p25, p75) = percentiles(&mut per_market_ratios);

    let report = ReconcileReport {
        markets_reconciled: reconciled.len(),
        markets_only_data_api: only_data_api,
        markets_only_on_chain: only_on_chain,
        data_api_volume_usd: data_api_total,
        on_chain_volume_usd: on_chain_total,
        aggregate_inflation_ratio,
        median_inflation_ratio: median,
        p25_inflation_ratio: p25,
        p75_inflation_ratio: p75,
        markets_above_threshold: markets_above,
        on_chain_legs_unattributed,
    };

    info!(
        markets_reconciled = report.markets_reconciled,
        markets_only_data_api = report.markets_only_data_api,
        markets_only_on_chain = report.markets_only_on_chain,
        data_api_usd = %report.data_api_volume_usd,
        on_chain_usd = %report.on_chain_volume_usd,
        aggregate_ratio = ?report.aggregate_inflation_ratio,
        median_ratio = ?report.median_inflation_ratio,
        markets_above_1_5 = report.markets_above_threshold,
        on_chain_legs_unattributed = report.on_chain_legs_unattributed,
        "reconcile-volume: complete"
    );

    Ok(report)
}

/// Open the cache **read-only** and run the reconciliation. Operationally
/// useful: lets `pe-bootstrap reconcile-volume` run alongside another
/// `pe-bootstrap` invocation that holds the writer (e.g. a long
/// `counterparty-edges` scan) without conflicting on the
/// `CacheMutationLock`, since SQLite WAL mode supports concurrent readers
/// during writes.
///
/// Mirrors the `coverage` subcommand's read-only path
/// ([`crate::coverage::run_coverage`], issue #208). The underlying
/// [`run_reconcile_volume`] does not mutate the cache (`&WalletCache`, not
/// `&mut`), so the read-only open is strictly safer than the shared
/// read-write open in `main.rs`.
///
/// The database at `cache_path` must already exist and have been migrated
/// (opened at least once via [`WalletCache::open`]); see
/// [`WalletCache::open_read_only`] for the open-time contract.
pub fn run_reconcile_volume_read_only(
    cache_path: &Path,
) -> Result<ReconcileReport, BootstrapError> {
    let cache = WalletCache::open_read_only(cache_path)?;
    run_reconcile_volume(&cache)
}

/// Iterate `counterparty_edges` and aggregate USDC volume per market.
fn aggregate_on_chain_usdc(
    cache: &WalletCache,
    out: &mut HashMap<String, Decimal>,
    out_unattributed: &mut usize,
) -> Result<(), BootstrapError> {
    let divisor = collateral_divisor();
    cache.for_each_counterparty_edge_resolved(|r: CounterpartyEdgeResolvedRow| {
        let resolution = resolve_usdc_leg(
            r.contract_version,
            r.side,
            &r.maker_condition,
            &r.taker_condition,
        );
        let (cond, usdc_raw) = match resolution {
            Some(LegResolution::MakerAmountIsUsdc { condition }) => {
                (condition, &r.maker_amount_raw)
            }
            Some(LegResolution::TakerAmountIsUsdc { condition }) => {
                (condition, &r.taker_amount_raw)
            }
            None => {
                *out_unattributed += 1;
                return Ok(());
            }
        };
        let raw = Decimal::from_str(usdc_raw).map_err(|_| BootstrapError::Internal)?;
        let usdc = raw / divisor;
        *out.entry(cond).or_insert(Decimal::ZERO) += usdc;
        Ok(())
    })
}

/// Disambiguate which leg of a fill carries USDC, returning the condition id
/// to attribute the volume to.
#[derive(Debug, PartialEq, Eq)]
enum LegResolution {
    MakerAmountIsUsdc { condition: String },
    TakerAmountIsUsdc { condition: String },
}

fn resolve_usdc_leg(
    contract_version: i64,
    side: Option<i64>,
    maker_cond: &Option<String>,
    taker_cond: &Option<String>,
) -> Option<LegResolution> {
    match contract_version {
        2 => {
            // V2: maker_asset_id_dec is the position tokenId (always); the
            // other leg is implicit USDC, with the amount determined by side.
            let condition = maker_cond.clone()?;
            match side {
                Some(0) => Some(LegResolution::MakerAmountIsUsdc { condition }),
                Some(1) => Some(LegResolution::TakerAmountIsUsdc { condition }),
                _ => None, // malformed V2 row (side must be 0 or 1)
            }
        }
        1 => {
            // V1: whichever leg resolves in token_conditions is the position
            // token; the other is USDC. If both or neither resolve, skip.
            match (maker_cond, taker_cond) {
                (Some(c), None) => Some(LegResolution::TakerAmountIsUsdc {
                    condition: c.clone(),
                }),
                (None, Some(c)) => Some(LegResolution::MakerAmountIsUsdc {
                    condition: c.clone(),
                }),
                _ => None, // both or neither resolved — anomalous
            }
        }
        _ => None, // unknown contract_version
    }
}

/// Per-market Data-API USD volume. `price_str` is a Decimal-string in $/contract;
/// `contracts` is the integer count. Sum in Rust (decimal-pure) so we don't
/// introduce f64 anywhere.
fn aggregate_data_api_usd(cache: &WalletCache) -> Result<HashMap<String, Decimal>, BootstrapError> {
    let mut out = HashMap::<String, Decimal>::new();
    cache.for_each_trade_volume(|market_id, price_str, contracts| {
        let price = Decimal::from_str(&price_str).map_err(|_| BootstrapError::Internal)?;
        let usd = price * Decimal::from(contracts);
        *out.entry(market_id).or_insert(Decimal::ZERO) += usd;
        Ok(())
    })?;
    Ok(out)
}

/// Partition the two market sets into (intersection, only-data-api, only-on-chain).
fn partition_markets(
    data_api: &HashMap<String, Decimal>,
    on_chain: &HashMap<String, Decimal>,
) -> (Vec<String>, usize, usize) {
    let mut intersection: Vec<String> = Vec::new();
    let mut only_data_api = 0usize;
    for k in data_api.keys() {
        if on_chain.contains_key(k) {
            intersection.push(k.clone());
        } else {
            only_data_api += 1;
        }
    }
    let only_on_chain = on_chain
        .keys()
        .filter(|k| !data_api.contains_key(*k))
        .count();
    intersection.sort(); // deterministic order
    (intersection, only_data_api, only_on_chain)
}

/// Median + p25 + p75 of a slice of decimals. Returns `None` for each if the
/// slice is empty. Decimal sort is total-ordered (no NaN concerns). Index
/// arithmetic is integer-only (no f64): for percentile `numerator/100`, the
/// chosen index is `floor((numerator * (n-1)) / 100)` — numpy's "lower"
/// interpolation, exactly.
fn percentiles(values: &mut [Decimal]) -> (Option<Decimal>, Option<Decimal>, Option<Decimal>) {
    if values.is_empty() {
        return (None, None, None);
    }
    values.sort();
    let n_minus_1 = values.len().saturating_sub(1);
    let pick = |numerator: usize| -> Decimal {
        let idx = (numerator.saturating_mul(n_minus_1) / 100).min(n_minus_1);
        values[idx]
    };
    (Some(pick(50)), Some(pick(25)), Some(pick(75)))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use rusqlite::params;
    use tempfile::TempDir;

    /// 1 USDC raw = 10^6.
    fn usdc(amount: u64) -> String {
        Decimal::from(amount * 1_000_000).to_string()
    }

    /// Build a populated cache with trades + token_conditions + counterparty_edges
    /// covering both V1 + V2 fills + unmapped tokens.
    fn populated_cache() -> (TempDir, WalletCache) {
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("c.db")).unwrap();

        // Token map: "111" → 0xAA (one market), "999" → 0xBB (another). Token
        // "222" intentionally NOT mapped (USDC leg on V1 trades for 0xAA).
        cache
            .upsert_token_conditions_batch(
                &[
                    ("111".to_string(), "0xaa".to_string()),
                    ("999".to_string(), "0xbb".to_string()),
                ],
                100,
            )
            .unwrap();

        // Data-API trades: market 0xaa has 10 USD volume; market 0xbb has 50;
        // 0xcc has only Data-API trades (no on-chain — only_data_api gap test).
        // Inserted via raw_conn_for_test so we control price_str without
        // going through the RawTrade -> Decimal -> string round-trip.
        let raw = cache.raw_conn_for_test();
        raw.execute(
            "INSERT INTO trades VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params!["t1", "0xa", "0xaa", 0, "buy", "0.50", 20i64, 100i64],
        )
        .unwrap(); // 10 USD
        raw.execute(
            "INSERT INTO trades VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params!["t2", "0xa", "0xbb", 0, "buy", "0.25", 200i64, 100i64],
        )
        .unwrap(); // 50 USD
        raw.execute(
            "INSERT INTO trades VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params!["t3", "0xa", "0xcc", 0, "buy", "1.00", 1i64, 100i64],
        )
        .unwrap();

        // counterparty_edges:
        // (1) V2 fill on market 0xbb, side=0 (maker BUY) → USDC = maker_amount.
        //     5 USDC, so Data-API/on-chain ratio for 0xbb = 50/5 = 10.0.
        // (2) V1 fill on market 0xaa: maker_asset_id="222" (USDC, unmapped),
        //     taker_asset_id="111" (position). maker_amount is USDC = 8 USDC,
        //     so Data-API/on-chain ratio for 0xaa = 10/8 = 1.25.
        // (3) V1 fill where BOTH legs are mapped — must skip (anomalous).
        // (4) V1 fill where NEITHER leg is mapped — must skip + count as
        //     unattributed.
        #[allow(clippy::type_complexity)]
        let edges: Vec<(
            String,
            i64,
            i64,
            i64,
            String,
            i64,
            String,
            String,
            String,
            Option<String>,
            Option<i64>,
            String,
            String,
            String,
        )> = vec![
            (
                "0x1".to_string(),
                0,
                100,
                100,
                "0xc".to_string(),
                2,
                "0xa".to_string(),
                "0xb".to_string(),
                "999".to_string(),
                None,
                Some(0),
                usdc(5),
                usdc(0), // ignored for V2 side=0
                "0".to_string(),
            ),
            (
                "0x2".to_string(),
                0,
                100,
                100,
                "0xc".to_string(),
                1,
                "0xa".to_string(),
                "0xb".to_string(),
                "222".to_string(),
                Some("111".to_string()),
                None,
                usdc(8),  // maker amount = USDC
                usdc(20), // taker amount = position tokens (ignored for ratio)
                "0".to_string(),
            ),
            (
                "0x3".to_string(),
                0,
                100,
                100,
                "0xc".to_string(),
                1,
                "0xa".to_string(),
                "0xb".to_string(),
                "111".to_string(), // both
                Some("999".to_string()),
                None,
                usdc(1),
                usdc(1),
                "0".to_string(),
            ),
            (
                "0x4".to_string(),
                0,
                100,
                100,
                "0xc".to_string(),
                1,
                "0xa".to_string(),
                "0xb".to_string(),
                "555".to_string(), // neither
                Some("777".to_string()),
                None,
                usdc(1),
                usdc(1),
                "0".to_string(),
            ),
        ];
        cache.upsert_counterparty_edges_batch(&edges).unwrap();

        (dir, cache)
    }

    #[test]
    fn aggregates_v1_and_v2_and_computes_per_market_ratios() {
        let (_dir, cache) = populated_cache();
        let report = run_reconcile_volume(&cache).unwrap();

        assert_eq!(report.markets_reconciled, 2); // 0xaa + 0xbb
        assert_eq!(report.markets_only_data_api, 1); // 0xcc (trades only)
        assert_eq!(report.markets_only_on_chain, 0);
        assert_eq!(
            report.on_chain_legs_unattributed, 2,
            "two V1 rows (both/neither resolved) must skip + count as unattributed"
        );

        // Totals: data_api = 10 + 50 = 60; on_chain = 8 + 5 = 13.
        // Aggregate ratio = 60/13 ≈ 4.615.
        assert_eq!(report.data_api_volume_usd, Decimal::from(60));
        assert_eq!(report.on_chain_volume_usd, Decimal::from(13));
        let agg = report.aggregate_inflation_ratio.unwrap();
        let expected = Decimal::from(60) / Decimal::from(13);
        assert!(
            (agg - expected).abs() < Decimal::from_str("0.000001").unwrap(),
            "aggregate ratio {agg} vs expected {expected}"
        );

        // Per-market ratios: 0xaa = 10/8 = 1.25; 0xbb = 50/5 = 10.
        // Median of 2 values: lower-interpolation picks index floor(0.5*1)=0 → 1.25.
        let median = report.median_inflation_ratio.unwrap();
        assert_eq!(median, Decimal::from_str("1.25").unwrap());

        // Threshold = 1.5; only 0xbb's 10.0 ratio exceeds it (0xaa's 1.25 does not).
        assert_eq!(report.markets_above_threshold, 1);
    }

    #[test]
    fn empty_cache_yields_empty_report_without_error() {
        let dir = TempDir::new().unwrap();
        let cache = WalletCache::open(&dir.path().join("c.db")).unwrap();
        let report = run_reconcile_volume(&cache).unwrap();
        assert_eq!(report.markets_reconciled, 0);
        assert_eq!(report.markets_only_data_api, 0);
        assert_eq!(report.markets_only_on_chain, 0);
        assert_eq!(report.on_chain_legs_unattributed, 0);
        assert_eq!(report.data_api_volume_usd, Decimal::ZERO);
        assert_eq!(report.on_chain_volume_usd, Decimal::ZERO);
        assert!(report.aggregate_inflation_ratio.is_none());
        assert!(report.median_inflation_ratio.is_none());
        assert_eq!(report.markets_above_threshold, 0);
    }

    #[test]
    fn resolve_usdc_leg_v2_side_zero_means_maker_paid_usdc() {
        let r = resolve_usdc_leg(2, Some(0), &Some("0xaa".to_string()), &None);
        assert_eq!(
            r,
            Some(LegResolution::MakerAmountIsUsdc {
                condition: "0xaa".to_string()
            })
        );
    }

    #[test]
    fn resolve_usdc_leg_v2_side_one_means_maker_received_usdc() {
        let r = resolve_usdc_leg(2, Some(1), &Some("0xaa".to_string()), &None);
        assert_eq!(
            r,
            Some(LegResolution::TakerAmountIsUsdc {
                condition: "0xaa".to_string()
            })
        );
    }

    #[test]
    fn resolve_usdc_leg_v1_only_maker_resolves_means_taker_is_usdc() {
        let r = resolve_usdc_leg(1, None, &Some("0xaa".to_string()), &None);
        assert_eq!(
            r,
            Some(LegResolution::TakerAmountIsUsdc {
                condition: "0xaa".to_string()
            })
        );
    }

    #[test]
    fn resolve_usdc_leg_v1_only_taker_resolves_means_maker_is_usdc() {
        let r = resolve_usdc_leg(1, None, &None, &Some("0xbb".to_string()));
        assert_eq!(
            r,
            Some(LegResolution::MakerAmountIsUsdc {
                condition: "0xbb".to_string()
            })
        );
    }

    #[test]
    fn resolve_usdc_leg_v1_both_or_neither_resolved_skips() {
        assert!(
            resolve_usdc_leg(1, None, &Some("a".into()), &Some("b".into())).is_none(),
            "both legs resolved is anomalous"
        );
        assert!(
            resolve_usdc_leg(1, None, &None, &None).is_none(),
            "neither resolved → no condition attribution"
        );
    }

    #[test]
    fn percentiles_returns_lower_interpolation_indices() {
        let mut v = vec![
            Decimal::from(1),
            Decimal::from(2),
            Decimal::from(3),
            Decimal::from(4),
            Decimal::from(5),
        ];
        let (median, p25, p75) = percentiles(&mut v);
        // n=5, q=0.5 → idx = floor(0.5*4)=2 → values[2]=3
        // q=0.25 → idx = floor(0.25*4)=1 → values[1]=2
        // q=0.75 → idx = floor(0.75*4)=3 → values[3]=4
        assert_eq!(median, Some(Decimal::from(3)));
        assert_eq!(p25, Some(Decimal::from(2)));
        assert_eq!(p75, Some(Decimal::from(4)));
    }
}
