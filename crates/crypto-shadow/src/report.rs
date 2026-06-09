//! Realized-edge report: aggregate `observations` per series (5m vs 15m) and per
//! price-bucket into count / mean / p50 / p95 net-edge stats. Pure and
//! deterministic given the input rows.

use std::collections::BTreeMap;

use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive as _;
use serde::Serialize;

use crate::db::{ObsRow, RealizedRow};
use crate::fees::taker_fee_per_share;

/// Top-level report payload, serialized to JSON for `report` stdout.
#[derive(Debug, Serialize, PartialEq)]
pub struct Report {
    /// Verified `crypto_fees_v2` provenance the net edges were computed under.
    pub fee_provenance: String,
    /// Where the data was collected (region/host tag). See issue: vantage point.
    pub vantage_label: String,
    /// Startup RTT-to-endpoint summary, if probed (free-text JSON).
    pub vantage_rtt: Option<String>,
    pub total_observations: usize,
    /// **Signal** edge groups (per series × price-bucket), using the
    /// exchange-median-implied P(up) at trigger time.
    pub groups: Vec<ReportGroup>,
    /// **Realized** edge groups (per series × move-direction), using the actual
    /// Gamma market resolution as ground truth. The `up` rows are the buy-YES
    /// signal's realized performance. Empty until `resolve` has run.
    pub realized: Vec<RealizedGroup>,
}

/// One (series, move-direction) realized aggregate over observations whose
/// market has resolved. Realized YES-buy edge =
/// `(yes_won ? 1 : 0) − best_ask − fee(best_ask)`.
#[derive(Debug, Serialize, PartialEq)]
pub struct RealizedGroup {
    pub series: String,
    pub move_direction: String,
    /// Observations in this group whose market has resolved and had an ask.
    pub count_scored: usize,
    pub mean_realized_net_vs_ask: Option<Decimal>,
    pub p50_realized_net_vs_ask: Option<Decimal>,
    pub frac_realized_positive: Option<Decimal>,
}

/// One (series, price-bucket) aggregate. Edge stats are over observations whose
/// inputs were available; `count` is all observations in the group.
#[derive(Debug, Serialize, PartialEq)]
pub struct ReportGroup {
    pub series: String,
    pub price_bucket: String,
    pub count: usize,
    pub mean_net_edge_vs_ask: Option<Decimal>,
    pub p50_net_edge_vs_ask: Option<Decimal>,
    pub p95_net_edge_vs_ask: Option<Decimal>,
    pub mean_net_edge_vs_mid: Option<Decimal>,
    pub frac_net_positive_vs_ask: Option<Decimal>,
    pub p50_lag_ms: Option<i64>,
    pub p95_lag_ms: Option<i64>,
}

fn price_bucket(ask: Option<Decimal>) -> String {
    match ask {
        None => "no-ask".to_string(),
        Some(a) => {
            let scaled = (a * Decimal::from(10u32)).floor();
            let idx = scaled.to_i64().unwrap_or(0).clamp(0, 9);
            let lo = Decimal::from(idx) / Decimal::from(10u32);
            let hi = Decimal::from(idx + 1) / Decimal::from(10u32);
            format!("{lo:.2}-{hi:.2}")
        }
    }
}

fn mean_decimal(values: &[Decimal]) -> Option<Decimal> {
    if values.is_empty() {
        return None;
    }
    let sum: Decimal = values.iter().copied().sum();
    Some(sum / Decimal::from(values.len()))
}

/// Nearest-rank percentile (`q` in [0,1]) over an already-sorted slice.
fn percentile_decimal(sorted: &[Decimal], q: Decimal) -> Option<Decimal> {
    if sorted.is_empty() {
        return None;
    }
    let rank = (q * Decimal::from(sorted.len()))
        .ceil()
        .to_i64()
        .unwrap_or(1)
        .max(1);
    let idx = usize::try_from(rank - 1).unwrap_or(0).min(sorted.len() - 1);
    sorted.get(idx).copied()
}

fn percentile_i64(sorted: &[i64], q: Decimal) -> Option<i64> {
    if sorted.is_empty() {
        return None;
    }
    let rank = (q * Decimal::from(sorted.len()))
        .ceil()
        .to_i64()
        .unwrap_or(1)
        .max(1);
    let idx = usize::try_from(rank - 1).unwrap_or(0).min(sorted.len() - 1);
    sorted.get(idx).copied()
}

fn frac_positive(values: &[Decimal]) -> Option<Decimal> {
    if values.is_empty() {
        return None;
    }
    let pos = values.iter().filter(|v| **v > Decimal::ZERO).count();
    Some(Decimal::from(pos) / Decimal::from(values.len()))
}

#[derive(Default)]
struct Accum {
    count: usize,
    net_ask: Vec<Decimal>,
    net_mid: Vec<Decimal>,
    lag: Vec<i64>,
}

/// Build the report from observation rows, realized-join rows, and provenance.
pub fn build_report(
    rows: &[ObsRow],
    realized_rows: &[RealizedRow],
    fee_provenance: String,
    vantage_label: String,
    vantage_rtt: Option<String>,
) -> Report {
    let mut groups: BTreeMap<(String, String), Accum> = BTreeMap::new();
    for r in rows {
        let key = (r.series.clone(), price_bucket(r.best_ask));
        let acc = groups.entry(key).or_default();
        acc.count += 1;
        if let Some(v) = r.net_edge_vs_ask {
            acc.net_ask.push(v);
        }
        if let Some(v) = r.net_edge_vs_mid {
            acc.net_mid.push(v);
        }
        if let Some(v) = r.feed_to_book_lag_ms {
            acc.lag.push(v);
        }
    }

    let q50 = Decimal::new(5, 1);
    let q95 = Decimal::new(95, 2);
    let mut out = Vec::with_capacity(groups.len());
    for ((series, price_bucket), mut acc) in groups {
        acc.net_ask.sort_unstable();
        acc.net_mid.sort_unstable();
        acc.lag.sort_unstable();
        out.push(ReportGroup {
            series,
            price_bucket,
            count: acc.count,
            mean_net_edge_vs_ask: mean_decimal(&acc.net_ask),
            p50_net_edge_vs_ask: percentile_decimal(&acc.net_ask, q50),
            p95_net_edge_vs_ask: percentile_decimal(&acc.net_ask, q95),
            mean_net_edge_vs_mid: mean_decimal(&acc.net_mid),
            frac_net_positive_vs_ask: frac_positive(&acc.net_ask),
            p50_lag_ms: percentile_i64(&acc.lag, q50),
            p95_lag_ms: percentile_i64(&acc.lag, q95),
        });
    }

    Report {
        fee_provenance,
        vantage_label,
        vantage_rtt,
        total_observations: rows.len(),
        groups: out,
        realized: build_realized(realized_rows),
    }
}

/// Aggregate realized YES-buy edge per (series, move-direction) over the
/// observations whose market has resolved. Pure.
pub fn build_realized(rows: &[RealizedRow]) -> Vec<RealizedGroup> {
    let mut groups: BTreeMap<(String, String), Vec<Decimal>> = BTreeMap::new();
    for r in rows {
        // Score only observations with an ask and a resolved outcome.
        let (Some(ask), Some(yes_won)) = (r.best_ask, r.yes_won) else {
            continue;
        };
        let realized_yes = if yes_won { Decimal::ONE } else { Decimal::ZERO };
        let net = realized_yes - ask - taker_fee_per_share(ask);
        groups
            .entry((r.series.clone(), r.move_direction.clone()))
            .or_default()
            .push(net);
    }

    let q50 = Decimal::new(5, 1);
    let mut out = Vec::with_capacity(groups.len());
    for ((series, move_direction), mut nets) in groups {
        nets.sort_unstable();
        out.push(RealizedGroup {
            series,
            move_direction,
            count_scored: nets.len(),
            mean_realized_net_vs_ask: mean_decimal(&nets),
            p50_realized_net_vs_ask: percentile_decimal(&nets, q50),
            frac_realized_positive: frac_positive(&nets),
        });
    }
    out
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn row(series: &str, ask: &str, net_ask: &str, lag: i64) -> ObsRow {
        ObsRow {
            series: series.to_string(),
            best_ask: Some(ask.parse().unwrap()),
            net_edge_vs_ask: Some(net_ask.parse().unwrap()),
            net_edge_vs_mid: Some(net_ask.parse().unwrap()),
            feed_to_book_lag_ms: Some(lag),
        }
    }

    #[test]
    fn buckets_by_series_and_price_decile() {
        assert_eq!(price_bucket(Some(dec!(0.52))), "0.50-0.60");
        assert_eq!(price_bucket(Some(dec!(0.00))), "0.00-0.10");
        assert_eq!(price_bucket(Some(dec!(0.999))), "0.90-1.00");
        assert_eq!(price_bucket(None), "no-ask");
    }

    #[test]
    fn percentile_nearest_rank() {
        let v = vec![dec!(1), dec!(2), dec!(3), dec!(4), dec!(5)];
        assert_eq!(percentile_decimal(&v, dec!(0.5)), Some(dec!(3)));
        assert_eq!(percentile_decimal(&v, dec!(0.95)), Some(dec!(5)));
        assert_eq!(percentile_decimal(&[], dec!(0.5)), None);
    }

    #[test]
    fn mean_and_frac_positive() {
        let v = vec![dec!(-1), dec!(1), dec!(3)];
        assert_eq!(mean_decimal(&v), Some(dec!(1)));
        assert_eq!(frac_positive(&v), Some(dec!(2) / dec!(3)));
    }

    #[test]
    fn build_groups_and_counts() {
        let rows = vec![
            row("5m", "0.52", "0.10", 100),
            row("5m", "0.55", "0.20", 300),
            row("15m", "0.31", "-0.05", 50),
        ];
        let report = build_report(&rows, &[], "prov".to_string(), "local".to_string(), None);
        assert_eq!(report.total_observations, 3);
        assert!(
            report.realized.is_empty(),
            "no resolutions -> no realized groups"
        );
        // 5m/0.50-0.60 has 2 rows; 15m/0.30-0.40 has 1.
        let g = report
            .groups
            .iter()
            .find(|g| g.series == "5m" && g.price_bucket == "0.50-0.60")
            .unwrap();
        assert_eq!(g.count, 2);
        assert_eq!(g.mean_net_edge_vs_ask, Some(dec!(0.15)));
        assert_eq!(g.frac_net_positive_vs_ask, Some(dec!(1)));
        assert_eq!(g.p50_lag_ms, Some(100));
    }

    #[test]
    fn realized_edge_scores_resolved_observations_only() {
        let rr = |dir: &str, ask: &str, won: Option<bool>| RealizedRow {
            series: "5m".to_string(),
            move_direction: dir.to_string(),
            best_ask: Some(ask.parse().unwrap()),
            yes_won: won,
        };
        let rows = vec![
            // up move, ask 0.52, Up won -> 1 - 0.52 - fee(0.52) = 0.462528
            rr("up", "0.52", Some(true)),
            // up move, ask 0.60, Up lost -> 0 - 0.60 - fee(0.60) = -0.6168
            rr("up", "0.60", Some(false)),
            // unresolved -> not scored, forms no group
            rr("down", "0.40", None),
        ];
        let groups = build_realized(&rows);
        assert_eq!(groups.len(), 1, "only the resolved 'up' group is scored");
        let g = &groups[0];
        assert_eq!(g.move_direction, "up");
        assert_eq!(g.count_scored, 2);
        assert_eq!(g.frac_realized_positive, Some(dec!(0.5)));
        // mean = (0.462528 + (-0.6168)) / 2 = -0.077136
        assert_eq!(g.mean_realized_net_vs_ask, Some(dec!(-0.077136)));
    }
}
