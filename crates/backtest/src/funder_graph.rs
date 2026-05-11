//! Temporal funder/operator graph for walk-forward backtests.
//!
//! `FunderGraphTimeline` owns the full funder-edge list pre-sorted by
//! `fetched_at_unix` and exposes a `view_at(t)` binary-search slice.
//! `build_operator_identities_at` runs the existing clustering logic on that
//! time-bounded slice, eliminating the look-forward bias of the old
//! build-once-at-startup approach.

use std::collections::HashMap;

use pe_bootstrap::cache::WalletCache;
use pe_core_types::{SourceTimestamp, WalletAddress};
use pe_operator_graph::{
    ClusteringConfig, FundingEdge, FundingSnapshot, OperatorIdentity, build_operator_identities,
};
use pe_trader_index::snapshot::RawTrade;
use rust_decimal::Decimal;
use time::OffsetDateTime;
use tracing::{info, warn};

use crate::error::BacktestError;

/// Funder-edge list sorted ascending by `event_at_unix`.
///
/// Tuple layout: `(event_at_unix, funder, funded)` — timestamp first so
/// `partition_point` uses natural ordering without a field-accessor closure.
pub struct FunderGraphTimeline {
    /// `(event_at_unix, funder, funded)` sorted ascending by `event_at_unix`.
    edges: Vec<(i64, WalletAddress, WalletAddress)>,
}

impl FunderGraphTimeline {
    /// Load all funder edges from the cache, sorted by discovery timestamp.
    pub fn from_cache(cache: &WalletCache) -> Result<Self, BacktestError> {
        let raw = cache
            .load_funder_edges_with_timestamp()
            .map_err(BacktestError::Cache)?;

        if raw.is_empty() {
            warn!(
                "funder edge cache is empty — run pe-bootstrap with \
                 PE_BOOTSTRAP_FETCH_FUNDER_GRAPH=1; operator clustering disabled"
            );
            return Ok(Self { edges: Vec::new() });
        }

        let mut edges: Vec<(i64, WalletAddress, WalletAddress)> = raw
            .into_iter()
            .map(|(funder, funded, ts)| (ts, funder, funded))
            .collect();
        // Guarantee ascending order (SQL ORDER BY should already ensure this,
        // but be defensive against out-of-order rows).
        edges.sort_unstable_by_key(|(ts, _, _)| *ts);

        info!(edges = edges.len(), "funder timeline loaded from cache");
        Ok(Self { edges })
    }

    /// Empty timeline — no edges visible at any time. Used in tests that do not
    /// exercise operator clustering and in the empty-cache fast path.
    pub fn empty() -> Self {
        Self { edges: Vec::new() }
    }

    /// Return all edges whose `event_at_unix <= t`.
    ///
    /// Edges with `event_at_unix = 0` (epoch sentinel, written by the pre-`event_at_unix`
    /// schema migration) are always included because `0 <= t` for any positive simulation date.
    ///
    /// Uses `partition_point` (O(log N)). Calling with the same `t` twice returns the same slice.
    pub fn view_at(&self, t: i64) -> &[(i64, WalletAddress, WalletAddress)] {
        let count = self.edges.partition_point(|(ts, _, _)| *ts <= t);
        &self.edges[..count]
    }

    pub fn total_edge_count(&self) -> usize {
        self.edges.len()
    }

    pub fn is_empty(&self) -> bool {
        self.edges.is_empty()
    }
}

/// Build operator identities using only the funder edges visible at time `t`.
///
/// Calls `view_at(t)` for the temporal slice, then runs the same
/// `build_operator_identities` clustering logic. No look-forward bias.
pub fn build_operator_identities_at(
    timeline: &FunderGraphTimeline,
    all_trades: &[RawTrade],
    t: i64,
) -> Result<Vec<OperatorIdentity>, BacktestError> {
    let visible = timeline.view_at(t);

    if visible.is_empty() {
        return Ok(Vec::new());
    }

    let snapshot_ts = SourceTimestamp(
        OffsetDateTime::from_unix_timestamp(t).unwrap_or(OffsetDateTime::UNIX_EPOCH),
    );

    let edges: Vec<FundingEdge> = visible
        .iter()
        .map(|(_, funder, funded)| FundingEdge {
            funder: *funder,
            funded: *funded,
            amount_usd: Decimal::ZERO,
            timestamp: snapshot_ts.clone(),
        })
        .collect();

    let mut closed_trades_per_wallet: HashMap<WalletAddress, u32> = HashMap::new();
    for trade in all_trades {
        *closed_trades_per_wallet.entry(trade.wallet).or_default() += 1;
    }

    let funding_snapshot = FundingSnapshot {
        edges,
        wallet_ages: HashMap::new(),
        known_external: HashMap::new(),
        closed_trade_counts: closed_trades_per_wallet,
        realized_pnl_usd: HashMap::new(),
        snapshot_at: snapshot_ts,
    };

    build_operator_identities(&funding_snapshot, &ClusteringConfig::default())
        .map_err(BacktestError::OperatorGraph)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn addr(b: u8) -> WalletAddress {
        WalletAddress::from_hex(&format!("0x{:040x}", b)).unwrap()
    }

    fn timeline(edges: &[(i64, u8, u8)]) -> FunderGraphTimeline {
        // Build from raw (event_at_unix, funder_byte, funded_byte) triples.
        let mut sorted: Vec<(i64, WalletAddress, WalletAddress)> = edges
            .iter()
            .map(|(ts, funder, funded)| (*ts, addr(*funder), addr(*funded)))
            .collect();
        sorted.sort_unstable_by_key(|(ts, _, _)| *ts);
        FunderGraphTimeline { edges: sorted }
    }

    // ── Sentinel (event_at_unix = 0) is always visible ────────────────────────

    #[test]
    fn sentinel_edge_visible_at_any_positive_timestamp() {
        let tl = timeline(&[(0, 1, 2)]);
        // epoch 0 is always ≤ any positive sim date.
        assert_eq!(tl.view_at(1).len(), 1);
        assert_eq!(tl.view_at(1_700_000_000).len(), 1);
        assert_eq!(tl.view_at(i64::MAX).len(), 1);
    }

    #[test]
    fn future_edge_not_visible_before_its_timestamp() {
        let future_ts = 1_800_000_000_i64;
        let tl = timeline(&[(future_ts, 1, 2)]);
        assert_eq!(tl.view_at(future_ts - 1).len(), 0);
    }

    #[test]
    fn future_edge_visible_at_and_after_its_timestamp() {
        let future_ts = 1_800_000_000_i64;
        let tl = timeline(&[(future_ts, 1, 2)]);
        assert_eq!(tl.view_at(future_ts).len(), 1);
        assert_eq!(tl.view_at(future_ts + 1).len(), 1);
    }

    #[test]
    fn sentinel_always_visible_future_edge_gated() {
        // Simulate the migration scenario: some edges have timestamp=0 (migrated
        // from old schema), some have real timestamps from new bootstrap runs.
        let real_ts = 1_760_000_000_i64; // a 2025-ish timestamp
        let tl = timeline(&[(0, 1, 2), (real_ts, 3, 4)]);

        // Before real_ts: only the sentinel edge is visible.
        assert_eq!(tl.view_at(real_ts - 1).len(), 1);
        // At and after real_ts: both visible.
        assert_eq!(tl.view_at(real_ts).len(), 2);
        assert_eq!(tl.view_at(real_ts + 86_400).len(), 2);
    }

    #[test]
    fn multiple_sentinels_all_visible_at_any_time() {
        let tl = timeline(&[(0, 1, 2), (0, 3, 4), (0, 5, 6)]);
        assert_eq!(tl.view_at(1).len(), 3);
    }

    #[test]
    fn empty_timeline_returns_empty_slice() {
        let tl = FunderGraphTimeline::empty();
        assert_eq!(tl.view_at(1_700_000_000).len(), 0);
        assert!(tl.is_empty());
    }

    // ── view_at boundary semantics (inclusive) ────────────────────────────────

    #[test]
    fn view_at_is_inclusive_of_exact_timestamp() {
        let ts = 1_700_000_000_i64;
        let tl = timeline(&[(ts, 1, 2)]);
        assert_eq!(
            tl.view_at(ts).len(),
            1,
            "view_at must include edges at exactly t"
        );
        assert_eq!(tl.view_at(ts - 1).len(), 0);
    }

    #[test]
    fn view_at_multiple_edges_different_timestamps() {
        let tl = timeline(&[
            (0, 1, 2), // sentinel
            (1_700_000_000, 3, 4),
            (1_750_000_000, 5, 6),
        ]);
        assert_eq!(tl.view_at(1_699_999_999).len(), 1); // only sentinel
        assert_eq!(tl.view_at(1_700_000_000).len(), 2);
        assert_eq!(tl.view_at(1_750_000_000).len(), 3);
    }
}
