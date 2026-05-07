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

/// Funder-edge list sorted ascending by `fetched_at_unix`.
///
/// Tuple layout: `(fetched_at_unix, funder, funded)` — timestamp first so
/// `partition_point` uses natural ordering without a field-accessor closure.
pub struct FunderGraphTimeline {
    /// `(fetched_at_unix, funder, funded)` sorted ascending by `fetched_at_unix`.
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

    /// Return all edges whose `fetched_at_unix <= t`.
    ///
    /// Uses `partition_point` (O(log N)). Calling with the same `t` twice
    /// returns the same slice.
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
