//! Build operator identities from cached Etherscan funder edges.
//!
//! Reads pre-populated `funder_edges` from the bootstrap SQLite cache instead of calling
//! Etherscan at backtest time. The cache is populated by `pe-bootstrap` when
//! `PE_BOOTSTRAP_FETCH_FUNDER_GRAPH=1`.
//!
//! Known approximation: funder relationships are discovered at run time, not at each
//! simulated time T. For Polymarket proxy wallets the funder relationship is established
//! at deposit time, so the bias is small. Documented in `WinnerFollowReport.funder_graph_snapshot_caveat`.

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

/// Build operator identities from cached funder edges.
///
/// Returns an empty `Vec` with a warning when the cache has no funder edges — run
/// `pe-bootstrap` with `PE_BOOTSTRAP_FETCH_FUNDER_GRAPH=1` to populate the cache.
///
/// # Precondition
/// `cache` must have been populated by a prior bootstrap run with
/// `PE_BOOTSTRAP_FETCH_FUNDER_GRAPH=1`. Empty cache is handled gracefully.
pub fn build_funder_graph(
    cache: &WalletCache,
    all_trades: &[RawTrade],
) -> Result<Vec<OperatorIdentity>, BacktestError> {
    let edge_pairs = cache.load_funder_edges().map_err(BacktestError::Cache)?;

    if edge_pairs.is_empty() {
        warn!(
            "funder edge cache is empty — run pe-bootstrap with \
             PE_BOOTSTRAP_FETCH_FUNDER_GRAPH=1 to populate; operator clustering disabled"
        );
        return Ok(Vec::new());
    }

    let snapshot_ts = SourceTimestamp(OffsetDateTime::now_utc());

    let edges: Vec<FundingEdge> = edge_pairs
        .into_iter()
        .map(|(funder, funded)| FundingEdge {
            funder,
            funded,
            amount_usd: Decimal::ZERO,
            timestamp: snapshot_ts.clone(),
        })
        .collect();

    info!(edges = edges.len(), "funder edges loaded from cache");

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

    let identities = build_operator_identities(&funding_snapshot, &ClusteringConfig::default())
        .map_err(BacktestError::OperatorGraph)?;

    info!(
        operators = identities.len(),
        "operator graph built from cache"
    );
    Ok(identities)
}
