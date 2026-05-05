//! Phase 0: build funder graph from Etherscan before simulation.
//!
//! For each wallet in the cache, calls `EtherscanFunderLookup::funders_of` one at a
//! time (batch size 1 to obtain exact funder→funded edges). Rate limiting is handled
//! internally by `EtherscanFunderLookup` (200 ms between requests).
//!
//! Known approximation: funder relationships are discovered at run time, not at each
//! simulated time T. For Polymarket proxy wallets the funder relationship is established
//! at deposit time, so the bias is small. Documented in `WinnerFollowReport.funder_graph_snapshot_caveat`.

use std::collections::{HashMap, HashSet};

use pe_core_types::{SourceTimestamp, WalletAddress};
use pe_operator_graph::{
    ClusteringConfig, FundingEdge, FundingSnapshot, OperatorIdentity, build_operator_identities,
};
use pe_source_onchain_polygon::{BlockRange, EtherscanFunderLookup, FunderLookup};
use pe_trader_index::snapshot::RawTrade;
use rust_decimal::Decimal;
use time::OffsetDateTime;
use tracing::info;

use crate::error::BacktestError;

// Polygon CTF Exchange V1 deploy block — same constant used in source-onchain-polygon.
const CTF_V1_DEPLOY_BLOCK: u64 = 33_605_403;
// Generous upper bound; Etherscan paginates internally if needed.
const FUNDER_DISCOVERY_TO_BLOCK: u64 = 80_000_000;

/// Build operator identities from Etherscan funder discovery.
///
/// Returns an empty `Vec` if `api_key` is `None` (Phase 0 skipped).
pub async fn build_funder_graph(
    api_key: Option<&str>,
    wallet_addrs: &[String],
    all_trades: &[RawTrade],
) -> Result<Vec<OperatorIdentity>, BacktestError> {
    let Some(api_key) = api_key else {
        tracing::warn!(
            "PE_ETHERSCAN_API_KEY not set — skipping Phase 0 funder discovery; operator clustering disabled"
        );
        return Ok(Vec::new());
    };

    let block_range = BlockRange {
        from: CTF_V1_DEPLOY_BLOCK,
        to: FUNDER_DISCOVERY_TO_BLOCK,
    };

    let lookup = EtherscanFunderLookup::new(api_key.to_owned());
    let snapshot_ts = SourceTimestamp(OffsetDateTime::now_utc());

    let total = wallet_addrs.len();
    let mut edges: Vec<FundingEdge> = Vec::new();

    for (i, hex) in wallet_addrs.iter().enumerate() {
        let wallet = WalletAddress::from_hex(hex.trim())
            .map_err(|e| BacktestError::InvalidAddress(format!("{hex}: {e}")))?;

        if (i + 1) % 500 == 0 || i + 1 == total {
            info!(
                progress = i + 1,
                total,
                "funder discovery: {}/{} wallets",
                i + 1,
                total
            );
        }

        let wallets: HashSet<WalletAddress> = std::iter::once(wallet).collect();
        let funders = lookup.funders_of(&wallets, block_range).await?;
        for funder in funders {
            edges.push(FundingEdge {
                funder,
                funded: wallet,
                amount_usd: Decimal::ZERO,
                timestamp: snapshot_ts.clone(),
            });
        }
    }

    info!(edges = edges.len(), "funder discovery complete");

    // Count closed trades per wallet for anti-gaming detection.
    let mut closed_trades_per_wallet: HashMap<WalletAddress, u32> = HashMap::new();
    for trade in all_trades {
        *closed_trades_per_wallet.entry(trade.wallet).or_default() += 1;
    }

    let funding_snapshot = FundingSnapshot {
        edges,
        wallet_ages: HashMap::new(), // unknown — age data not available at this stage
        known_external: HashMap::new(), // no CEX/bridge classification at bootstrap
        closed_trade_counts: closed_trades_per_wallet,
        realized_pnl_usd: HashMap::new(), // not available at bootstrap stage
        snapshot_at: snapshot_ts,
    };

    let identities = build_operator_identities(&funding_snapshot, &ClusteringConfig::default())
        .map_err(BacktestError::OperatorGraph)?;

    info!(operators = identities.len(), "operator graph built");
    Ok(identities)
}
