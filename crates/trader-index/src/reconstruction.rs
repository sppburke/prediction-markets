//! Ledger reconstruction algorithm.
//!
//! [`build_trader_ledgers`] groups raw trades by wallet and FIFO-matches entry
//! fills with exit fills within each `(market_id, outcome_id)` bucket.
//!
//! # Algorithm
//! 1. Group trades by wallet, sort each wallet's trades by timestamp (FIFO).
//! 2. Per wallet, group by `(market_id, outcome_id)` and maintain two queues:
//!    one for open longs (Buy-entry) and one for open shorts (Sell-entry).
//! 3. A Buy fill closes open shorts first; remaining contracts open a long.
//!    A Sell fill closes open longs first; remaining contracts open a short.
//! 4. `realized_pnl_usd = (exit_price − entry_price) × contracts`.
//! 5. `reconstruction_quality = (closed_contracts / (closed + open)) × 100`.

use std::collections::{HashMap, HashSet};

use pe_core_types::{
    ContractQty, MarketId, OutcomeId, Price, ReconstructionQuality, Side, SourceTradeId,
    WalletAddress,
};
use rust_decimal::Decimal;

use crate::{
    ledger::{ClosedTrade, OpenPosition, TraderLedger},
    snapshot::RawTrade,
};

/// Build wallet-level [`TraderLedger`]s from a trade slice.
///
/// - `trades` — all raw trades to reconstruct; may be any order (sorted per-wallet internally).
/// - `audit_window_days` — stored on each returned ledger for downstream scoring.
/// - `wallet_filter` — when `Some`, only builds ledgers for wallets in the set; when `None`,
///   builds for every wallet present in `trades`.
///
/// Pure and deterministic: same inputs always produce the same output.
pub fn build_trader_ledgers(
    trades: &[RawTrade],
    audit_window_days: u32,
    wallet_filter: Option<&HashSet<WalletAddress>>,
) -> Vec<TraderLedger> {
    let mut by_wallet: HashMap<WalletAddress, Vec<&RawTrade>> = HashMap::new();
    for trade in trades {
        if wallet_filter.is_some_and(|f| !f.contains(&trade.wallet)) {
            continue;
        }
        by_wallet.entry(trade.wallet).or_default().push(trade);
    }

    let mut wallets: Vec<WalletAddress> = by_wallet.keys().copied().collect();
    wallets.sort_by_key(|w| w.0);

    let mut ledgers = Vec::with_capacity(wallets.len());

    for wallet in wallets {
        let trades = &by_wallet[&wallet];

        let mut sorted: Vec<&RawTrade> = trades.to_vec();
        sorted.sort_by_key(|t| t.timestamp.0.unix_timestamp());

        let mut closed: Vec<ClosedTrade> = Vec::new();
        // Per-bucket: (buy_queue, sell_queue) — both FIFO (oldest first).
        let mut buckets: HashMap<BucketKey, (Vec<Fill>, Vec<Fill>)> = HashMap::new();

        for trade in &sorted {
            let key = BucketKey {
                market_id: trade.market_id.clone(),
                outcome_id: trade.outcome_id,
            };
            let fill = Fill {
                price: trade.price,
                contracts: trade.contracts.0,
                timestamp_unix: trade.timestamp.0.unix_timestamp(),
                source_trade_id: trade.source_trade_id.clone(),
            };
            let (buy_q, sell_q) = buckets
                .entry(key.clone())
                .or_insert_with(|| (Vec::new(), Vec::new()));

            match trade.side {
                Side::Buy => {
                    if sell_q.is_empty() {
                        buy_q.push(fill);
                    } else {
                        if let Some(rem) =
                            match_against_queue(fill, sell_q, Side::Sell, &key, &mut closed)
                        {
                            buy_q.push(rem);
                        }
                    }
                }
                Side::Sell => {
                    if buy_q.is_empty() {
                        sell_q.push(fill);
                    } else {
                        if let Some(rem) =
                            match_against_queue(fill, buy_q, Side::Buy, &key, &mut closed)
                        {
                            sell_q.push(rem);
                        }
                    }
                }
            }
        }

        let mut open: Vec<OpenPosition> = Vec::new();
        for (key, (buy_fills, sell_fills)) in &buckets {
            if let Some(pos) = fills_to_open(key, Side::Buy, buy_fills) {
                open.push(pos);
            }
            if let Some(pos) = fills_to_open(key, Side::Sell, sell_fills) {
                open.push(pos);
            }
        }
        open.sort_by(|a, b| {
            a.market_id
                .0
                .0
                .cmp(&b.market_id.0.0)
                .then(a.outcome_id.0.cmp(&b.outcome_id.0))
        });

        let closed_c: u64 = closed
            .iter()
            .map(|c| c.contracts.0)
            .fold(0u64, |a, b| a.saturating_add(b));
        let open_c: u64 = open
            .iter()
            .map(|o| o.contracts.0)
            .fold(0u64, |a, b| a.saturating_add(b));

        let quality_raw = quality_score(closed_c, open_c);
        let reconstruction_quality = match ReconstructionQuality::new(quality_raw) {
            Ok(q) => q,
            Err(_) => match ReconstructionQuality::new(0) {
                Ok(q) => q,
                Err(_) => continue, // structurally unreachable
            },
        };

        ledgers.push(TraderLedger {
            wallet,
            reconstruction_quality,
            closed_trades: closed,
            open_positions: open,
            audit_window_days,
        });
    }

    ledgers
}

/// Compound key for per-wallet, per-(market, outcome) position buckets.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct BucketKey {
    pub(crate) market_id: MarketId,
    pub(crate) outcome_id: OutcomeId,
}

/// A single trade fill tracked inside a FIFO queue.
pub(crate) struct Fill {
    pub(crate) price: Price,
    pub(crate) contracts: u64,
    pub(crate) timestamp_unix: i64,
    pub(crate) source_trade_id: SourceTradeId,
}

pub(crate) fn quality_score(closed_contracts: u64, open_contracts: u64) -> u8 {
    let total = closed_contracts.saturating_add(open_contracts);
    if total == 0 {
        return 0;
    }
    ((closed_contracts as u128 * 100) / total as u128).min(100) as u8
}

/// FIFO-match a closing fill against the head of `entries`.
///
/// Creates [`ClosedTrade`] records for each matched lot.
/// Returns `Some(Fill)` with remaining contracts if the closing fill was only
/// partially matched (entries exhausted before closing fill was fully consumed).
pub(crate) fn match_against_queue(
    mut closing: Fill,
    entries: &mut Vec<Fill>,
    entry_side: Side,
    key: &BucketKey,
    closed: &mut Vec<ClosedTrade>,
) -> Option<Fill> {
    let mut n_consumed = 0usize;

    while closing.contracts > 0 && n_consumed < entries.len() {
        let matched = closing.contracts.min(entries[n_consumed].contracts);

        let (entry_price, exit_price) = match entry_side {
            Side::Buy => (entries[n_consumed].price, closing.price),
            Side::Sell => (closing.price, entries[n_consumed].price),
        };

        // pnl = (exit − entry) × contracts; $1 face value per contract.
        let pnl = (exit_price.0 - entry_price.0) * Decimal::from(matched);

        let hold_secs = closing
            .timestamp_unix
            .saturating_sub(entries[n_consumed].timestamp_unix)
            .max(0) as u64;

        let entry_tid = entries[n_consumed].source_trade_id.clone();
        let opened_at_unix = entries[n_consumed].timestamp_unix;

        closed.push(ClosedTrade {
            market_id: key.market_id.clone(),
            outcome_id: key.outcome_id,
            side: entry_side,
            entry_price,
            exit_price,
            contracts: ContractQty(matched),
            hold_duration_seconds: hold_secs,
            realized_pnl_usd: pnl,
            opened_at_unix,
            closed_at_unix: closing.timestamp_unix,
            source_trade_ids: vec![entry_tid, closing.source_trade_id.clone()],
        });

        closing.contracts -= matched;
        entries[n_consumed].contracts -= matched;
        if entries[n_consumed].contracts == 0 {
            n_consumed += 1;
        }
    }

    // Remove fully consumed entries in one pass.
    entries.drain(0..n_consumed);

    if closing.contracts > 0 {
        Some(closing)
    } else {
        None
    }
}

/// Aggregate remaining fills in a queue into a single [`OpenPosition`].
///
/// Returns `None` when all fills are fully consumed (contracts == 0).
pub(crate) fn fills_to_open(key: &BucketKey, side: Side, fills: &[Fill]) -> Option<OpenPosition> {
    let total_contracts: u64 = fills
        .iter()
        .map(|f| f.contracts)
        .fold(0u64, |a, b| a.saturating_add(b));
    if total_contracts == 0 {
        return None;
    }

    // FIFO-weighted average entry price.
    let total_cost: Decimal = fills.iter().fold(Decimal::ZERO, |acc, f| {
        acc + f.price.0 * Decimal::from(f.contracts)
    });
    let avg_decimal = (total_cost / Decimal::from(total_contracts))
        .max(Decimal::ZERO)
        .min(Decimal::ONE);

    // avg_decimal is clamped to [0, 1]; Price::new cannot fail here.
    let avg_entry_price = match Price::new(avg_decimal) {
        Ok(p) => p,
        Err(_) => Price::ZERO, // structurally unreachable after clamp
    };

    let source_trade_ids: Vec<SourceTradeId> =
        fills.iter().map(|f| f.source_trade_id.clone()).collect();

    Some(OpenPosition {
        market_id: key.market_id.clone(),
        outcome_id: key.outcome_id,
        side,
        avg_entry_price,
        contracts: ContractQty(total_contracts),
        source_trade_ids,
    })
}
