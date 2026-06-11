//! Incremental FIFO ledger for the walk-forward ranker.
//!
//! [`IncrementalLedger`] maintains per-wallet FIFO bucket state and accumulates
//! [`ClosedTrade`]s as raw trades arrive in timestamp order. Advancing to day D
//! costs O(T_on_day) instead of O(T_<D), making 90-day backtests significantly
//! faster at large trade volumes.

use std::collections::{HashMap, HashSet};

use pe_core_types::{ReconstructionQuality, Side, WalletAddress};

use crate::{
    ledger::{ClosedTrade, OpenPosition, TraderLedger},
    reconstruction::{BucketKey, Fill, fills_to_open, match_against_queue, quality_score},
    snapshot::RawTrade,
};

struct WalletState {
    closed_trades: Vec<ClosedTrade>,
    /// Per `(market_id, outcome_id)`: (buy_queue, sell_queue) — both FIFO, oldest first.
    buckets: HashMap<BucketKey, (Vec<Fill>, Vec<Fill>)>,
}

impl WalletState {
    fn new() -> Self {
        Self {
            closed_trades: Vec::new(),
            buckets: HashMap::new(),
        }
    }

    fn apply(&mut self, trade: &RawTrade) {
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
        let (buy_q, sell_q) = self
            .buckets
            .entry(key.clone())
            .or_insert_with(|| (Vec::new(), Vec::new()));

        match trade.side {
            Side::Buy => {
                if sell_q.is_empty() {
                    buy_q.push(fill);
                } else if let Some(rem) =
                    match_against_queue(fill, sell_q, Side::Sell, &key, &mut self.closed_trades)
                {
                    buy_q.push(rem);
                }
            }
            Side::Sell => {
                if buy_q.is_empty() {
                    sell_q.push(fill);
                } else if let Some(rem) =
                    match_against_queue(fill, buy_q, Side::Buy, &key, &mut self.closed_trades)
                {
                    sell_q.push(rem);
                }
            }
        }
    }

    fn to_ledger(&self, wallet: WalletAddress, audit_window_days: u32) -> Option<TraderLedger> {
        let mut open: Vec<OpenPosition> = Vec::new();
        for (key, (buy_fills, sell_fills)) in &self.buckets {
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

        let closed_c: u64 = self
            .closed_trades
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
                Err(_) => return None, // structurally unreachable
            },
        };

        Some(TraderLedger {
            wallet,
            reconstruction_quality,
            closed_trades: self.closed_trades.clone(),
            open_positions: open,
            audit_window_days,
        })
    }
}

/// Maintains per-wallet FIFO ledger state and updates it incrementally as new
/// trades arrive in timestamp order.
///
/// Advancing to day D costs O(T_on_day) rather than O(T_<D), dramatically
/// reducing per-day work in walk-forward backtests.
///
/// # Precondition
///
/// Trades passed to [`apply_batch`] must be sorted ascending by `timestamp.0`.
/// The caller is responsible for this invariant (e.g. by passing a sorted
/// `all_trades` slice from `run_simulation`).
#[derive(Default)]
pub struct IncrementalLedger {
    states: HashMap<WalletAddress, WalletState>,
}

impl IncrementalLedger {
    pub fn new() -> Self {
        Self {
            states: HashMap::new(),
        }
    }

    /// Incorporate a sorted slice of new trades into per-wallet FIFO state.
    ///
    /// # Precondition
    ///
    /// `trades` must be sorted ascending by `timestamp.0`. Violating this
    /// produces incorrect FIFO matching without panicking.
    pub fn apply_batch(&mut self, trades: &[RawTrade]) {
        for trade in trades {
            self.states
                .entry(trade.wallet)
                .or_insert_with(WalletState::new)
                .apply(trade);
        }
    }

    /// Materialise [`TraderLedger`]s for `pool` wallets (or all tracked wallets
    /// when `pool` is `None`).
    ///
    /// This is a snapshot read — it borrows current state without mutating it.
    pub fn build_ledgers(
        &self,
        pool: Option<&HashSet<WalletAddress>>,
        audit_window_days: u32,
    ) -> Vec<TraderLedger> {
        let mut ledgers: Vec<TraderLedger> = match pool {
            Some(p) => p
                .iter()
                .filter_map(|wallet| {
                    self.states
                        .get(wallet)?
                        .to_ledger(*wallet, audit_window_days)
                })
                .collect(),
            None => self
                .states
                .iter()
                .filter_map(|(wallet, state)| state.to_ledger(*wallet, audit_window_days))
                .collect(),
        };
        ledgers.sort_by_key(|l| l.wallet.0);
        ledgers
    }
}
