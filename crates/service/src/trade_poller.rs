//! Per-wallet Polymarket trade poller.
//!
//! Polls `UserTradeActivity` for every watchlisted wallet on a fixed interval and pushes
//! parsed [`IncomingTrade`]s into a bounded mpsc channel for the orchestrator.
//! This module contains all the I/O for Polymarket trade ingestion; the orchestrator
//! itself is pure dispatch logic.

use std::sync::Arc;
use std::time::Duration;

use pe_copy_signal_engine::IncomingTrade;
use pe_core_types::WalletAddress;
use pe_paper_state::PaperStateDb;
use pe_source_polymarket_public::{PageFetcher, PolymarketEndpoint};
use tokio::sync::mpsc;
use tracing::warn;

use crate::trade_parser;

/// Configuration for the trade poller.
#[derive(Debug, Clone)]
pub struct TradePollerConfig {
    /// Base URL for the Polymarket Data API (no trailing slash).
    pub base_url: String,
    /// Seconds between polling rounds (one round = all wallets).
    /// Set to 0 in tests to poll once without sleeping.
    pub poll_interval_secs: u64,
}

/// A live trade poller that drives a [`PageFetcher`] per wallet.
pub struct TradePoller<F: PageFetcher> {
    config: TradePollerConfig,
    wallets: Vec<WalletAddress>,
    fetcher: F,
    tx: mpsc::Sender<IncomingTrade>,
    paper_state: Arc<PaperStateDb>,
}

impl<F: PageFetcher + Send + 'static> TradePoller<F> {
    pub fn new(
        config: TradePollerConfig,
        wallets: Vec<WalletAddress>,
        fetcher: F,
        tx: mpsc::Sender<IncomingTrade>,
        paper_state: Arc<PaperStateDb>,
    ) -> Self {
        Self {
            config,
            wallets,
            fetcher,
            tx,
            paper_state,
        }
    }

    /// Run the polling loop until the channel is closed (i.e. orchestrator dropped).
    ///
    /// Each round fetches trades for every wallet in sequence, then sleeps for
    /// `poll_interval_secs`. Returns when the downstream channel is closed.
    pub async fn run(self) {
        loop {
            for &wallet in &self.wallets {
                // Timestamp cursor: the stored cursor is the newest observed_at (whole
                // seconds) seen for this wallet. The endpoint's `start` is *exclusive*
                // (`timestamp > start`), so we fetch from `cursor - 1` to re-include the
                // boundary second — otherwise an unseen trade sharing the newest second
                // would never be fetched, and the orchestrator's source_trade_id dedup
                // can only drop trades that *are* fetched. Re-fetched already-seen trades
                // are deduped. (A trade arriving more than one second out of order
                // relative to the cursor is not re-fetched; the cursor is a bandwidth
                // optimisation on the backstop poll path, not a correctness gate.)
                let start = match self.paper_state.cursor(&wallet) {
                    Ok(cursor) => cursor_start(cursor),
                    Err(e) => {
                        warn!(wallet = %wallet, error = %e, "paper-state cursor read failed; full fetch");
                        None
                    }
                };
                let url = PolymarketEndpoint::UserTradeActivity {
                    user: format!("{wallet}"),
                    end: None,
                    start,
                }
                .url(&self.config.base_url);

                match self.fetcher.fetch_page(&url).await {
                    Err(e) => warn!(wallet = %wallet, error = %e, "trade fetch error"),
                    Ok(bytes) => match trade_parser::parse_trades(&bytes, wallet) {
                        Err(e) => warn!(wallet = %wallet, error = %e, "trade parse error"),
                        Ok(trades) => {
                            // Advance the cursor to the newest trade observed this round.
                            let max_ts =
                                trades.iter().map(|t| t.observed_at.unix_timestamp()).max();
                            for trade in trades {
                                if self.tx.send(trade).await.is_err() {
                                    // Channel closed; orchestrator is shutting down.
                                    return;
                                }
                            }
                            if let Some(ts) = max_ts
                                && let Err(e) = self.paper_state.set_cursor(&wallet, ts)
                            {
                                warn!(wallet = %wallet, error = %e, "paper-state set_cursor failed");
                            }
                        }
                    },
                }
            }

            if self.config.poll_interval_secs > 0 {
                tokio::time::sleep(Duration::from_secs(self.config.poll_interval_secs)).await;
            } else {
                // poll_interval_secs == 0 → single-shot (used in tests).
                return;
            }
        }
    }
}

/// Exclusive `start` bound for the next `/activity` fetch given the stored cursor.
///
/// The endpoint's `start` is exclusive (`timestamp > start`), so we step back one
/// second to re-include the cursor's boundary second; downstream dedup drops the
/// already-seen trades. `None` (no cursor yet) means an unbounded fetch.
fn cursor_start(cursor: Option<i64>) -> Option<i64> {
    cursor.map(|c| c.saturating_sub(1))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::cursor_start;

    #[test]
    fn cursor_start_steps_back_one_second_to_cover_the_boundary() {
        // No cursor → unbounded.
        assert_eq!(cursor_start(None), None);
        // Exclusive `start = cursor - 1` re-includes the boundary second `cursor`.
        assert_eq!(cursor_start(Some(1_700_000_000)), Some(1_699_999_999));
        // Never underflows.
        assert_eq!(cursor_start(Some(0)), Some(-1));
    }
}
