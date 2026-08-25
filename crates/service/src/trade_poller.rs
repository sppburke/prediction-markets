//! Per-wallet Polymarket trade poller.
//!
//! Polls `UserTradeActivity` for every watchlisted wallet on a fixed interval and pushes
//! parsed [`IncomingTrade`]s into a bounded mpsc channel for the orchestrator.
//! This module contains all the I/O for Polymarket trade ingestion; the orchestrator
//! itself is pure dispatch logic.
//!
//! #511 held cursor: the per-wallet delivery cursor NEVER advances past a trade the
//! orchestrator has not durably marked seen — it is a correctness-preserving lower bound
//! (the #357 "cursor is only ever a lower bound" invariant, now load-bearing), so every
//! abandoned-unseen trade (RPC failure, staging failure, crash between enqueue and
//! commit) is refetched until seen. The activity clock (`last_activity_unix`) advances
//! independently so holding can never fake inactivity. A window whose completeness is
//! unprovable (unparseable row, failed/oversized paged scan) FREEZES the cursor — loud,
//! and self-healing via seen-dedup on the refetch.

use std::sync::Arc;
use std::time::Duration;

use pe_copy_signal_engine::IncomingTrade;
use pe_paper_state::PaperStateDb;
use pe_source_polymarket_public::{PageFetcher, PolymarketEndpoint};
use std::collections::HashSet;
use time::OffsetDateTime;
use tokio::sync::mpsc;
use tracing::warn;

use crate::health::SharedHealth;
use crate::live_watchlist::LiveWatchlist;
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
    live_watchlist: LiveWatchlist,
    fetcher: F,
    tx: mpsc::Sender<IncomingTrade>,
    paper_state: Arc<PaperStateDb>,
    health: SharedHealth,
}

impl<F: PageFetcher + Send + 'static> TradePoller<F> {
    pub fn new(
        config: TradePollerConfig,
        live_watchlist: LiveWatchlist,
        fetcher: F,
        tx: mpsc::Sender<IncomingTrade>,
        paper_state: Arc<PaperStateDb>,
        health: SharedHealth,
    ) -> Self {
        Self {
            config,
            live_watchlist,
            fetcher,
            tx,
            paper_state,
            health,
        }
    }

    /// Run the polling loop until the channel is closed (i.e. orchestrator dropped).
    ///
    /// Each round re-reads the live wallet set (so Supabase refreshes take effect without
    /// a restart, #339), fetches trades for every wallet in sequence, then sleeps for
    /// `poll_interval_secs`. Returns when the downstream channel is closed.
    pub async fn run(self) {
        loop {
            let watchlist = self.live_watchlist.snapshot();
            // #530 poll-round health: distinguishes "round ran and reached the
            // API" from "a trade was admitted" (the conflation the health split
            // fixes). Any successful fetch marks the round good; an all-error
            // round grows the streak.
            let mut round_fetch_ok = 0usize;
            let mut round_fetch_err = 0usize;
            for entry in &watchlist.entries {
                let wallet = entry.wallet;
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
                    Err(e) => {
                        round_fetch_err += 1;
                        warn!(wallet = %wallet, error = %e, "trade fetch error");
                    }
                    Ok(bytes) => {
                        round_fetch_ok += 1;
                        // A successful fetch means the Polymarket source is reachable —
                        // mark liveness even when the wallet had no new trades. The
                        // orchestrator advances this too on each trade, but a sparse
                        // buy-and-hold cohort can go minutes without one; without this,
                        // /health/ready would false-flag polymarket_source_stale.
                        {
                            let mut h = self
                                .health
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner);
                            h.polymarket_last_event_at = Some(OffsetDateTime::now_utc());
                        }
                        match trade_parser::parse_trades_counted(&bytes, wallet) {
                            Err(e) => {
                                warn!(wallet = %wallet, error = %e, "trade parse error")
                            }
                            Ok((trades, malformed)) => {
                                let mut window_complete = malformed == 0;
                                let mut window = trades;
                                // #511: a full first page means the window may be
                                // truncated — an unseen trade behind 500 rows must
                                // still hold the cursor. Rescan via the strict
                                // descending page endpoint with a FIXED end bound.
                                if window.len() >= PAGE_ROWS {
                                    // Data-derived fixed end bound (deterministic; `end`
                                    // is inclusive): the rescan proves (start, end];
                                    // anything newer arrives next round.
                                    let end = window
                                        .iter()
                                        .map(|t| t.observed_at.unix_timestamp())
                                        .max()
                                        .unwrap_or(0);
                                    match self.paged_window(wallet, start, end).await {
                                        Some(paged) => window = paged,
                                        None => window_complete = false,
                                    }
                                }
                                if self
                                    .deliver_and_advance(wallet, window, window_complete)
                                    .await
                                {
                                    // Channel closed; orchestrator is shutting down.
                                    return;
                                }
                            }
                        }
                    }
                }
            }

            // #530: commit the round's health verdict. An empty watchlist round
            // proves nothing either way, so it leaves both fields untouched.
            if round_fetch_ok > 0 || round_fetch_err > 0 {
                let mut h = self
                    .health
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if round_fetch_ok > 0 {
                    h.poll_last_round_at = Some(OffsetDateTime::now_utc());
                    h.poll_error_streak = 0;
                } else {
                    h.poll_error_streak = h.poll_error_streak.saturating_add(1);
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

impl<F: PageFetcher + Send + 'static> TradePoller<F> {
    /// Rescan the window `(start, now]` with the strict descending page endpoint under a
    /// FIXED end bound (#511): dedup by `source_trade_id`, stop at a short page. Returns
    /// `None` when completeness is unprovable (page error, parse error, malformed row,
    /// page cap) — the caller then FREEZES the cursor.
    async fn paged_window(
        &self,
        wallet: pe_core_types::WalletAddress,
        start: Option<i64>,
        end: i64,
    ) -> Option<Vec<IncomingTrade>> {
        let mut seen_ids = HashSet::new();
        let mut out = Vec::new();
        for page in 0..POLLER_MAX_PAGES {
            let url = PolymarketEndpoint::UserTradeActivityPage {
                user: format!("{wallet}"),
                end,
                start,
                offset: page.checked_mul(PAGE_ROWS_U32)?,
            }
            .url(&self.config.base_url);
            let bytes = match self.fetcher.fetch_page(&url).await {
                Ok(b) => b,
                Err(e) => {
                    warn!(wallet = %wallet, error = %e, page, "paged rescan fetch error; freezing cursor");
                    return None;
                }
            };
            let (trades, malformed) = match trade_parser::parse_trades_counted(&bytes, wallet) {
                Ok(v) => v,
                Err(e) => {
                    warn!(wallet = %wallet, error = %e, page, "paged rescan parse error; freezing cursor");
                    return None;
                }
            };
            if malformed > 0 {
                warn!(wallet = %wallet, malformed, page, "paged rescan malformed rows; freezing cursor");
                return None;
            }
            let n = trades.len();
            for t in trades {
                if seen_ids.insert(t.source_trade_id.clone()) {
                    out.push(t);
                }
            }
            if n < PAGE_ROWS {
                return Some(out);
            }
        }
        warn!(
            wallet = %wallet,
            max_pages = POLLER_MAX_PAGES,
            "paged rescan exceeded the page cap; freezing cursor (window too large to prove)"
        );
        None
    }

    /// Deliver the window's unseen trades oldest-first, advance the activity clock, and
    /// advance or hold the delivery cursor (#511). Returns `true` when the downstream
    /// channel closed.
    async fn deliver_and_advance(
        &self,
        wallet: pe_core_types::WalletAddress,
        mut window: Vec<IncomingTrade>,
        window_complete: bool,
    ) -> bool {
        // Oldest-first: ledger ingestion and Entry/Add classification are order-sensitive
        // (paged rescans arrive newest-first).
        window.sort_by_key(|t| t.observed_at.unix_timestamp());
        // Activity clock: the newest observed trade, ALWAYS (independent of holds).
        if let Some(ts) = window.iter().map(|t| t.observed_at.unix_timestamp()).max()
            && let Err(e) = self.paper_state.set_activity(&wallet, ts)
        {
            warn!(wallet = %wallet, error = %e, "paper-state set_activity failed");
        }
        let now_unix = OffsetDateTime::now_utc().unix_timestamp();
        let mut min_unseen: Option<i64> = None;
        let mut max_ts: Option<i64> = None;
        let mut unseen = Vec::new();
        for trade in window {
            let ts = trade.observed_at.unix_timestamp();
            max_ts = Some(max_ts.map_or(ts, |m: i64| m.max(ts)));
            // Read failure ⇒ treat as unseen (fail closed: hold rather than lose).
            let is_seen = self
                .paper_state
                .is_seen(&trade.source_trade_id)
                .unwrap_or(false);
            if !is_seen {
                min_unseen = Some(min_unseen.map_or(ts, |m: i64| m.min(ts)));
                if now_unix.saturating_sub(ts) > HELD_WARN_AFTER_SECS {
                    warn!(
                        wallet = %wallet,
                        trade = %trade.source_trade_id,
                        age_secs = now_unix.saturating_sub(ts),
                        "trade unseen past the warn horizon; cursor held (never abandoned)"
                    );
                }
                unseen.push(trade);
            }
        }
        for trade in unseen {
            if self.tx.send(trade).await.is_err() {
                return true;
            }
        }
        match cursor_advance(max_ts, min_unseen, window_complete) {
            None => {}
            Some(ts) => {
                // MAX-upsert: a hold below the stored cursor keeps the stored value, and
                // the boundary-second refetch (`cursor − 1`) keeps a held `ts == cursor`
                // trade reachable.
                if let Err(e) = self.paper_state.set_cursor(&wallet, ts) {
                    warn!(wallet = %wallet, error = %e, "paper-state set_cursor failed");
                }
            }
        }
        false
    }
}

/// One `/activity` response page (`limit=500` in [`PolymarketEndpoint`]).
const PAGE_ROWS: usize = 500;
const PAGE_ROWS_U32: u32 = 500;
/// Paged-rescan cap, mirroring the canary's `ACTIVITY_MAX_PAGES` posture: a window this
/// large (>5,500 trades) cannot be proven complete — freeze instead.
const POLLER_MAX_PAGES: u32 = 11;
/// Unseen trades older than this WARN every round (observability only; never abandoned).
const HELD_WARN_AFTER_SECS: i64 = 3_600;

/// #511 cursor decision, pure for tests: `None` = freeze/no-write (incomplete window or
/// empty page); `Some(min_unseen − 1)` = hold below the oldest unseen trade;
/// `Some(max_ts)` = the whole window is seen — advance.
fn cursor_advance(
    max_ts: Option<i64>,
    min_unseen: Option<i64>,
    window_complete: bool,
) -> Option<i64> {
    if !window_complete {
        return None;
    }
    match (max_ts, min_unseen) {
        (_, Some(unseen)) => Some(unseen.saturating_sub(1)),
        (Some(max), None) => Some(max),
        (None, None) => None,
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
    fn cursor_advance_holds_below_oldest_unseen() {
        // All seen → advance to max.
        assert_eq!(super::cursor_advance(Some(100), None, true), Some(100));
        // One unseen at 90 → hold at 89 (boundary-second refetch keeps ts==90 reachable).
        assert_eq!(super::cursor_advance(Some(100), Some(90), true), Some(89));
        // Incomplete window (malformed row / failed or capped paged rescan) → freeze.
        assert_eq!(super::cursor_advance(Some(100), Some(90), false), None);
        assert_eq!(super::cursor_advance(Some(100), None, false), None);
        // Empty page → no write.
        assert_eq!(super::cursor_advance(None, None, true), None);
    }

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
