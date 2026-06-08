//! Crate-local domain types for the BTC latency-arb shadow harness.
//!
//! All money/price/probability values are [`rust_decimal::Decimal`] — no `f64`
//! (see `CLAUDE.md` "Hard rules"). Book quotes reuse [`pe_core_types::Price`];
//! the BTC/USD spot value gets a crate-local newtype because `core-types` has no
//! BTC/USD price type and `VenueId` is closed to polymarket/kalshi.

use pe_core_types::Price;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// BTC/USD spot price from the Chainlink BTC/USD Data Stream. Crate-local.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BtcUsdPrice(pub Decimal);

/// Which short-horizon BTC up/down series a market belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BtcSeriesKind {
    /// `btc-up-or-down-5m`.
    Five,
    /// `btc-up-or-down-15m`.
    Fifteen,
}

impl BtcSeriesKind {
    /// Gamma `series_slug` for this series.
    pub fn gamma_series_slug(self) -> &'static str {
        match self {
            Self::Five => "btc-up-or-down-5m",
            Self::Fifteen => "btc-up-or-down-15m",
        }
    }

    /// Short stable label used in the DB and report (`"5m"` / `"15m"`).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Five => "5m",
            Self::Fifteen => "15m",
        }
    }

    /// Parse the stable label back to a series kind.
    pub fn from_str_label(s: &str) -> Option<Self> {
        match s {
            "5m" => Some(Self::Five),
            "15m" => Some(Self::Fifteen),
            _ => None,
        }
    }
}

/// A decoded Chainlink BTC/USD tick from the RTDS `crypto_prices_chainlink` feed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainlinkTick {
    /// Feed symbol, e.g. `"btc/usd"`.
    pub symbol: String,
    /// Source-supplied observation time, epoch milliseconds.
    pub observed_at_ms: i64,
    /// The BTC/USD value (this is the 5m/15m settling value).
    pub value: BtcUsdPrice,
}

/// A decoded CLOB book update for a single YES token.
#[derive(Debug, Clone, PartialEq)]
pub struct BookUpdate {
    /// CLOB asset (token) id — the YES outcome token.
    pub token_id: String,
    /// Best bid (highest buy), if the book had any bids.
    pub best_bid: Option<Price>,
    /// Best ask (lowest sell), if the book had any asks.
    pub best_ask: Option<Price>,
    /// Source-supplied observation time, epoch milliseconds; `None` if the frame
    /// omitted or carried an unparseable timestamp. Keeping it optional means a
    /// missing timestamp yields a null `feed_to_book_lag_ms`, not a spurious
    /// epoch-sized lag that would corrupt the p50/p95 latency stats.
    pub observed_at_ms: Option<i64>,
}

impl BookUpdate {
    /// Mid price `(bid + ask) / 2`, only when both sides are present.
    pub fn mid(&self) -> Option<Decimal> {
        match (self.best_bid, self.best_ask) {
            (Some(b), Some(a)) => Some((b.0 + a.0) / Decimal::from(2u32)),
            _ => None,
        }
    }
}

/// Static metadata for one BTC up/down market.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BtcMarketMeta {
    /// Polymarket `conditionId`.
    pub condition_id: String,
    /// `clobTokenIds[0]` — the YES outcome token id.
    pub yes_token_id: String,
    /// 5m or 15m series.
    pub series: BtcSeriesKind,
    /// Window start (range-start reference time), epoch milliseconds.
    pub range_start_ms: i64,
    /// Window end (settlement time), epoch milliseconds.
    pub range_end_ms: i64,
    /// Order price tick size.
    pub tick: Decimal,
}

/// One computed edge observation row. All edges are net of the verified
/// `crypto_fees_v2` taker fee; `None` fields mean an input was unavailable
/// (book not yet seen, range-start not yet captured).
#[derive(Debug, Clone, PartialEq)]
pub struct EdgeObservation {
    pub condition_id: String,
    pub series: BtcSeriesKind,
    pub observed_at_ms: i64,
    pub chainlink_value: Decimal,
    pub range_start_value: Option<Decimal>,
    /// Binary indicator P(up): 1 if `c > r`, 0.5 on tie, 0 if `c < r`; `None`
    /// if range-start is not yet known. Overstates certainty mid-window (see
    /// the issue's open risks).
    pub instantaneous_prob_up: Option<Decimal>,
    pub best_ask: Option<Decimal>,
    pub mid: Option<Decimal>,
    pub gross_edge_vs_ask: Option<Decimal>,
    pub gross_edge_vs_mid: Option<Decimal>,
    pub fee_cost: Option<Decimal>,
    pub net_edge_vs_ask: Option<Decimal>,
    pub net_edge_vs_mid: Option<Decimal>,
    /// `chainlink.observed_at_ms - book.observed_at_ms` (positive = book lags
    /// the feed). `None` if no book seen for this market.
    pub feed_to_book_lag_ms: Option<i64>,
}

/// Source of a raw inbound frame, persisted with every `raw_ticks` row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeedSource {
    Chainlink,
    Clob,
}

impl FeedSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Chainlink => "chainlink",
            Self::Clob => "clob",
        }
    }
}

/// A raw inbound frame forwarded from a WS task to the join loop. The join loop
/// persists every frame to `raw_ticks` (always) before decoding, so a corrected
/// decoder/fee can recompute offline without re-collecting live data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedFrame {
    pub source: FeedSource,
    pub received_ms: i64,
    pub raw: String,
}

/// Error decoding a raw WS frame into a typed value.
#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error("json: {0}")]
    Json(String),
    #[error("bad decimal: {0}")]
    Decimal(String),
    #[error("missing field: {0}")]
    Missing(&'static str),
}

/// Current wall-clock as epoch milliseconds. Live-path only (non-deterministic);
/// never used inside pure compute or tests.
pub fn now_unix_ms() -> i64 {
    let nanos = time::OffsetDateTime::now_utc().unix_timestamp_nanos();
    i64::try_from(nanos / 1_000_000).unwrap_or(i64::MAX)
}
