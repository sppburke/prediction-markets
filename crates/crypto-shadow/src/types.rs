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

/// A decoded Chainlink BTC/USD tick from the RTDS `crypto_prices` feed
/// (Chainlink source). This is the **settlement** value the 5m/15m markets
/// resolve on. The live feed needs a sponsored Chainlink key (issue #300 AC2.3,
/// deferred); the corrected decoder is landed and captured to `raw_ticks` now,
/// but does not yet drive observations — the realized-outcome join is future
/// work. The observation **trigger** is the exchange-consensus median (see
/// [`ExchangeTick`] / [`MoveEvent`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainlinkTick {
    /// Feed symbol, e.g. `"btc/usd"`.
    pub symbol: String,
    /// Source-supplied observation time, epoch milliseconds.
    pub observed_at_ms: i64,
    /// The BTC/USD value (this is the 5m/15m settling value).
    pub value: BtcUsdPrice,
}

/// A BTC spot exchange whose public trade/ticker stream feeds the consensus
/// move detector. These are the free trigger feeds chosen by the feed bake-off
/// (`docs/27`): from the Ireland vantage, bybit/okx/coinbase lead Polymarket's
/// book reprice by ~135–188 ms.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ExchangeVenue {
    Bybit,
    Okx,
    Coinbase,
}

impl ExchangeVenue {
    /// Short stable label used in `raw_ticks.source`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Bybit => "bybit",
            Self::Okx => "okx",
            Self::Coinbase => "coinbase",
        }
    }
}

/// A decoded BTC spot trade/ticker tick from one exchange.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExchangeTick {
    pub venue: ExchangeVenue,
    /// Last trade/ticker price. USDT venues (bybit/okx) carry a ~5 bps basis over
    /// the USD venue (coinbase); it is a near-constant offset that cancels in the
    /// **relative** (bps) move detection, so the raw prices are medianed as-is.
    pub price: Decimal,
    /// Source-supplied trade time, epoch milliseconds.
    pub observed_at_ms: i64,
}

/// Direction of a detected consensus move.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MoveDirection {
    Up,
    Down,
}

impl MoveDirection {
    /// Short stable label persisted in `observations.move_direction`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Up => "up",
            Self::Down => "down",
        }
    }
}

/// An outsized BTC move detected on the exchange-consensus median — the
/// observation **trigger**. Fired when the median shifts by at least
/// `move_threshold_bps` within `move_window_ms` (subject to a cooldown). The
/// receive clock is threaded separately (mirroring [`FeedFrame::received_ms`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MoveEvent {
    pub direction: MoveDirection,
    /// Consensus median at the move.
    pub median_price: Decimal,
    /// Reference median (≈`move_window_ms` ago) the move is measured against.
    pub ref_price: Decimal,
    /// `|median − ref| / ref`, in basis points (always non-negative).
    pub magnitude_bps: Decimal,
    /// Source clock of the median tick that triggered the move, epoch ms.
    pub observed_at_ms: i64,
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
    /// omitted or carried an unparseable timestamp (e.g. `price_change` frames,
    /// which carry none). This is the source publisher's clock, kept for offline
    /// analysis and `raw_ticks` correlation; it does **not** drive
    /// `feed_to_book_lag_ms`, which uses the node-receive clock (see
    /// [`EdgeObservation::feed_to_book_lag_ms`]).
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
    /// Exchange-consensus median BTC value at the triggering move — the **signal**
    /// value. This is **not** the Chainlink settling value; settlement validation
    /// is deferred (issue #300 AC2.3, blocked on the sponsored Chainlink key).
    pub signal_value: Decimal,
    /// Window-open consensus median (the directional reference), captured at the
    /// first in-window tick. `None` until captured.
    pub range_start_value: Option<Decimal>,
    /// Signal-implied P(up): `1` if `signal_value > range_start`, `0.5` on a tie,
    /// `0` if below; `None` if range-start is not yet known. This is the
    /// exchange-signal direction **at trigger time**, not the settled outcome
    /// (which needs the deferred Chainlink settlement feed). Overstates certainty
    /// mid-window (see the issue's open risks).
    pub instantaneous_prob_up: Option<Decimal>,
    pub best_ask: Option<Decimal>,
    pub mid: Option<Decimal>,
    pub gross_edge_vs_ask: Option<Decimal>,
    pub gross_edge_vs_mid: Option<Decimal>,
    pub fee_cost: Option<Decimal>,
    pub net_edge_vs_ask: Option<Decimal>,
    pub net_edge_vs_mid: Option<Decimal>,
    /// `signal_received_ms - book_received_ms`: the latest book's staleness **at
    /// the node** when the triggering median tick was received — a single coherent
    /// at-the-node clock for both feeds, **not** cross-venue propagation latency.
    /// `None` only if no book has been seen for this market. With ~1k
    /// `price_change`/s the latest book is near-always fresh, so the p50/p95 skew
    /// is small; read it alongside the `meta` vantage point.
    pub feed_to_book_lag_ms: Option<i64>,
    /// Magnitude of the triggering consensus move, in basis points (non-negative).
    pub move_magnitude_bps: Decimal,
    /// Direction of the triggering consensus move.
    pub move_direction: MoveDirection,
}

/// Source of a raw inbound frame, persisted with every `raw_ticks` row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeedSource {
    Chainlink,
    Clob,
    Bybit,
    Okx,
    Coinbase,
}

impl FeedSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Chainlink => "chainlink",
            Self::Clob => "clob",
            Self::Bybit => "bybit",
            Self::Okx => "okx",
            Self::Coinbase => "coinbase",
        }
    }

    /// The exchange venue this source maps to, or `None` for non-exchange feeds
    /// (Chainlink / CLOB). Lets the drive loop route exchange frames without an
    /// unchecked `unreachable!` over the variant set.
    pub fn exchange_venue(self) -> Option<ExchangeVenue> {
        match self {
            Self::Bybit => Some(ExchangeVenue::Bybit),
            Self::Okx => Some(ExchangeVenue::Okx),
            Self::Coinbase => Some(ExchangeVenue::Coinbase),
            Self::Chainlink | Self::Clob => None,
        }
    }
}

impl From<ExchangeVenue> for FeedSource {
    fn from(v: ExchangeVenue) -> Self {
        match v {
            ExchangeVenue::Bybit => Self::Bybit,
            ExchangeVenue::Okx => Self::Okx,
            ExchangeVenue::Coinbase => Self::Coinbase,
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
