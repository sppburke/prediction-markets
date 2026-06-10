//! Figment-based configuration (TOML + `PE_CRYPTO_SHADOW_*` env), mirroring the
//! repo's config-loading convention. Numeric defaults are mirrored in
//! `docs/_GLOSSARY.md` ("BTC shadow harness defaults") per the
//! new-numeric-threshold rule.

use std::path::Path;

use figment::Figment;
use figment::providers::{Env, Format, Toml};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::consensus::ConsensusParams;
use crate::types::BtcSeriesKind;

fn default_db_path() -> String {
    "crypto_shadow.db".to_string()
}
fn default_gamma_base_url() -> String {
    "https://gamma-api.polymarket.com".to_string()
}
fn default_chainlink_ws_url() -> String {
    "wss://ws-live-data.polymarket.com".to_string()
}
fn default_clob_ws_url() -> String {
    "wss://ws-subscriptions-clob.polymarket.com/ws/market".to_string()
}
fn default_channel_capacity() -> usize {
    // Raised 4096 -> 8192 (issue #311): with batched flushes the consumer drains
    // far faster than it writes, and the deeper buffer absorbs a full flush
    // window of bursty book+trade activity around a BTC move without drops.
    // See `docs/_GLOSSARY.md`: `crypto_shadow_channel_capacity`.
    8192
}
fn default_flush_interval_ms() -> u64 {
    250
}
fn default_flush_max_frames() -> usize {
    256
}
fn default_prune_grace_ms() -> i64 {
    120_000
}
fn default_force_reconnect_after_ms() -> u64 {
    900_000
}
fn default_market_refresh_interval_secs() -> u64 {
    60
}
fn default_max_open_markets() -> usize {
    64
}
fn default_vantage_label() -> String {
    "local".to_string()
}
fn default_rtt_probe_pings() -> u32 {
    5
}
fn default_true() -> bool {
    true
}
fn default_bybit_ws_url() -> String {
    "wss://stream.bybit.com/v5/public/spot".to_string()
}
fn default_okx_ws_url() -> String {
    "wss://ws.okx.com:8443/ws/v5/public".to_string()
}
fn default_coinbase_ws_url() -> String {
    "wss://ws-feed.exchange.coinbase.com".to_string()
}
fn default_move_threshold_bps() -> Decimal {
    Decimal::new(30, 1) // 3.0 bps
}
fn default_move_window_ms() -> i64 {
    300
}
fn default_move_cooldown_ms() -> i64 {
    1000
}
fn default_min_venues() -> usize {
    2
}
fn default_write_channel_capacity() -> usize {
    // Bounded `mpsc` capacity for the decoupled DB-writer channel (issue #317):
    // `drive` enqueues already-batched `WriteCmd`s (~4 messages/s steady-state),
    // and the `spawn_blocking` writer drains far faster, so a shallow buffer
    // suffices. Backpressure is `send().await` (never drop inside the pipeline).
    // See `docs/_GLOSSARY.md`: `crypto_shadow_write_channel_capacity`.
    64
}
fn default_clob_ping_interval_secs() -> u64 {
    // The Polymarket CLOB market channel's documented application-level heartbeat
    // cadence (issue #317): the client sends the text `PING` every 10s and the
    // server replies the text `PONG`; missing heartbeats are the documented cause
    // of connection drops. See `docs/15-SOURCES.md` (wss-overview, 2026-06-10).
    10
}
fn default_clob_read_idle_limit_secs() -> u64 {
    // Reconnect the CLOB socket if no inbound frame arrives for this long (issue
    // #317): the half-open-peer detector. 120s = 12× the heartbeat cadence, so a
    // healthy connection (inbound traffic at least every ~10s) never trips it,
    // while a silently-vanished peer is converted to a reconnect well under the
    // 300s `crypto_shadow_tape_validity_max_gap_secs` gate.
    120
}

/// Harness configuration. See `docs/_GLOSSARY.md` for the canonical defaults.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShadowConfig {
    #[serde(default = "default_db_path")]
    pub db_path: String,
    #[serde(default = "default_gamma_base_url")]
    pub gamma_base_url: String,
    #[serde(default = "default_chainlink_ws_url")]
    pub chainlink_ws_url: String,
    #[serde(default = "default_clob_ws_url")]
    pub clob_ws_url: String,
    #[serde(default = "default_channel_capacity")]
    pub channel_capacity: usize,
    /// Frame-buffer flush cadence in `drive` (issue #311): buffered frames are
    /// written in one batched transaction every this-many ms, OR as soon as
    /// `flush_max_frames` are buffered, whichever comes first. Crash-loss is
    /// bounded by `write_channel_capacity` in-flight batches + one flush window
    /// (issue #317: the batch is enqueued to the decoupled writer, not committed
    /// inline; steady-state the channel stays shallow as the writer outpaces the
    /// ~4-batch/s producer rate, and a crashed capture is not-clean regardless).
    #[serde(default = "default_flush_interval_ms")]
    pub flush_interval_ms: u64,
    #[serde(default = "default_flush_max_frames")]
    pub flush_max_frames: usize,
    /// Bounded `mpsc` capacity for the decoupled DB-writer channel (issue #317):
    /// `drive` sends batched `WriteCmd`s to a `spawn_blocking` writer so the
    /// socket-drain task never blocks on SQLite. See `docs/_GLOSSARY.md`.
    #[serde(default = "default_write_channel_capacity")]
    pub write_channel_capacity: usize,
    /// Grace after a market's `range_end_ms` before it is pruned from the join
    /// state and the CLOB subscription set (late settlement prints land within
    /// this window).
    #[serde(default = "default_prune_grace_ms")]
    pub prune_grace_ms: i64,
    /// Minimum elapsed time since the last CLOB (re)connect before a
    /// `ForceReconnect` (sent after a delivered prune) is honored — natural
    /// reconnects apply the pruned set for free in the common case.
    #[serde(default = "default_force_reconnect_after_ms")]
    pub force_reconnect_after_ms: u64,
    /// CLOB market-channel application-level heartbeat cadence (issue #317): the
    /// client sends the text `PING` every this-many seconds; the server replies
    /// the text `PONG`. The venue-documented anti-reaping contract. See
    /// `docs/15-SOURCES.md` (wss-overview).
    #[serde(default = "default_clob_ping_interval_secs")]
    pub clob_ping_interval_secs: u64,
    /// CLOB read-idle reconnect deadline (issue #317): if no inbound frame
    /// arrives for this long, the socket is treated as half-open and the task
    /// reconnects + resubscribes — the half-open-peer detector that bounds
    /// `max_clob_gap_secs` under the tape-validity gate.
    #[serde(default = "default_clob_read_idle_limit_secs")]
    pub clob_read_idle_limit_secs: u64,
    #[serde(default = "default_market_refresh_interval_secs")]
    pub market_refresh_interval_secs: u64,
    #[serde(default = "default_max_open_markets")]
    pub max_open_markets: usize,
    /// Free-text region/host tag stamped into `meta` and the report header so
    /// runs from different locations (local baseline vs colo re-run) compare.
    #[serde(default = "default_vantage_label")]
    pub vantage_label: String,
    #[serde(default = "default_rtt_probe_pings")]
    pub rtt_probe_pings: u32,
    #[serde(default = "default_true")]
    pub track_5m: bool,
    #[serde(default = "default_true")]
    pub track_15m: bool,
    /// Exchange trade/ticker WS URLs feeding the consensus median (the trigger).
    #[serde(default = "default_bybit_ws_url")]
    pub bybit_ws_url: String,
    #[serde(default = "default_okx_ws_url")]
    pub okx_ws_url: String,
    #[serde(default = "default_coinbase_ws_url")]
    pub coinbase_ws_url: String,
    /// Consensus move-detector tuning (see `docs/_GLOSSARY.md`).
    #[serde(default = "default_move_threshold_bps")]
    pub move_threshold_bps: Decimal,
    #[serde(default = "default_move_window_ms")]
    pub move_window_ms: i64,
    #[serde(default = "default_move_cooldown_ms")]
    pub move_cooldown_ms: i64,
    #[serde(default = "default_min_venues")]
    pub min_venues: usize,
    /// Sponsored Chainlink Data Streams API key for the `btc/usd` settlement
    /// feed. **Deferred** (issue #300 AC2.3): off by default; without it the
    /// Chainlink leg captures `raw_ticks` only and yields no live settlement.
    #[serde(default)]
    pub chainlink_api_key: Option<String>,
}

impl Default for ShadowConfig {
    fn default() -> Self {
        Self {
            db_path: default_db_path(),
            gamma_base_url: default_gamma_base_url(),
            chainlink_ws_url: default_chainlink_ws_url(),
            clob_ws_url: default_clob_ws_url(),
            channel_capacity: default_channel_capacity(),
            flush_interval_ms: default_flush_interval_ms(),
            flush_max_frames: default_flush_max_frames(),
            write_channel_capacity: default_write_channel_capacity(),
            prune_grace_ms: default_prune_grace_ms(),
            force_reconnect_after_ms: default_force_reconnect_after_ms(),
            clob_ping_interval_secs: default_clob_ping_interval_secs(),
            clob_read_idle_limit_secs: default_clob_read_idle_limit_secs(),
            market_refresh_interval_secs: default_market_refresh_interval_secs(),
            max_open_markets: default_max_open_markets(),
            vantage_label: default_vantage_label(),
            rtt_probe_pings: default_rtt_probe_pings(),
            track_5m: default_true(),
            track_15m: default_true(),
            bybit_ws_url: default_bybit_ws_url(),
            okx_ws_url: default_okx_ws_url(),
            coinbase_ws_url: default_coinbase_ws_url(),
            move_threshold_bps: default_move_threshold_bps(),
            move_window_ms: default_move_window_ms(),
            move_cooldown_ms: default_move_cooldown_ms(),
            min_venues: default_min_venues(),
            chainlink_api_key: None,
        }
    }
}

impl ShadowConfig {
    /// The series this run tracks.
    pub fn series(&self) -> Vec<BtcSeriesKind> {
        let mut v = Vec::new();
        if self.track_5m {
            v.push(BtcSeriesKind::Five);
        }
        if self.track_15m {
            v.push(BtcSeriesKind::Fifteen);
        }
        v
    }

    /// Consensus median + move-detector tuning derived from this config.
    pub fn consensus_params(&self) -> ConsensusParams {
        ConsensusParams {
            min_venues: self.min_venues,
            threshold_bps: self.move_threshold_bps,
            window_ms: self.move_window_ms,
            cooldown_ms: self.move_cooldown_ms,
        }
    }
}

/// Configuration load error.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("load config: {0}")]
    Figment(Box<figment::Error>),
}

impl From<figment::Error> for ConfigError {
    fn from(e: figment::Error) -> Self {
        ConfigError::Figment(Box::new(e))
    }
}

/// Load config from an optional TOML file overlaid with `PE_CRYPTO_SHADOW_*`
/// env vars. A missing TOML path is tolerated (figment default).
pub fn load(path: Option<&Path>) -> Result<ShadowConfig, ConfigError> {
    let mut fig = Figment::new();
    if let Some(p) = path {
        fig = fig.merge(Toml::file(p));
    }
    let cfg: ShadowConfig = fig
        .merge(Env::prefixed("PE_CRYPTO_SHADOW_").lowercase(true))
        .extract()?;
    Ok(cfg)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn defaults_load_without_a_file() {
        let cfg = load(None).unwrap();
        assert_eq!(cfg.channel_capacity, 8192);
        assert_eq!(cfg.flush_interval_ms, 250);
        assert_eq!(cfg.flush_max_frames, 256);
        assert_eq!(cfg.write_channel_capacity, 64);
        assert_eq!(cfg.prune_grace_ms, 120_000);
        assert_eq!(cfg.force_reconnect_after_ms, 900_000);
        assert_eq!(cfg.clob_ping_interval_secs, 10);
        assert_eq!(cfg.clob_read_idle_limit_secs, 120);
        assert_eq!(cfg.max_open_markets, 64);
        assert_eq!(cfg.vantage_label, "local");
        assert_eq!(cfg.series().len(), 2);
    }

    #[test]
    #[allow(clippy::result_large_err)] // figment::Jail::expect_with dictates the closure's Result type
    fn env_overrides_default() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("PE_CRYPTO_SHADOW_VANTAGE_LABEL", "aws-us-east-1");
            jail.set_env("PE_CRYPTO_SHADOW_MAX_OPEN_MARKETS", "8");
            let cfg = load(None).unwrap();
            assert_eq!(cfg.vantage_label, "aws-us-east-1");
            assert_eq!(cfg.max_open_markets, 8);
            Ok(())
        });
    }

    #[test]
    fn series_selection_respects_flags() {
        let cfg = ShadowConfig {
            track_5m: true,
            track_15m: false,
            ..ShadowConfig::default()
        };
        assert_eq!(cfg.series(), vec![BtcSeriesKind::Five]);
    }
}
