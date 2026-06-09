//! Live orchestration (`run`) and offline report generation.
//!
//! `run` is the live path: it opens the DB, stamps run provenance + vantage,
//! probes endpoint RTT, enumerates markets, spawns the two WS tasks, and drives
//! the join loop (extracted into `drive`) until an injected `shutdown` fires. It
//! is **not** exercised by the offline CI gate (no network); its building blocks
//! (decoders, join, db, report) and the `drive` select loop are.

use std::path::Path;
use std::time::Duration;

use pe_source_polymarket_public::{PageFetcher, ReqwestFetcher};
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::config::ShadowConfig;
use crate::db::{LAG_CLOCK, SCHEMA_VERSION, ShadowDb};
use crate::error::Error;
use crate::fees::CRYPTO_FEES_V2_PROVENANCE;
use crate::gamma::BtcMarketFetcher;
use crate::join::JoinState;
use crate::report::build_report;
use crate::resolve::BtcResolutionFetcher;
use crate::types::{ExchangeVenue, FeedFrame, FeedSource, now_unix_ms};
use crate::{chainlink_ws, clob_ws, exchange_ws};

/// Summary returned by a completed `run`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunSummary {
    pub observations: i64,
    pub raw_ticks: i64,
}

/// Per-source decode-error tallies returned by [`drive`]. Decode failures
/// increment a counter and are surfaced as a bounded periodic summary (never a
/// per-frame log line), so a load test can assert the count without scraping
/// logs and a 72k-frame run does not emit 72k warnings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct DriveStats {
    decode_errors_chainlink: u64,
    decode_errors_clob: u64,
    decode_errors_exchange: u64,
}

/// Drive the live shadow collection until `rx` closes or `shutdown` resolves.
pub async fn run(
    config: &ShadowConfig,
    shutdown: impl std::future::Future<Output = ()>,
) -> Result<RunSummary, Error> {
    let db = ShadowDb::open(Path::new(&config.db_path))?;
    db.set_meta("schema_version", &SCHEMA_VERSION.to_string())?;
    db.set_meta("fee_provenance", CRYPTO_FEES_V2_PROVENANCE)?;
    db.set_meta("vantage_label", &config.vantage_label)?;
    db.set_meta("lag_clock", LAG_CLOCK)?;
    let rtt = probe_rtt(config).await;
    db.set_meta("vantage_rtt", &rtt)?;
    info!(vantage = %config.vantage_label, rtt = %rtt, "shadow: vantage recorded");

    let fetcher = ReqwestFetcher::new(reqwest::Client::new());
    let gamma = BtcMarketFetcher::new(config.gamma_base_url.clone(), fetcher);

    let mut markets = gamma.fetch_markets(&config.series()).await?;
    markets.truncate(config.max_open_markets);
    for m in &markets {
        db.upsert_market(m)?;
    }
    // Subscribe BOTH outcome books: the YES (Up) token and the NO (Down) token,
    // so a down-move can be priced against the real NO ask.
    let token_ids: Vec<String> = markets
        .iter()
        .flat_map(|m| [m.yes_token_id.clone(), m.no_token_id.clone()])
        .collect();
    info!(markets = markets.len(), "shadow: enumerated markets");
    let mut state = JoinState::new(markets, config.consensus_params());

    let (tx, mut rx) = mpsc::channel(config.channel_capacity);
    let _chainlink = chainlink_ws::spawn(config.chainlink_ws_url.clone(), tx.clone());
    let _clob = clob_ws::spawn(config.clob_ws_url.clone(), token_ids, tx.clone());
    // Exchange trigger feeds (free; chosen by the bake-off, docs/27).
    let _bybit = exchange_ws::spawn(
        ExchangeVenue::Bybit,
        config.bybit_ws_url.clone(),
        tx.clone(),
    );
    let _okx = exchange_ws::spawn(ExchangeVenue::Okx, config.okx_ws_url.clone(), tx.clone());
    let _coinbase = exchange_ws::spawn(
        ExchangeVenue::Coinbase,
        config.coinbase_ws_url.clone(),
        tx.clone(),
    );
    drop(tx); // only the WS tasks hold senders now

    let mut refresh = tokio::time::interval(Duration::from_secs(
        config.market_refresh_interval_secs.max(1),
    ));
    refresh.tick().await; // discard the immediate first tick

    let stats = drive(
        &db,
        &mut rx,
        &mut state,
        &gamma,
        &mut refresh,
        config,
        shutdown,
    )
    .await?;
    info!(
        chainlink_decode_errors = stats.decode_errors_chainlink,
        clob_decode_errors = stats.decode_errors_clob,
        exchange_decode_errors = stats.decode_errors_exchange,
        "shadow: drive loop ended"
    );

    Ok(RunSummary {
        observations: db.observation_count()?,
        raw_ticks: db.raw_tick_count()?,
    })
}

/// Drive the join loop over inbound frames until `rx` closes or `shutdown`
/// resolves. Extracted from [`run`] (which owns the network preamble that errors
/// under the no-network gate) so the select loop is testable offline: an
/// injected `shutdown`, a controlled `rx`, and a `FixtureFetcher`-backed `gamma`
/// exercise every arm with no live socket. `shutdown` is polled first
/// (`biased;`), so it wins over a still-draining `rx` — mirroring the shutdown
/// idiom in `crates/service/src/orchestrator.rs`.
async fn drive<F: PageFetcher + Send + Sync>(
    db: &ShadowDb,
    rx: &mut mpsc::Receiver<FeedFrame>,
    state: &mut JoinState,
    gamma: &BtcMarketFetcher<F>,
    refresh: &mut tokio::time::Interval,
    config: &ShadowConfig,
    shutdown: impl std::future::Future<Output = ()>,
) -> Result<DriveStats, Error> {
    let mut stats = DriveStats::default();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => {
                info!("shadow: shutdown signalled, stopping drive loop");
                break;
            }
            maybe = rx.recv() => {
                let Some(frame) = maybe else { break };
                db.insert_raw_tick(frame.source, frame.received_ms, &frame.raw)?;
                match frame.source {
                    // Chainlink is the deferred settlement reader: decode to
                    // exercise the corrected decoder + tally, but emit no
                    // observations — the realized-outcome join needs the
                    // sponsored key (issue #300 AC2.3). The raw frame is already
                    // persisted above for offline recompute.
                    FeedSource::Chainlink => {
                        if chainlink_ws::parse_chainlink_frame(&frame.raw).is_err() {
                            stats.decode_errors_chainlink += 1;
                        }
                    }
                    FeedSource::Clob => match clob_ws::parse_clob_frame(&frame.raw) {
                        Ok(updates) => {
                            for u in updates {
                                state.on_book_update(u, frame.received_ms);
                            }
                        }
                        Err(_) => stats.decode_errors_clob += 1,
                    },
                    // Exchange trigger feeds drive the consensus median + move
                    // detector; a fired move emits observations.
                    FeedSource::Bybit | FeedSource::Okx | FeedSource::Coinbase => {
                        if let Some(venue) = frame.source.exchange_venue() {
                            match exchange_ws::parse_trade_frame(venue, &frame.raw) {
                                Ok(Some(tick)) => {
                                    let obs = state.on_exchange_tick(&tick, frame.received_ms);
                                    db.insert_observations(&obs)?;
                                }
                                Ok(None) => {} // non-trade frame (ack/heartbeat)
                                Err(_) => stats.decode_errors_exchange += 1,
                            }
                        }
                    }
                }
            }
            _ = refresh.tick() => {
                // Best-effort market refresh + the bounded periodic decode-error
                // summary (tied to the refresh cadence, not the frame rate).
                log_decode_errors(&stats, "cumulative");
                match gamma.fetch_markets(&config.series()).await {
                    Ok(fresh) => {
                        for m in fresh.into_iter().take(config.max_open_markets) {
                            state.upsert_market(m.clone());
                            if let Err(e) = db.upsert_market(&m) {
                                warn!(error = %e, "shadow: market upsert error");
                            }
                        }
                    }
                    Err(e) => warn!(error = %e, "shadow: market refresh error"),
                }
            }
        }
    }
    log_decode_errors(&stats, "final");
    Ok(stats)
}

/// Emit a bounded decode-error summary (one line per call, only when non-zero),
/// replacing the per-frame warning that spammed ~72k lines under load.
fn log_decode_errors(stats: &DriveStats, phase: &str) {
    if stats.decode_errors_chainlink > 0
        || stats.decode_errors_clob > 0
        || stats.decode_errors_exchange > 0
    {
        warn!(
            chainlink = stats.decode_errors_chainlink,
            clob = stats.decode_errors_clob,
            exchange = stats.decode_errors_exchange,
            phase,
            "shadow: decode errors"
        );
    }
}

/// Build the report JSON from the configured DB. Offline. Includes realized-edge
/// groups when `resolve` has populated the `resolutions` table; otherwise the
/// `realized` section is empty and only signal-edge groups are present.
pub fn generate_report(config: &ShadowConfig) -> Result<String, Error> {
    let db = ShadowDb::open(Path::new(&config.db_path))?;
    let rows = db.all_observations_for_report()?;
    let realized_rows = db.all_realized_rows()?;
    let fee_provenance = db
        .get_meta("fee_provenance")?
        .unwrap_or_else(|| CRYPTO_FEES_V2_PROVENANCE.to_string());
    let vantage_label = db
        .get_meta("vantage_label")?
        .unwrap_or_else(|| config.vantage_label.clone());
    let vantage_rtt = db.get_meta("vantage_rtt")?;
    let report = build_report(
        &rows,
        &realized_rows,
        fee_provenance,
        vantage_label,
        vantage_rtt,
    );
    Ok(serde_json::to_string_pretty(&report)?)
}

/// Fetch + persist market resolutions for every observed market — the key-free
/// realized ground truth (Gamma `outcomePrices`; no Chainlink key). Network;
/// returns the number of resolutions stored. Markets still open simply yield no
/// row yet, so this is safe to re-run as 5m/15m markets settle.
pub async fn resolve(config: &ShadowConfig) -> Result<usize, Error> {
    let db = ShadowDb::open(Path::new(&config.db_path))?;
    let condition_ids = db.distinct_observation_condition_ids()?;
    if condition_ids.is_empty() {
        info!("shadow: no observed markets to resolve");
        return Ok(0);
    }
    let fetcher = ReqwestFetcher::new(reqwest::Client::new());
    let resolver = BtcResolutionFetcher::new(config.gamma_base_url.clone(), fetcher);
    let resolutions = resolver.fetch_resolutions(&condition_ids).await?;
    let now = now_unix_ms();
    for r in &resolutions {
        db.upsert_resolution(&r.condition_id, r.yes_won, now)?;
    }
    info!(
        observed_markets = condition_ids.len(),
        resolved = resolutions.len(),
        "shadow: resolutions fetched"
    );
    Ok(resolutions.len())
}

fn host_port(url: &str) -> Option<(String, u16)> {
    let rest = url.split("://").nth(1).unwrap_or(url);
    let authority = rest.split('/').next().unwrap_or(rest);
    match authority.rsplit_once(':') {
        Some((h, p)) => Some((h.to_string(), p.parse().ok()?)),
        None => {
            let port = if url.starts_with("wss") || url.starts_with("https") {
                443
            } else {
                80
            };
            Some((authority.to_string(), port))
        }
    }
}

async fn probe_one(host: &str, port: u16, pings: u32) -> Option<i64> {
    let mut samples = Vec::new();
    for _ in 0..pings {
        let start = tokio::time::Instant::now();
        let connect = tokio::net::TcpStream::connect((host, port));
        if let Ok(Ok(_stream)) = tokio::time::timeout(Duration::from_secs(3), connect).await {
            samples.push(i64::try_from(start.elapsed().as_millis()).unwrap_or(i64::MAX));
        }
    }
    if samples.is_empty() {
        return None;
    }
    samples.sort_unstable();
    samples.get(samples.len() / 2).copied()
}

/// Best-effort TCP-connect RTT probe to each endpoint; returns a JSON summary
/// (`{"<label>_p50_ms": <ms|null>}`) stamped into `meta` so the vantage point is
/// quantitative, not just a label.
async fn probe_rtt(config: &ShadowConfig) -> String {
    let mut map = serde_json::Map::new();
    let endpoints = [
        ("chainlink", config.chainlink_ws_url.as_str()),
        ("clob", config.clob_ws_url.as_str()),
        ("gamma", config.gamma_base_url.as_str()),
        ("bybit", config.bybit_ws_url.as_str()),
        ("okx", config.okx_ws_url.as_str()),
        ("coinbase", config.coinbase_ws_url.as_str()),
    ];
    for (label, url) in endpoints {
        let value = match host_port(url) {
            Some((host, port)) => match probe_one(&host, port, config.rtt_probe_pings).await {
                Some(ms) => serde_json::Value::from(ms),
                None => serde_json::Value::Null,
            },
            None => serde_json::Value::Null,
        };
        map.insert(format!("{label}_p50_ms"), value);
    }
    serde_json::Value::Object(map).to_string()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use pe_source_polymarket_public::FixtureFetcher;
    use std::collections::HashMap;

    fn test_gamma() -> BtcMarketFetcher<FixtureFetcher> {
        // The refresh arm is never reached in these tests (long interval, loop
        // exits first), so the fetcher is never invoked; empty fixtures suffice.
        BtcMarketFetcher::new(
            "https://gamma.test".to_string(),
            FixtureFetcher::new(HashMap::new()),
        )
    }

    fn garbage_clob_frame() -> FeedFrame {
        // Fails `parse_clob_frame` (not valid JSON) → a decode error.
        FeedFrame {
            source: FeedSource::Clob,
            received_ms: 1,
            raw: "{not json".to_string(),
        }
    }

    // AC1.4(a): under a flood of undecodable frames, `drive` returns the exact
    // per-source error count, persists every raw frame, and emits no per-frame
    // log line (summary only).
    #[tokio::test]
    async fn drive_counts_decode_errors_without_per_frame_logs() {
        let dir = tempfile::tempdir().unwrap();
        let db = ShadowDb::open(&dir.path().join("s.db")).unwrap();
        let mut state = JoinState::new(Vec::new(), ShadowConfig::default().consensus_params());
        let gamma = test_gamma();
        let cfg = ShadowConfig::default();
        let mut refresh = tokio::time::interval(Duration::from_secs(3_600));
        refresh.tick().await; // discard immediate first tick

        let (tx, mut rx) = mpsc::channel(16);
        let injected: u64 = 10_000;
        tokio::spawn(async move {
            for _ in 0..injected {
                if tx.send(garbage_clob_frame()).await.is_err() {
                    break;
                }
            }
            // tx dropped here → rx closes once drained, ending the loop.
        });

        // A `pending` shutdown never fires, so the rx arm runs to completion.
        let stats = drive(
            &db,
            &mut rx,
            &mut state,
            &gamma,
            &mut refresh,
            &cfg,
            std::future::pending::<()>(),
        )
        .await
        .unwrap();

        assert_eq!(stats.decode_errors_clob, injected);
        assert_eq!(stats.decode_errors_chainlink, 0);
        // Every frame was persisted before the (failed) decode — the recovery path.
        assert_eq!(
            db.raw_tick_count().unwrap(),
            i64::try_from(injected).unwrap()
        );
        println!(
            "PASS: drive_counts_decode_errors_without_per_frame_logs clob={}",
            stats.decode_errors_clob
        );
    }

    // AC1.4(b): a ready `shutdown` wins over a still-draining rx (`biased;`).
    #[tokio::test]
    async fn drive_shutdown_wins_over_draining_rx() {
        let dir = tempfile::tempdir().unwrap();
        let db = ShadowDb::open(&dir.path().join("s.db")).unwrap();
        let mut state = JoinState::new(Vec::new(), ShadowConfig::default().consensus_params());
        let gamma = test_gamma();
        let cfg = ShadowConfig::default();
        let mut refresh = tokio::time::interval(Duration::from_secs(3_600));
        refresh.tick().await;

        let (tx, mut rx) = mpsc::channel(16);
        for _ in 0..8 {
            tx.try_send(garbage_clob_frame()).unwrap();
        }
        let _keep_open = tx; // keep the sender alive: rx stays "still-draining"

        let stats = drive(
            &db,
            &mut rx,
            &mut state,
            &gamma,
            &mut refresh,
            &cfg,
            std::future::ready(()),
        )
        .await
        .unwrap();

        // Shutdown was polled first and won before any frame was drained.
        assert_eq!(stats.decode_errors_clob, 0);
        assert_eq!(db.raw_tick_count().unwrap(), 0);
        println!("PASS: drive_shutdown_wins_over_draining_rx");
    }

    #[test]
    fn host_port_parses_schemes_and_ports() {
        assert_eq!(
            host_port("wss://ws-live-data.polymarket.com"),
            Some(("ws-live-data.polymarket.com".to_string(), 443))
        );
        assert_eq!(
            host_port("https://gamma-api.polymarket.com"),
            Some(("gamma-api.polymarket.com".to_string(), 443))
        );
        assert_eq!(
            host_port("wss://example.com:8443/ws/market"),
            Some(("example.com".to_string(), 8443))
        );
    }

    #[test]
    fn generate_report_on_empty_db_is_ok() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = ShadowConfig {
            db_path: dir.path().join("s.db").to_string_lossy().into_owned(),
            ..ShadowConfig::default()
        };
        let json = generate_report(&cfg).unwrap();
        assert!(json.contains("\"total_observations\": 0"));
        assert!(json.contains("crypto_fees_v2"));
    }
}
