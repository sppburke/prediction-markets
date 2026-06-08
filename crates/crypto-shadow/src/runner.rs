//! Live orchestration (`run`) and offline report generation.
//!
//! `run` is the live path: it opens the DB, stamps run provenance + vantage,
//! probes endpoint RTT, enumerates markets, spawns the two WS tasks, and drives
//! the join loop until `ctrl_c`. It is **not** exercised by the offline CI gate
//! (no network); its building blocks (decoders, join, db, report) are.

use std::path::Path;
use std::time::Duration;

use pe_source_polymarket_public::ReqwestFetcher;
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::config::ShadowConfig;
use crate::db::{SCHEMA_VERSION, ShadowDb};
use crate::error::Error;
use crate::fees::CRYPTO_FEES_V2_PROVENANCE;
use crate::gamma::BtcMarketFetcher;
use crate::join::JoinState;
use crate::report::build_report;
use crate::types::FeedSource;
use crate::{chainlink_ws, clob_ws};

/// Summary returned by a completed `run`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunSummary {
    pub observations: i64,
    pub raw_ticks: i64,
}

/// Drive the live shadow collection until `ctrl_c`.
pub async fn run(config: &ShadowConfig) -> Result<RunSummary, Error> {
    let db = ShadowDb::open(Path::new(&config.db_path))?;
    db.set_meta("schema_version", &SCHEMA_VERSION.to_string())?;
    db.set_meta("fee_provenance", CRYPTO_FEES_V2_PROVENANCE)?;
    db.set_meta("vantage_label", &config.vantage_label)?;
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
    let token_ids: Vec<String> = markets.iter().map(|m| m.yes_token_id.clone()).collect();
    info!(markets = markets.len(), "shadow: enumerated markets");
    let mut state = JoinState::new(markets);

    let (tx, mut rx) = mpsc::channel(config.channel_capacity);
    let _chainlink = chainlink_ws::spawn(config.chainlink_ws_url.clone(), tx.clone());
    let _clob = clob_ws::spawn(config.clob_ws_url.clone(), token_ids, tx.clone());
    drop(tx); // only the WS tasks hold senders now

    let mut refresh = tokio::time::interval(Duration::from_secs(
        config.market_refresh_interval_secs.max(1),
    ));
    refresh.tick().await; // discard the immediate first tick

    loop {
        tokio::select! {
            maybe = rx.recv() => {
                let Some(frame) = maybe else { break };
                db.insert_raw_tick(frame.source, frame.received_ms, &frame.raw)?;
                match frame.source {
                    FeedSource::Chainlink => match chainlink_ws::parse_chainlink_frame(&frame.raw) {
                        Ok(tick) => {
                            let obs = state.on_chainlink_tick(&tick);
                            db.insert_observations(&obs)?;
                        }
                        Err(e) => warn!(error = %e, "shadow: chainlink decode error"),
                    },
                    FeedSource::Clob => match clob_ws::parse_clob_frame(&frame.raw) {
                        Ok(updates) => {
                            for u in updates {
                                state.on_book_update(u);
                            }
                        }
                        Err(e) => warn!(error = %e, "shadow: clob decode error"),
                    },
                }
            }
            _ = refresh.tick() => {
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
            _ = tokio::signal::ctrl_c() => {
                info!("shadow: ctrl_c received, shutting down");
                break;
            }
        }
    }

    Ok(RunSummary {
        observations: db.observation_count()?,
        raw_ticks: db.raw_tick_count()?,
    })
}

/// Build the realized-edge report JSON from the configured DB. Offline.
pub fn generate_report(config: &ShadowConfig) -> Result<String, Error> {
    let db = ShadowDb::open(Path::new(&config.db_path))?;
    let rows = db.all_observations_for_report()?;
    let fee_provenance = db
        .get_meta("fee_provenance")?
        .unwrap_or_else(|| CRYPTO_FEES_V2_PROVENANCE.to_string());
    let vantage_label = db
        .get_meta("vantage_label")?
        .unwrap_or_else(|| config.vantage_label.clone());
    let vantage_rtt = db.get_meta("vantage_rtt")?;
    let report = build_report(&rows, fee_provenance, vantage_label, vantage_rtt);
    Ok(serde_json::to_string_pretty(&report)?)
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
