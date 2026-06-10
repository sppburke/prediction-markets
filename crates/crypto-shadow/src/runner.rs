//! Live orchestration (`run`) and offline report generation.
//!
//! `run` is the live path: it opens the DB, stamps run provenance + vantage,
//! probes endpoint RTT, enumerates markets, spawns the two WS tasks, and drives
//! the join loop (extracted into `drive`) until an injected `shutdown` fires. It
//! is **not** exercised by the offline CI gate (no network); its building blocks
//! (decoders, join, db, report) and the `drive` select loop are.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use pe_source_polymarket_public::{PageFetcher, ReqwestFetcher};
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::config::ShadowConfig;
use crate::db::{LAG_CLOCK, SCHEMA_VERSION, ShadowDb};
use crate::error::Error;
use crate::fees::CRYPTO_FEES_V2_PROVENANCE;
use crate::gamma::BtcMarketFetcher;
use crate::join::{JoinState, market_expired};
use crate::report::build_report;
use crate::resolve::BtcResolutionFetcher;
use crate::types::{BtcMarketMeta, ClobTrade, ExchangeVenue, FeedFrame, FeedSource, now_unix_ms};
use crate::{chainlink_ws, clob_ws, exchange_ws};

/// Summary returned by a completed `run`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunSummary {
    pub observations: i64,
    pub raw_ticks: i64,
}

/// Per-source decode-error tallies + flush count returned by [`drive`]. Decode
/// failures increment a counter and are surfaced as a bounded periodic summary
/// (never a per-frame log line), so a load test can assert the count without
/// scraping logs and a 72k-frame run does not emit 72k warnings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct DriveStats {
    decode_errors_chainlink: u64,
    decode_errors_clob: u64,
    decode_errors_exchange: u64,
    /// Non-empty frame-batch flushes committed (size-triggered, interval, or
    /// loop-exit). The saturation test asserts the size trigger ran.
    flushes: u64,
}

/// Per-source frames-dropped counters, shared with the WS producer tasks
/// (issue #311). A producer increments its counter when the frame channel is
/// full; `drive` surfaces non-zero tallies in the bounded periodic summary and
/// [`run`] stamps them into `meta` (`frames_dropped_<source>`) at run end. Any
/// non-zero mid-run value is run-invalidating for the #310 sweep, not cosmetic.
#[derive(Debug, Clone, Default)]
struct DropCounters {
    chainlink: Arc<AtomicU64>,
    clob: Arc<AtomicU64>,
    bybit: Arc<AtomicU64>,
    okx: Arc<AtomicU64>,
    coinbase: Arc<AtomicU64>,
}

impl DropCounters {
    /// `(meta key, current value)` per source, for the `meta` stamp.
    fn snapshot(&self) -> [(&'static str, u64); 5] {
        [
            (
                "frames_dropped_chainlink",
                self.chainlink.load(Ordering::Relaxed),
            ),
            ("frames_dropped_clob", self.clob.load(Ordering::Relaxed)),
            ("frames_dropped_bybit", self.bybit.load(Ordering::Relaxed)),
            ("frames_dropped_okx", self.okx.load(Ordering::Relaxed)),
            (
                "frames_dropped_coinbase",
                self.coinbase.load(Ordering::Relaxed),
            ),
        ]
    }
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
    // Admission guard (issue #311): Gamma's `closed=false` lags `range_end_ms`,
    // so boot must not subscribe already-dead markets or let them crowd the
    // `max_open_markets` window — same predicate as the refresh arm and prune.
    let boot_now_ms = now_unix_ms();
    markets.retain(|m| !market_expired(m, boot_now_ms, config.prune_grace_ms));
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

    let drops = DropCounters::default();
    let (tx, mut rx) = mpsc::channel(config.channel_capacity);
    let _chainlink = chainlink_ws::spawn(
        config.chainlink_ws_url.clone(),
        tx.clone(),
        Arc::clone(&drops.chainlink),
    );
    // The CLOB task returns a handle so `drive` can subscribe markets enumerated
    // after startup (without it, only the startup batch ever gets book frames).
    let (_clob, clob_sub) = clob_ws::spawn(
        config.clob_ws_url.clone(),
        token_ids,
        tx.clone(),
        Duration::from_millis(config.force_reconnect_after_ms),
        Arc::clone(&drops.clob),
    );
    // Exchange trigger feeds (free; chosen by the bake-off, docs/27).
    let _bybit = exchange_ws::spawn(
        ExchangeVenue::Bybit,
        config.bybit_ws_url.clone(),
        tx.clone(),
        Arc::clone(&drops.bybit),
    );
    let _okx = exchange_ws::spawn(
        ExchangeVenue::Okx,
        config.okx_ws_url.clone(),
        tx.clone(),
        Arc::clone(&drops.okx),
    );
    let _coinbase = exchange_ws::spawn(
        ExchangeVenue::Coinbase,
        config.coinbase_ws_url.clone(),
        tx.clone(),
        Arc::clone(&drops.coinbase),
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
        &clob_sub,
        &drops,
        shutdown,
    )
    .await?;
    info!(
        chainlink_decode_errors = stats.decode_errors_chainlink,
        clob_decode_errors = stats.decode_errors_clob,
        exchange_decode_errors = stats.decode_errors_exchange,
        flushes = stats.flushes,
        "shadow: drive loop ended"
    );
    // Stamp the drop tallies so the post-run validation AC (`frames_dropped_*`
    // all zero) is a `meta` query, not a log scrape. Meaningful at run end only.
    for (key, value) in drops.snapshot() {
        db.set_meta(key, &value.to_string())?;
    }

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
///
/// Persistence is **batched** (issue #311): raw frames and trade prints buffer
/// in-memory and flush in one transaction every `flush_interval_ms` OR
/// `flush_max_frames`, whichever first, plus once after the loop exits — so the
/// invariant is "every received frame is persisted by loop exit", not "before
/// decode". Crash-loss is bounded by one flush window (the post-loop flush does
/// not run on panic). A flush error is fail-fast: it stops the run.
#[allow(clippy::too_many_arguments)] // single orchestration loop; each arg is a distinct live dependency.
async fn drive<F: PageFetcher + Send + Sync>(
    db: &ShadowDb,
    rx: &mut mpsc::Receiver<FeedFrame>,
    state: &mut JoinState,
    gamma: &BtcMarketFetcher<F>,
    refresh: &mut tokio::time::Interval,
    config: &ShadowConfig,
    clob_sub: &clob_ws::ClobSubscribeHandle,
    drops: &DropCounters,
    shutdown: impl std::future::Future<Output = ()>,
) -> Result<DriveStats, Error> {
    let mut stats = DriveStats::default();
    let mut tick_buf: Vec<(FeedSource, i64, String)> = Vec::with_capacity(config.flush_max_frames);
    let mut trade_buf: Vec<(ClobTrade, i64, Option<&'static str>)> = Vec::new();
    let mut flush_tick =
        tokio::time::interval(Duration::from_millis(config.flush_interval_ms.max(1)));
    flush_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    flush_tick.tick().await; // discard the immediate first tick
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
                match frame.source {
                    // Chainlink is the deferred settlement reader: decode to
                    // exercise the corrected decoder + tally, but emit no
                    // observations — the realized-outcome join needs the
                    // sponsored key (issue #300 AC2.3). The raw frame is
                    // buffered below for offline recompute.
                    FeedSource::Chainlink => {
                        if chainlink_ws::parse_chainlink_frame(&frame.raw).is_err() {
                            stats.decode_errors_chainlink += 1;
                        }
                    }
                    // A CLOB frame is at most one of: a book/price_change update
                    // (book state) or a `last_trade_price` print (executed-trade
                    // tape). Both share the `market` channel, so we attempt each
                    // decoder: book updates feed the join; trades are persisted
                    // with their condition/series for the offline maker-vs-taker
                    // comparison. A genuinely malformed frame fails BOTH decoders
                    // but is ONE unprocessable frame, so the error tally is
                    // incremented at most once per frame.
                    FeedSource::Clob => {
                        let mut clob_decode_failed = false;
                        match clob_ws::parse_clob_frame(&frame.raw) {
                            Ok(updates) => {
                                for u in updates {
                                    state.on_book_update(u, frame.received_ms);
                                }
                            }
                            Err(_) => clob_decode_failed = true,
                        }
                        match clob_ws::parse_clob_trade(&frame.raw) {
                            Ok(Some(trade)) => {
                                // condition_id is on the trade (frame's `market`);
                                // the join only supplies the 5m/15m series label,
                                // which is `None` if the token is not yet known
                                // (or already pruned). Buffered into the same
                                // flush transaction as the raw ticks.
                                let (_cond, series) = state.lookup_token(&trade.token_id);
                                trade_buf.push((trade, frame.received_ms, series));
                            }
                            Ok(None) => {} // book/price_change frame, not a trade
                            Err(_) => clob_decode_failed = true,
                        }
                        if clob_decode_failed {
                            stats.decode_errors_clob += 1;
                        }
                    }
                    // Exchange trigger feeds drive the consensus median + move
                    // detector; a fired move emits observations. Observations
                    // stay synchronous (rare; their own transaction) — after a
                    // crash they may precede their causal raw frames by up to
                    // one flush window, so an offline recompute over a crashed
                    // tail is a lower bound on live observations.
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
                // Every received frame produces a raw tick (trades are a
                // subset), so the size trigger keys off `tick_buf` alone.
                tick_buf.push((frame.source, frame.received_ms, frame.raw));
                if tick_buf.len() >= config.flush_max_frames {
                    flush_frames(db, &mut tick_buf, &mut trade_buf, &mut stats)?;
                }
            }
            _ = refresh.tick() => {
                // Best-effort market refresh + the bounded periodic summaries
                // (tied to the refresh cadence, not the frame rate). With the
                // biased select this arm is polled only when `rx` is momentarily
                // empty: if the producers permanently outpace the consumer,
                // pruning stops exactly when it is most needed — the tell is a
                // non-zero drop tally below, which invalidates the run.
                log_decode_errors(&stats, "cumulative");
                log_dropped_frames(drops, "cumulative");
                let now_ms = now_unix_ms();
                match gamma.fetch_markets(&config.series()).await {
                    Ok(fresh) => {
                        // Tokens the join has never seen need a fresh CLOB
                        // subscription, else a market enumerated after startup
                        // (e.g. a new 5m expiry) never receives book frames — the
                        // startup-only-subscription bug that made the first 4hr
                        // run mostly unscorable (#300).
                        //
                        // Admission guard (issue #311): the shared expiry
                        // predicate filters BEFORE the `take`, so an expired
                        // market Gamma still lists (`closed=false` lag) is
                        // neither re-admitted after a prune nor allowed to
                        // crowd the `max_open_markets` window.
                        let mut new_tokens: Vec<String> = Vec::new();
                        let mut new_markets: Vec<BtcMarketMeta> = Vec::new();
                        for m in fresh
                            .into_iter()
                            .filter(|m| !market_expired(m, now_ms, config.prune_grace_ms))
                            .take(config.max_open_markets)
                        {
                            let mut toks: Vec<String> = Vec::new();
                            if !state.knows_token(&m.yes_token_id) {
                                toks.push(m.yes_token_id.clone());
                            }
                            if !state.knows_token(&m.no_token_id) {
                                toks.push(m.no_token_id.clone());
                            }
                            if toks.is_empty() {
                                // Already-known market: metadata refresh only.
                                state.upsert_market(m.clone());
                                if let Err(e) = db.upsert_market(&m) {
                                    warn!(error = %e, "shadow: market upsert error");
                                }
                            } else {
                                new_tokens.extend(toks);
                                new_markets.push(m);
                            }
                        }
                        if !new_tokens.is_empty() {
                            let count = new_tokens.len();
                            // Add-delivery gating (issue #311): register the new
                            // markets only once their subscribe command is
                            // accepted. On `false`, `knows_token` stays false,
                            // so the add self-retries next tick — without this,
                            // a dropped Add was a permanent unsubscribe.
                            if clob_sub.add_tokens(new_tokens) {
                                for m in new_markets {
                                    state.upsert_market(m.clone());
                                    if let Err(e) = db.upsert_market(&m) {
                                        warn!(error = %e, "shadow: market upsert error");
                                    }
                                }
                                info!(count, "shadow: subscribed new CLOB tokens");
                            } else {
                                warn!(count, "shadow: new CLOB subscribe dropped; retrying next refresh");
                            }
                        }
                        // Prune expired markets (issue #311), delivery-gated the
                        // same way: state mutates only after the Prune command
                        // is accepted, so a dropped command retries next tick.
                        let expired = state.expired_condition_ids(now_ms, config.prune_grace_ms);
                        let mut pruned = 0_usize;
                        for cid in &expired {
                            let tokens = state.market_tokens(cid);
                            if clob_sub.prune_tokens(tokens) {
                                state.remove_market(cid);
                                pruned += 1;
                            }
                        }
                        if pruned > 0 {
                            info!(pruned, "shadow: pruned expired markets");
                            // The pruned set rides the next natural reconnect in
                            // the common case; the force only fires in the task
                            // when reconnects have stalled past the gate.
                            clob_sub.force_reconnect();
                        }
                    }
                    Err(e) => warn!(error = %e, "shadow: market refresh error"),
                }
            }
            _ = flush_tick.tick() => {
                flush_frames(db, &mut tick_buf, &mut trade_buf, &mut stats)?;
            }
        }
    }
    // Post-loop flush: covers both the shutdown and the rx-closed exits, so
    // every received frame is persisted by the time `drive` returns.
    flush_frames(db, &mut tick_buf, &mut trade_buf, &mut stats)?;
    log_decode_errors(&stats, "final");
    log_dropped_frames(drops, "final");
    Ok(stats)
}

/// Flush the frame buffers in one transaction (no-op when both are empty).
/// Fail-fast on a DB error — a tape with silent holes is worse than a stopped
/// run (this tightens trade persistence from warn-and-continue to fail-fast,
/// per the issue #311 locked decision).
fn flush_frames(
    db: &ShadowDb,
    tick_buf: &mut Vec<(FeedSource, i64, String)>,
    trade_buf: &mut Vec<(ClobTrade, i64, Option<&'static str>)>,
    stats: &mut DriveStats,
) -> Result<(), Error> {
    if tick_buf.is_empty() && trade_buf.is_empty() {
        return Ok(());
    }
    db.insert_frame_batch(tick_buf, trade_buf)?;
    tick_buf.clear();
    trade_buf.clear();
    stats.flushes += 1;
    Ok(())
}

/// Emit a bounded frames-dropped summary (one line per call, only when
/// non-zero) — the saturation tell that was invisible in the v2 run's per-frame
/// warns. Any non-zero mid-run value invalidates the tape for the #310 sweep.
fn log_dropped_frames(drops: &DropCounters, phase: &str) {
    let snapshot = drops.snapshot();
    if snapshot.iter().any(|(_, v)| *v > 0) {
        warn!(
            chainlink = snapshot[0].1,
            clob = snapshot[1].1,
            bybit = snapshot[2].1,
            okx = snapshot[3].1,
            coinbase = snapshot[4].1,
            phase,
            "shadow: frames dropped (run-invalidating)"
        );
    }
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
    use crate::types::BtcSeriesKind;
    use pe_source_polymarket_public::FixtureFetcher;
    use rust_decimal_macros::dec;
    use std::collections::HashMap;

    fn test_gamma() -> BtcMarketFetcher<FixtureFetcher> {
        // The refresh arm is never reached in these tests (long interval, loop
        // exits first), so the fetcher is never invoked; empty fixtures suffice.
        BtcMarketFetcher::new(
            "https://gamma.test".to_string(),
            FixtureFetcher::new(HashMap::new()),
        )
    }

    /// Gamma fetcher answering the (5m-only) refresh-arm URL with `events`.
    fn fixture_gamma(events: Vec<u8>) -> BtcMarketFetcher<FixtureFetcher> {
        let mut fixtures = HashMap::new();
        fixtures.insert(
            "https://gamma.test/events?series_slug=btc-up-or-down-5m&closed=false".to_string(),
            events,
        );
        BtcMarketFetcher::new(
            "https://gamma.test".to_string(),
            FixtureFetcher::new(fixtures),
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
            &clob_ws::ClobSubscribeHandle::disconnected_for_test(),
            &DropCounters::default(),
            std::future::pending::<()>(),
        )
        .await
        .unwrap();

        assert_eq!(stats.decode_errors_clob, injected);
        assert_eq!(stats.decode_errors_chainlink, 0);
        // Every frame is persisted by loop exit (batched flush, issue #311) —
        // including frames whose decode failed: the recovery path.
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
            &clob_ws::ClobSubscribeHandle::disconnected_for_test(),
            &DropCounters::default(),
            std::future::ready(()),
        )
        .await
        .unwrap();

        // Shutdown was polled first and won before any frame was drained: the
        // buffer is empty, so the post-loop flush is a no-op.
        assert_eq!(stats.decode_errors_clob, 0);
        assert_eq!(stats.flushes, 0);
        assert_eq!(db.raw_tick_count().unwrap(), 0);
        println!("PASS: drive_shutdown_wins_over_draining_rx");
    }

    // Issue #311: frames buffered when shutdown fires (below both the size and
    // interval triggers) are persisted by the post-loop flush.
    #[tokio::test(start_paused = true)]
    async fn drive_flushes_on_shutdown_with_buffered_frames() {
        let dir = tempfile::tempdir().unwrap();
        let db = ShadowDb::open(&dir.path().join("s.db")).unwrap();
        let mut state = JoinState::new(Vec::new(), ShadowConfig::default().consensus_params());
        let gamma = test_gamma();
        let cfg = ShadowConfig::default();
        let mut refresh = tokio::time::interval(Duration::from_secs(3_600));
        refresh.tick().await;

        let (tx, mut rx) = mpsc::channel(64);
        for _ in 0..8 {
            tx.try_send(garbage_clob_frame()).unwrap();
        }
        let _keep_open = tx; // rx never closes; 8 < 256 so no size trigger

        // Shutdown at 10ms: the frames are drained into the buffer first (the
        // shutdown timer is pending at t=0), and the 250ms interval flush never
        // arrives — only the post-loop flush can persist them.
        let stats = drive(
            &db,
            &mut rx,
            &mut state,
            &gamma,
            &mut refresh,
            &cfg,
            &clob_ws::ClobSubscribeHandle::disconnected_for_test(),
            &DropCounters::default(),
            async {
                tokio::time::sleep(Duration::from_millis(10)).await;
            },
        )
        .await
        .unwrap();

        assert_eq!(db.raw_tick_count().unwrap(), 8);
        assert_eq!(stats.flushes, 1, "exactly the post-loop flush");
        println!("PASS: drive_flushes_on_shutdown_with_buffered_frames");
    }

    // Issue #311: the interval flush persists buffered frames MID-RUN (no size
    // trigger, shutdown far away) — the crash-loss ≤ one-flush-window bound
    // depends on this, so the rows must be visible BEFORE shutdown fires.
    #[tokio::test(start_paused = true)]
    async fn drive_flushes_on_interval_without_size_trigger() {
        let dir = tempfile::tempdir().unwrap();
        let db = ShadowDb::open(&dir.path().join("s.db")).unwrap();
        let mut state = JoinState::new(Vec::new(), ShadowConfig::default().consensus_params());
        let gamma = test_gamma();
        let cfg = ShadowConfig::default();
        let mut refresh = tokio::time::interval(Duration::from_secs(3_600));
        refresh.tick().await;

        let (tx, mut rx) = mpsc::channel(64);
        for _ in 0..5 {
            tx.try_send(garbage_clob_frame()).unwrap();
        }
        let _keep_open = tx; // rx never closes; 5 < 256 so no size trigger

        let handle = clob_ws::ClobSubscribeHandle::disconnected_for_test();
        let drops = DropCounters::default();
        let drive_fut = drive(
            &db,
            &mut rx,
            &mut state,
            &gamma,
            &mut refresh,
            &cfg,
            &handle,
            &drops,
            async {
                tokio::time::sleep(Duration::from_millis(600)).await;
            },
        );
        let probe = async {
            // After the 250ms interval flush, well before the 600ms shutdown.
            tokio::time::sleep(Duration::from_millis(400)).await;
            db.raw_tick_count().unwrap()
        };
        let (stats, mid_run_count) = tokio::join!(drive_fut, probe);
        let stats = stats.unwrap();

        assert_eq!(mid_run_count, 5, "interval flush visible before shutdown");
        assert_eq!(db.raw_tick_count().unwrap(), 5);
        assert_eq!(
            stats.flushes, 1,
            "one interval flush; the post-loop flush of an empty buffer is a no-op"
        );
        println!("PASS: drive_flushes_on_interval_without_size_trigger");
    }

    // Issue #311 AC: a 100k-frame flood through `drive` with production-shaped
    // settings persists EVERY frame. The producer uses awaited `send`
    // (deterministic backpressure, never drops); the flush-count assertion
    // proves the size-trigger path ran — a broken trigger masked by the
    // post-loop flush would show ~1 flush.
    #[tokio::test]
    async fn scenario_saturation_no_drops() {
        let dir = tempfile::tempdir().unwrap();
        let db = ShadowDb::open(&dir.path().join("s.db")).unwrap();
        let mut state = JoinState::new(Vec::new(), ShadowConfig::default().consensus_params());
        let gamma = test_gamma();
        let cfg = ShadowConfig::default(); // channel 8192, flush 250ms / 256
        let mut refresh = tokio::time::interval(Duration::from_secs(3_600));
        refresh.tick().await;

        let (tx, mut rx) = mpsc::channel(cfg.channel_capacity);
        let injected: u64 = 100_000;
        tokio::spawn(async move {
            for _ in 0..injected {
                if tx.send(garbage_clob_frame()).await.is_err() {
                    break;
                }
            }
            // tx dropped here → rx closes once drained, ending the loop.
        });

        let stats = drive(
            &db,
            &mut rx,
            &mut state,
            &gamma,
            &mut refresh,
            &cfg,
            &clob_ws::ClobSubscribeHandle::disconnected_for_test(),
            &DropCounters::default(),
            std::future::pending::<()>(),
        )
        .await
        .unwrap();

        assert_eq!(
            db.raw_tick_count().unwrap(),
            i64::try_from(injected).unwrap()
        );
        // The buffer never exceeds `flush_max_frames`, so a full persist takes
        // at least ⌈100_000 / 256⌉ flushes.
        let min_flushes = injected.div_ceil(u64::try_from(cfg.flush_max_frames).unwrap());
        assert!(
            stats.flushes >= min_flushes,
            "flushes {} < required minimum {min_flushes}: size trigger did not run",
            stats.flushes
        );
        println!(
            "PASS: scenario_saturation_no_drops frames={injected} flushes={}",
            stats.flushes
        );
    }

    // Issue #311 (V2): an Add the sub-channel cannot accept must NOT register
    // the market — `knows_token` stays false, so the next refresh tick retries;
    // once the channel has room, the market registers.
    #[tokio::test(start_paused = true)]
    async fn refresh_add_is_delivery_gated_and_retries() {
        let dir = tempfile::tempdir().unwrap();
        let db = ShadowDb::open(&dir.path().join("s.db")).unwrap();
        let cfg = ShadowConfig {
            track_15m: false,
            ..ShadowConfig::default()
        };
        let mut state = JoinState::new(Vec::new(), cfg.consensus_params());

        // A live 5m market: slug-derived window well in the future of the wall
        // clock the refresh arm reads (the expiry compare is offset-robust).
        let slug_secs = (now_unix_ms() / 1000) + 3_600;
        let events = format!(
            r#"[{{"slug":"btc-updown-5m-{slug_secs}",
                 "startDate":"2025-12-08T00:00:00Z","endDate":"2025-12-09T00:00:00Z",
                 "markets":[{{"conditionId":"0xcondNew",
                              "clobTokenIds":"[\"tokY\",\"tokN\"]",
                              "orderPriceMinTickSize":"0.01"}}]}}]"#
        );
        let gamma = fixture_gamma(events.into_bytes());

        let mut refresh = tokio::time::interval(Duration::from_millis(50));
        refresh.tick().await;

        // Capacity-1 channel pre-filled: tick 1's Add is rejected (Full).
        let (handle, mut sub_rx) = clob_ws::ClobSubscribeHandle::for_test_with_capacity(1);
        handle.force_reconnect();

        let (tx, mut rx) = mpsc::channel::<FeedFrame>(4);
        let _keep_open = tx;
        let drops = DropCounters::default();
        let drive_fut = drive(
            &db,
            &mut rx,
            &mut state,
            &gamma,
            &mut refresh,
            &cfg,
            &handle,
            &drops,
            async {
                tokio::time::sleep(Duration::from_millis(125)).await;
            },
        );
        let probe = async {
            // Drain the blocker between tick 1 (50ms) and tick 2 (100ms).
            tokio::time::sleep(Duration::from_millis(75)).await;
            sub_rx.recv().await
        };
        let (stats, blocker) = tokio::join!(drive_fut, probe);
        stats.unwrap();
        assert_eq!(blocker, Some(clob_ws::SubCmd::ForceReconnect));

        // Tick 1 failed delivery and did NOT register; tick 2 retried and won.
        assert_eq!(state.market_count(), 1);
        assert!(state.knows_token("tokY") && state.knows_token("tokN"));
        assert_eq!(
            sub_rx.try_recv().unwrap(),
            clob_ws::SubCmd::Add(vec!["tokY".to_string(), "tokN".to_string()])
        );
        println!("PASS: refresh_add_is_delivery_gated_and_retries");
    }

    // Issue #311 (S3): a Prune the sub-channel cannot accept must NOT mutate
    // the join state — the market stays in `expired_condition_ids`, so the next
    // refresh tick retries; once delivered, the market is removed.
    #[tokio::test(start_paused = true)]
    async fn refresh_prune_is_delivery_gated_and_retries() {
        let dir = tempfile::tempdir().unwrap();
        let db = ShadowDb::open(&dir.path().join("s.db")).unwrap();
        let cfg = ShadowConfig {
            track_15m: false,
            ..ShadowConfig::default()
        };
        // One tracked market already long past range_end + grace on the wall
        // clock the refresh arm reads.
        let expired = BtcMarketMeta {
            condition_id: "0xdead".to_string(),
            yes_token_id: "dead-yes".to_string(),
            no_token_id: "dead-no".to_string(),
            series: BtcSeriesKind::Five,
            range_start_ms: now_unix_ms() - 900_000,
            range_end_ms: now_unix_ms() - 600_000,
            tick: dec!(0.01),
        };
        let mut state = JoinState::new(vec![expired], cfg.consensus_params());
        // Gamma answers with no events, so the refresh arm reaches the prune.
        let gamma = fixture_gamma(b"[]".to_vec());

        let mut refresh = tokio::time::interval(Duration::from_millis(50));
        refresh.tick().await;

        // Capacity-1 channel pre-filled: tick 1's Prune is rejected (Full).
        let (handle, mut sub_rx) = clob_ws::ClobSubscribeHandle::for_test_with_capacity(1);
        handle.force_reconnect();

        let (tx, mut rx) = mpsc::channel::<FeedFrame>(4);
        let _keep_open = tx;
        let drops = DropCounters::default();
        let drive_fut = drive(
            &db,
            &mut rx,
            &mut state,
            &gamma,
            &mut refresh,
            &cfg,
            &handle,
            &drops,
            async {
                tokio::time::sleep(Duration::from_millis(125)).await;
            },
        );
        let probe = async {
            tokio::time::sleep(Duration::from_millis(75)).await;
            sub_rx.recv().await
        };
        let (stats, blocker) = tokio::join!(drive_fut, probe);
        stats.unwrap();
        assert_eq!(blocker, Some(clob_ws::SubCmd::ForceReconnect));

        assert_eq!(state.market_count(), 0, "prune delivered on the retry tick");
        assert!(!state.knows_token("dead-yes"));
        assert_eq!(
            sub_rx.try_recv().unwrap(),
            clob_ws::SubCmd::Prune(vec!["dead-yes".to_string(), "dead-no".to_string()])
        );
        println!("PASS: refresh_prune_is_delivery_gated_and_retries");
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
