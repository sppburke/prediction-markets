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
use crate::db::{FRAMES_DROPPED_META_KEYS, LAG_CLOCK, SCHEMA_VERSION, ShadowDb};
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

/// Per-source decode-error tallies + the max CLOB inter-frame gap returned by
/// [`drive`]. Decode failures increment a counter surfaced as a bounded periodic
/// summary (never a per-frame log line), so a load test can assert the count
/// without scraping logs and a 72k-frame run does not emit 72k warnings. The
/// frame-batch commit count moved to [`WriterStats`] (issue #317: the decoupled
/// writer owns it).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct DriveStats {
    decode_errors_chainlink: u64,
    decode_errors_clob: u64,
    decode_errors_exchange: u64,
    /// Largest gap (ms) between consecutive CLOB frames over the run, with the
    /// final `now − last` gap folded in at loop exit (issue #317), so a CLOB feed
    /// that dies near run-end is visible. Stamped to `meta` as `max_clob_gap_secs`
    /// — a tape-validity PASS gate (< 300s).
    max_clob_gap_ms: i64,
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
    /// `(meta key, current value)` per source, for the `meta` stamp. Keys come
    /// from the shared [`FRAMES_DROPPED_META_KEYS`] so the #310 sweep's
    /// tape-validity read cannot drift from what is stamped here.
    fn snapshot(&self) -> [(&'static str, u64); 5] {
        let values = [
            self.chainlink.load(Ordering::Relaxed),
            self.clob.load(Ordering::Relaxed),
            self.bybit.load(Ordering::Relaxed),
            self.okx.load(Ordering::Relaxed),
            self.coinbase.load(Ordering::Relaxed),
        ];
        std::array::from_fn(|i| (FRAMES_DROPPED_META_KEYS[i], values[i]))
    }
}

/// A unit of persistence work for the decoupled DB-writer task (issue #317).
/// Both variants carry owned, `Send + 'static` data (`String`, owned `ClobTrade`,
/// `&'static str` series), so the message crosses cleanly to the blocking writer.
enum WriteCmd {
    /// One flush window: raw ticks + trade prints, persisted in one transaction.
    FrameBatch {
        ticks: Vec<(FeedSource, i64, String)>,
        trades: Vec<(ClobTrade, i64, Option<&'static str>)>,
    },
    /// Computed observations from a fired move (rare; their own transaction).
    Observations(Vec<crate::types::EdgeObservation>),
}

/// Commit tallies returned by [`writer_loop`]. `flushes` is the non-empty
/// frame-batch commit count (the saturation test asserts the size trigger ran);
/// `frames_written` is the total raw ticks persisted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct WriterStats {
    flushes: u64,
    frames_written: u64,
}

/// One iteration's selected event in `drive`'s inner (fair) select (issue #317).
/// Separating it from the outer biased shutdown branch keeps shutdown winning,
/// while the inner `rx`/`refresh`/`flush` select is non-biased so the refresh +
/// prune + drop-log arm no longer starves exactly under saturation (the fully
/// `biased;` loop's failure mode).
enum DriveEvent {
    Frame(FeedFrame),
    RxClosed,
    Refresh,
    Flush,
}

/// Dedicated blocking DB-writer (issue #317). Drains `write_rx` and commits each
/// [`WriteCmd`] to SQLite **off** the socket-drain task, so `drive` never blocks
/// on a `rusqlite` transaction. Hosted on `tokio::task::spawn_blocking`, so
/// `blocking_recv` is valid (it runs off the async worker pool).
///
/// **Fail-fast**: the first `DbError` returns `Err(Error::Db(..))` immediately,
/// preserving the issue #311 invariant that a tape with silent holes is worse
/// than a stopped run (the write no longer happens inline under `?` in `drive`,
/// so `run` recovers this error from the awaited writer handle and propagates it
/// in preference to the channel-closed signal `drive` sees). On clean channel
/// close (all senders dropped) returns `Ok(WriterStats)`. There is deliberately
/// **no** `write_errors` counter — a fail-fast writer has at most one (fatal)
/// error, so a counter would imply the disallowed continue-on-error semantics.
fn writer_loop(
    db: Arc<ShadowDb>,
    mut write_rx: mpsc::Receiver<WriteCmd>,
) -> Result<WriterStats, Error> {
    let mut stats = WriterStats::default();
    while let Some(cmd) = write_rx.blocking_recv() {
        match cmd {
            WriteCmd::FrameBatch { ticks, trades } => {
                db.insert_frame_batch(&ticks, &trades)?;
                stats.flushes += 1;
                stats.frames_written += u64::try_from(ticks.len()).unwrap_or(u64::MAX);
            }
            WriteCmd::Observations(obs) => {
                db.insert_observations(&obs)?;
            }
        }
    }
    Ok(stats)
}

/// Drive the live shadow collection until `rx` closes or `shutdown` resolves.
pub async fn run(
    config: &ShadowConfig,
    shutdown: impl std::future::Future<Output = ()>,
) -> Result<RunSummary, Error> {
    let db = Arc::new(ShadowDb::open(Path::new(&config.db_path))?);
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
        Duration::from_secs(config.clob_ping_interval_secs),
        Duration::from_secs(config.clob_read_idle_limit_secs),
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

    // Decoupled DB-writer (issue #317): `drive` sends batched `WriteCmd`s here so
    // the socket-drain task never blocks on a `rusqlite` transaction. Bounded
    // channel; backpressure is `send().await` (never a silent drop in-pipeline).
    let (write_tx, write_rx) = mpsc::channel::<WriteCmd>(config.write_channel_capacity);
    let writer = {
        let db = Arc::clone(&db);
        tokio::task::spawn_blocking(move || writer_loop(db, write_rx))
    };

    let mut refresh = tokio::time::interval(Duration::from_secs(
        config.market_refresh_interval_secs.max(1),
    ));
    refresh.tick().await; // discard the immediate first tick

    let drive_result = drive(
        &db,
        &mut rx,
        &mut state,
        &gamma,
        &mut refresh,
        config,
        &clob_sub,
        &drops,
        &write_tx,
        shutdown,
    )
    .await;
    // Close the writer channel so its `blocking_recv` returns None → Ok(stats).
    drop(write_tx);
    // Error precedence (issue #317): the writer's fail-fast `DbError` is the true
    // cause and wins over the channel-closed signal `drive` observed. Only a clean
    // writer join proceeds to stamp `meta` + build the deferred indexes.
    let writer_stats = match writer.await {
        Ok(Ok(stats)) => stats,
        Ok(Err(e)) => return Err(e),
        Err(join_err) => {
            warn!(error = %join_err, "shadow: DB-writer task panicked");
            return Err(Error::Io(std::io::Error::other(format!(
                "db-writer task panicked: {join_err}"
            ))));
        }
    };
    let drive_stats = drive_result?;
    info!(
        chainlink_decode_errors = drive_stats.decode_errors_chainlink,
        clob_decode_errors = drive_stats.decode_errors_clob,
        exchange_decode_errors = drive_stats.decode_errors_exchange,
        max_clob_gap_ms = drive_stats.max_clob_gap_ms,
        flushes = writer_stats.flushes,
        frames_written = writer_stats.frames_written,
        "shadow: drive loop ended"
    );
    // Stamp the drop tallies + the max CLOB gap so the post-run validation AC
    // (`frames_dropped_*` = 0 AND `max_clob_gap_secs` < 300) is a `meta` query,
    // not a log scrape. Meaningful at run end only.
    for (key, value) in drops.snapshot() {
        db.set_meta(key, &value.to_string())?;
    }
    db.set_meta(
        "max_clob_gap_secs",
        &(drive_stats.max_clob_gap_ms / 1000).to_string(),
    )?;
    // Build the deferred `clob_trades` indexes once, now that capture is done
    // (issue #317) — a cleanly-finished tape ends with the same indexes as before.
    db.build_clob_trade_indexes()?;

    Ok(RunSummary {
        observations: db.observation_count()?,
        raw_ticks: db.raw_tick_count()?,
    })
}

/// Drive the join loop over inbound frames until `rx` closes or `shutdown`
/// resolves. Extracted from [`run`] (which owns the network preamble that errors
/// under the no-network gate) so the select loop is testable offline: an
/// injected `shutdown`, a controlled `rx`, and a `FixtureFetcher`-backed `gamma`
/// exercise every arm with no live socket.
///
/// The loop is a **nested** select (issue #317): an outer `biased;` select polls
/// `shutdown` first (so it still wins over a still-draining `rx`, mirroring the
/// shutdown idiom in `crates/service/src/orchestrator.rs`), then a **fair** inner
/// select over `rx`/`refresh`/`flush` — so the refresh + prune + drop-log arm no
/// longer starves under sustained load (the prior fully-`biased;` failure mode).
/// `rx.recv()` is cancel-safe, so a shutdown that drops the in-flight inner
/// branch loses no buffered frame.
///
/// Persistence is **batched + decoupled** (issues #311, #317): raw frames and
/// trade prints buffer in-memory and are enqueued as one [`WriteCmd::FrameBatch`]
/// to the [`writer_loop`] task every `flush_interval_ms` OR `flush_max_frames`,
/// whichever first, plus once after the loop exits — so `drive` itself never
/// touches SQLite on the hot path. The persistence invariant is "every received
/// frame is persisted by the time [`run`] finishes draining the writer" (was "by
/// loop exit"). A writer `DbError` is fail-fast: it closes the write channel,
/// `drive` breaks, and `run` surfaces the true error from the writer handle.
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
    write_tx: &mpsc::Sender<WriteCmd>,
    shutdown: impl std::future::Future<Output = ()>,
) -> Result<DriveStats, Error> {
    let mut stats = DriveStats::default();
    let mut tick_buf: Vec<(FeedSource, i64, String)> = Vec::with_capacity(config.flush_max_frames);
    let mut trade_buf: Vec<(ClobTrade, i64, Option<&'static str>)> = Vec::new();
    let mut last_clob_received_ms: Option<i64> = None;
    let mut flush_tick =
        tokio::time::interval(Duration::from_millis(config.flush_interval_ms.max(1)));
    flush_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    flush_tick.tick().await; // discard the immediate first tick
    tokio::pin!(shutdown);
    loop {
        // Outer biased select: shutdown wins. Inner select (the async block) is
        // fair, so refresh/flush no longer starve under load (issue #317).
        // `rx.recv()`/`Interval::tick()` are cancel-safe, so dropping the inner
        // branch when shutdown fires loses no frame or tick.
        let event = tokio::select! {
            biased;
            _ = &mut shutdown => {
                info!("shadow: shutdown signalled, stopping drive loop");
                break;
            }
            ev = async {
                tokio::select! {
                    maybe = rx.recv() => match maybe {
                        Some(frame) => DriveEvent::Frame(frame),
                        None => DriveEvent::RxClosed,
                    },
                    _ = refresh.tick() => DriveEvent::Refresh,
                    _ = flush_tick.tick() => DriveEvent::Flush,
                }
            } => ev,
        };
        match event {
            DriveEvent::RxClosed => break,
            DriveEvent::Frame(frame) => {
                // Max CLOB inter-frame gap (issue #317): the tail (now − last) is
                // folded at loop exit so a feed that dies near run-end shows too.
                if frame.source == FeedSource::Clob {
                    if let Some(prev) = last_clob_received_ms {
                        let gap = frame.received_ms - prev;
                        if gap > stats.max_clob_gap_ms {
                            stats.max_clob_gap_ms = gap;
                        }
                    }
                    last_clob_received_ms = Some(frame.received_ms);
                }
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
                    // detector; a fired move emits observations. They ride the
                    // same FIFO `WriteCmd` channel (issue #317), enqueued before
                    // the causal raw frame's `FrameBatch`, so the "may precede
                    // their causal raw frames by up to one flush window" ordering
                    // is preserved — an offline recompute over a crashed tail is a
                    // lower bound on live observations. The non-empty guard mirrors
                    // `insert_observations`' empty early-return.
                    FeedSource::Bybit | FeedSource::Okx | FeedSource::Coinbase => {
                        if let Some(venue) = frame.source.exchange_venue() {
                            match exchange_ws::parse_trade_frame(venue, &frame.raw) {
                                Ok(Some(tick)) => {
                                    let obs = state.on_exchange_tick(&tick, frame.received_ms);
                                    if !obs.is_empty()
                                        && write_tx.send(WriteCmd::Observations(obs)).await.is_err()
                                    {
                                        break; // writer exited; `run` surfaces it
                                    }
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
                if tick_buf.len() >= config.flush_max_frames
                    && !send_batch(write_tx, &mut tick_buf, &mut trade_buf).await
                {
                    break; // writer exited; `run` surfaces the true error
                }
            }
            DriveEvent::Refresh => {
                // Best-effort market refresh + the bounded periodic summaries
                // (tied to the refresh cadence, not the frame rate). The inner
                // select is fair (issue #317), so this arm gets a turn even under
                // sustained inbound load — the prior fully-`biased;` loop starved
                // it exactly when pruning mattered most.
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
                                warn!(
                                    count,
                                    "shadow: new CLOB subscribe dropped; retrying next refresh"
                                );
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
            DriveEvent::Flush => {
                if !send_batch(write_tx, &mut tick_buf, &mut trade_buf).await {
                    break; // writer exited; `run` surfaces the true error
                }
            }
        }
    }
    // Post-loop flush: covers both the shutdown and the rx-closed exits, enqueuing
    // any buffered frames. Best-effort — if the writer already exited on a
    // `DbError`, the send fails and `run` surfaces the true error from the handle.
    let _ = send_batch(write_tx, &mut tick_buf, &mut trade_buf).await;
    // Tail-fold the final CLOB gap (issue #317): now − last seen.
    if let Some(prev) = last_clob_received_ms {
        let tail = now_unix_ms() - prev;
        if tail > stats.max_clob_gap_ms {
            stats.max_clob_gap_ms = tail;
        }
    }
    log_decode_errors(&stats, "final");
    log_dropped_frames(drops, "final");
    Ok(stats)
}

/// Enqueue the buffered frames to the decoupled writer as one
/// [`WriteCmd::FrameBatch`] (no-op when both buffers are empty). Returns `false`
/// if the writer channel is closed — during a live run that means the writer task
/// exited (a `DbError` that `run` recovers from the writer handle); the caller
/// breaks the drive loop. The commit count is owned by the writer
/// ([`WriterStats::flushes`]), not incremented here. `mem::take` moves the buffer
/// contents to the writer thread (no copy); the next window re-grows the buffer.
async fn send_batch(
    write_tx: &mpsc::Sender<WriteCmd>,
    tick_buf: &mut Vec<(FeedSource, i64, String)>,
    trade_buf: &mut Vec<(ClobTrade, i64, Option<&'static str>)>,
) -> bool {
    if tick_buf.is_empty() && trade_buf.is_empty() {
        return true;
    }
    let cmd = WriteCmd::FrameBatch {
        ticks: std::mem::take(tick_buf),
        trades: std::mem::take(trade_buf),
    };
    write_tx.send(cmd).await.is_ok()
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
    // Defensive self-identifying UA on the `&closed=true` resolution client (issue #382),
    // matching the bootstrap Gamma clients. The Phase-0 probe showed a bare client is not
    // 403'd here, so this is hardening, not a correctness fix; `BtcMarketFetcher`'s
    // `closed=false` enumeration (gamma.rs) keeps the bare client.
    let gamma_client = reqwest::Client::builder()
        .user_agent(pe_source_polymarket_public::GAMMA_BROWSER_UA)
        .build()
        .map_err(|e| crate::resolve::ResolveError::Fetch(e.to_string()))?;
    let fetcher = ReqwestFetcher::new(gamma_client);
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

    fn clob_frame_at(received_ms: i64) -> FeedFrame {
        FeedFrame {
            source: FeedSource::Clob,
            received_ms,
            raw: "{not json".to_string(),
        }
    }

    /// Spawn the real `writer_loop` on a fresh write channel (issue #317). Used by
    /// the **non-paused** persistence tests, where a live `spawn_blocking` writer
    /// is safe to run concurrently with `drive`.
    fn spawn_test_writer(
        db: Arc<ShadowDb>,
    ) -> (
        mpsc::Sender<WriteCmd>,
        tokio::task::JoinHandle<Result<WriterStats, Error>>,
    ) {
        let (write_tx, write_rx) = mpsc::channel::<WriteCmd>(64);
        (write_tx, spawn_writer_with(db, write_rx))
    }

    /// Spawn the real `writer_loop` on a caller-provided `write_rx` — for the
    /// spawn-writer-**after**-drive shape (paused-clock tests, where a live
    /// blocking task during `drive` would inhibit the clock's auto-advance).
    fn spawn_writer_with(
        db: Arc<ShadowDb>,
        write_rx: mpsc::Receiver<WriteCmd>,
    ) -> tokio::task::JoinHandle<Result<WriterStats, Error>> {
        tokio::task::spawn_blocking(move || writer_loop(db, write_rx))
    }

    /// A regular async sink (`tokio::spawn`, NOT `spawn_blocking`, so it does not
    /// inhibit the paused-clock auto-advance — issue #317) that counts the
    /// `FrameBatch`es it receives into a shared counter.
    fn spawn_counting_sink(
        mut write_rx: mpsc::Receiver<WriteCmd>,
    ) -> (tokio::task::JoinHandle<()>, Arc<AtomicU64>) {
        let count = Arc::new(AtomicU64::new(0));
        let c = Arc::clone(&count);
        let handle = tokio::spawn(async move {
            while let Some(cmd) = write_rx.recv().await {
                if matches!(cmd, WriteCmd::FrameBatch { .. }) {
                    c.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
        (handle, count)
    }

    // AC1.4(a): under a flood of undecodable frames, `drive` returns the exact
    // per-source error count, persists every raw frame, and emits no per-frame
    // log line (summary only).
    #[tokio::test]
    async fn drive_counts_decode_errors_without_per_frame_logs() {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(ShadowDb::open(&dir.path().join("s.db")).unwrap());
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

        // Live writer join-first shape (issue #317): non-paused, so a concurrent
        // `spawn_blocking` writer is safe. A `pending` shutdown never fires, so
        // the rx arm runs to completion.
        let (write_tx, writer) = spawn_test_writer(Arc::clone(&db));
        let stats = drive(
            &db,
            &mut rx,
            &mut state,
            &gamma,
            &mut refresh,
            &cfg,
            &clob_ws::ClobSubscribeHandle::disconnected_for_test(),
            &DropCounters::default(),
            &write_tx,
            std::future::pending::<()>(),
        )
        .await
        .unwrap();
        drop(write_tx);
        let writer_stats = writer.await.unwrap().unwrap();

        assert_eq!(stats.decode_errors_clob, injected);
        assert_eq!(stats.decode_errors_chainlink, 0);
        // Every frame is persisted by the writer (batched, issues #311/#317) —
        // including frames whose decode failed: the recovery path.
        assert_eq!(
            db.raw_tick_count().unwrap(),
            i64::try_from(injected).unwrap()
        );
        assert_eq!(writer_stats.frames_written, injected);
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

        // Counting sink (issue #317): the stronger no-drain check is that the
        // writer channel received ZERO `WriteCmd`s, not just that `flushes == 0`.
        let (write_tx, write_rx) = mpsc::channel::<WriteCmd>(64);
        let (sink, sink_count) = spawn_counting_sink(write_rx);
        let stats = drive(
            &db,
            &mut rx,
            &mut state,
            &gamma,
            &mut refresh,
            &cfg,
            &clob_ws::ClobSubscribeHandle::disconnected_for_test(),
            &DropCounters::default(),
            &write_tx,
            std::future::ready(()),
        )
        .await
        .unwrap();
        drop(write_tx);
        sink.await.unwrap();

        // Shutdown was polled first and won before any frame was drained: no
        // `WriteCmd` was ever enqueued, and nothing was persisted.
        assert_eq!(
            sink_count.load(Ordering::Relaxed),
            0,
            "no WriteCmd enqueued"
        );
        assert_eq!(stats.decode_errors_clob, 0);
        assert_eq!(db.raw_tick_count().unwrap(), 0);
        println!("PASS: drive_shutdown_wins_over_draining_rx");
    }

    // Issue #311: frames buffered when shutdown fires (below both the size and
    // interval triggers) are persisted by the post-loop flush.
    #[tokio::test(start_paused = true)]
    async fn drive_flushes_on_shutdown_with_buffered_frames() {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(ShadowDb::open(&dir.path().join("s.db")).unwrap());
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

        // Spawn-writer-AFTER-drive (issue #317): no `spawn_blocking` task is alive
        // during `drive`, so the paused clock's 10ms shutdown sleep auto-advances
        // normally (a live blocking task would inhibit it → deadlock). The frames
        // are drained into the buffer first (shutdown timer pending at t=0) and
        // the 250ms interval flush never arrives, so only the post-loop flush
        // enqueues them — into the cap-64 channel, where the single batch fits
        // without blocking. The writer then persists it.
        let (write_tx, write_rx) = mpsc::channel::<WriteCmd>(64);
        drive(
            &db,
            &mut rx,
            &mut state,
            &gamma,
            &mut refresh,
            &cfg,
            &clob_ws::ClobSubscribeHandle::disconnected_for_test(),
            &DropCounters::default(),
            &write_tx,
            async {
                tokio::time::sleep(Duration::from_millis(10)).await;
            },
        )
        .await
        .unwrap();
        drop(write_tx);
        let writer_stats = spawn_writer_with(Arc::clone(&db), write_rx)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(db.raw_tick_count().unwrap(), 8);
        assert_eq!(writer_stats.flushes, 1, "exactly the post-loop flush");
        println!("PASS: drive_flushes_on_shutdown_with_buffered_frames");
    }

    // Issue #311/#317: the interval flush enqueues buffered frames MID-RUN (no
    // size trigger, shutdown far away). After the writer split the persistence is
    // the writer's job, so this asserts the interval flush *fired* (one
    // `FrameBatch` reached the channel) before any size trigger, via a counting
    // sink — a regular async task that does not inhibit the paused clock.
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
        let (write_tx, write_rx) = mpsc::channel::<WriteCmd>(64);
        let (sink, sink_count) = spawn_counting_sink(write_rx);
        let drive_fut = drive(
            &db,
            &mut rx,
            &mut state,
            &gamma,
            &mut refresh,
            &cfg,
            &handle,
            &drops,
            &write_tx,
            async {
                tokio::time::sleep(Duration::from_millis(600)).await;
            },
        );
        let probe = async {
            // After the 250ms interval flush, well before the 600ms shutdown.
            tokio::time::sleep(Duration::from_millis(400)).await;
            sink_count.load(Ordering::Relaxed)
        };
        let (stats, mid_run_flushes) = tokio::join!(drive_fut, probe);
        stats.unwrap();
        drop(write_tx);
        sink.await.unwrap();

        assert_eq!(
            mid_run_flushes, 1,
            "interval flush fired before shutdown (and before any size trigger)"
        );
        assert_eq!(
            sink_count.load(Ordering::Relaxed),
            1,
            "exactly one FrameBatch; the post-loop flush of an empty buffer is a no-op"
        );
        println!("PASS: drive_flushes_on_interval_without_size_trigger");
    }

    // Issue #311/#317 AC: a 100k-frame flood through `drive` → write channel →
    // real `spawn_blocking` writer persists EVERY frame. The producer uses
    // awaited `send` (deterministic backpressure, never drops); `drive` likewise
    // backpressures on the write channel. The writer's flush-count assertion
    // proves the size-trigger path ran — a broken trigger masked by the post-loop
    // flush would show ~1 flush.
    #[tokio::test]
    async fn scenario_saturation_no_drops() {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(ShadowDb::open(&dir.path().join("s.db")).unwrap());
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

        let (write_tx, writer) = spawn_test_writer(Arc::clone(&db));
        drive(
            &db,
            &mut rx,
            &mut state,
            &gamma,
            &mut refresh,
            &cfg,
            &clob_ws::ClobSubscribeHandle::disconnected_for_test(),
            &DropCounters::default(),
            &write_tx,
            std::future::pending::<()>(),
        )
        .await
        .unwrap();
        drop(write_tx);
        let writer_stats = writer.await.unwrap().unwrap();

        assert_eq!(
            db.raw_tick_count().unwrap(),
            i64::try_from(injected).unwrap()
        );
        assert_eq!(writer_stats.frames_written, injected);
        // The buffer never exceeds `flush_max_frames`, so a full persist takes
        // at least ⌈100_000 / 256⌉ flushes.
        let min_flushes = injected.div_ceil(u64::try_from(cfg.flush_max_frames).unwrap());
        assert!(
            writer_stats.flushes >= min_flushes,
            "flushes {} < required minimum {min_flushes}: size trigger did not run",
            writer_stats.flushes
        );
        println!(
            "PASS: scenario_saturation_no_drops frames={injected} flushes={}",
            writer_stats.flushes
        );
    }

    // Issue #317: `drive` tracks the largest gap between consecutive CLOB frames
    // (stamped to `meta` as `max_clob_gap_secs`, a tape-validity PASS gate). The
    // receive times are in the recent past (`base = now − 5000`), so the
    // loop-exit tail fold (`now − last`, a few ms) stays well under the 4000ms
    // inter-frame max — making the captured max exactly the inter-frame gap.
    #[tokio::test(start_paused = true)]
    async fn drive_tracks_max_clob_gap() {
        let dir = tempfile::tempdir().unwrap();
        let db = ShadowDb::open(&dir.path().join("s.db")).unwrap();
        let mut state = JoinState::new(Vec::new(), ShadowConfig::default().consensus_params());
        let gamma = test_gamma();
        let cfg = ShadowConfig::default();
        let mut refresh = tokio::time::interval(Duration::from_secs(3_600));
        refresh.tick().await;

        let base = now_unix_ms() - 5_000;
        let (tx, mut rx) = mpsc::channel(8);
        tx.try_send(clob_frame_at(base)).unwrap(); // first CLOB frame: no gap yet
        tx.try_send(clob_frame_at(base + 1_000)).unwrap(); // gap 1000 ms
        tx.try_send(clob_frame_at(base + 5_000)).unwrap(); // gap 4000 ms (the max)
        let _keep_open = tx;

        let (write_tx, _write_rx) = mpsc::channel::<WriteCmd>(64);
        let stats = drive(
            &db,
            &mut rx,
            &mut state,
            &gamma,
            &mut refresh,
            &cfg,
            &clob_ws::ClobSubscribeHandle::disconnected_for_test(),
            &DropCounters::default(),
            &write_tx,
            async {
                tokio::time::sleep(Duration::from_millis(10)).await;
            },
        )
        .await
        .unwrap();

        assert_eq!(
            stats.max_clob_gap_ms, 4_000,
            "largest inter-frame CLOB gap captured"
        );
        println!(
            "PASS: drive_tracks_max_clob_gap gap_ms={}",
            stats.max_clob_gap_ms
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
        // The write channel is unused here (no frames injected), but `drive`
        // requires it (issue #317); a held receiver keeps it open.
        let (write_tx, _write_rx) = mpsc::channel::<WriteCmd>(64);
        let drive_fut = drive(
            &db,
            &mut rx,
            &mut state,
            &gamma,
            &mut refresh,
            &cfg,
            &handle,
            &drops,
            &write_tx,
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
        // The write channel is unused here (no frames injected), but `drive`
        // requires it (issue #317); a held receiver keeps it open.
        let (write_tx, _write_rx) = mpsc::channel::<WriteCmd>(64);
        let drive_fut = drive(
            &db,
            &mut rx,
            &mut state,
            &gamma,
            &mut refresh,
            &cfg,
            &handle,
            &drops,
            &write_tx,
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
