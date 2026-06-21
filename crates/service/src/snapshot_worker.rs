//! Off-hot-path liquidity-at-fill capture worker (issue #350 WS2 PR-H).
//!
//! After a BUY paper fill commits, the orchestrator enqueues a [`SnapshotRequest`] onto a
//! bounded, drop-on-full channel ([`SnapshotHandle`]); [`run_snapshot_worker`] drains it and
//! performs the ~200 ms Gamma + CLOB `/book` I/O **off the trade hot path**. It derives
//! [`absorbable_usd_within_bps`] from the ask side, then writes a `fill_market_snapshots` row
//! to SQLite (canonical) plus a best-effort Supabase mirror.
//!
//! The fill path never blocks or changes: a full channel drops the request (capture is
//! best-effort analytics), and a `/book` failure (or a missing token) yields a **partial**
//! row carrying only the Gamma scalars. Buy-only — a SELL fill enqueues nothing.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pe_core_types::{MarketId, OutcomeId, Side};
use pe_paper_state::{FillMarketSnapshot, PaperStateDb, PaperStateError};
use pe_source_polymarket_public::PageFetcher;
use rust_decimal::Decimal;
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::clob_book::{ClobBookFetcher, OrderBook};
use crate::mid_price_cache::MidPriceCache;
use crate::supabase_sink::SinkWriter;

/// Ask-depth band for `absorbable_usd_100bps`: ask levels priced within this many basis
/// points of the best (lowest) ask are summed (Σ price·size). 100 bps = 1 %. Baked into the
/// column name `absorbable_usd_100bps`, so it is a fixed constant, not an operator knob.
/// Registered in `docs/_GLOSSARY.md` (`absorbable_depth_bps`).
pub const ABSORBABLE_DEPTH_BPS: u64 = 100;

/// Basis-point denominator (`10_000` bps = 100 %).
const BPS_DENOMINATOR: u64 = 10_000;

/// A committed BUY fill awaiting an off-path liquidity snapshot.
#[derive(Debug, Clone)]
pub struct SnapshotRequest {
    /// `fills(idempotency_key)` of the BUY fill this snapshot describes. The relationship is
    /// documentary — the sidecar declares no SQL foreign key (see PR-F).
    pub idempotency_key: String,
    /// Market whose Gamma snapshot + CLOB book is captured.
    pub market_id: MarketId,
    /// Filled outcome; indexes `clob_token_ids[outcome_id]` for the `/book` target.
    pub outcome_id: OutcomeId,
    /// Capture time (Unix seconds), stamped at enqueue — i.e. fill time.
    pub captured_at_unix: i64,
}

/// Cheap, clonable handle the trade path uses to enqueue a [`SnapshotRequest`] without ever
/// blocking. Mirrors [`crate::supabase_sink::SinkHandle`]: drop-on-full + a dropped counter.
#[derive(Clone)]
pub struct SnapshotHandle {
    tx: mpsc::Sender<SnapshotRequest>,
    dropped: Arc<AtomicU64>,
}

impl SnapshotHandle {
    /// Build a handle + its receiver with a bounded channel of `capacity`.
    #[must_use]
    pub fn channel(capacity: usize) -> (SnapshotHandle, mpsc::Receiver<SnapshotRequest>) {
        let (tx, rx) = mpsc::channel(capacity);
        (
            SnapshotHandle {
                tx,
                dropped: Arc::new(AtomicU64::new(0)),
            },
            rx,
        )
    }

    /// The shared dropped-request counter, handed to [`run_snapshot_worker`] for logging.
    #[must_use]
    pub fn dropped_counter(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.dropped)
    }

    /// Enqueue a request. Non-blocking: on a full/closed channel the request is dropped and
    /// the dropped-counter incremented. Never errors, never blocks, never panics — safe to
    /// call from the trade hot path.
    pub fn send(&self, req: SnapshotRequest) {
        if self.tx.try_send(req).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Buy-only enqueue seam (capture is buy-only per issue #350 WS2). Enqueues a snapshot request
/// iff `side` is [`Side::Buy`] **and** a `handle` is configured; a SELL — or a `None` handle
/// (capture disabled) — enqueues nothing. Non-blocking (drop-on-full via [`SnapshotHandle`]).
pub fn enqueue_if_buy(
    handle: Option<&SnapshotHandle>,
    side: Side,
    idempotency_key: &str,
    market_id: &MarketId,
    outcome_id: OutcomeId,
    captured_at_unix: i64,
) {
    let Some(handle) = handle else { return };
    if side != Side::Buy {
        return;
    }
    handle.send(SnapshotRequest {
        idempotency_key: idempotency_key.to_string(),
        market_id: market_id.clone(),
        outcome_id,
        captured_at_unix,
    });
}

/// Σ price·size over ask levels priced within `bps` of the best (lowest) ask — the USD
/// notional absorbable without moving the price more than `bps`.
///
/// Returns `None` when the book has no asks (no best ask to anchor the band), or when the
/// running total overflows `Decimal` (capture is best-effort — a corrupt sum is never
/// persisted; the row degrades to a partial instead).
#[must_use]
pub fn absorbable_usd_within_bps(book: &OrderBook, bps: u64) -> Option<Decimal> {
    let best = book.best_ask()?;
    // ceiling = best * (BPS_DENOMINATOR + bps) / BPS_DENOMINATOR  (best * 1.01 at 100 bps).
    let numerator = Decimal::from(BPS_DENOMINATOR.checked_add(bps)?);
    let ceiling = best
        .checked_mul(numerator)?
        .checked_div(Decimal::from(BPS_DENOMINATOR))?;
    let mut total = Decimal::ZERO;
    for level in &book.asks {
        if level.price <= ceiling {
            total = total.checked_add(level.price.checked_mul(level.size)?)?;
        }
    }
    Some(total)
}

/// Σ size (contract count) over ask levels priced within `bps` of the best (lowest) ask — the
/// number of contracts absorbable without moving the price more than `bps`. Preferred over
/// `absorbable_usd_within_bps / fill_price` (which blends levels at differing prices) for the
/// price-impact book cap (#398 WS2). Returns `None` when the book has no asks or the running sum
/// overflows `Decimal`.
#[must_use]
pub fn absorbable_contracts_within_bps(book: &OrderBook, bps: u64) -> Option<Decimal> {
    let best = book.best_ask()?;
    let numerator = Decimal::from(BPS_DENOMINATOR.checked_add(bps)?);
    let ceiling = best
        .checked_mul(numerator)?
        .checked_div(Decimal::from(BPS_DENOMINATOR))?;
    let mut total = Decimal::ZERO;
    for level in &book.asks {
        if level.price <= ceiling {
            total = total.checked_add(level.size)?;
        }
    }
    Some(total)
}

/// Serialize the ask side as a compact JSON array of `{price, size}` string-decimals for
/// ad-hoc capacity queries. `None` only on a serialization failure (unreachable for this
/// shape — the worker degrades rather than panics).
fn ask_levels_json(book: &OrderBook) -> Option<String> {
    let levels: Vec<serde_json::Value> = book
        .asks
        .iter()
        .map(|level| {
            serde_json::json!({
                "price": level.price.to_string(),
                "size": level.size.to_string(),
            })
        })
        .collect();
    serde_json::to_string(&levels).ok()
}

/// Capture one fill's market snapshot off the trade hot path: Gamma `liquidity`/`volume` plus
/// the filled outcome's CLOB `/book` depth (`absorbable_usd_100bps` + raw `ask_levels_json`).
///
/// Best-effort: a `/book` failure — or an absent CLOB token — yields a **partial** row (Gamma
/// scalars only). The canonical SQLite row is always written; the Supabase mirror, when
/// configured, is best-effort (a failed mirror is logged, not propagated).
///
/// # Errors
/// Returns [`PaperStateError`] only when the canonical SQLite write fails.
pub async fn capture_snapshot<F, B, W>(
    req: &SnapshotRequest,
    mid_cache: &MidPriceCache<F>,
    book_fetcher: &B,
    paper_state: &PaperStateDb,
    sink: Option<&W>,
) -> Result<(), PaperStateError>
where
    F: PageFetcher + Send + Sync,
    B: ClobBookFetcher,
    W: SinkWriter,
{
    // Gamma scalars + outcome-ordered CLOB token ids (served from the TTL cache when fresh).
    let snapshots = mid_cache
        .fetch_snapshots(std::slice::from_ref(&req.market_id))
        .await;
    let gamma = snapshots.get(&req.market_id);
    let liquidity = gamma.and_then(|s| s.liquidity);
    let volume = gamma.and_then(|s| s.volume);
    let token = gamma.and_then(|s| s.clob_token_ids.get(usize::from(req.outcome_id.0)).cloned());

    // CLOB /book → absorbable depth + raw asks. Best-effort: failure/absent token ⇒ partial.
    let (absorbable_usd_100bps, ask_levels_json) = match token.as_deref() {
        Some(token_id) => match book_fetcher.fetch_book(token_id).await {
            Ok(book) => (
                absorbable_usd_within_bps(&book, ABSORBABLE_DEPTH_BPS),
                ask_levels_json(&book),
            ),
            Err(e) => {
                warn!(
                    key = %req.idempotency_key,
                    token_id,
                    error = %e,
                    "snapshot worker: /book fetch failed; writing partial Gamma-only row"
                );
                (None, None)
            }
        },
        None => (None, None),
    };

    let row = FillMarketSnapshot {
        idempotency_key: req.idempotency_key.clone(),
        liquidity,
        volume,
        absorbable_usd_100bps,
        ask_levels_json,
        captured_at_unix: req.captured_at_unix,
    };

    // Canonical write (always). A failure here is the only propagated error.
    paper_state.upsert_fill_market_snapshot(&row)?;

    // Best-effort Supabase mirror — the canonical row is already durable.
    if let Some(sink) = sink
        && let Err(e) = sink.upsert_snapshot(&row).await
    {
        warn!(
            key = %row.idempotency_key,
            error = %e,
            "snapshot worker: supabase mirror failed (canonical row persisted)"
        );
    }
    Ok(())
}

/// Drain [`SnapshotRequest`]s, capturing each fill's market snapshot off the hot path. Exits
/// when the channel closes (all [`SnapshotHandle`]s dropped). A canonical-write failure is
/// logged and the worker continues; dropped-on-full requests are surfaced via `dropped`.
pub async fn run_snapshot_worker<F, B, W>(
    mut rx: mpsc::Receiver<SnapshotRequest>,
    mid_cache: MidPriceCache<F>,
    book_fetcher: Arc<B>,
    paper_state: Arc<PaperStateDb>,
    sink: Option<W>,
    dropped: Arc<AtomicU64>,
) where
    F: PageFetcher + Send + Sync,
    B: ClobBookFetcher,
    W: SinkWriter,
{
    while let Some(req) = rx.recv().await {
        let n = dropped.swap(0, Ordering::Relaxed);
        if n > 0 {
            warn!(
                dropped = n,
                "snapshot worker: requests dropped (channel full)"
            );
        }
        if let Err(e) = capture_snapshot(
            &req,
            &mid_cache,
            book_fetcher.as_ref(),
            paper_state.as_ref(),
            sink.as_ref(),
        )
        .await
        {
            warn!(
                key = %req.idempotency_key,
                error = %e,
                "snapshot worker: canonical snapshot write failed"
            );
        }
    }
    info!("snapshot worker: request channel closed; shutting down");
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::clob_book::BookLevel;
    use rust_decimal_macros::dec;

    fn book(asks: &[(Decimal, Decimal)]) -> OrderBook {
        OrderBook {
            asks: asks
                .iter()
                .map(|&(price, size)| BookLevel { price, size })
                .collect(),
        }
    }

    #[test]
    fn absorbable_sums_only_levels_within_band() {
        // best ask = 0.50 → ceiling = 0.505. Levels at 0.50 and 0.505 count; 0.51 excluded.
        let b = book(&[
            (dec!(0.50), dec!(100)), // 50.0
            (dec!(0.505), dec!(40)), // 20.2
            (dec!(0.51), dec!(1000)),
        ]);
        assert_eq!(absorbable_usd_within_bps(&b, 100), Some(dec!(70.200)));
    }

    #[test]
    fn absorbable_empty_book_is_none() {
        assert_eq!(absorbable_usd_within_bps(&book(&[]), 100), None);
        assert_eq!(absorbable_contracts_within_bps(&book(&[]), 100), None);
    }

    #[test]
    fn absorbable_contracts_sums_sizes_within_band() {
        // best ask = 0.50 → ceiling = 0.505. Sizes at 0.50 and 0.505 count (140); 0.51 excluded.
        // Contract count, NOT USD: the price-impact book cap (#398 WS2) caps the contract size.
        let b = book(&[
            (dec!(0.50), dec!(100)),
            (dec!(0.505), dec!(40)),
            (dec!(0.51), dec!(1000)),
        ]);
        assert_eq!(absorbable_contracts_within_bps(&b, 100), Some(dec!(140)));
    }

    #[test]
    fn absorbable_contracts_zero_when_no_level_within_band() {
        // best 0.50 → ceiling 0.505; the only other level (0.60) is outside the band → just the
        // best level's size. With a tiny band (1 bps → ceiling 0.50005) only the best counts.
        let b = book(&[(dec!(0.50), dec!(7)), (dec!(0.60), dec!(1000))]);
        assert_eq!(absorbable_contracts_within_bps(&b, 1), Some(dec!(7)));
    }

    #[test]
    fn absorbable_band_is_inclusive_of_exact_ceiling() {
        // best 0.40 → ceiling 0.404; a level exactly at 0.404 is included.
        let b = book(&[(dec!(0.40), dec!(10)), (dec!(0.404), dec!(10))]);
        assert_eq!(absorbable_usd_within_bps(&b, 100), Some(dec!(8.04)));
    }

    #[test]
    fn enqueue_if_buy_drops_sell() {
        let (handle, mut rx) = SnapshotHandle::channel(4);
        let market: MarketId = "0xcond".parse().unwrap();
        enqueue_if_buy(Some(&handle), Side::Sell, "k", &market, OutcomeId(0), 1);
        assert!(rx.try_recv().is_err(), "a SELL must enqueue nothing");
    }

    #[test]
    fn enqueue_if_buy_enqueues_buy() {
        let (handle, mut rx) = SnapshotHandle::channel(4);
        let market: MarketId = "0xcond".parse().unwrap();
        enqueue_if_buy(Some(&handle), Side::Buy, "k1", &market, OutcomeId(1), 42);
        let req = rx.try_recv().expect("a BUY must enqueue one request");
        assert_eq!(req.idempotency_key, "k1");
        assert_eq!(req.outcome_id, OutcomeId(1));
        assert_eq!(req.captured_at_unix, 42);
    }

    #[test]
    fn enqueue_if_buy_none_handle_is_noop() {
        let market: MarketId = "0xcond".parse().unwrap();
        // No panic, no effect when capture is disabled.
        enqueue_if_buy(None, Side::Buy, "k", &market, OutcomeId(0), 1);
    }
}
