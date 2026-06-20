#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! Scenario: liquidity-at-fill capture worker (issue #350 WS2 PR-H) end-to-end through
//! deterministic fixtures — no network, no clock, no RNG. Drives the public surface
//! (`pe_service::snapshot_worker::*`) the orchestrator wires after a BUY fill:
//!
//! - AC-FULL — `/book` success ⇒ a full SQLite row (Gamma scalars + `absorbable_usd_100bps`
//!   + raw asks); the Supabase mirror is invoked once.
//! - AC-PARTIAL — `/book` failure (absent token) ⇒ a partial row (Gamma scalars only).
//! - AC-SELL — a SELL fill enqueues nothing.
//! - AC-BLOCK — a full channel drops the request without blocking; the dropped counter
//!   advances and the queued request is retained (the fill path never blocks).
//! - AC-LOOP — `run_snapshot_worker` drains every queued request and exits on close.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use pe_core_types::{MarketId, OutcomeId, Side};
use pe_paper_state::{FillMarketSnapshot, PaperStateDb, SettledMarketRow};
use pe_service::clob_book::{BookLevel, FixtureClobBookFetcher, OrderBook};
use pe_service::mid_price_cache::MidPriceCache;
use pe_service::snapshot_worker::{
    SnapshotHandle, SnapshotRequest, capture_snapshot, enqueue_if_buy, run_snapshot_worker,
};
use pe_service::supabase_sink::{SinkError, SinkWriter, SupabaseFillRow};
use pe_source_polymarket_public::FixtureFetcher;
use rust_decimal_macros::dec;

const GAMMA_BASE: &str = "https://gamma-api.polymarket.com";

fn mid(s: &str) -> MarketId {
    s.parse().unwrap()
}

/// A mid cache whose `0xcond` market carries Gamma `liquidity`/`volume` and outcome-ordered
/// `clobTokenIds` `["111", "222"]` (so outcome 1 → token `222`).
fn snapshot_cache() -> MidPriceCache<FixtureFetcher> {
    let mut fx = HashMap::new();
    fx.insert(
        format!("{GAMMA_BASE}/markets?condition_ids=0xcond&limit=500"),
        br#"[{"conditionId":"0xcond","outcomePrices":"[\"0.62\",\"0.38\"]","liquidity":"6434.84","volume":"99995.018095","clobTokenIds":"[\"111\",\"222\"]"}]"#.to_vec(),
    );
    MidPriceCache::with_fetcher(FixtureFetcher::new(fx), GAMMA_BASE.to_string())
}

/// A `/book` for token `222`: best ask 0.40 → 100-bps ceiling 0.404. Levels at 0.40 and 0.404
/// count (40.0 + 20.2 = 60.2 USD); the 0.41 level is outside the band.
fn books_for_222() -> HashMap<String, OrderBook> {
    let mut books = HashMap::new();
    books.insert(
        "222".to_string(),
        OrderBook {
            asks: vec![
                BookLevel {
                    price: dec!(0.40),
                    size: dec!(100),
                },
                BookLevel {
                    price: dec!(0.404),
                    size: dec!(50),
                },
                BookLevel {
                    price: dec!(0.41),
                    size: dec!(1000),
                },
            ],
        },
    );
    books
}

/// Records every `upsert_snapshot` mirror call; all other sink methods are inert.
#[derive(Default)]
struct RecordingWriter {
    snapshots: std::sync::Mutex<Vec<String>>,
}

impl SinkWriter for RecordingWriter {
    async fn upsert_fill(&self, _row: &SupabaseFillRow) -> Result<(), SinkError> {
        Ok(())
    }
    async fn upsert_settled(&self, _row: &SettledMarketRow) -> Result<(), SinkError> {
        Ok(())
    }
    async fn upsert_snapshot(&self, row: &FillMarketSnapshot) -> Result<(), SinkError> {
        self.snapshots
            .lock()
            .unwrap()
            .push(row.idempotency_key.clone());
        Ok(())
    }
    async fn read_hwm(&self) -> Result<i64, SinkError> {
        Ok(0)
    }
    async fn write_hwm(&self, _last_event_seq: i64) -> Result<(), SinkError> {
        Ok(())
    }
}

// PASS: a /book success writes one full SQLite row (Gamma scalars + absorbable 60.2 + asks)
//       and invokes the Supabase mirror exactly once with the same idempotency key.
// FAIL: missing/empty row, wrong absorbable, null absorbable/asks, or mirror not invoked.
#[tokio::test]
async fn ac_full_writes_full_row_and_mirrors() {
    let dir = tempfile::tempdir().unwrap();
    let db = PaperStateDb::open(&dir.path().join("paper_state.db")).unwrap();
    let cache = snapshot_cache();
    let fetcher = FixtureClobBookFetcher::new(books_for_222());
    let writer = RecordingWriter::default();

    let req = SnapshotRequest {
        idempotency_key: "wf|leader|src|0xcond|1|buy|0".to_string(),
        market_id: mid("0xcond"),
        outcome_id: OutcomeId(1),
        captured_at_unix: 1_700_000_000,
    };
    capture_snapshot(&req, &cache, &fetcher, &db, Some(&writer))
        .await
        .unwrap();

    let rows = db.list_fill_snapshots().unwrap();
    assert_eq!(rows.len(), 1, "exactly one snapshot row");
    let row = &rows[0];
    assert_eq!(row.idempotency_key, req.idempotency_key);
    assert_eq!(row.liquidity, Some(dec!(6434.84)));
    assert_eq!(row.volume, Some(dec!(99995.018095)));
    assert_eq!(
        row.absorbable_usd_100bps,
        Some(dec!(60.2)),
        "Σ price·size within 100 bps of best ask"
    );
    assert!(
        row.ask_levels_json.is_some(),
        "raw asks captured on success"
    );
    assert_eq!(row.captured_at_unix, 1_700_000_000);
    assert_eq!(
        writer.snapshots.lock().unwrap().as_slice(),
        std::slice::from_ref(&req.idempotency_key),
        "Supabase mirror invoked once with the same key"
    );
}

// PASS: an absent CLOB token (book fetch fails) writes a partial row — Gamma scalars present,
//       absorbable + asks null.
// FAIL: no row, or absorbable/asks populated, or Gamma scalars dropped.
#[tokio::test]
async fn ac_partial_book_failure_yields_gamma_only_row() {
    let dir = tempfile::tempdir().unwrap();
    let db = PaperStateDb::open(&dir.path().join("paper_state.db")).unwrap();
    let cache = snapshot_cache();
    // No book configured for token 222 → MissingFixture → partial row.
    let fetcher = FixtureClobBookFetcher::new(HashMap::new());

    let req = SnapshotRequest {
        idempotency_key: "wf|leader|src|0xcond|1|buy|0".to_string(),
        market_id: mid("0xcond"),
        outcome_id: OutcomeId(1),
        captured_at_unix: 1_700_000_001,
    };
    capture_snapshot(&req, &cache, &fetcher, &db, None::<&RecordingWriter>)
        .await
        .unwrap();

    let rows = db.list_fill_snapshots().unwrap();
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row.liquidity, Some(dec!(6434.84)), "Gamma scalar survives");
    assert_eq!(
        row.volume,
        Some(dec!(99995.018095)),
        "Gamma scalar survives"
    );
    assert_eq!(row.absorbable_usd_100bps, None, "partial: no absorbable");
    assert_eq!(row.ask_levels_json, None, "partial: no raw asks");
}

// PASS: a SELL fill enqueues nothing.
// FAIL: any request lands on the channel.
#[tokio::test]
async fn ac_sell_enqueues_nothing() {
    let (handle, mut rx) = SnapshotHandle::channel(8);
    enqueue_if_buy(
        Some(&handle),
        Side::Sell,
        "k",
        &mid("0xcond"),
        OutcomeId(0),
        1,
    );
    assert!(rx.try_recv().is_err(), "a SELL must enqueue no snapshot");
}

// PASS: a full channel drops the over-send (dropped counter = 1) without blocking; the queued
//       request is retained — proving the fill path never blocks on a saturated channel.
// FAIL: a block/panic, a wrong dropped count, or the retained request lost.
#[tokio::test]
async fn ac_block_full_channel_drops_without_blocking() {
    let (handle, mut rx) = SnapshotHandle::channel(1);
    let dropped = handle.dropped_counter();
    enqueue_if_buy(
        Some(&handle),
        Side::Buy,
        "k1",
        &mid("0xcond"),
        OutcomeId(1),
        1,
    );
    // Channel is now full; the second BUY must drop rather than block.
    enqueue_if_buy(
        Some(&handle),
        Side::Buy,
        "k2",
        &mid("0xcond"),
        OutcomeId(1),
        2,
    );
    assert_eq!(
        dropped.load(Ordering::Relaxed),
        1,
        "the over-send was dropped"
    );
    let first = rx.try_recv().expect("the queued request is retained");
    assert_eq!(first.idempotency_key, "k1");
    assert!(rx.try_recv().is_err(), "only the retained request remains");
}

// PASS: the worker drains every queued request (full + partial) and exits when the channel
//       closes — two rows persisted.
// FAIL: missing rows, or the worker hangs after the channel closes.
#[tokio::test]
async fn ac_loop_drains_all_then_exits_on_close() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(PaperStateDb::open(&dir.path().join("paper_state.db")).unwrap());
    let cache = snapshot_cache();
    let (handle, rx) = SnapshotHandle::channel(8);
    let dropped = handle.dropped_counter();

    // outcome 1 → token 222 (book present → full); outcome 0 → token 111 (absent → partial).
    handle.send(SnapshotRequest {
        idempotency_key: "wf|a".to_string(),
        market_id: mid("0xcond"),
        outcome_id: OutcomeId(1),
        captured_at_unix: 10,
    });
    handle.send(SnapshotRequest {
        idempotency_key: "wf|b".to_string(),
        market_id: mid("0xcond"),
        outcome_id: OutcomeId(0),
        captured_at_unix: 11,
    });
    drop(handle); // close the channel so the worker returns after draining.

    run_snapshot_worker(
        rx,
        cache,
        FixtureClobBookFetcher::new(books_for_222()),
        db.clone(),
        None::<RecordingWriter>,
        dropped,
    )
    .await;

    let mut keys: Vec<String> = db
        .list_fill_snapshots()
        .unwrap()
        .into_iter()
        .map(|r| r.idempotency_key)
        .collect();
    keys.sort();
    assert_eq!(keys, vec!["wf|a".to_string(), "wf|b".to_string()]);
}
