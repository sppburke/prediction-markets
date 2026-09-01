//! Scenario tests for the Supabase analytics sink (issue #343 PR2).
//!
//! Drives the sink's reconcile + parse logic with an in-memory [`FakeWriter`] (no live
//! network, deterministic failure injection) over a tempfile-backed [`PaperStateDb`].
//!
//! Scenarios:
//!   AC-HWM   — fill catch-up advances the contiguous-prefix HWM along the ordered
//!              `list_fills()` stream and **halts at the first failed write**, leaving the
//!              tail for the next reconcile.
//!   AC-DEDUP — re-running catch-up from the advanced HWM writes nothing new (idempotent;
//!              no dupes / no gaps).
//!   AC-SKIP  — a non-`wf|` fill (no leader) is skipped, never written, and the HWM
//!              advances past it.
//!   AC-BLOCK — `SinkHandle::send_fill` on a full channel drops + counts, never blocking
//!              the trade path.
//!   AC-HEAL  — the periodic settled reconcile re-upserts the full settled set each pass,
//!              healing any dropped live resolution.
//!
//! Run with: cargo nextest run -p pe-service --features scenario

#![cfg(feature = "scenario")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::arithmetic_side_effects
)]

use std::sync::Mutex;
use std::sync::atomic::Ordering;

use pe_core_types::{EventSeq, MarketId, OutcomeId, Price, Side, SourceTradeId, VenueMarketId};
use pe_paper_state::{
    FillMarketSnapshot, FillRecord, FillRow, LeaderPositionRow, PaperStateDb, SettledMarketRow,
};
use pe_service::supabase_sink::{
    SinkError, SinkHandle, SinkWriter, SupabaseFillRow, reconcile_fills, reconcile_settled,
    supabase_fill_from,
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tempfile::TempDir;

fn wallet_hex() -> &'static str {
    "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
}

/// In-memory [`SinkWriter`]: records every confirmed upsert; optionally fails `upsert_fill`
/// for one `event_seq` to exercise the halt-at-first-failure path. Deterministic.
#[derive(Default)]
struct FakeWriter {
    fills: Mutex<Vec<i64>>,
    settled: Mutex<Vec<String>>,
    hwm: Mutex<i64>,
    fail_on_seq: Option<i64>,
}

impl SinkWriter for FakeWriter {
    async fn upsert_fill(&self, row: &SupabaseFillRow) -> Result<(), SinkError> {
        if Some(row.fill.event_seq) == self.fail_on_seq {
            return Err(SinkError::Status(503));
        }
        self.fills.lock().unwrap().push(row.fill.event_seq);
        Ok(())
    }
    async fn upsert_settled(&self, row: &SettledMarketRow) -> Result<(), SinkError> {
        self.settled.lock().unwrap().push(row.market_id.0.0.clone());
        Ok(())
    }
    async fn upsert_snapshot(&self, _row: &FillMarketSnapshot) -> Result<(), SinkError> {
        Ok(())
    }
    async fn read_hwm(&self) -> Result<i64, SinkError> {
        Ok(*self.hwm.lock().unwrap())
    }
    async fn write_hwm(&self, last_event_seq: i64) -> Result<(), SinkError> {
        *self.hwm.lock().unwrap() = last_event_seq;
        Ok(())
    }
}

fn market(id: &str) -> MarketId {
    MarketId(VenueMarketId(id.to_string()))
}

/// Seed a tempfile-backed `PaperStateDb` with fills at the given `(event_seq, key)` pairs.
fn seed_fills(specs: &[(u64, &str)]) -> (TempDir, PaperStateDb) {
    let dir = tempfile::tempdir().unwrap();
    let db = PaperStateDb::open(&dir.path().join("paper_state.db")).unwrap();
    db.init_bankroll(Decimal::from(100_000u32)).unwrap();
    for (i, (seq, key)) in specs.iter().enumerate() {
        let record = FillRecord {
            idempotency_key: (*key).to_string(),
            market_id: market("0xmkt"),
            outcome_id: OutcomeId(0),
            side: Side::Buy,
            contracts: 10,
            fill_price: Price(dec!(0.40)),
        };
        let leader = LeaderPositionRow {
            wallet: pe_core_types::WalletAddress::from_hex(wallet_hex()).unwrap(),
            market_id: market("0xmkt"),
            outcome_id: OutcomeId(0),
            long_contracts: pe_core_types::ShareAmount::ZERO,
            short_contracts: pe_core_types::ShareAmount::ZERO,
        };
        db.commit_fill(
            &SourceTradeId(format!("src{i}")),
            &leader,
            &record,
            EventSeq(*seq),
        )
        .unwrap();
    }
    (dir, db)
}

fn wf_key(seq: u64) -> String {
    format!("wf|{}|s{seq}|0xmkt|0|buy|17000000{seq:02}", wallet_hex())
}

#[tokio::test]
async fn ac_hwm_advances_prefix_and_halts_at_first_failure() {
    // Sparse event_seq stream [5, 9, 14]; the writer fails on 9.
    let (_dir, db) = seed_fills(&[(5, &wf_key(5)), (9, &wf_key(9)), (14, &wf_key(14))]);
    let writer = FakeWriter {
        fail_on_seq: Some(9),
        ..Default::default()
    };

    let (new_hwm, _complete) = reconcile_fills(&writer, &db, 0).await.unwrap();

    // PASS: HWM advances only over the confirmed prefix (5) and halts at the first failure
    //       (9); 14 is left for the next reconcile. Not integer adjacency.
    assert_eq!(*writer.fills.lock().unwrap(), vec![5]);
    assert_eq!(new_hwm, 5);
    println!("PASS: AC-HWM — wrote [5], halted at 9, HWM=5 (14 deferred)");
}

#[tokio::test]
async fn ac_dedup_reconcile_is_idempotent() {
    let (_dir, db) = seed_fills(&[(5, &wf_key(5)), (9, &wf_key(9)), (14, &wf_key(14))]);
    let writer = FakeWriter::default();

    let (hwm1, _c1) = reconcile_fills(&writer, &db, 0).await.unwrap();
    let (hwm2, _c2) = reconcile_fills(&writer, &db, hwm1).await.unwrap();

    // PASS: first pass writes all 3 and advances to 14; re-running from 14 writes nothing
    //       new (no dupes, no gaps).
    assert_eq!(*writer.fills.lock().unwrap(), vec![5, 9, 14]);
    assert_eq!((hwm1, hwm2), (14, 14));
    println!("PASS: AC-DEDUP — wrote [5,9,14] once, re-run from HWM=14 wrote nothing");
}

#[tokio::test]
async fn ac_skip_non_wf_fill_and_advance() {
    // A non-`wf|` key has no leader → must be skipped, never written.
    let non_wf = FillRow {
        idempotency_key: "legacy|nokey".to_string(),
        market_id: market("0xmkt"),
        outcome_id: OutcomeId(0),
        side: Side::Buy,
        contracts: 10,
        fill_price: Price(dec!(0.40)),
        event_seq: 9,
    };
    assert!(supabase_fill_from(&non_wf).is_none());

    let (_dir, db) = seed_fills(&[(5, &wf_key(5)), (9, "legacy|nokey")]);
    let writer = FakeWriter::default();

    let (new_hwm, _complete) = reconcile_fills(&writer, &db, 0).await.unwrap();

    // PASS: only the wf fill (5) is written; the non-wf fill (9) is skipped and the HWM
    //       advances past it so it is never re-examined.
    assert_eq!(*writer.fills.lock().unwrap(), vec![5]);
    assert_eq!(new_hwm, 9);
    println!("PASS: AC-SKIP — non-wf fill skipped (wrote [5]), HWM advanced past it to 9");
}

#[tokio::test]
async fn ac_send_fill_never_blocks_on_full_channel() {
    // Capacity-1 channel with no consumer draining it.
    let (handle, _rx) = SinkHandle::channel(1);
    let dropped = handle.dropped_counter();
    let row = || FillRow {
        idempotency_key: wf_key(1),
        market_id: market("0xmkt"),
        outcome_id: OutcomeId(0),
        side: Side::Buy,
        contracts: 10,
        fill_price: Price(dec!(0.40)),
        event_seq: 1,
    };

    handle.send_fill(row()); // buffers into the single slot
    handle.send_fill(row()); // full → dropped, must not block/panic
    handle.send_fill(row()); // full → dropped

    // PASS: the trade path never blocks — overflow is dropped and counted.
    assert_eq!(dropped.load(Ordering::Relaxed), 2);
    println!("PASS: AC-BLOCK — send_fill dropped 2 on full channel, never blocked");
}

#[tokio::test]
async fn ac_settled_reconcile_reupserts_full_set() {
    let dir = tempfile::tempdir().unwrap();
    let db = PaperStateDb::open(&dir.path().join("paper_state.db")).unwrap();
    db.record_settled_market(&market("0xm1"), "[\"1\",\"0\"]", dec!(100), 1_700_000_000)
        .unwrap();
    db.record_settled_market(&market("0xm2"), "[\"0\",\"1\"]", dec!(0), 1_700_000_001)
        .unwrap();
    let writer = FakeWriter::default();

    reconcile_settled(&writer, &db).await.unwrap();
    reconcile_settled(&writer, &db).await.unwrap();

    // PASS: each reconcile re-upserts the full settled set (the heal mechanism); the
    //       writer's upsert is idempotent on market_id, so re-upserting is safe.
    assert_eq!(writer.settled.lock().unwrap().len(), 4);
    println!("PASS: AC-HEAL — settled reconcile re-upserted the full set on each of 2 passes");
}

/// #510: the backfill sweeps from `-1` so fill seq 0 is included — with the sink-HWM `0` it
/// is structurally skipped (the `> hwm` filter), the defect this pins.
/// PASS: hwm=-1 sends [0, 5]; hwm=0 sends only [5] (the regression control).
#[tokio::test]
async fn ac_510_full_sweep_from_minus_one_includes_seq_zero() {
    let (_dir, db) = seed_fills(&[(0, &wf_key(0)), (5, &wf_key(5))]);
    let full = FakeWriter::default();
    let (hwm, complete) = reconcile_fills(&full, &db, -1).await.unwrap();
    assert_eq!((hwm, complete), (5, true));
    assert_eq!(
        *full.fills.lock().unwrap(),
        vec![0, 5],
        "-1 sweep includes seq 0"
    );

    let partial = FakeWriter::default();
    let (hwm0, _c) = reconcile_fills(&partial, &db, 0).await.unwrap();
    assert_eq!(hwm0, 5);
    assert_eq!(
        *partial.fills.lock().unwrap(),
        vec![5],
        "hwm=0 skips seq 0 — why -1 is passed"
    );
    println!("PASS: AC-510-SWEEP — -1 includes seq 0; 0 skips it");
}

/// #510: `reconcile_fills` reports completion so the one-time backfill can refuse to seed
/// cursors on a partial sweep (a halted sweep returning Ok used to look complete).
/// PASS: an injected mid-sweep failure yields complete=false; the rerun completes.
#[tokio::test]
async fn ac_510_incomplete_sweep_is_reported_and_rerun_completes() {
    let (_dir, db) = seed_fills(&[(0, &wf_key(0)), (1, &wf_key(1)), (2, &wf_key(2))]);
    let failing = FakeWriter {
        fail_on_seq: Some(1),
        ..Default::default()
    };
    let (hwm, complete) = reconcile_fills(&failing, &db, -1).await.unwrap();
    assert_eq!(
        (hwm, complete),
        (0, false),
        "halt at 1 → incomplete, prefix hwm 0"
    );

    // Rerun (idempotent, resumable): a healthy writer completes from the prefix.
    let healthy = FakeWriter::default();
    let (hwm2, complete2) = reconcile_fills(&healthy, &db, hwm).await.unwrap();
    assert_eq!((hwm2, complete2), (2, true));
    assert_eq!(*healthy.fills.lock().unwrap(), vec![1, 2]);
    println!("PASS: AC-510-STRICT — partial sweep reported; rerun resumes and completes");
}
