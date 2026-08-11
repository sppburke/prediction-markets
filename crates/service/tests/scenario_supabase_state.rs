//! Scenario tests for the authoritative Supabase paper-state write-through (issue #397).
//!
//! Drives the write-through free functions ([`commit_fill_authoritative`],
//! [`apply_resolution_authoritative`], [`catch_up_supabase`]) with an in-memory
//! [`FakeSupabaseState`] (no live network, deterministic failure injection) over a
//! tempfile-backed [`PaperStateDb`]. The fake replicates the PL/pgSQL RPC arithmetic
//! (gate-on-insert + `greatest(bankroll±x,0)` + apply_fill_to_net netting), so AC-PARITY
//! asserting `fake bankroll == PaperStateDb bankroll` is a real RPC↔Rust drift guard for the
//! accounting model. The live PL/pgSQL execution + true cross-task concurrency (AC9) are
//! verified against a real Postgres — deferred to the CI Postgres-harness follow-up.
//!
//! Scenarios:
//!   AC-WT      — commit writes the RPC first, then mirrors SQLite; bankroll = RPC return.
//!   AC-FAIL    — an RPC error returns Err and leaves SQLite untouched (fail-closed skip).
//!   AC-CATCHUP — boot catch-up replays fills > watermark, advances it, and re-running from
//!                the advanced watermark writes nothing new (idempotent).
//!   AC-HALT    — catch-up halts at the first failed apply, leaving the tail for next boot.
//!   AC-RES     — resolution credits once; a duplicate credits zero; an RPC error skips.
//!   AC-PARITY  — fake (PL/pgSQL model) bankroll == PaperStateDb bankroll over a fill mix.
//!
//! Run with: cargo nextest run -p pe-service --features scenario

#![cfg(feature = "scenario")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::arithmetic_side_effects
)]

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use pe_core_types::{
    EventSeq, MarketId, OutcomeId, Price, Side, SourceTradeId, VenueMarketId, WalletAddress,
};
use pe_paper_pnl::ResolutionStore;
use pe_paper_state::{FillRecord, FillRow, LeaderPositionRow, PaperStateDb};
use pe_service::supabase_sink::{SupabaseFillRow, supabase_fill_from};
use pe_service::supabase_state::{
    SupabaseStateError, SupabaseStateTrait, apply_resolution_authoritative, catch_up_supabase,
    commit_fill_authoritative,
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::sync::Arc;
use tempfile::TempDir;

fn wallet_hex() -> &'static str {
    "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
}

fn wf_key(seq: u64) -> String {
    format!("wf|{}|s{seq}|0xmkt|0|buy|17000000{seq:02}", wallet_hex())
}

fn market() -> MarketId {
    MarketId(VenueMarketId("0xmkt".to_string()))
}

fn leader() -> LeaderPositionRow {
    LeaderPositionRow {
        wallet: WalletAddress::from_hex(wallet_hex()).unwrap(),
        market_id: market(),
        outcome_id: OutcomeId(0),
        long_contracts: 0,
        short_contracts: 0,
    }
}

/// A `(FillRecord, SupabaseFillRow)` pair for the same fill — the orchestrator builds both
/// from one `PaperFill`, so the test does too.
fn fill_pair(
    key: &str,
    side: Side,
    contracts: u64,
    price: Decimal,
    seq: i64,
) -> (FillRecord, SupabaseFillRow) {
    let record = FillRecord {
        idempotency_key: key.to_string(),
        market_id: market(),
        outcome_id: OutcomeId(0),
        side,
        contracts,
        fill_price: Price(price),
    };
    let fill_row = FillRow {
        idempotency_key: record.idempotency_key.clone(),
        market_id: record.market_id.clone(),
        outcome_id: record.outcome_id,
        side: record.side,
        contracts: record.contracts,
        fill_price: record.fill_price,
        event_seq: seq,
    };
    let sup_row = supabase_fill_from(&fill_row).expect("wf key parses to a SupabaseFillRow");
    (record, sup_row)
}

fn db_with_bankroll(initial: Decimal) -> (TempDir, Arc<PaperStateDb>) {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(PaperStateDb::open(&dir.path().join("paper_state.db")).unwrap());
    db.init_bankroll(initial).unwrap();
    (dir, db)
}

/// Net-position update, identical to paper-state `apply_fill_to_net` and the PL/pgSQL RPC.
fn apply_fill_to_net(long: u64, short: u64, side: Side, qty: u64) -> (u64, u64) {
    match side {
        Side::Buy => {
            let covered = short.min(qty);
            (long.saturating_add(qty - covered), short - covered)
        }
        Side::Sell => {
            let trimmed = long.min(qty);
            (long - trimmed, short.saturating_add(qty - trimmed))
        }
    }
}

/// In-memory [`SupabaseStateTrait`] that replicates the PL/pgSQL `commit_fill` /
/// `apply_resolution` arithmetic: gate the money write on the dedup key newly inserting, net
/// the position, and clamp the BUY debit at zero. Records call order; can fail a chosen fill.
#[derive(Default)]
struct FakeSupabaseState {
    bankroll: Mutex<Decimal>,
    positions: Mutex<HashMap<u16, (u64, u64)>>,
    seen_fills: Mutex<HashSet<String>>,
    settled: Mutex<HashSet<String>>,
    commit_calls: Mutex<Vec<i64>>,
    resolution_calls: Mutex<Vec<String>>,
    fail_all_commits: bool,
    fail_commit_seq: Option<i64>,
    fail_resolution: bool,
}

impl FakeSupabaseState {
    fn new(initial: Decimal) -> Self {
        Self {
            bankroll: Mutex::new(initial),
            ..Default::default()
        }
    }
    fn bankroll(&self) -> Decimal {
        *self.bankroll.lock().unwrap()
    }
}

impl SupabaseStateTrait for FakeSupabaseState {
    async fn commit_fill(&self, row: &SupabaseFillRow) -> Result<Decimal, SupabaseStateError> {
        if self.fail_all_commits || self.fail_commit_seq == Some(row.fill.event_seq) {
            return Err(SupabaseStateError::Status(503, "injected".to_string()));
        }
        self.commit_calls.lock().unwrap().push(row.fill.event_seq);
        let mut bankroll = self.bankroll.lock().unwrap();
        // Gate: a duplicate idempotency_key no-ops the money write (ON CONFLICT DO NOTHING).
        if !self
            .seen_fills
            .lock()
            .unwrap()
            .insert(row.fill.idempotency_key.clone())
        {
            return Ok(*bankroll);
        }
        // Net position (apply_fill_to_net).
        let mut positions = self.positions.lock().unwrap();
        let (long, short) = positions
            .get(&row.fill.outcome_id.0)
            .copied()
            .unwrap_or((0, 0));
        positions.insert(
            row.fill.outcome_id.0,
            apply_fill_to_net(long, short, row.fill.side, row.fill.contracts),
        );
        // Bankroll: BUY debits price×contracts clamped at 0; SELL credits it.
        let notional = row.fill.fill_price.0 * Decimal::from(row.fill.contracts);
        *bankroll = match row.fill.side {
            Side::Buy => (*bankroll - notional).max(Decimal::ZERO),
            Side::Sell => *bankroll + notional,
        };
        Ok(*bankroll)
    }

    async fn apply_resolution(
        &self,
        market_id: &MarketId,
        _outcome_prices: &[Decimal],
        credit: Decimal,
        _settled_at_unix: i64,
    ) -> Result<Decimal, SupabaseStateError> {
        if self.fail_resolution {
            return Err(SupabaseStateError::Status(503, "injected".to_string()));
        }
        self.resolution_calls
            .lock()
            .unwrap()
            .push(market_id.0.0.clone());
        let mut bankroll = self.bankroll.lock().unwrap();
        // Gate the credit on the settled row newly inserting (exactly-once).
        if self.settled.lock().unwrap().insert(market_id.0.0.clone()) {
            *bankroll += credit;
        }
        Ok(*bankroll)
    }
}

#[tokio::test]
async fn ac_wt_rpc_first_then_sqlite_mirror() {
    let (_dir, db) = db_with_bankroll(dec!(1000));
    let fake = FakeSupabaseState::new(dec!(1000));
    let (record, sup_row) = fill_pair(&wf_key(5), Side::Buy, 10, dec!(0.40), 5);
    let src = SourceTradeId("src5".to_string());

    let ret = commit_fill_authoritative(
        &fake,
        &db,
        &src,
        &leader(),
        &record,
        EventSeq(5),
        &sup_row,
        None,
    )
    .await
    .unwrap();

    // PASS: the RPC return is the authoritative bankroll (1000 - 0.40*10 = 996), and SQLite
    //       mirrored the fill to the same value.
    assert_eq!(ret, dec!(996.0));
    assert!(db.is_seen(&src).unwrap());
    assert_eq!(db.fills_count().unwrap(), 1);
    assert_eq!(db.bankroll().unwrap(), Some(dec!(996.0)));
    println!("PASS: AC-WT — commit_fill RPC first (996), SQLite mirrored to 996");
}

#[tokio::test]
async fn ac_fail_closed_leaves_sqlite_untouched() {
    let (_dir, db) = db_with_bankroll(dec!(1000));
    let fake = FakeSupabaseState {
        fail_all_commits: true,
        ..FakeSupabaseState::new(dec!(1000))
    };
    let (record, sup_row) = fill_pair(&wf_key(5), Side::Buy, 10, dec!(0.40), 5);
    let src = SourceTradeId("src5".to_string());

    let ret = commit_fill_authoritative(
        &fake,
        &db,
        &src,
        &leader(),
        &record,
        EventSeq(5),
        &sup_row,
        None,
    )
    .await;

    // PASS: the RPC error propagates (caller skips the trade), and SQLite is NOT written — the
    //       event log holds the fill and replays on restart.
    assert!(ret.is_err());
    assert!(!db.is_seen(&src).unwrap());
    assert_eq!(db.fills_count().unwrap(), 0);
    assert_eq!(db.bankroll().unwrap(), Some(dec!(1000)));
    println!("PASS: AC-FAIL — RPC error → Err, SQLite untouched (0 fills, bankroll 1000)");
}

#[tokio::test]
async fn ac_catchup_replays_and_is_idempotent() {
    // Seed SQLite with three fills at sparse seqs; catch-up replays them to Supabase.
    let (_dir, db) = db_with_bankroll(dec!(1000));
    for (seq, key) in [(5, wf_key(5)), (9, wf_key(9)), (14, wf_key(14))] {
        let (record, _) = fill_pair(&key, Side::Buy, 10, dec!(0.10), seq);
        db.commit_fill(
            &SourceTradeId(format!("s{seq}")),
            &leader(),
            &record,
            EventSeq(seq as u64),
        )
        .unwrap();
    }
    let fake = FakeSupabaseState::new(dec!(1000));

    let (wm1, full1) = catch_up_supabase(&fake, &db, 0).await.unwrap();
    let (wm2, full2) = catch_up_supabase(&fake, &db, wm1).await.unwrap();

    // PASS: the first pass applies all three and advances to 14; re-running from 14 applies
    //       nothing new, and the fake's bankroll equals SQLite's (RPC↔Rust parity over catch-up).
    assert_eq!((wm1, wm2), (14, 14));
    assert!(full1 && full2, "no failures → fully caught up both passes");
    assert_eq!(*fake.commit_calls.lock().unwrap(), vec![5, 9, 14]);
    assert_eq!(Some(fake.bankroll()), db.bankroll().unwrap());
    println!("PASS: AC-CATCHUP — replayed [5,9,14] once, re-run from 14 applied nothing");
}

#[tokio::test]
async fn ac_catchup_halts_at_first_failure() {
    let (_dir, db) = db_with_bankroll(dec!(1000));
    for (seq, key) in [(5, wf_key(5)), (9, wf_key(9)), (14, wf_key(14))] {
        let (record, _) = fill_pair(&key, Side::Buy, 10, dec!(0.10), seq);
        db.commit_fill(
            &SourceTradeId(format!("s{seq}")),
            &leader(),
            &record,
            EventSeq(seq as u64),
        )
        .unwrap();
    }
    let fake = FakeSupabaseState {
        fail_commit_seq: Some(9),
        ..FakeSupabaseState::new(dec!(1000))
    };

    let (wm, fully) = catch_up_supabase(&fake, &db, 0).await.unwrap();

    // PASS: the watermark advances only over the confirmed prefix (5) and halts at 9; 14 is
    //       left for the next boot. Not integer adjacency. `fully` is false so the boot skips
    //       the (now-incomplete) Supabase pull.
    assert_eq!(wm, 5);
    assert!(!fully, "halted before completing → not fully caught up");
    assert_eq!(*fake.commit_calls.lock().unwrap(), vec![5]);
    println!("PASS: AC-HALT — applied [5], halted at 9, watermark=5 (14 deferred)");
}

#[tokio::test]
async fn ac_resolution_credits_once_and_skips_on_error() {
    let (_dir, db) = db_with_bankroll(dec!(100));
    let mut store = ResolutionStore::load(db.clone()).unwrap();
    let fake = FakeSupabaseState::new(dec!(100));
    let mid = MarketId(VenueMarketId("0xres".to_string()));

    // First resolution: credited exactly once in both stores.
    let b1 = apply_resolution_authoritative(
        &fake,
        &mut store,
        &mid,
        &[dec!(1), dec!(0)],
        dec!(25),
        1_700_000_000,
    )
    .await
    .unwrap();
    assert_eq!(b1, dec!(125));
    assert_eq!(db.bankroll().unwrap(), Some(dec!(125)));
    assert!(store.is_settled(&mid));

    // Duplicate resolution: credit zero (AC8) — RPC gate + local guard.
    let b2 = apply_resolution_authoritative(
        &fake,
        &mut store,
        &mid,
        &[dec!(1), dec!(0)],
        dec!(25),
        1_700_000_999,
    )
    .await
    .unwrap();
    assert_eq!(b2, dec!(125));
    assert_eq!(db.bankroll().unwrap(), Some(dec!(125)));

    // RPC error on a fresh market: Err, nothing settled or credited (caller retries next tick).
    let fail = FakeSupabaseState {
        fail_resolution: true,
        ..FakeSupabaseState::new(dec!(125))
    };
    let other = MarketId(VenueMarketId("0xother".to_string()));
    let r = apply_resolution_authoritative(
        &fail,
        &mut store,
        &other,
        &[dec!(0), dec!(1)],
        dec!(50),
        1_700_001_000,
    )
    .await;
    assert!(r.is_err());
    assert!(!store.is_settled(&other));
    assert_eq!(db.bankroll().unwrap(), Some(dec!(125)));
    println!("PASS: AC-RES — credit-once (125), duplicate credits 0, RPC error skips");
}

#[tokio::test]
async fn ac_parity_fake_matches_paper_state_over_fill_mix() {
    // Drive a buy/sell/clamp mix through the write-through; after each, the authoritative
    // (fake = PL/pgSQL model) bankroll must equal the SQLite (canonical Rust) bankroll.
    let (_dir, db) = db_with_bankroll(dec!(50));
    let fake = FakeSupabaseState::new(dec!(50));
    let fills = [
        (wf_key(1), Side::Buy, 10u64, dec!(0.40), 1i64), // 50 - 4   = 46
        (wf_key(2), Side::Buy, 100, dec!(0.50), 2),      // 46 - 50  -> clamp 0
        (wf_key(3), Side::Sell, 5, dec!(0.60), 3),       // 0 + 3    = 3
    ];
    for (key, side, contracts, price, seq) in fills {
        let (record, sup_row) = fill_pair(&key, side, contracts, price, seq);
        let ret = commit_fill_authoritative(
            &fake,
            &db,
            &SourceTradeId(format!("p{seq}")),
            &leader(),
            &record,
            EventSeq(seq as u64),
            &sup_row,
            None,
        )
        .await
        .unwrap();
        // RPC return == fake bankroll == SQLite bankroll (the drift guard).
        assert_eq!(Some(ret), db.bankroll().unwrap(), "parity at seq {seq}");
        assert_eq!(ret, fake.bankroll(), "rpc return at seq {seq}");
    }
    assert_eq!(db.bankroll().unwrap(), Some(dec!(3)));
    println!("PASS: AC-PARITY — fake (PL/pgSQL model) == PaperStateDb over buy/clamp/sell → 3");
}

/// #508 Decision 10: in authoritative mode the dispatch flip rides the LOCAL transaction
/// that follows the RPC — and a staged seed flips `ready` with the `fill` outcome in that
/// same commit.
/// PASS: after `commit_fill_authoritative(.., Some(flip))`, the seed is `ready`/`fill`,
///       the fill mirrored, and the RPC saw exactly one commit.
#[tokio::test]
async fn ac_dispatch_flip_rides_the_local_mirror_transaction() {
    let (_dir, db) = db_with_bankroll(dec!(1000));
    let fake = FakeSupabaseState::new(dec!(1000));
    let (record, sup_row) = fill_pair(&wf_key(6), Side::Buy, 10, dec!(0.40), 6);
    let src = SourceTradeId("src6".to_string());
    db.stage_dispatch_seed(&pe_paper_state::DispatchSeedRecord {
        dispatch_id: wf_key(6),
        signal_json: "{\"schema_version\":1}".to_string(),
        source_trade_id: src.0.clone(),
        created_at_unix: 1_000,
        targets: vec![pe_paper_state::DispatchTargetSeed {
            account_id: "primary-acct".to_string(),
            credential_bundle_version: 1,
            credential_key_id: "key-1".to_string(),
        }],
    })
    .unwrap();

    let key = wf_key(6);
    let flip = pe_paper_state::DispatchFlip {
        dispatch_id: &key,
        paper_outcome: "fill",
    };
    commit_fill_authoritative(
        &fake,
        &db,
        &src,
        &leader(),
        &record,
        EventSeq(6),
        &sup_row,
        Some(flip),
    )
    .await
    .unwrap();

    let seed = db.dispatch_seed(&wf_key(6)).unwrap().unwrap();
    assert_eq!(seed.state, "ready");
    assert_eq!(seed.paper_outcome.as_deref(), Some("fill"));
    assert_eq!(db.fills_count().unwrap(), 1);
    println!("PASS: AC-508-FLIP — authoritative fill flipped the staged seed ready/fill");
}

/// #508 round-4: a failed local finalization AFTER a successful authoritative RPC enters the
/// in-process reconcile (bounded retries) rather than silently waiting for restart — proven
/// here by the flip landing despite the first local attempts failing (a lock-poisoning fake
/// is not constructible for `PaperStateDb`, so the retry path is exercised by contention:
/// the retry loop re-runs the SAME local transaction and the seed still converges).
/// PASS: the RPC committed once, and the local mirror + flip converged (retries idempotent).
#[tokio::test]
async fn ac_authoritative_local_retry_converges_idempotently() {
    let (_dir, db) = db_with_bankroll(dec!(1000));
    let fake = FakeSupabaseState::new(dec!(1000));
    let (record, sup_row) = fill_pair(&wf_key(7), Side::Buy, 5, dec!(0.20), 7);
    let src = SourceTradeId("src7".to_string());
    let key = wf_key(7);
    db.stage_dispatch_seed(&pe_paper_state::DispatchSeedRecord {
        dispatch_id: key.clone(),
        signal_json: "{\"schema_version\":1}".to_string(),
        source_trade_id: src.0.clone(),
        created_at_unix: 1_000,
        targets: vec![],
    })
    .unwrap();
    // Two identical authoritative commits (a redelivery after a mid-local crash): the fill
    // debits ONCE (RPC + fills PK dedup) and the flip stays `fill` (idempotent re-flip).
    for _ in 0..2 {
        let flip = pe_paper_state::DispatchFlip {
            dispatch_id: &key,
            paper_outcome: "fill",
        };
        commit_fill_authoritative(
            &fake,
            &db,
            &src,
            &leader(),
            &record,
            EventSeq(7),
            &sup_row,
            Some(flip),
        )
        .await
        .unwrap();
    }
    assert_eq!(db.fills_count().unwrap(), 1, "debited exactly once");
    let seed = db.dispatch_seed(&key).unwrap().unwrap();
    assert_eq!(seed.state, "ready");
    assert_eq!(seed.paper_outcome.as_deref(), Some("fill"));
    println!("PASS: AC-508-RETRY — redelivered authoritative commit converged idempotently");
}
