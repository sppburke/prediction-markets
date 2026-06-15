#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Scenario: the settled-markets double-credit guard survives restart and the
//! JSON→SQLite cutover (issue #343 step 0 / PR1).
//!
//! Deterministic — fixed timestamps, no network, no `SystemTime::now()`. Proves the
//! trade-path invariant PR1 exists to protect: a still-closed market is credited to the
//! bankroll exactly once, across a restart and across the cutover from the JSON sidecar
//! to the SQLite `settled_markets` table.

use std::sync::Arc;

use pe_core_types::{
    EventSeq, MarketId, OutcomeId, Price, Side, SourceTradeId, VenueMarketId, WalletAddress,
};
use pe_paper_pnl::ResolutionStore;
use pe_paper_state::{FillRecord, LeaderPositionRow, PaperStateDb};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

const SETTLED_AT: i64 = 1_700_000_000;

fn market(s: &str) -> MarketId {
    MarketId(VenueMarketId(s.to_string()))
}

fn wallet() -> WalletAddress {
    WalletAddress::from_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap()
}

/// Commit a BUY of `contracts` YES @ `price` in `mkt` (debits the bankroll, keeps the
/// position row), mirroring a real entry fill.
fn enter(db: &Arc<PaperStateDb>, mkt: &MarketId, key: &str, contracts: u64, price: Decimal) {
    db.commit_fill(
        &SourceTradeId(format!("src-{key}")),
        &LeaderPositionRow {
            wallet: wallet(),
            market_id: mkt.clone(),
            outcome_id: OutcomeId(0),
            long_contracts: contracts,
            short_contracts: 0,
        },
        &FillRecord {
            idempotency_key: key.to_string(),
            market_id: mkt.clone(),
            outcome_id: OutcomeId(0),
            side: Side::Buy,
            contracts,
            fill_price: Price(price),
        },
        EventSeq(1),
    )
    .unwrap();
}

/// Replicate the resolution-tick credit guard (`main.rs::tick_resolution`): a closed
/// market is `mark_settled` + `credit_bankroll`-ed only if not already settled. Returns
/// whether a credit fired this pass.
fn resolve_once(
    db: &Arc<PaperStateDb>,
    store: &mut ResolutionStore,
    mkt: &MarketId,
    prices: &[Decimal],
    credit: Decimal,
) -> bool {
    if store.is_settled(mkt) {
        return false;
    }
    store
        .mark_settled(mkt.clone(), prices.to_vec(), credit, SETTLED_AT)
        .unwrap();
    if credit > Decimal::ZERO {
        db.credit_bankroll(credit).unwrap();
    }
    true
}

/// Scenario A — a restart does not re-credit an already-settled market.
/// PASS: after settling once and restarting (fresh store hydrated from SQLite), a second
///       resolve pass of the same still-closed market credits nothing and the bankroll
///       is unchanged.
/// FAIL: the second pass re-credits (bankroll grows) or any call panics.
#[test]
fn restart_does_not_recredit() {
    println!("Scenario: settled_set — restart does not re-credit");

    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
    let res_path = dir.path().join("resolutions.json");
    db.init_bankroll(dec!(1000)).unwrap();

    // BUY 100 YES @ 0.40 → bankroll 960; the market then resolves YES → credit 100.
    let mkt = market("0xrestart");
    enter(
        &db,
        &mkt,
        "wf|0xL|tx1|0xrestart|0|buy|1700000000",
        100,
        dec!(0.40),
    );

    let mut store = ResolutionStore::load(db.clone(), &res_path).unwrap();
    assert!(
        resolve_once(&db, &mut store, &mkt, &[dec!(1), dec!(0)], dec!(100)),
        "first resolve must credit"
    );
    let after_credit = db.bankroll().unwrap().unwrap();
    assert_eq!(after_credit, dec!(1060)); // 1000 − 40 + 100
    drop(store);

    // Restart: fresh store hydrated from SQLite; re-poll the same still-closed market.
    let mut store2 = ResolutionStore::load(db.clone(), &res_path).unwrap();
    let credited_again = resolve_once(&db, &mut store2, &mkt, &[dec!(1), dec!(0)], dec!(100));
    let final_bankroll = db.bankroll().unwrap().unwrap();

    assert!(
        !credited_again,
        "a settled market must not be re-credited on restart"
    );
    assert_eq!(
        final_bankroll, after_credit,
        "bankroll must be unchanged on the restart re-poll"
    );
    println!("PASS: restart does not re-credit (bankroll {final_bankroll})");
}

/// Scenario B — the PR1 cutover back-fills the JSON-only settled-set so nothing is
/// re-credited on the first SQLite-backed startup.
/// PASS: with the settled-set present only in the legacy JSON sidecar (SQLite empty), the
///       bankroll already reflecting that credit, and the position row still present, the
///       first `load` back-fills the market and the next resolve pass credits nothing.
/// FAIL: the cutover treats the market as unsettled and re-credits it.
#[test]
fn cutover_backfill_does_not_recredit() {
    println!("Scenario: settled_set — cutover back-fill does not re-credit");

    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
    let res_path = dir.path().join("resolutions.json");

    // Reconstruct the live pre-step-0 state: the prior (JSON-only) binary already settled
    // and credited this market. Bankroll = 1000 − 40 (entry) + 100 (credit) = 1060; the
    // position row is kept; the settled-set lives ONLY in the JSON sidecar (SQLite empty).
    db.init_bankroll(dec!(1000)).unwrap();
    let mkt = market("0xcutover");
    enter(
        &db,
        &mkt,
        "wf|0xL|tx1|0xcutover|0|buy|1700000000",
        100,
        dec!(0.40),
    );
    db.credit_bankroll(dec!(100)).unwrap();
    assert_eq!(db.bankroll().unwrap().unwrap(), dec!(1060));

    let sidecar = serde_json::json!({
        "settlements": [{
            "market_id": "0xcutover",
            "outcome_prices": ["1", "0"],
            "credit_applied": "100",
            "settled_at_unix": SETTLED_AT
        }]
    });
    std::fs::write(&res_path, serde_json::to_vec(&sidecar).unwrap()).unwrap();
    assert_eq!(
        db.list_settled_markets().unwrap().len(),
        0,
        "SQLite settled-set must be empty pre-cutover"
    );

    // First step-0 startup: load back-fills JSON→SQLite before hydrating the guard.
    let mut store = ResolutionStore::load(db.clone(), &res_path).unwrap();
    let credited_again = resolve_once(&db, &mut store, &mkt, &[dec!(1), dec!(0)], dec!(100));
    let final_bankroll = db.bankroll().unwrap().unwrap();

    assert!(
        !credited_again,
        "a cutover-back-filled market must not be re-credited"
    );
    assert_eq!(
        final_bankroll,
        dec!(1060),
        "bankroll must be unchanged at the cutover"
    );
    println!("PASS: cutover back-fill does not re-credit (bankroll {final_bankroll})");
}
