#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! Offline, deterministic fidelity AC for the #310 `sweep` subcommand (PR1).
//!
//! Each scenario builds a fixture DB, simulates the LIVE path over the same
//! frames (the exact `drive` arms: `parse_clob_frame` → `on_book_update`,
//! `parse_trade_frame` → `on_exchange_tick` → `insert_observations`), then runs
//! the sweep and compares the reference cell against the live `observations`
//! table.
//!
//! PASS criteria (one per scenario, printed):
//! - S1 reference cell reproduces live observations EXACTLY on a clean fixture.
//! - S2 the activation gate suppresses pre-registration observations live never
//!   emitted (and missing `frames_dropped_*` keys stamp unknown, never 0).
//! - S3 an unchanged top-of-book `price_change` between the last book change and
//!   the fire pins per-frame `feed_to_book_lag_ms` (a change-only index fails).
//! - S4 an absent-side `price_change` before a fire pins `None`-propagation
//!   (`best_ask`/`fee_cost`/net = None, never filled from a previous entry).
//! - S5 the buy-hold column equals the live `build_realized` formula.
//!
//! Determinism: every clock is a fixture integer (no wall clock), no RNG, no
//! network, DB under a `TempDir`.

use rust_decimal_macros::dec;
use tempfile::TempDir;

use pe_crypto_shadow::db::ShadowDb;
use pe_crypto_shadow::fees::taker_fee_per_share;
use pe_crypto_shadow::join::JoinState;
use pe_crypto_shadow::types::{BtcMarketMeta, BtcSeriesKind, FeedSource};
use pe_crypto_shadow::{ShadowConfig, SweepArgs, clob_ws, exchange_ws, sweep};

fn market(condition: &str, yes: &str, no: &str) -> BtcMarketMeta {
    BtcMarketMeta {
        condition_id: condition.to_string(),
        yes_token_id: yes.to_string(),
        no_token_id: no.to_string(),
        series: BtcSeriesKind::Five,
        range_start_ms: 1_000,
        range_end_ms: 301_000,
        tick: dec!(0.01),
    }
}

/// A `price_change` frame for one token; `None` sides are omitted from the JSON
/// (the absent-side case live stores as `None`).
fn price_change(market: &str, token: &str, bid: Option<&str>, ask: Option<&str>) -> String {
    let mut fields = format!(r#""asset_id":"{token}""#);
    if let Some(b) = bid {
        fields.push_str(&format!(r#","best_bid":"{b}""#));
    }
    if let Some(a) = ask {
        fields.push_str(&format!(r#","best_ask":"{a}""#));
    }
    format!(r#"{{"market":"{market}","price_changes":[{{{fields}}}]}}"#)
}

/// One `price_change` frame carrying both outcome tokens of a market.
fn both_books(market: &str, yes: &str, no: &str) -> String {
    format!(
        r#"{{"market":"{market}","price_changes":[
            {{"asset_id":"{yes}","best_bid":"0.48","best_ask":"0.52"}},
            {{"asset_id":"{no}","best_bid":"0.46","best_ask":"0.50"}}]}}"#
    )
}

fn bybit(price: &str, t: i64) -> String {
    format!(r#"{{"data":[{{"p":"{price}","T":{t}}}]}}"#)
}

fn okx(price: &str, t: i64) -> String {
    format!(r#"{{"data":[{{"px":"{price}","ts":"{t}"}}]}}"#)
}

/// Fixture: a DB plus the simulated LIVE join state. `feed` persists frames to
/// `raw_ticks` (tape order) AND processes them exactly like the live `drive`
/// arms, writing fired observations synchronously.
struct Fixture {
    _dir: TempDir,
    db_path: std::path::PathBuf,
    db: ShadowDb,
    live: JoinState,
}

impl Fixture {
    fn new(markets: &[BtcMarketMeta], boot_registered: &[BtcMarketMeta]) -> Self {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("tape.db");
        let db = ShadowDb::open(&db_path).unwrap();
        for m in markets {
            db.upsert_market(m).unwrap();
        }
        let live = JoinState::new(
            boot_registered.to_vec(),
            ShadowConfig::default().consensus_params(),
        );
        Self {
            _dir: dir,
            db_path,
            db,
            live,
        }
    }

    fn feed(&mut self, frames: &[(FeedSource, i64, &str)]) {
        for (source, received_ms, payload) in frames {
            match source {
                FeedSource::Clob => {
                    for update in clob_ws::parse_clob_frame(payload).unwrap() {
                        self.live.on_book_update(update, *received_ms);
                    }
                }
                FeedSource::Bybit | FeedSource::Okx | FeedSource::Coinbase => {
                    let venue = source.exchange_venue().unwrap();
                    if let Some(tick) = exchange_ws::parse_trade_frame(venue, payload).unwrap() {
                        let observations = self.live.on_exchange_tick(&tick, *received_ms);
                        self.db.insert_observations(&observations).unwrap();
                    }
                }
                FeedSource::Chainlink => {}
            }
        }
        let owned: Vec<(FeedSource, i64, String)> = frames
            .iter()
            .map(|(s, r, p)| (*s, *r, (*p).to_string()))
            .collect();
        self.db.insert_frame_batch(&owned, &[]).unwrap();
    }

    fn stamp_clean_drop_counters(&self) {
        for key in [
            "frames_dropped_chainlink",
            "frames_dropped_clob",
            "frames_dropped_bybit",
            "frames_dropped_okx",
            "frames_dropped_coinbase",
        ] {
            self.db.set_meta(key, "0").unwrap();
        }
    }

    fn run_sweep(&self) -> pe_crypto_shadow::SweepOutput {
        let args = SweepArgs {
            db: self.db_path.clone(),
            out: None,
        };
        sweep(&ShadowConfig::default(), &args).unwrap()
    }
}

/// The S1 tape: one market, books first (controlled registration timing), then
/// a +4 bps consensus move that fires once at t=1450.
fn fire_once_frames(condition: &str, yes: &str, no: &str) -> Vec<(FeedSource, i64, String)> {
    vec![
        (FeedSource::Clob, 1_050, both_books(condition, yes, no)),
        (FeedSource::Bybit, 1_100, bybit("60000", 1_100)),
        (FeedSource::Okx, 1_150, okx("60000", 1_150)),
        (FeedSource::Bybit, 1_400, bybit("60024", 1_400)),
        (FeedSource::Okx, 1_450, okx("60024", 1_450)),
    ]
}

fn as_refs(frames: &[(FeedSource, i64, String)]) -> Vec<(FeedSource, i64, &str)> {
    frames
        .iter()
        .map(|(s, r, p)| (*s, *r, p.as_str()))
        .collect()
}

#[test]
fn s1_reference_cell_reproduces_live_observations_exactly() {
    let m = market("0xc1", "y1", "n1");
    let mut fx = Fixture::new(std::slice::from_ref(&m), std::slice::from_ref(&m));
    let frames = fire_once_frames("0xc1", "y1", "n1");
    fx.feed(&as_refs(&frames));
    fx.stamp_clean_drop_counters();

    // Fixture sanity: live emitted exactly one observation with the full field set.
    let live = fx.db.all_observations_full().unwrap();
    assert_eq!(live.len(), 1);
    assert_eq!(live[0].best_ask, Some(dec!(0.52)));
    assert_eq!(live[0].no_best_ask, Some(dec!(0.50)));
    assert_eq!(live[0].range_start_value, Some(dec!(60000)));
    assert_eq!(live[0].feed_to_book_lag_ms, Some(400)); // 1450 - 1050

    let out = fx.run_sweep();
    assert!(out.tape_validity.valid, "all drop counters stamped 0");
    assert_eq!(out.tape_validity.frames_dropped_status, "clean");
    let f = &out.tape_validity.fidelity;
    assert_eq!(
        (
            f.live_rows,
            f.replay_rows,
            f.matched,
            f.missing_rows,
            f.extra_rows
        ),
        (1, 1, 1, 0, 0),
        "reference cell must reproduce live EXACTLY: {f:?}"
    );
    assert!(f.first_divergence.is_none());

    // The grid cell matching the live trigger params fires identically.
    let cell = out
        .cells
        .iter()
        .find(|c| {
            c.threshold_bps == dec!(3)
                && c.window_ms == 300
                && c.cooldown_ms == 1000
                && c.top_n == 3
        })
        .unwrap();
    assert_eq!(cell.fires, 1);
    println!("PASS: s1_reference_cell_reproduces_live_observations_exactly");
}

#[test]
fn s2_activation_gate_suppresses_pre_registration_observations() {
    let m1 = market("0xc1", "y1", "n1");
    let m2 = market("0xc2", "y2", "n2");
    // Both markets in the markets table (replay's universe); live boot-registers
    // only m1 — m2 registers mid-tape, like a refresh-added market.
    let mut fx = Fixture::new(&[m1.clone(), m2.clone()], &[m1]);

    // Chunk A: m1 books + fire 1 at t=1450. m2 is unknown to live here.
    let chunk_a = fire_once_frames("0xc1", "y1", "n1");
    fx.feed(&as_refs(&chunk_a));

    // Live registers m2 (the refresh arm), THEN its first CLOB frame arrives —
    // the same order as live (registration precedes subscription frames).
    fx.live.upsert_market(m2);
    let chunk_b = vec![
        (FeedSource::Clob, 1_500, both_books("0xc2", "y2", "n2")),
        // Fresh in-window reference after the cooldown (median 60024 at 2600),
        // then a move past 3 bps: median (60024+60061)/2 = 60042.5 vs 60024 =
        // +3.08 bps -> fire 2 at t=2700 emits for BOTH markets.
        (FeedSource::Bybit, 2_600, bybit("60024", 2_600)),
        (FeedSource::Okx, 2_700, okx("60061", 2_700)),
    ];
    fx.feed(&as_refs(&chunk_b));
    // NOTE: frames_dropped_* deliberately NOT stamped (crashed-capture shape).

    let live = fx.db.all_observations_full().unwrap();
    assert_eq!(
        live.len(),
        3,
        "fire1 -> m1 only (m2 unregistered); fire2 -> m1 + m2"
    );

    let out = fx.run_sweep();
    // The activation gate must suppress the m2 row at fire 1 that live never
    // emitted: zero missing AND zero extra.
    let f = &out.tape_validity.fidelity;
    assert_eq!(
        (
            f.live_rows,
            f.replay_rows,
            f.matched,
            f.missing_rows,
            f.extra_rows
        ),
        (3, 3, 3, 0, 0),
        "activation gate must reproduce live registration timing: {f:?}"
    );

    // Round-3 delta: missing meta keys stamp unknown/not-clean — never 0.
    assert!(!out.tape_validity.valid);
    assert!(
        out.tape_validity
            .frames_dropped_status
            .starts_with("unknown"),
        "{}",
        out.tape_validity.frames_dropped_status
    );
    println!("PASS: s2_activation_gate_suppresses_pre_registration_observations");
}

#[test]
fn s3_unchanged_price_change_pins_per_frame_lag_reproduction() {
    let m = market("0xc1", "y1", "n1");
    let mut fx = Fixture::new(std::slice::from_ref(&m), std::slice::from_ref(&m));
    let frames = vec![
        (FeedSource::Clob, 1_050, both_books("0xc1", "y1", "n1")),
        (FeedSource::Bybit, 1_100, bybit("60000", 1_100)),
        (FeedSource::Okx, 1_150, okx("60000", 1_150)),
        // Unchanged top-of-book price_change (deeper-level move): live
        // re-stamps the book's received_ms to 1300.
        (
            FeedSource::Clob,
            1_300,
            price_change("0xc1", "y1", Some("0.48"), Some("0.52")),
        ),
        (FeedSource::Bybit, 1_400, bybit("60024", 1_400)),
        (FeedSource::Okx, 1_450, okx("60024", 1_450)),
    ];
    fx.feed(&as_refs(&frames));
    fx.stamp_clean_drop_counters();

    let live = fx.db.all_observations_full().unwrap();
    assert_eq!(live.len(), 1);
    assert_eq!(
        live[0].feed_to_book_lag_ms,
        Some(150),
        "live lag = 1450 - 1300 (the unchanged frame re-stamps); a change-only \
         index would compute 400"
    );

    let out = fx.run_sweep();
    let f = &out.tape_validity.fidelity;
    assert_eq!(
        (f.matched, f.missing_rows, f.extra_rows),
        (1, 0, 0),
        "per-frame BookIndex must reproduce the re-stamped lag: {f:?}"
    );
    println!("PASS: s3_unchanged_price_change_pins_per_frame_lag_reproduction");
}

#[test]
fn s4_absent_side_price_change_pins_none_propagation() {
    let m = market("0xc1", "y1", "n1");
    let mut fx = Fixture::new(std::slice::from_ref(&m), std::slice::from_ref(&m));
    let frames = vec![
        (FeedSource::Clob, 1_050, both_books("0xc1", "y1", "n1")),
        (FeedSource::Bybit, 1_100, bybit("60000", 1_100)),
        (FeedSource::Okx, 1_150, okx("60000", 1_150)),
        // The YES ask side disappears: live stores best_ask = None VERBATIM.
        (
            FeedSource::Clob,
            1_300,
            price_change("0xc1", "y1", Some("0.49"), None),
        ),
        (FeedSource::Bybit, 1_400, bybit("60024", 1_400)),
        (FeedSource::Okx, 1_450, okx("60024", 1_450)),
    ];
    fx.feed(&as_refs(&frames));
    fx.stamp_clean_drop_counters();

    let live = fx.db.all_observations_full().unwrap();
    assert_eq!(live.len(), 1);
    assert_eq!(live[0].best_ask, None, "absent side stays None");
    assert_eq!(live[0].fee_cost, None);
    assert_eq!(live[0].net_edge_vs_ask, None);
    assert_eq!(live[0].mid, None);
    assert_eq!(live[0].feed_to_book_lag_ms, Some(150));

    let out = fx.run_sweep();
    let f = &out.tape_validity.fidelity;
    assert_eq!(
        (f.matched, f.missing_rows, f.extra_rows),
        (1, 0, 0),
        "the index must never fill an absent side from a previous entry: {f:?}"
    );
    println!("PASS: s4_absent_side_price_change_pins_none_propagation");
}

#[test]
fn s5_buy_hold_column_matches_build_realized_formula() {
    let m = market("0xc1", "y1", "n1");
    let mut fx = Fixture::new(std::slice::from_ref(&m), std::slice::from_ref(&m));
    let frames = fire_once_frames("0xc1", "y1", "n1");
    fx.feed(&as_refs(&frames));
    fx.stamp_clean_drop_counters();
    // The up-move bought YES @ 0.52 and the market resolved Up.
    fx.db.upsert_resolution("0xc1", true, 9_999).unwrap();

    let out = fx.run_sweep();
    let cell = out
        .cells
        .iter()
        .find(|c| {
            c.threshold_bps == dec!(3)
                && c.window_ms == 300
                && c.cooldown_ms == 1000
                && c.top_n == 3
        })
        .unwrap();
    assert_eq!(cell.fires, 1);
    assert_eq!(cell.buy_hold.len(), 1);
    let group = &cell.buy_hold[0];
    assert_eq!(
        (group.series.as_str(), group.move_direction.as_str()),
        ("5m", "up")
    );
    assert_eq!(group.count_scored, 1);
    let expected = dec!(1) - dec!(0.52) - taker_fee_per_share(dec!(0.52));
    assert_eq!(group.mean_realized_net_vs_ask, Some(expected));
    assert_eq!(group.frac_realized_positive, Some(dec!(1)));
    println!("PASS: s5_buy_hold_column_matches_build_realized_formula");
}

#[test]
fn s6_scalp_exits_at_each_horizon_on_the_post_fire_book() {
    let m = market("0xc1", "y1", "n1");
    let mut fx = Fixture::new(std::slice::from_ref(&m), std::slice::from_ref(&m));
    let mut frames = fire_once_frames("0xc1", "y1", "n1");
    // Post-fire book path on the YES (direction-side) token: bid 0.55 until
    // 35s, then bid 0.40 — so h=10/30 exit at 0.55 and h=60/120 at 0.40.
    frames.push((
        FeedSource::Clob,
        5_000,
        price_change("0xc1", "y1", Some("0.55"), Some("0.58")),
    ));
    frames.push((
        FeedSource::Clob,
        35_000,
        price_change("0xc1", "y1", Some("0.40"), Some("0.43")),
    ));
    fx.feed(&as_refs(&frames));
    fx.stamp_clean_drop_counters();

    let out = fx.run_sweep();
    // Post-fire book frames must not disturb the fidelity gate.
    let f = &out.tape_validity.fidelity;
    assert_eq!((f.matched, f.missing_rows, f.extra_rows), (1, 0, 0));

    let cell = out
        .cells
        .iter()
        .find(|c| {
            c.threshold_bps == dec!(3)
                && c.window_ms == 300
                && c.cooldown_ms == 1000
                && c.top_n == 3
        })
        .unwrap();
    assert_eq!(
        cell.scalp.len(),
        4,
        "all four horizons scored: {:?}",
        cell.scalp
    );
    let entry_fee = taker_fee_per_share(dec!(0.52));
    for g in &cell.scalp {
        assert_eq!((g.series.as_str(), g.move_direction.as_str()), ("5m", "up"));
        assert_eq!(g.count_scored, 1);
        let exit_bid = if g.horizon_s <= 30 {
            dec!(0.55)
        } else {
            dec!(0.40)
        };
        let expected = exit_bid - dec!(0.52) - entry_fee - taker_fee_per_share(exit_bid);
        assert_eq!(g.mean_net, Some(expected), "h={}", g.horizon_s);
    }
    println!("PASS: s6_scalp_exits_at_each_horizon_on_the_post_fire_book");
}

#[test]
fn s7_mm_fills_at_the_pre_move_quote_and_holds_to_resolution() {
    use pe_core_types::Price;
    use pe_crypto_shadow::fees::maker_rebate_per_share;
    use pe_crypto_shadow::types::ClobTrade;

    let m = market("0xc1", "y1", "n1");
    let mut fx = Fixture::new(std::slice::from_ref(&m), std::slice::from_ref(&m));
    let frames = fire_once_frames("0xc1", "y1", "n1");
    fx.feed(&as_refs(&frames));
    fx.stamp_clean_drop_counters();
    fx.db.upsert_resolution("0xc1", true, 9_999).unwrap();

    // Trade tape on the YES token around the fire (fire received at 1_450;
    // resting bid = pre-move best bid 0.48 from the tape-1 book frame):
    let print = |hash: &str, price, taker_is_buy, received| {
        let trade = ClobTrade {
            token_id: "y1".to_string(),
            condition_id: "0xc1".to_string(),
            price: Price(price),
            size: dec!(10),
            taker_is_buy,
            traded_at_ms: received - 50,
            fee_rate_bps: 0,
            transaction_hash: hash.to_string(),
        };
        fx.db
            .insert_clob_trade(&trade, received, Some("5m"))
            .unwrap();
    };
    print("0xa", dec!(0.47), true, 1_600); // taker BUY: wrong side, no fill
    print("0xb", dec!(0.47), false, 1_900); // taker sell <= bid inside window: FILL
    print("0xc", dec!(0.30), false, 9_000); // outside the 1000 ms window

    let out = fx.run_sweep();
    let cell = out
        .cells
        .iter()
        .find(|c| {
            c.threshold_bps == dec!(3)
                && c.window_ms == 300
                && c.cooldown_ms == 1000
                && c.top_n == 3
        })
        .unwrap();
    assert_eq!(cell.mm.len(), 1, "{:?}", cell.mm);
    let g = &cell.mm[0];
    assert_eq!((g.series.as_str(), g.move_direction.as_str()), ("5m", "up"));
    assert_eq!(
        (g.count_resting, g.count_filled, g.count_scored),
        (1, 1, 1),
        "one quote rested, one qualifying print filled it"
    );
    // Fill at the QUOTE (0.48); resolved Up -> won; rebate = 0.20 * taker_fee.
    let expected = dec!(1) - dec!(0.48) + maker_rebate_per_share(dec!(0.48));
    assert_eq!(g.mean_net, Some(expected));
    assert_eq!(g.frac_positive, Some(dec!(1)));
    println!("PASS: s7_mm_fills_at_the_pre_move_quote_and_holds_to_resolution");
}
