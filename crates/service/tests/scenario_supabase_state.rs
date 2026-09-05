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

use std::collections::HashMap;

/// `(credit, outcome_prices, settled_at_unix)` recorded for a settled market.
type SettledEntry = (Decimal, Vec<Decimal>, i64);
use std::sync::Mutex;

use pe_core_types::{ContractQty, ReceivedAt, SourceId, SourceTimestamp, StrategyId};
use pe_core_types::{
    EventSeq, MarketId, OutcomeId, Price, Side, SourceTradeId, VenueMarketId, WalletAddress,
};
use pe_event_log::{ContentType, EnvelopeIn, Writer};
use pe_paper_pnl::ResolutionStore;
use pe_paper_state::{FillRecord, FillRow, LeaderPositionRow, PaperStateDb};
use pe_service::paper_recovery::{LegacyFillSource, LegacyPaperFill};
use pe_service::supabase_sink::{SupabaseFillRow, supabase_fill_from};
use pe_service::supabase_state::{
    AuthoritativeFillOutcome, CanonicalFill, FillV2Outcome, ResolutionV2Outcome, SupabaseBootTrait,
    SupabaseStateError, SupabaseStateTrait, apply_resolution_authoritative,
    commit_fill_authoritative, resolve_event_frames, supabase_authoritative_boot,
};
use pe_venue_core::OrderIntent;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::sync::Arc;
use tempfile::TempDir;
use time::OffsetDateTime;

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
        long_contracts: pe_core_types::ShareAmount::ZERO,
        short_contracts: pe_core_types::ShareAmount::ZERO,
    }
}

/// A `(FillRecord, SupabaseFillRow)` pair for the same fill — the orchestrator builds both
/// from one `LegacyPaperFill`, so the test does too.
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

/// In-memory [`SupabaseStateTrait`] replicating the #511 v2 PL/pgSQL semantics:
/// existing-key returns the CANONICAL row (ordering before the settled check), settled
/// markets refuse absent keys, resolution credit is computed from the fake's positions
/// under the same lock discipline. Failure injection: `fail_all_commits`,
/// `fail_commit_seq` (clean failure — nothing applies), and `apply_then_error_seq`
/// (AMBIGUOUS failure — the apply lands, then an error is returned; the Jul-24 502 class).
#[derive(Default)]
struct FakeSupabaseState {
    bankroll: Mutex<Decimal>,
    positions: Mutex<HashMap<(String, u16), (u64, u64)>>,
    rows: Mutex<HashMap<String, CanonicalFill>>,
    settled: Mutex<HashMap<String, SettledEntry>>,
    commit_calls: Mutex<Vec<i64>>,
    resolution_calls: Mutex<Vec<String>>,
    fail_all_commits: bool,
    fail_commit_seq: Option<i64>,
    apply_then_error_seq: Option<i64>,
    fail_resolution: bool,
    fail_boot_positions: bool,
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
    fn settle_market(&self, market: &str) {
        self.settled
            .lock()
            .unwrap()
            .insert(market.to_string(), (Decimal::ZERO, vec![], 0));
    }

    /// The v1-arithmetic apply (insert gate + netting + clamped debit), returning the row.
    fn apply(&self, row: &SupabaseFillRow) -> CanonicalFill {
        let mut bankroll = self.bankroll.lock().unwrap();
        let mut positions = self.positions.lock().unwrap();
        let pos_key = (row.fill.market_id.0.0.clone(), row.fill.outcome_id.0);
        let (long, short) = positions.get(&pos_key).copied().unwrap_or((0, 0));
        positions.insert(
            pos_key,
            apply_fill_to_net(long, short, row.fill.side, row.fill.contracts),
        );
        let notional = row.fill.fill_price.0 * Decimal::from(row.fill.contracts);
        *bankroll = match row.fill.side {
            Side::Buy => (*bankroll - notional).max(Decimal::ZERO),
            Side::Sell => *bankroll + notional,
        };
        let canonical = CanonicalFill {
            record: FillRecord {
                idempotency_key: row.fill.idempotency_key.clone(),
                market_id: row.fill.market_id.clone(),
                outcome_id: row.fill.outcome_id,
                side: row.fill.side,
                contracts: row.fill.contracts,
                fill_price: row.fill.fill_price,
            },
            event_seq: EventSeq(u64::try_from(row.fill.event_seq).unwrap()),
            source_trade_id: row.source_trade_id.clone().unwrap_or_default(),
        };
        self.rows
            .lock()
            .unwrap()
            .insert(row.fill.idempotency_key.clone(), canonical.clone());
        canonical
    }
}

impl SupabaseStateTrait for FakeSupabaseState {
    async fn commit_fill_v2(
        &self,
        row: &SupabaseFillRow,
    ) -> Result<FillV2Outcome, SupabaseStateError> {
        if self.fail_all_commits || self.fail_commit_seq == Some(row.fill.event_seq) {
            return Err(SupabaseStateError::Status(503, "injected".to_string()));
        }
        if self.apply_then_error_seq == Some(row.fill.event_seq)
            && !self
                .rows
                .lock()
                .unwrap()
                .contains_key(&row.fill.idempotency_key)
        {
            // AMBIGUOUS: the server applied, the client sees an error.
            self.apply(row);
            return Err(SupabaseStateError::Status(
                504,
                "gateway timeout".to_string(),
            ));
        }
        self.commit_calls.lock().unwrap().push(row.fill.event_seq);
        // v2 ordering (#511 R1): existing key FIRST — even if the market settled later.
        if let Some(existing) = self.rows.lock().unwrap().get(&row.fill.idempotency_key) {
            return Ok(FillV2Outcome::Existing {
                bankroll: self.bankroll(),
                row: existing.clone(),
            });
        }
        if self
            .settled
            .lock()
            .unwrap()
            .contains_key(&row.fill.market_id.0.0)
        {
            return Ok(FillV2Outcome::Settled {
                bankroll: self.bankroll(),
            });
        }
        let canonical = self.apply(row);
        Ok(FillV2Outcome::Applied {
            bankroll: self.bankroll(),
            row: canonical,
        })
    }

    async fn apply_resolution_v2(
        &self,
        market_id: &MarketId,
        outcome_prices: &[Decimal],
        settled_at_unix: i64,
    ) -> Result<ResolutionV2Outcome, SupabaseStateError> {
        if self.fail_resolution {
            return Err(SupabaseStateError::Status(503, "injected".to_string()));
        }
        self.resolution_calls
            .lock()
            .unwrap()
            .push(market_id.0.0.clone());
        let mut settled = self.settled.lock().unwrap();
        if let Some((credit, prices, at)) = settled.get(&market_id.0.0) {
            return Ok(ResolutionV2Outcome {
                applied: false,
                credit: *credit,
                outcome_prices: prices.clone(),
                settled_at_unix: *at,
                bankroll: self.bankroll(),
            });
        }
        // In-RPC credit from the authority's positions (PnlLedger::resolution_credit).
        let positions = self.positions.lock().unwrap();
        let credit = positions
            .iter()
            .filter(|((m, _), _)| m == &market_id.0.0)
            .map(|((_, outcome), (long, short))| {
                let price = outcome_prices
                    .get(usize::from(*outcome))
                    .copied()
                    .unwrap_or(Decimal::ZERO);
                (Decimal::from(*long) - Decimal::from(*short)) * price
            })
            .sum::<Decimal>()
            .max(Decimal::ZERO);
        settled.insert(
            market_id.0.0.clone(),
            (credit, outcome_prices.to_vec(), settled_at_unix),
        );
        let mut bankroll = self.bankroll.lock().unwrap();
        *bankroll += credit;
        Ok(ResolutionV2Outcome {
            applied: true,
            credit,
            outcome_prices: outcome_prices.to_vec(),
            settled_at_unix,
            bankroll: *bankroll,
        })
    }
}

impl SupabaseBootTrait for FakeSupabaseState {
    async fn fetch_boot_bankroll(&self) -> Result<Option<Decimal>, SupabaseStateError> {
        Ok(Some(self.bankroll()))
    }

    async fn fetch_boot_positions(
        &self,
    ) -> Result<Vec<pe_paper_state::PaperPositionRow>, SupabaseStateError> {
        if self.fail_boot_positions {
            return Err(SupabaseStateError::Corrupt(
                "injected mid-page positions failure".to_owned(),
            ));
        }
        self.positions
            .lock()
            .unwrap()
            .iter()
            .map(
                |((market_id, outcome_id), (long_contracts, short_contracts))| {
                    Ok(pe_paper_state::PaperPositionRow {
                        market_id: MarketId(VenueMarketId(market_id.clone())),
                        outcome_id: OutcomeId(*outcome_id),
                        long_contracts: *long_contracts,
                        short_contracts: *short_contracts,
                    })
                },
            )
            .collect()
    }
}

/// Append legacy `wf|`-keyed fill frames to a fresh event log (dense seqs from 0), returning the
/// log path. Frames only — no local commits.
fn write_frames(dir: &TempDir, fills: &[(u64, Side, u64, Decimal)]) -> std::path::PathBuf {
    let log_path = dir.path().join("paper.log");
    let mut writer = Writer::open(&log_path).unwrap();
    let ts = SourceTimestamp(OffsetDateTime::UNIX_EPOCH);
    for (key_seq, side, contracts, price) in fills {
        let intent = OrderIntent {
            strategy_id: StrategyId("winner-follow".to_string()),
            market_id: market(),
            outcome_id: OutcomeId(0),
            side: *side,
            contracts: ContractQty(*contracts),
            limit_price: Price(*price),
            validity_seconds: 30,
            idempotency_key: wf_key(*key_seq),
        };
        let fill = LegacyPaperFill {
            simulated_fill_price: intent.limit_price,
            intent,
            simulated_at: ts.clone(),
            fill_source: LegacyFillSource::LeaderHaircut,
        };
        writer
            .append_synced(EnvelopeIn {
                source_id: SourceId("test".into()),
                schema_version: 1,
                parser_version: 1,
                observed_at: ts.clone(),
                received_at: ReceivedAt(ts.0),
                content_type: ContentType::Json,
                payload: serde_json::to_vec(&fill).unwrap(),
            })
            .unwrap();
    }
    log_path
}

#[tokio::test]
async fn ac_wt_rpc_first_then_sqlite_mirror() {
    let (_dir, db) = db_with_bankroll(dec!(1000));
    let fake = FakeSupabaseState::new(dec!(1000));
    let (record, sup_row) = fill_pair(&wf_key(0), Side::Buy, 10, dec!(0.40), 0);
    let src = SourceTradeId("src0".to_string());

    let ret = commit_fill_authoritative(
        &fake,
        &db,
        &src,
        &leader(),
        &record,
        EventSeq(0),
        &sup_row,
        None,
        None,
    )
    .await
    .unwrap();

    // PASS: the RPC outcome is the authoritative bankroll (1000 - 0.40*10 = 996); SQLite
    //       mirrored the CANONICAL row and SET the bankroll to the authority value (#511).
    assert_eq!(ret, AuthoritativeFillOutcome::Filled(dec!(996.0)));
    assert!(db.is_seen(&src).unwrap());
    assert_eq!(db.fills_count().unwrap(), 1);
    assert_eq!(db.bankroll().unwrap(), Some(dec!(996.0)));
    // #510/#511: the watermark advanced only AFTER the local convergence txn.
    assert_eq!(
        db.last_supabase_applied_event_seq().unwrap(),
        Some(EventSeq(0))
    );
    println!("PASS: AC-WT — v2 first (996), SQLite converged, watermark advanced post-txn");
}

#[tokio::test]
async fn ac_fail_closed_leaves_sqlite_untouched() {
    let (_dir, db) = db_with_bankroll(dec!(1000));
    let fake = FakeSupabaseState {
        fail_all_commits: true,
        ..FakeSupabaseState::new(dec!(1000))
    };
    let (record, sup_row) = fill_pair(&wf_key(0), Side::Buy, 10, dec!(0.40), 0);
    let src = SourceTradeId("src0".to_string());

    let err = commit_fill_authoritative(
        &fake,
        &db,
        &src,
        &leader(),
        &record,
        EventSeq(0),
        &sup_row,
        None,
        None,
    )
    .await;

    // PASS: a clean RPC failure propagates; SQLite untouched (unseen, no fill, bankroll
    //       intact, watermark frozen) — the caller parks the frozen record (#511).
    assert!(err.is_err());
    assert!(!db.is_seen(&src).unwrap());
    assert_eq!(db.fills_count().unwrap(), 0);
    assert_eq!(db.bankroll().unwrap(), Some(dec!(1000)));
    assert_eq!(db.last_supabase_applied_event_seq().unwrap(), None);
    println!("PASS: AC-FAIL — clean failure leaves SQLite untouched, watermark frozen");
}

/// #511: the boot frame-walk resolves every frame above the frozen watermark through v2 —
/// fresh frames apply; a rerun is pure `existing` re-confirmation (idempotent).
#[tokio::test]
async fn ac_walk_resolves_frames_and_is_idempotent() {
    let (dir, db) = db_with_bankroll(dec!(1000));
    let log = write_frames(
        &dir,
        &[
            (0, Side::Buy, 10, dec!(0.40)),
            (1, Side::Buy, 5, dec!(0.60)),
        ],
    );
    let fake = FakeSupabaseState::new(dec!(1000));

    let (wm, fully, resolved) = resolve_event_frames(&fake, &db, &log).await.unwrap();
    assert_eq!((wm, fully, resolved), (1, true, 2));
    assert_eq!(db.fills_count().unwrap(), 2);
    assert_eq!(db.bankroll().unwrap(), Some(dec!(993.0)));
    assert_eq!(
        db.last_supabase_applied_event_seq().unwrap(),
        Some(EventSeq(1))
    );
    // Seen was written from the canonical row's source_trade_id (frame self-sufficiency).
    assert!(db.is_seen(&SourceTradeId("s0".to_string())).unwrap());

    // Rerun from the advanced watermark: nothing to do.
    let (wm2, fully2, resolved2) = resolve_event_frames(&fake, &db, &log).await.unwrap();
    assert_eq!((wm2, fully2, resolved2), (1, true, 0));
    println!("PASS: AC-WALK — frames resolved (993), idempotent rerun no-op");
}

/// #511 R3-1: an EARLIER failed frame after a LATER success is still revisited — the
/// successor-gated watermark froze below it, and the walk starts there.
#[tokio::test]
async fn ac_walk_revisits_earlier_failed_frame_after_later_success() {
    let (dir, db) = db_with_bankroll(dec!(1000));
    let log = write_frames(
        &dir,
        &[
            (0, Side::Buy, 10, dec!(0.40)),
            (1, Side::Buy, 5, dec!(0.60)),
        ],
    );
    // Runtime: frame 0 fails cleanly; frame 1 succeeds (non-successor → watermark frozen).
    let fake = FakeSupabaseState {
        fail_commit_seq: Some(0),
        ..FakeSupabaseState::new(dec!(1000))
    };
    let (r0, s0) = fill_pair(&wf_key(0), Side::Buy, 10, dec!(0.40), 0);
    let err = commit_fill_authoritative(
        &fake,
        &db,
        &SourceTradeId("s0".into()),
        &leader(),
        &r0,
        EventSeq(0),
        &s0,
        None,
        None,
    )
    .await;
    assert!(err.is_err());
    let (r1, s1) = fill_pair(&wf_key(1), Side::Buy, 5, dec!(0.60), 1);
    let ret = commit_fill_authoritative(
        &fake,
        &db,
        &SourceTradeId("s1".into()),
        &leader(),
        &r1,
        EventSeq(1),
        &s1,
        None,
        None,
    )
    .await
    .unwrap();
    assert_eq!(ret, AuthoritativeFillOutcome::Filled(dec!(997.0)));
    // Frozen below the gap despite the later success.
    assert_eq!(db.last_supabase_applied_event_seq().unwrap(), None);

    // Boot walk (fake now healthy): resolves frame 0 fresh, re-confirms frame 1 existing.
    let healthy = FakeSupabaseState {
        rows: Mutex::new(fake.rows.lock().unwrap().clone()),
        positions: Mutex::new(fake.positions.lock().unwrap().clone()),
        ..FakeSupabaseState::new(fake.bankroll())
    };
    let (wm, fully, resolved) = resolve_event_frames(&healthy, &db, &log).await.unwrap();
    assert_eq!((wm, fully, resolved), (1, true, 2));
    assert_eq!(db.fills_count().unwrap(), 2);
    assert_eq!(db.bankroll().unwrap(), Some(dec!(993.0)));
    println!("PASS: AC-WALK-GAP — earlier failed frame recovered after later success");
}

#[tokio::test]
async fn ac_authoritative_boot_rejects_an_incomplete_frame_walk() {
    let (dir, db) = db_with_bankroll(dec!(1000));
    let log = write_frames(&dir, &[(0, Side::Buy, 10, dec!(0.40))]);
    let failing = FakeSupabaseState {
        fail_commit_seq: Some(0),
        ..FakeSupabaseState::new(dec!(500))
    };

    let result = supabase_authoritative_boot(&failing, &db, &log).await;

    assert!(matches!(
        result,
        Err(SupabaseStateError::IncompleteFrameWalk { last_watermark: -1 })
    ));
    assert_eq!(db.bankroll().unwrap(), Some(dec!(1000)));
    assert_eq!(db.fills_count().unwrap(), 0);
    assert!(db.paper_positions().unwrap().is_empty());
    println!("PASS: AC-BOOT-INCOMPLETE — incomplete frame walk aborts before authority pull");
}

#[tokio::test]
async fn ac_authoritative_boot_rejects_a_physically_incomplete_final_frame() {
    let (dir, db) = db_with_bankroll(dec!(1000));
    let log = write_frames(&dir, &[(0, Side::Buy, 10, dec!(0.40))]);
    let prior_len = std::fs::metadata(&log).unwrap().len();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&log)
        .unwrap()
        .set_len(prior_len - 1)
        .unwrap();
    let fake = FakeSupabaseState::new(dec!(500));

    let result = supabase_authoritative_boot(&fake, &db, &log).await;

    assert!(matches!(result, Err(SupabaseStateError::Corrupt(_))));
    assert_eq!(db.bankroll().unwrap(), Some(dec!(1000)));
    assert_eq!(db.fills_count().unwrap(), 0);
    assert!(db.paper_positions().unwrap().is_empty());
    println!("PASS: AC-BOOT-PHYSICAL-TAIL — truncated final frame aborts before authority pull");
}

#[tokio::test]
async fn ac_authoritative_boot_replaces_the_complete_local_position_set() {
    let (dir, db) = db_with_bankroll(dec!(1000));
    let stale = MarketId(VenueMarketId("0xstale".to_owned()));
    db.upsert_position(&stale, OutcomeId(0), 9, 0).unwrap();
    let fake = FakeSupabaseState::new(dec!(123.45));
    fake.positions
        .lock()
        .unwrap()
        .insert(("0xfresh".to_owned(), 1), (7, 2));

    supabase_authoritative_boot(&fake, &db, &dir.path().join("absent.log"))
        .await
        .unwrap();

    assert_eq!(db.bankroll().unwrap(), Some(dec!(123.45)));
    let positions = db.paper_positions().unwrap();
    assert_eq!(positions.len(), 1);
    assert_eq!(positions[0].market_id.to_string(), "0xfresh");
    println!("PASS: AC-BOOT-REPLACE — stale position deleted by one complete replacement");
}

#[tokio::test]
async fn ac_authoritative_boot_page_failure_mutates_no_local_authority() {
    let (dir, db) = db_with_bankroll(dec!(1000));
    let stale = MarketId(VenueMarketId("0xstale".to_owned()));
    db.upsert_position(&stale, OutcomeId(0), 9, 0).unwrap();
    let failing = FakeSupabaseState {
        fail_boot_positions: true,
        ..FakeSupabaseState::new(dec!(123.45))
    };

    let result = supabase_authoritative_boot(&failing, &db, &dir.path().join("absent.log")).await;

    assert!(matches!(result, Err(SupabaseStateError::Corrupt(_))));
    assert_eq!(db.bankroll().unwrap(), Some(dec!(1000)));
    assert_eq!(db.paper_positions().unwrap()[0].market_id, stale);
    println!("PASS: AC-BOOT-PAGE-FAIL — pre-pull bankroll and positions remain intact");
}

/// #511: an AMBIGUOUS failure (applied server-side, error returned — the Jul-24 502
/// class) converges: the retry gets `existing` and mirrors the CANONICAL row byte-for-byte
/// even though the retry re-priced differently.
#[tokio::test]
async fn ac_ambiguous_apply_converges_on_canonical_row() {
    let (_dir, db) = db_with_bankroll(dec!(1000));
    let fake = FakeSupabaseState {
        apply_then_error_seq: Some(0),
        ..FakeSupabaseState::new(dec!(1000))
    };
    let (r0, s0) = fill_pair(&wf_key(0), Side::Buy, 10, dec!(0.40), 0);
    let err = commit_fill_authoritative(
        &fake,
        &db,
        &SourceTradeId("s0".into()),
        &leader(),
        &r0,
        EventSeq(0),
        &s0,
        None,
        None,
    )
    .await;
    assert!(err.is_err(), "ambiguous failure surfaces as an error");
    assert_eq!(db.fills_count().unwrap(), 0, "local untouched");
    assert_eq!(fake.bankroll(), dec!(996.0), "but the authority applied");

    // Frozen retry: SAME key, RE-PRICED record (the book moved). v2 returns existing.
    let (r0b, s0b) = fill_pair(&wf_key(0), Side::Buy, 10, dec!(0.55), 0);
    let ret = commit_fill_authoritative(
        &fake,
        &db,
        &SourceTradeId("s0".into()),
        &leader(),
        &r0b,
        EventSeq(0),
        &s0b,
        None,
        None,
    )
    .await
    .unwrap();
    assert_eq!(ret, AuthoritativeFillOutcome::Filled(dec!(996.0)));
    // Local mirrors the CANONICAL (attempt-A) price, not the retry's.
    let fills = db.list_fills().unwrap();
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].fill_price.0, dec!(0.40));
    assert_eq!(db.bankroll().unwrap(), Some(dec!(996.0)));
    println!("PASS: AC-AMBIG — existing returns canonical row; stores converge on attempt A");
}

/// #511: a fill for a settled market is REFUSED by the authority → terminal refused
/// disposition (seen + last_applied advanced, no fills row, no money) → boot replay can
/// never resurrect it and the watermark advances over it.
#[tokio::test]
async fn ac_settled_refusal_is_terminal_and_walk_advances() {
    let (dir, db) = db_with_bankroll(dec!(1000));
    let log = write_frames(&dir, &[(0, Side::Buy, 10, dec!(0.40))]);
    let fake = FakeSupabaseState::new(dec!(1000));
    fake.settle_market("0xmkt");

    let (wm, fully, resolved) = resolve_event_frames(&fake, &db, &log).await.unwrap();
    assert_eq!((wm, fully, resolved), (0, true, 1));
    assert_eq!(db.fills_count().unwrap(), 0, "no resurrection");
    assert_eq!(db.bankroll().unwrap(), Some(dec!(1000)));
    assert!(db.is_seen(&SourceTradeId("s0".to_string())).unwrap());
    assert_eq!(db.last_applied_event_seq().unwrap(), EventSeq(0));
    assert_eq!(
        db.last_supabase_applied_event_seq().unwrap(),
        Some(EventSeq(0))
    );
    println!("PASS: AC-SETTLED-WALK — refused frame disposed terminally, no resurrection");
}

/// #511: runtime settled refusal — v2 returns `settled`, the orchestrator-facing outcome
/// is `RefusedSettled`, and the refused disposition (seen + typed flip) is written.
#[tokio::test]
async fn ac_runtime_settled_refusal_writes_disposition() {
    let (_dir, db) = db_with_bankroll(dec!(1000));
    let fake = FakeSupabaseState::new(dec!(1000));
    fake.settle_market("0xmkt");
    let (r0, s0) = fill_pair(&wf_key(0), Side::Buy, 10, dec!(0.40), 0);
    let src = SourceTradeId("s0".to_string());

    let ret = commit_fill_authoritative(
        &fake,
        &db,
        &src,
        &leader(),
        &r0,
        EventSeq(0),
        &s0,
        None,
        None,
    )
    .await
    .unwrap();
    assert_eq!(ret, AuthoritativeFillOutcome::RefusedSettled(dec!(1000)));
    assert!(db.is_seen(&src).unwrap());
    assert_eq!(db.fills_count().unwrap(), 0);
    assert_eq!(db.last_applied_event_seq().unwrap(), EventSeq(0));
    println!("PASS: AC-SETTLED-RT — runtime refusal is terminal (seen, no fill, no money)");
}

/// v2 ordering (#511 R3): an EXISTING key returns its canonical row even after the market
/// settles — only an absent key is refused.
#[tokio::test]
async fn ac_existing_key_wins_over_later_settlement() {
    let (_dir, db) = db_with_bankroll(dec!(1000));
    let fake = FakeSupabaseState::new(dec!(1000));
    let (r0, s0) = fill_pair(&wf_key(0), Side::Buy, 10, dec!(0.40), 0);
    commit_fill_authoritative(
        &fake,
        &db,
        &SourceTradeId("s0".into()),
        &leader(),
        &r0,
        EventSeq(0),
        &s0,
        None,
        None,
    )
    .await
    .unwrap();
    fake.settle_market("0xmkt");

    // Duplicate-key retry AFTER settlement: existing, not settled-refusal.
    let ret = commit_fill_authoritative(
        &fake,
        &db,
        &SourceTradeId("s0".into()),
        &leader(),
        &r0,
        EventSeq(1),
        &s0,
        None,
        None,
    )
    .await
    .unwrap();
    assert_eq!(ret, AuthoritativeFillOutcome::Filled(dec!(996.0)));
    println!("PASS: AC-EXISTING-ORDER — existing key beats later settlement");
}

#[tokio::test]
async fn ac_resolution_credits_once_and_skips_on_error() {
    let (_dir, db) = db_with_bankroll(dec!(996));
    let mut store = ResolutionStore::load(Arc::clone(&db)).unwrap();
    let fake = FakeSupabaseState::new(dec!(996));
    // Authority holds 10 long @ outcome 0 for the market.
    fake.positions
        .lock()
        .unwrap()
        .insert(("0xmkt".to_string(), 0), (10, 0));

    // #511: credit computed IN the authority from ITS positions (10 × 1 = 10).
    let bankroll =
        apply_resolution_authoritative(&fake, &mut store, &market(), &[dec!(1), dec!(0)], 100)
            .await
            .unwrap();
    assert_eq!(bankroll, dec!(1006.0));
    assert_eq!(
        db.bankroll().unwrap(),
        Some(dec!(1006.0)),
        "mirror applied returned credit"
    );

    // Duplicate: existing → zero additional credit in both stores.
    let bankroll2 =
        apply_resolution_authoritative(&fake, &mut store, &market(), &[dec!(1), dec!(0)], 100)
            .await
            .unwrap();
    assert_eq!(bankroll2, dec!(1006.0));
    assert_eq!(db.bankroll().unwrap(), Some(dec!(1006.0)));

    // RPC error: fail-closed skip (unsettled locally, retried next tick).
    let failing = FakeSupabaseState {
        fail_resolution: true,
        ..FakeSupabaseState::new(dec!(1006))
    };
    let market2 = MarketId(VenueMarketId("0xother".to_string()));
    let err = apply_resolution_authoritative(&failing, &mut store, &market2, &[dec!(1)], 101).await;
    assert!(err.is_err());
    println!("PASS: AC-RES — in-RPC credit applied once, duplicate zero, error skips");
}

/// #511: crash-after-RPC-before-local-mirror retry converges on the CANONICAL recorded
/// credit (the `existing` branch returns the original values).
#[tokio::test]
async fn ac_resolution_crash_retry_mirrors_canonical_credit() {
    let (_dir, db) = db_with_bankroll(dec!(996));
    let fake = FakeSupabaseState::new(dec!(996));
    fake.positions
        .lock()
        .unwrap()
        .insert(("0xmkt".to_string(), 0), (10, 0));
    // "Crash before local": apply on the authority only.
    let first = fake
        .apply_resolution_v2(&market(), &[dec!(1), dec!(0)], 100)
        .await
        .unwrap();
    assert!(first.applied);
    assert_eq!(first.credit, dec!(10));
    assert_eq!(
        db.bankroll().unwrap(),
        Some(dec!(996)),
        "local never saw it"
    );

    // Restart retry through the full authoritative path: existing → canonical mirror.
    let mut store = ResolutionStore::load(Arc::clone(&db)).unwrap();
    let bankroll =
        apply_resolution_authoritative(&fake, &mut store, &market(), &[dec!(1), dec!(0)], 999)
            .await
            .unwrap();
    assert_eq!(bankroll, dec!(1006.0));
    assert_eq!(db.bankroll().unwrap(), Some(dec!(1006.0)));
    // Canonical settled_at (100), not the retry's (999).
    assert_eq!(store.settled_credit(&market()), Some(dec!(10)));
    println!("PASS: AC-RES-RETRY — no double credit; canonical values mirrored");
}

#[tokio::test]
async fn ac_parity_fake_matches_paper_state_over_fill_mix() {
    let (_dir, db) = db_with_bankroll(dec!(100));
    let fake = FakeSupabaseState::new(dec!(100));
    let mix = [
        (0u64, Side::Buy, 10u64, dec!(0.40)),
        (1, Side::Buy, 5, dec!(0.60)),
        (2, Side::Sell, 4, dec!(0.70)),
        (3, Side::Buy, 200, dec!(0.90)), // clamps at zero in both models
    ];
    for (seq, side, contracts, price) in mix {
        let (record, sup_row) = fill_pair(&wf_key(seq), side, contracts, price, seq as i64);
        commit_fill_authoritative(
            &fake,
            &db,
            &SourceTradeId(format!("s{seq}")),
            &leader(),
            &record,
            EventSeq(seq),
            &sup_row,
            None,
            None,
        )
        .await
        .unwrap();
    }
    // PASS: the PL/pgSQL model and PaperStateDb agree on the final bankroll.
    assert_eq!(Some(fake.bankroll()), db.bankroll().unwrap());
    println!(
        "PASS: AC-PARITY — fake RPC model and SQLite agree: {}",
        fake.bankroll()
    );
}

#[tokio::test]
async fn ac_dispatch_flip_rides_the_local_mirror_transaction() {
    let (_dir, db) = db_with_bankroll(dec!(1000));
    let fake = FakeSupabaseState::new(dec!(1000));
    let (record, sup_row) = fill_pair(&wf_key(0), Side::Buy, 10, dec!(0.40), 0);
    db.stage_dispatch_seed(&pe_paper_state::DispatchSeedRecord {
        dispatch_id: record.idempotency_key.clone(),
        signal_json: "{}".to_string(),
        source_trade_id: "s0".to_string(),
        created_at_unix: 0,
        targets: vec![pe_paper_state::DispatchTargetSeed {
            account_id: "acct".to_string(),
            credential_bundle_version: 1,
            credential_key_id: "k".to_string(),
        }],
    })
    .unwrap();

    commit_fill_authoritative(
        &fake,
        &db,
        &SourceTradeId("s0".into()),
        &leader(),
        &record,
        EventSeq(0),
        &sup_row,
        Some(pe_paper_state::DispatchFlip {
            dispatch_id: &record.idempotency_key,
            paper_outcome: "fill",
        }),
        None,
    )
    .await
    .unwrap();
    let ready = db.unfinalized_ready_dispatch_seeds().unwrap();
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].paper_outcome.as_deref(), Some("fill"));
    println!("PASS: AC-FLIP — dispatch flip rides the convergence transaction");
}
