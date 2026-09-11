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

use pe_core_types::{
    BasisPoints, CollateralAmount, ContractQty, KellyFraction, PolymarketConditionId,
    PolymarketTokenId, Probability, ReceivedAt, ShareAmount, SourceId, SourceTimestamp, StrategyId,
};
use pe_core_types::{
    EventSeq, MarketId, OutcomeId, Price, Side, SourceTradeId, VenueMarketId, WalletAddress,
};
use pe_event_log::{AppendReceipt, ContentType, EnvelopeIn, Writer};
use pe_execution_core::{
    AdmissionReceipts, BalanceAudit, ECONOMIC_PREPARED_VERSION, EconomicPrepared, FeeAudit,
    LadderAskAudit, LadderPlanAudit, LiveAdmissionArtifactAudit, LiveMarketEvidenceAudit,
    MarketSelection, ObservationEvidence, RiskAudit, RiskDecisionAudit, SizingAudit,
    SizingModeAudit,
};
use pe_paper_state::{FillRecord, PaperStateDb};
use pe_resolver_card::{
    VENUE_SETTLEMENT_SCHEMA_VERSION, VenueResolutionStatus, VenueSettlementRecord,
};
use pe_risk_engine::RiskSnapshot;
use pe_service::paper_recovery::{
    CanonicalFillResult, CanonicalResolutionResult, ExpectedAuthority, FinancialPayload,
    FinancialResult, PaperFillOperationIdentity, PaperLogFrame, PaperLogRecord,
    QualificationStarted, TailBinding, scan_paper_log,
};
use pe_service::risk_inputs::SourceReceiptIndex;
use pe_service::supabase_sink::SupabaseFillRow;
use pe_service::supabase_state::{
    CanonicalFill, FillV2Outcome, PreparedFillRequest, PreparedResolutionRequest, SourceEvidence,
    SupabaseBootTrait, SupabaseStateError, SupabaseStateTrait, reconcile_active_financial_frames,
    resolve_event_frames, supabase_authoritative_boot,
};
use pe_venue_core::OrderIntent;
use pe_venue_polymarket::CompactFeeSchedule;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::sync::Arc;
use tempfile::TempDir;
use time::OffsetDateTime;

mod support;
use support::{LegacyFillSource, LegacyPaperFill};

fn wallet_hex() -> &'static str {
    "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
}

fn wf_key(seq: u64) -> String {
    format!("wf|{}|s{seq}|0xmkt|0|buy|17000000{seq:02}", wallet_hex())
}

fn market() -> MarketId {
    MarketId(VenueMarketId("0xmkt".to_string()))
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
    fail_all_commits: bool,
    fail_commit_seq: Option<i64>,
    apply_then_error_seq: Option<i64>,
    fail_boot_positions: bool,
    prepared: Mutex<PreparedAuthority>,
}

#[derive(Default)]
struct PreparedAuthority {
    start: Option<AppendReceipt>,
    last_prepared: Option<EventSeq>,
    fills: HashMap<String, (PreparedFillRequest, CanonicalFillResult)>,
    resolutions: HashMap<String, (PreparedResolutionRequest, CanonicalResolutionResult)>,
    mutations: usize,
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

    fn prepared(initial: Decimal, start: AppendReceipt) -> Self {
        let state = Self::new(initial);
        state.prepared.lock().unwrap().start = Some(start);
        state
    }

    fn prepared_mutations(&self) -> usize {
        self.prepared.lock().unwrap().mutations
    }

    /// The v1-arithmetic apply (insert gate + netting + clamped debit), returning the row.
    fn apply(&self, row: &SupabaseFillRow) -> CanonicalFill {
        const ATOMIC_SCALE: u64 = 1_000_000;
        assert_eq!(row.fill.quantity.atomic() % ATOMIC_SCALE, 0);
        let contracts = row.fill.quantity.atomic() / ATOMIC_SCALE;
        let mut bankroll = self.bankroll.lock().unwrap();
        let mut positions = self.positions.lock().unwrap();
        let pos_key = (row.fill.market_id.0.0.clone(), row.fill.outcome_id.0);
        let (long, short) = positions.get(&pos_key).copied().unwrap_or((0, 0));
        positions.insert(
            pos_key,
            apply_fill_to_net(long, short, row.fill.side, contracts),
        );
        let notional = row.fill.principal.to_decimal();
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
                quantity: row.fill.quantity,
                fill_price: row.fill.fill_price,
                principal: row.fill.principal,
                fee: row.fill.fee,
            },
            event_seq: row.fill.event_seq,
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
    async fn commit_prepared_fill(
        &self,
        request: &PreparedFillRequest,
    ) -> Result<CanonicalFillResult, SupabaseStateError> {
        let mut prepared = self.prepared.lock().unwrap();
        if prepared.start != Some(request.expected_authority.qualification_start_receipt) {
            return Err(SupabaseStateError::Conflict {
                reason: "financial Start differs".to_owned(),
            });
        }
        if let Some((stored_request, stored_result)) = prepared.fills.get(&request.idempotency_key)
        {
            if stored_request != request
                || prepared.last_prepared != Some(request.prepared_receipt.sequence)
            {
                return Err(SupabaseStateError::Conflict {
                    reason: "fill retry identity or economics changed".to_owned(),
                });
            }
            let mut result = stored_result.clone();
            result.outcome = "existing".to_owned();
            return Ok(result);
        }
        if prepared.last_prepared != request.expected_authority.prior_completed_prepared_sequence {
            return Err(SupabaseStateError::Conflict {
                reason: "fill predecessor differs".to_owned(),
            });
        }
        let debit = request
            .principal
            .checked_add(request.fee)
            .map_err(|error| SupabaseStateError::Corrupt(error.to_string()))?
            .to_decimal();
        let mut bankroll = self.bankroll.lock().unwrap();
        *bankroll = bankroll
            .checked_sub(debit)
            .filter(|value| *value >= Decimal::ZERO)
            .ok_or_else(|| SupabaseStateError::Conflict {
                reason: "fill debit exceeds bankroll".to_owned(),
            })?;
        let result = CanonicalFillResult {
            outcome: "applied".to_owned(),
            bankroll: *bankroll,
            applied_prepared_seq: request.prepared_receipt.sequence,
            quantity: request.quantity,
            principal: request.principal,
            fee: request.fee,
            fill_price: request.fill_price,
        };
        prepared.fills.insert(
            request.idempotency_key.clone(),
            (request.clone(), result.clone()),
        );
        prepared.last_prepared = Some(request.prepared_receipt.sequence);
        prepared.mutations += 1;
        Ok(result)
    }

    async fn apply_prepared_resolution(
        &self,
        request: &PreparedResolutionRequest,
    ) -> Result<CanonicalResolutionResult, SupabaseStateError> {
        let mut prepared = self.prepared.lock().unwrap();
        if prepared.start != Some(request.expected_authority.qualification_start_receipt) {
            return Err(SupabaseStateError::Conflict {
                reason: "financial Start differs".to_owned(),
            });
        }
        if let Some((stored_request, stored_result)) =
            prepared.resolutions.get(&request.condition.0)
        {
            if stored_request != request
                || prepared.last_prepared != Some(request.prepared_receipt.sequence)
            {
                return Err(SupabaseStateError::Conflict {
                    reason: "resolution retry identity or economics changed".to_owned(),
                });
            }
            let mut result = stored_result.clone();
            result.outcome = "existing".to_owned();
            return Ok(result);
        }
        if prepared.last_prepared != request.expected_authority.prior_completed_prepared_sequence {
            return Err(SupabaseStateError::Conflict {
                reason: "resolution predecessor differs".to_owned(),
            });
        }
        let result = CanonicalResolutionResult {
            outcome: "applied".to_owned(),
            bankroll: self.bankroll(),
            applied_prepared_seq: request.prepared_receipt.sequence,
            credit: CollateralAmount::ZERO,
            settled_at_unix: request.settled_at_unix,
        };
        prepared.resolutions.insert(
            request.condition.0.clone(),
            (request.clone(), result.clone()),
        );
        prepared.last_prepared = Some(request.prepared_receipt.sequence);
        prepared.mutations += 1;
        Ok(result)
    }

    async fn commit_fill_v2(
        &self,
        row: &SupabaseFillRow,
    ) -> Result<FillV2Outcome, SupabaseStateError> {
        if self.fail_all_commits
            || self.fail_commit_seq == Some(i64::try_from(row.fill.event_seq.0).unwrap())
        {
            return Err(SupabaseStateError::Status(503, "injected".to_string()));
        }
        if self.apply_then_error_seq == Some(i64::try_from(row.fill.event_seq.0).unwrap())
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
        self.commit_calls
            .lock()
            .unwrap()
            .push(i64::try_from(row.fill.event_seq.0).unwrap());
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
                        long: ShareAmount::from_whole(*long_contracts).unwrap(),
                        short: ShareAmount::from_whole(*short_contracts).unwrap(),
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
    db.upsert_position(
        &stale,
        OutcomeId(0),
        ShareAmount::from_whole(9).unwrap(),
        ShareAmount::ZERO,
    )
    .unwrap();
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
    db.upsert_position(
        &stale,
        OutcomeId(0),
        ShareAmount::from_whole(9).unwrap(),
        ShareAmount::ZERO,
    )
    .unwrap();
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

fn active_receipt(sequence: u64, byte: u8) -> AppendReceipt {
    AppendReceipt {
        sequence: EventSeq(sequence),
        this_hash: blake3::Hash::from_bytes([byte; 32]),
    }
}

fn empty_tail() -> TailBinding {
    TailBinding {
        physical_tail: 5,
        last_sequence: None,
        last_hash: "00".repeat(32),
    }
}

fn active_start_record() -> PaperLogRecord {
    PaperLogRecord::QualificationStarted(Box::new(QualificationStarted {
        starting_bankroll: CollateralAmount::from_decimal_exact(dec!(10)).unwrap(),
        paper_prefix: empty_tail(),
        source_prefix: empty_tail(),
        live_prefix: empty_tail(),
        artifact_blake3: "artifact".to_owned(),
        static_config_hash: "static".to_owned(),
        hot_config_hash: "config".to_owned(),
        generation: "golden".to_owned(),
        activation_id: "golden".to_owned(),
        ranking_batch_id: 545,
        membership: vec![WalletAddress::from_hex(wallet_hex()).unwrap()],
        membership_proofs_hash: "proof".to_owned(),
        schema_version: 3,
        parser_version: 1,
        financial_semantic_version: 1,
    }))
}

fn active_economic(
    source_receipt: AppendReceipt,
    financial_prefix: AppendReceipt,
) -> EconomicPrepared {
    let price = Price::new(dec!(0.5)).unwrap();
    let shares = ShareAmount::from_whole(2).unwrap();
    let principal = CollateralAmount::from_decimal_exact(dec!(1)).unwrap();
    EconomicPrepared {
        version: ECONOMIC_PREPARED_VERSION,
        market: MarketSelection {
            condition_id: PolymarketConditionId("condition".to_owned()),
            outcome_index: 0,
            token_id: PolymarketTokenId("token-0".to_owned()),
            side: Side::Buy,
            market_id: "condition".to_owned(),
        },
        admission: LiveAdmissionArtifactAudit {
            market: LiveMarketEvidenceAudit {
                condition_id: PolymarketConditionId("condition".to_owned()),
                ordered_outcome_token_ids: [
                    PolymarketTokenId("token-0".to_owned()),
                    PolymarketTokenId("token-1".to_owned()),
                ],
                neg_risk: false,
                minimum_tick_size: Price::new(dec!(0.01)).unwrap(),
                minimum_order_size: shares,
                observed_at_unix: 1_800_000_000,
                schema_version: 1,
                parser_version: 1,
                freshness_window_secs: 60,
            },
            settlement: VenueSettlementRecord {
                schema_version: VENUE_SETTLEMENT_SCHEMA_VERSION,
                condition_id: PolymarketConditionId("condition".to_owned()),
                status: VenueResolutionStatus::Unresolved,
                raw_evidence_hash: "settlement".to_owned(),
                source_timestamp_unix: Some(1_800_000_000),
                observed_at_unix: 1_800_000_000,
                parser_version: 1,
                freshness_window_secs: 60,
            },
            fee_schedule: CompactFeeSchedule::Zero,
            scheduled_end_unix: Some(1_800_003_600),
            receipts: AdmissionReceipts {
                gamma: active_receipt(1, 1),
                clob_long: active_receipt(2, 2),
                clob_compact: active_receipt(3, 3),
            },
        },
        ladder: LadderPlanAudit {
            used_asks: vec![LadderAskAudit { price, shares }],
            best_ask: price,
            limit_price: price,
            minimum_shares: shares,
            principal,
        },
        book_receipt: active_receipt(4, 4),
        observation: Some(ObservationEvidence {
            source_receipt,
            complete_bound_receipt: source_receipt,
            observed_unix_ms: 1_700_000_000_000,
            provenance: "scenario_rest".to_owned(),
        }),
        sizing: SizingAudit {
            mode: SizingModeAudit::Kelly {
                fraction: KellyFraction::new(dec!(0.25)).unwrap(),
                probability: Probability::new(dec!(0.6)).unwrap(),
            },
            budget: principal,
            principal,
            minimum_shares: shares,
            expected_shares: shares,
            expected_vwap: price,
            all_in_price: price,
            slippage_rate: Decimal::ZERO,
        },
        fee: FeeAudit {
            schedule: CompactFeeSchedule::Zero,
            expected_fee: CollateralAmount::ZERO,
            reserve: CollateralAmount::ZERO,
        },
        risk: RiskAudit {
            financial_prefix,
            snapshot: RiskSnapshot {
                leader_exposure_bps: BasisPoints::ZERO,
                market_exposure_bps: BasisPoints::ZERO,
                family_exposure_bps: BasisPoints::ZERO,
                total_copy_exposure_bps: BasisPoints::ZERO,
                intraday_pnl_bps: BasisPoints::ZERO,
                rolling_7d_pnl_bps: BasisPoints::ZERO,
                absolute_pnl_bps: BasisPoints::ZERO,
                copy_latency_kill_switch_active: false,
                proposed_trade_bps: BasisPoints(1_000),
                per_trade_cap_bps: 1_000,
                concentration_caps: None,
            },
            decision: RiskDecisionAudit::Approved,
            price_receipts: Vec::new(),
            evaluated_at_unix_ms: 1_800_000_000_000,
        },
        balance: BalanceAudit {
            cash_before: CollateralAmount::from_decimal_exact(dec!(10)).unwrap(),
            worst_case_debit: principal,
            price_impact_cap_bps: 100,
            chase_ceiling: price,
            band_floor: Price::ZERO,
            band_ceiling_exclusive: Price::ONE,
        },
        applied_configuration_hash: "config".to_owned(),
    }
}

fn append_active_record(writer: &mut Writer, record: &PaperLogRecord) -> AppendReceipt {
    let timestamp = OffsetDateTime::from_unix_timestamp(1_800_000_000).unwrap();
    writer
        .append_synced(EnvelopeIn {
            source_id: SourceId("pe-service.paper".to_owned()),
            schema_version: 2,
            parser_version: 1,
            observed_at: SourceTimestamp(timestamp),
            received_at: ReceivedAt(timestamp),
            content_type: ContentType::Json,
            payload: serde_json::to_vec(record).unwrap(),
        })
        .unwrap()
}

fn append_source_observation(writer: &mut Writer) -> AppendReceipt {
    let timestamp = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    writer
        .append_synced(EnvelopeIn {
            source_id: SourceId("scenario.activity".to_owned()),
            schema_version: 1,
            parser_version: 1,
            observed_at: SourceTimestamp(timestamp),
            received_at: ReceivedAt(timestamp),
            content_type: ContentType::Json,
            payload: br#"{"type":"TRADE"}"#.to_vec(),
        })
        .unwrap()
}

#[derive(Debug, Clone, Copy)]
enum FillCrashSeam {
    PreparedAppend,
    AuthorityResponse,
    LocalProjection,
    FinalAppend,
}

/// I16-C8-CRASH-MATRIX
///
/// Preconditions: each case starts from the same synchronized Start and Prepared fill, then stops
/// after Prepared append, authority response, local projection, or Final append.
/// PASS: boot recovery converges each case to one authority mutation, one exact local fill, and one
/// FinancialFinal; a second restart is a no-op.
/// FAIL: money is applied twice, more than one Final exists, or any seam remains unmatched.
#[tokio::test]
async fn active_fill_crash_matrix_converges_once() {
    for (seam, use_index) in [
        (FillCrashSeam::PreparedAppend, false),
        (FillCrashSeam::AuthorityResponse, false),
        (FillCrashSeam::LocalProjection, false),
        (FillCrashSeam::FinalAppend, false),
        (FillCrashSeam::PreparedAppend, true),
        (FillCrashSeam::AuthorityResponse, true),
        (FillCrashSeam::LocalProjection, true),
        (FillCrashSeam::FinalAppend, true),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let paper_log = dir.path().join("active-paper.log");
        let source_log = dir.path().join("active-source.log");
        let mut source_writer = Writer::open(&source_log).unwrap();
        let source_receipt = append_source_observation(&mut source_writer);
        drop(source_writer);
        let source_index = SourceReceiptIndex::replay(&source_log).unwrap();
        let source_evidence = if use_index {
            SourceEvidence::Index(&source_index)
        } else {
            SourceEvidence::Log(&source_log)
        };
        let mut writer = Writer::open(&paper_log).unwrap();
        let start = append_active_record(&mut writer, &active_start_record());
        let state = PaperStateDb::open(&dir.path().join("active-paper.db")).unwrap();
        state
            .reset_financial_era(
                start,
                CollateralAmount::from_decimal_exact(dec!(10)).unwrap(),
            )
            .unwrap();
        let authority = FakeSupabaseState::prepared(dec!(10), start);
        let expected = ExpectedAuthority {
            qualification_start_receipt: start,
            prior_completed_prepared_sequence: None,
        };
        let operation = PaperFillOperationIdentity {
            leader_wallet: WalletAddress::from_hex(wallet_hex()).unwrap(),
            source_trade_id: SourceTradeId("g2:golden-fill".to_owned()),
            observed_at_bucket: 1_700_000_000,
        };
        let gate = install_aged_continuation_five_checkpoint(
            &dir.path().join("active-paper.db"),
            &operation,
        );
        let payload = FinancialPayload::Fill {
            operation: operation.clone(),
            economic: active_economic(source_receipt, start),
        };
        let prepared_receipt = append_active_record(
            &mut writer,
            &PaperLogRecord::FinancialPrepared {
                expected_authority: expected.clone(),
                payload: payload.clone(),
            },
        );
        let request = PreparedFillRequest::from_prepared(
            expected.clone(),
            prepared_receipt,
            &operation,
            match &payload {
                FinancialPayload::Fill { economic, .. } => economic,
                FinancialPayload::Resolution { .. } => unreachable!(),
            },
        );

        let prior_result = if matches!(
            seam,
            FillCrashSeam::AuthorityResponse
                | FillCrashSeam::LocalProjection
                | FillCrashSeam::FinalAppend
        ) {
            Some(authority.commit_prepared_fill(&request).await.unwrap())
        } else {
            None
        };
        if matches!(
            seam,
            FillCrashSeam::LocalProjection | FillCrashSeam::FinalAppend
        ) {
            let canonical = prior_result.as_ref().unwrap();
            state
                .apply_financial_fill(
                    start,
                    None,
                    prepared_receipt.sequence,
                    source_receipt,
                    1_700_000_000,
                    &FillRecord {
                        idempotency_key: request.idempotency_key.clone(),
                        market_id: MarketId(VenueMarketId(request.market_id.clone())),
                        outcome_id: OutcomeId(request.outcome_id),
                        side: request.side,
                        quantity: request.quantity,
                        fill_price: request.fill_price,
                        principal: request.principal,
                        fee: request.fee,
                    },
                    canonical.bankroll,
                )
                .unwrap();
        }
        if matches!(seam, FillCrashSeam::FinalAppend) {
            append_active_record(
                &mut writer,
                &PaperLogRecord::FinancialFinal {
                    prepared_receipt,
                    result: FinancialResult::Fill {
                        canonical: prior_result.clone().unwrap(),
                    },
                },
            );
        }

        let appended = reconcile_active_financial_frames(
            &authority,
            &state,
            &paper_log,
            source_evidence,
            &mut writer,
        )
        .await
        .unwrap();
        assert_eq!(
            appended,
            usize::from(!matches!(seam, FillCrashSeam::FinalAppend)),
            "unexpected recovered Final count at {seam:?}"
        );
        assert_eq!(authority.prepared_mutations(), 1, "seam {seam:?}");
        assert_eq!(state.list_fills().unwrap().len(), 1);
        assert_eq!(
            state.financial_snapshot(1_800_000_001).unwrap().cash,
            dec!(9)
        );
        let frames = scan_paper_log(&paper_log).unwrap();
        assert_eq!(
            frames
                .iter()
                .filter(|frame| matches!(
                    frame.frame,
                    PaperLogFrame::Record(PaperLogRecord::FinancialFinal { .. })
                ))
                .count(),
            1,
            "seam {seam:?}"
        );
        assert_eq!(
            reconcile_active_financial_frames(
                &authority,
                &state,
                &paper_log,
                source_evidence,
                &mut writer,
            )
            .await
            .unwrap(),
            0
        );
        assert_eq!(authority.prepared_mutations(), 1);
        let terminal = state
            .decision_pending_for(&operation.source_trade_id)
            .unwrap()
            .unwrap();
        let replayed = pe_service::decision_replay::replay_decision_pending(&terminal).unwrap();
        assert_eq!(replayed.post_boundary.body.terminal.disposition, "fill");
        assert_eq!(
            replayed
                .post_boundary
                .body
                .clocks
                .iter()
                .filter(|clock| clock.purpose == "paper_prepared_staleness_gate")
                .collect::<Vec<_>>(),
            vec![&gate]
        );
    }
}

/// The already-prepared crash matrix needs a stored checkpoint, not a new admission attempt.
/// Keep the v5 wire fixture and checkpoint serializer's field order explicit in this fixture.
fn install_aged_continuation_five_checkpoint(
    state_path: &std::path::Path,
    operation: &PaperFillOperationIdentity,
) -> pe_service::decision_replay::DecisionClockEvidence {
    use pe_service::decision_replay::DecisionClockEvidence;
    #[derive(serde::Serialize)]
    struct CheckpointBody<'a> {
        version: u16,
        owners: [&'static str; 2],
        source_trade_id: &'a SourceTradeId,
        applied_configuration_hash: &'a str,
        market_end: Option<()>,
        market_price: Option<()>,
        book: Option<()>,
        clocks: Vec<DecisionClockEvidence>,
    }
    let mut continuation: pe_service::bucket_commit::DecisionContinuationV3 =
        serde_json::from_str(include_str!("fixtures/decision_continuation_v5.json")).unwrap();
    continuation.facts.source_trade_id = operation.source_trade_id.clone();
    continuation.facts.wallet = operation.leader_wallet;
    // Recovery runs at the matrix's much later financial time, but must retain this accepted gate.
    continuation.facts.source_epoch = operation.observed_at_bucket;
    let gate =
        DecisionClockEvidence::precise("paper_prepared_staleness_gate", 1_700_000_002_000_000_000)
            .unwrap();
    let body = CheckpointBody {
        version: pe_service::decision_replay::POST_BOUNDARY_EVIDENCE_VERSION,
        owners: ["source_log", "paper_log"],
        source_trade_id: &operation.source_trade_id,
        applied_configuration_hash: &continuation.facts.applied_configuration_hash,
        market_end: None,
        market_price: None,
        book: None,
        clocks: vec![
            gate.clone(),
            DecisionClockEvidence {
                purpose: "paper_dispatch".to_owned(),
                unix_millis: gate.unix_millis,
                submillisecond_nanos: None,
            },
        ],
    };
    let hash = blake3::hash(&serde_json::to_vec(&(1_u32, &body)).unwrap())
        .to_hex()
        .to_string();
    let mut document = serde_json::to_value(body).unwrap();
    document["financial_semantic_version"] = serde_json::json!(1);
    document["document_blake3"] = serde_json::json!(hash);
    rusqlite::Connection::open(state_path).unwrap().execute(
        "INSERT INTO decision_pending (source_trade_id, semantic_revision, wallet_hex, source_epoch, frozen_inputs_json, post_commit_inputs_json, state, terminal_disposition, updated_at_unix) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'open', NULL, ?4)",
        rusqlite::params![operation.source_trade_id.0, continuation.facts.semantic_revision, operation.leader_wallet.to_string(), continuation.facts.source_epoch, serde_json::to_string(&continuation).unwrap(), document.to_string()],
    ).unwrap();
    gate
}

fn assert_authority_conflict<T>(result: Result<T, SupabaseStateError>, field: &str) {
    assert!(
        matches!(result, Err(SupabaseStateError::Conflict { .. })),
        "changed {field} must return the typed authority conflict"
    );
}

/// I16-C8-CONFLICT-AUTHORITY
///
/// Preconditions: the fake authority has accepted one exact fill and one exact resolution.
/// PASS: every changed fill identity/economic input and resolution condition/payout/time/era/
/// sequence input returns `SupabaseStateError::Conflict` without another mutation.
/// FAIL: a changed retry is reported existing/applied or changes the authority mutation count.
#[tokio::test]
async fn prepared_authority_changed_field_conflict_matrix() {
    let start = active_receipt(10, 10);
    let authority = FakeSupabaseState::prepared(dec!(10), start);
    let expected = ExpectedAuthority {
        qualification_start_receipt: start,
        prior_completed_prepared_sequence: None,
    };
    let operation = PaperFillOperationIdentity {
        leader_wallet: WalletAddress::from_hex(wallet_hex()).unwrap(),
        source_trade_id: SourceTradeId("g2:authority-fill".to_owned()),
        observed_at_bucket: 1_800_000_000,
    };
    let economic = active_economic(active_receipt(5, 5), start);
    let request =
        PreparedFillRequest::from_prepared(expected, active_receipt(11, 11), &operation, &economic);
    authority.commit_prepared_fill(&request).await.unwrap();
    assert_eq!(authority.prepared_mutations(), 1);
    assert_eq!(
        authority
            .commit_prepared_fill(&request)
            .await
            .unwrap()
            .outcome,
        "existing"
    );

    let mut fill_retries = Vec::new();
    macro_rules! changed_fill {
        ($field:literal, $change:expr) => {{
            let mut changed = request.clone();
            $change(&mut changed);
            fill_retries.push(($field, changed));
        }};
    }
    changed_fill!("idempotency_key", |value: &mut PreparedFillRequest| value
        .idempotency_key =
        "different".to_owned());
    changed_fill!("leader_wallet", |value: &mut PreparedFillRequest| value
        .leader_wallet =
        WalletAddress::from_hex("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",).unwrap());
    changed_fill!("source_trade_id", |value: &mut PreparedFillRequest| value
        .source_trade_id =
        SourceTradeId("g2:different".to_owned()));
    changed_fill!("market_id", |value: &mut PreparedFillRequest| value
        .market_id =
        "different".to_owned());
    changed_fill!("outcome_id", |value: &mut PreparedFillRequest| value
        .outcome_id =
        1);
    changed_fill!("side", |value: &mut PreparedFillRequest| value.side =
        Side::Sell);
    changed_fill!("quantity", |value: &mut PreparedFillRequest| value
        .quantity =
        ShareAmount::from_decimal_exact(dec!(2.000001)).unwrap());
    changed_fill!("fill_price", |value: &mut PreparedFillRequest| value
        .fill_price =
        Price::new(dec!(0.500001)).unwrap());
    changed_fill!("principal", |value: &mut PreparedFillRequest| value
        .principal =
        CollateralAmount::from_decimal_exact(dec!(1.000001)).unwrap());
    changed_fill!("fee", |value: &mut PreparedFillRequest| value.fee =
        CollateralAmount::from_decimal_exact(dec!(0.000001)).unwrap());
    changed_fill!("entry_unix", |value: &mut PreparedFillRequest| value
        .entry_unix +=
        1);
    changed_fill!("era", |value: &mut PreparedFillRequest| value
        .expected_authority
        .qualification_start_receipt =
        active_receipt(10, 99));
    changed_fill!("prior_sequence", |value: &mut PreparedFillRequest| value
        .expected_authority
        .prior_completed_prepared_sequence =
        Some(EventSeq(9)));
    changed_fill!("prepared_sequence", |value: &mut PreparedFillRequest| {
        value.prepared_receipt = active_receipt(12, 12)
    });
    for (field, changed) in fill_retries {
        assert_authority_conflict(authority.commit_prepared_fill(&changed).await, field);
    }

    let resolution = PreparedResolutionRequest {
        expected_authority: ExpectedAuthority {
            qualification_start_receipt: start,
            prior_completed_prepared_sequence: Some(EventSeq(11)),
        },
        prepared_receipt: active_receipt(12, 12),
        condition: PolymarketConditionId("condition".to_owned()),
        payout_by_outcome_index_json: "[\"1\",\"0\"]".to_owned(),
        settled_at_unix: 1_800_000_100,
    };
    authority
        .apply_prepared_resolution(&resolution)
        .await
        .unwrap();
    assert_eq!(authority.prepared_mutations(), 2);
    assert_eq!(
        authority
            .apply_prepared_resolution(&resolution)
            .await
            .unwrap()
            .outcome,
        "existing"
    );
    let mut resolution_retries = Vec::new();
    let mut changed = resolution.clone();
    changed.condition = PolymarketConditionId("different".to_owned());
    resolution_retries.push(("condition", changed));
    let mut changed = resolution.clone();
    changed.payout_by_outcome_index_json = "[\"0\",\"1\"]".to_owned();
    resolution_retries.push(("payout", changed));
    let mut changed = resolution.clone();
    changed.settled_at_unix += 1;
    resolution_retries.push(("settled_at_unix", changed));
    let mut changed = resolution.clone();
    changed.expected_authority.qualification_start_receipt = active_receipt(10, 99);
    resolution_retries.push(("era", changed));
    let mut changed = resolution.clone();
    changed.expected_authority.prior_completed_prepared_sequence = None;
    resolution_retries.push(("prior_sequence", changed));
    let mut changed = resolution.clone();
    changed.prepared_receipt = active_receipt(13, 13);
    resolution_retries.push(("prepared_sequence", changed));
    for (field, changed) in resolution_retries {
        assert_authority_conflict(authority.apply_prepared_resolution(&changed).await, field);
    }
    assert_eq!(authority.prepared_mutations(), 2);
}
