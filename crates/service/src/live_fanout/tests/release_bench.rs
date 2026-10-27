//! AC12 release measurements for #808. Every generated order is production's wire-two shape (its
//! admission also verifies the copy deadline) and passes strict replay. Fixture generation,
//! retained evidence, source indexes and ancillary restart dependencies are outside the measured
//! windows. The gate window is six summary reads, two token reads, two risk compositions and the
//! newest order's admission and prepared-order applications, all local computation; cold_restart
//! includes one LiveJournal::open (file verification and decoding) through the first sync_live
//! publication of both financial and recovery state. VmHWM is the process high-water RSS, including setup; use
//! the fresh-process test to exclude history generation from that process's high-water mark.
//!
//! Run from the repository root with `PAPER_LOG` set to a copy of production's `paper.log` (CPU,
//! profile, frame count and command are printed):
//! ```text
//! CARGO_BUILD_JOBS=6 BENCH_PAPER_LOG="$PAPER_LOG" BENCH_SHAPE=settled BENCH_ORDERS=50 cargo test --release -p pe-service --all-features --lib live_fanout::tests::release_bench::live_state_gate_reads_and_cold_restart -- --exact --ignored --nocapture
//! CARGO_BUILD_JOBS=6 BENCH_PAPER_LOG="$PAPER_LOG" BENCH_SHAPE=settled BENCH_ORDERS=5000 cargo test --release -p pe-service --all-features --lib live_fanout::tests::release_bench::live_state_gate_reads_and_cold_restart -- --exact --ignored --nocapture
//! CARGO_BUILD_JOBS=6 BENCH_PAPER_LOG="$PAPER_LOG" BENCH_SHAPE=settled BENCH_ORDERS=5000 BENCH_OPEN=95 cargo test --release -p pe-service --all-features --lib live_fanout::tests::release_bench::live_state_gate_reads_and_cold_restart -- --exact --ignored --nocapture
//! CARGO_BUILD_JOBS=6 BENCH_PAPER_LOG="$PAPER_LOG" BENCH_SHAPE=open BENCH_ORDERS=5000 cargo test --release -p pe-service --all-features --lib live_fanout::tests::release_bench::live_state_gate_reads_and_cold_restart -- --exact --ignored --nocapture
//! CARGO_BUILD_JOBS=6 BENCH_DIR="$PWD/target/ac12-restart" BENCH_PAPER_LOG="$PAPER_LOG" BENCH_ORDERS=5000 cargo test --release -p pe-service --all-features --lib live_fanout::tests::release_bench::live_state_gate_reads_and_cold_restart -- --exact --ignored --nocapture
//! CARGO_BUILD_JOBS=6 BENCH_DIR="$PWD/target/ac12-restart" cargo test --release -p pe-service --all-features --lib live_fanout::tests::release_bench::live_state_cold_restart -- --exact --ignored --nocapture
//! ```
//! BENCH_OPEN (default 10) unresolved orders, each in its own market, follow the settled history.
//! The open stress shape leaves all BENCH_ORDERS unresolved in one shared market. Without
//! BENCH_PAPER_LOG the paper prefix has one frame.
//! BENCH_DIR retains a fixture subdirectory and expected.json; it must not already contain a
//! benchmark snapshot. Remove that directory after the fresh-process run when no longer needed.

use super::resolution_and_cutoffs::{
    append_resolution_to_state, condition_prepared, push_custody, push_fill,
    push_redemption_attempt, retain_source,
};
use super::*;
use std::path::Path;
use std::time::{Duration as StdDuration, Instant};

const GENERATING_TEST: &str =
    "live_fanout::tests::release_bench::live_state_gate_reads_and_cold_restart";
const RESTART_TEST: &str = "live_fanout::tests::release_bench::live_state_cold_restart";

fn proc_status_kib(field: &str) -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .unwrap()
        .lines()
        .find_map(|line| {
            line.strip_prefix(field)
                .map(|rest| rest.trim().trim_end_matches(" kB").trim().parse().unwrap())
        })
        .unwrap()
}

fn cpu() -> String {
    std::fs::read_to_string("/proc/cpuinfo")
        .unwrap()
        .lines()
        .find_map(|line| line.strip_prefix("model name\t:").map(str::trim))
        .unwrap()
        .to_owned()
}

fn release_command(test: &str) -> String {
    format!(
        "CARGO_BUILD_JOBS=6 cargo test --release -p pe-service --all-features --lib {test} -- --exact --ignored --nocapture"
    )
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .map(|value| value.parse().unwrap())
        .unwrap_or(default)
}

/// A Baseline whose collateral keeps thousands of fixture fills far inside the risk caps.
async fn large_baseline(state: &FanoutState, account_id: &AccountId, now: OffsetDateTime) {
    let cash = CollateralAmount::from_atomic(10_000_000_000_000);
    let binding = account_binding_fixture(account_id, "0x1111111111111111111111111111111111111111");
    let (mut position_evidence, source_envelopes) = empty_position_evidence(0, now, &binding);
    for (page, envelope) in position_evidence.pages.iter_mut().zip(source_envelopes) {
        page.receipt = retain_source(state, envelope).await;
    }
    state
        .config
        .journal
        .append(
            account_id.clone(),
            now,
            LiveJournalPayload::AccountPortfolioMarked(Box::new(
                pe_execution_core::AccountPortfolioMarkedAudit {
                    kind: MarkKind::Baseline,
                    cutoff_unix: now.unix_timestamp(),
                    account_binding: binding.clone(),
                    account_state: account_state_fixture_for_binding(cash, now, &binding),
                    venue_positions: Vec::new(),
                    venue_position_evidence: position_evidence,
                    marked_positions: Vec::new(),
                    prices: Vec::new(),
                    equity: cash,
                },
            )),
        )
        .unwrap();
}

/// Production composes wire-two economics, whose admission also verifies the copy deadline.
fn wire_two(
    mut prepared: Box<pe_execution_core::LiveOrderPreparedAudit>,
) -> Box<pe_execution_core::LiveOrderPreparedAudit> {
    prepared.economic.version = pe_execution_core::economic::PAPER_ECONOMIC_PREPARED_VERSION;
    prepared.economic.balance.chase_ceiling = Price::ONE;
    prepared
}

fn bench_prepared(
    index: usize,
    separate_conditions: bool,
) -> Box<pe_execution_core::LiveOrderPreparedAudit> {
    let number = u64::try_from(index).unwrap();
    if !separate_conditions {
        let mut prepared = wire_two(finality_prepared());
        prepared.identity = projection_identity(&format!("bench-{index}"));
        prepared.prepared.order_hash = format!("0x{:064x}", 0xb000_0000_u64 + number);
        return prepared;
    }
    let mut prepared = wire_two(condition_prepared(
        "88",
        10_000 + number * 2,
        &format!("bench-{index}"),
    ));
    // The helper's repeated-byte identity covers 256 conditions; the benchmark needs 5,000.
    let condition = PolymarketConditionId(format!("0x{:064x}", 0xa000_0000_u64 + number));
    prepared
        .identity
        .fill_projection
        .as_mut()
        .unwrap()
        .market_id = condition.0.clone();
    prepared.economic.market.market_id = condition.0.clone();
    prepared.economic.market.condition_id = condition.clone();
    prepared.economic.admission.market.condition_id = condition.clone();
    prepared.economic.admission.settlement.condition_id = condition.clone();
    prepared.prepared.condition_id = condition;
    prepared.prepared.order_hash = format!("0x{:064x}", 0xb000_0000_u64 + number);
    prepared
}

/// Retain the shared economic fixture with this order's distinct token pair. Rewrite JSON string
/// values exactly, including Gamma's encoded token array, so condition IDs containing 123 or 124
/// cannot be altered by substring replacement.
async fn retain_token_economics(
    state: &FanoutState,
    prepared: &mut pe_execution_core::LiveOrderPreparedAudit,
    now: OffsetDateTime,
) {
    fn replace_tokens(
        value: &mut serde_json::Value,
        tokens: &[pe_core_types::PolymarketTokenId; 2],
    ) {
        match value {
            serde_json::Value::String(text) => match text.as_str() {
                "123" => *text = tokens[0].0.clone(),
                "124" => *text = tokens[1].0.clone(),
                "[\"123\",\"124\"]" => {
                    *text = serde_json::json!([tokens[0].0, tokens[1].0]).to_string();
                }
                _ => {}
            },
            serde_json::Value::Array(values) => {
                for value in values {
                    replace_tokens(value, tokens);
                }
            }
            serde_json::Value::Object(values) => {
                for value in values.values_mut() {
                    replace_tokens(value, tokens);
                }
            }
            _ => {}
        }
    }
    let tokens = &prepared.economic.admission.market.ordered_outcome_token_ids;
    let mut receipts = Vec::new();
    for mut source in economic_source_envelopes(prepared, now).into_iter().take(4) {
        let mut body = serde_json::from_slice(&source.payload).unwrap();
        replace_tokens(&mut body, tokens);
        source.payload = serde_json::to_vec(&body).unwrap();
        if source.source_id.0 == CLOB_LONG_MARKET_SOURCE_ID {
            prepared.economic.admission.settlement.raw_evidence_hash =
                blake3::hash(&source.payload).to_hex().to_string();
        }
        receipts.push(retain_source(state, source).await);
    }
    prepared.economic.admission.receipts = AdmissionReceipts {
        gamma: receipts[0],
        clob_long: receipts[1],
        clob_compact: receipts[2],
    };
    prepared.economic.book_receipt = receipts[3];
}

fn risk_receipts(
    derived: &ProjectionDerivation,
    prices: &BTreeMap<String, AppendReceipt>,
) -> Vec<AppendReceipt> {
    let mut receipts = derived
        .positions
        .iter()
        .map(|position| prices[&position.market_id])
        .collect::<Vec<_>>();
    receipts.sort_by_key(|receipt| receipt.sequence);
    receipts.dedup();
    receipts
}

/// Prepare all retained evidence and the strict admission outside its application timer. The
/// risk clock can precede the journal stamp by two seconds for the midnight-crossing measurement.
async fn strict_order_events(
    state: &FanoutState,
    account: &AccountId,
    prices: &BTreeMap<String, AppendReceipt>,
    evaluated_at: OffsetDateTime,
    stamp: OffsetDateTime,
    index: usize,
    separate_conditions: bool,
) -> (
    Box<pe_execution_core::LiveOrderPreparedAudit>,
    Vec<LiveJournalEvent>,
) {
    let derived = live_summary(state, account).unwrap();
    let mut prepared = bench_prepared(index, separate_conditions);
    prepared.identity.idempotency_key =
        LiveOrderIdentity::idempotency_key_for(&prepared.identity.dispatch_id, account);
    prepared.economic.admission.scheduled_end_unix = Some(evaluated_at.unix_timestamp() + 300);
    let cash =
        CollateralAmount::from_decimal_exact(derived.economic_cash.unwrap() - derived.receivable)
            .unwrap();
    prepared.economic.balance.cash_before = cash;
    prepared.account_state = account_state_fixture_for_binding(
        cash,
        evaluated_at,
        derived.account_binding.as_ref().unwrap(),
    );
    let copy_policy = crate::bucket_commit::PaperFreshnessPolicy {
        activity_ws_enabled: true,
        copy_latency_budget_secs: 120,
    };
    append_economic_sources(
        state,
        &mut prepared,
        account,
        evaluated_at,
        Some(copy_policy),
    )
    .await;
    if separate_conditions {
        retain_token_economics(state, &mut prepared, evaluated_at).await;
    }
    let receipts = risk_receipts(&derived, prices);
    let mids = replay_live_risk_prices(
        &derived.positions,
        &receipts,
        unix_millis_i64(evaluated_at),
        &[],
        Some(&state.config.source_receipts),
    )
    .unwrap();
    let frames = state.config.paper_log.snapshot().unwrap();
    let projection = prepared.identity.fill_projection.as_ref().unwrap();
    prepared.economic.risk = compose_live_risk_audit(
        &derived,
        &paper_era_at_evaluation(&frames, unix_millis_i64(evaluated_at)).unwrap(),
        &mids,
        &receipts,
        evaluated_at,
        LiveRiskProposal {
            leader_wallet: Some(projection.leader_wallet.clone()),
            market_id: Some(projection.market_id.clone()),
            proposed_debit: prepared.economic.balance.worst_case_debit,
            per_trade_cap_bps: 10_000,
            drawdown_policy: pe_risk_engine::DrawdownPolicy::KillOnly,
        },
    )
    .unwrap();
    assert_eq!(prepared.economic.risk.decision, RiskDecisionAudit::Approved);
    assert_eq!(
        prepared.economic.version,
        pe_execution_core::economic::PAPER_ECONOMIC_PREPARED_VERSION
    );
    prepared.prepared.worst_case_debit = prepared.prepared.maker_collateral;
    prepared.prepared.metadata_hashes = prepared.identity.evidence_hashes.clone();
    // Seed the stream helper with the actual tail so its finalized-fill Prepared sequence is
    // the sequence the journal will assign. The seed itself is never appended again.
    let tail = sync_live(state)
        .unwrap()
        .view(account)
        .unwrap()
        .unwrap()
        .records
        .last()
        .unwrap()
        .as_ref()
        .clone();
    let mut events = vec![tail];
    push_fill(&mut events, account, &prepared, stamp);
    events.remove(0);
    let LiveJournalPayload::OrderFillFinalized(fill) = &mut events.last_mut().unwrap().payload
    else {
        unreachable!()
    };
    // The recorded receipt's order topic must name this distinct prepared order.
    for attempt in &mut Arc::make_mut(fill).receipts {
        if let RawHttpAttempt::Response(response) = attempt
            && response.endpoint_kind == "polygon-transaction-receipt"
        {
            let mut body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
            for log in body["result"]["logs"].as_array_mut().unwrap() {
                log["topics"][1] = serde_json::json!(prepared.prepared.order_hash);
            }
            response.body = serde_json::to_vec(&body).unwrap();
        }
    }
    (prepared, events)
}

/// Append outside timing, then measure the two applications that precede POST: the strict
/// admission (financial, source, economic, copy-deadline and recovery checks) and its prepared
/// order. The matched and finalized records are completed and published afterwards.
fn apply_order(
    state: &FanoutState,
    account: &AccountId,
    events: Vec<LiveJournalEvent>,
) -> StdDuration {
    let mut events = events.into_iter();
    let mut applied = StdDuration::ZERO;
    for event in events.by_ref().take(2) {
        let written = state
            .config
            .journal
            .append(account.clone(), event.timestamp, event.payload)
            .unwrap();
        assert_eq!(written.seq, event.seq);
        let started = Instant::now();
        drop(sync_live(state).unwrap());
        applied += started.elapsed();
    }
    for event in events {
        let written = state
            .config
            .journal
            .append(account.clone(), event.timestamp, event.payload)
            .unwrap();
        assert_eq!(written.seq, event.seq);
    }
    assert_live(state, account);
    applied
}

fn assert_live(state: &FanoutState, account: &AccountId) {
    let live = sync_live(state).unwrap();
    let view = live.accounts.get(account).unwrap();
    assert!(view.rejected.is_none(), "{:?}", view.rejected);
    assert_eq!(view.applied.len(), view.records.len());
    assert_eq!(view.applied.len(), usize::try_from(live.next_seq).unwrap());
    assert!(view.applied.values().all(|count| *count == 1));
    assert!(
        view.financial
            .late_applied
            .values()
            .all(|count| *count == 1)
    );
}

async fn settle_order(
    state: &FanoutState,
    account: &AccountId,
    prepared: &pe_execution_core::LiveOrderPreparedAudit,
    now: OffsetDateTime,
) {
    append_resolution_to_state(state, account, prepared, now).await;
    let resolved = live_summary(state, account).unwrap();
    let mut events = Vec::new();
    let identity =
        push_redemption_attempt(&mut events, account, &prepared.prepared.condition_id, now);
    let mut sources = Vec::new();
    // At this point the one condition is the only receivable, and custody returns its payout.
    push_custody(
        &mut events,
        &mut sources,
        identity,
        resolved.economic_cash.unwrap(),
        &[],
        now,
        100,
    );
    let mut receipts = Vec::new();
    for source in sources {
        receipts.push(retain_source(state, source).await);
    }
    for event in events {
        let payload = match event.payload {
            LiveJournalPayload::RedemptionCustodyReconciled(custody) => {
                let mut audit = custody.as_ref().clone();
                audit.venue_position_receipts = receipts.clone();
                LiveJournalPayload::RedemptionCustodyReconciled(audit.into())
            }
            payload => payload,
        };
        state
            .config
            .journal
            .append(account.clone(), now, payload)
            .unwrap();
    }
    assert_live(state, account);
    let settled = live_summary(state, account).unwrap();
    assert!(settled.positions.is_empty());
    assert!(settled.custody_positions.is_empty());
    assert!(settled.receivable_by_condition.is_empty());
}

async fn retain_price(state: &FanoutState, market: &str, now: OffsetDateTime) -> AppendReceipt {
    retain_source(
        state,
        gamma_risk_price_source(fixture_receipt(0), market, ["0.9", "0.1"], now),
    )
    .await
}

/// Full projection history and full recovery inventory, rather than counts. Recovery's public
/// type has Eq but no Serialize; its deterministic Debug form preserves all fields in the JSON.
pub(super) fn complete_snapshot(state: &FanoutState, account: &AccountId) -> serde_json::Value {
    let live = sync_live(state).unwrap();
    serde_json::json!({
        "projection": projection_snapshot(&live.derivation(account).unwrap()),
        "recovery": format!("{:?}", live.recovery_inventory().unwrap()),
    })
}

/// Reuse the standard fixture for ancillary clients/channels, then install the retained read
/// indexes before timing. Its scratch journal is empty; the measured history is opened once.
pub(super) fn cold_restart(
    fixture: &Path,
    account: &AccountId,
    now: OffsetDateTime,
    expected: &serde_json::Value,
    label: &str,
    test: &str,
) -> LiveRecoveryInventory {
    let scratch = tempdir().unwrap();
    let db = Arc::new(PaperStateDb::open(&fixture.join("paper.db")).unwrap());
    let mut snapshot = armed_snapshot(account.as_str());
    snapshot.fetched_at_unix = Some(now.unix_timestamp());
    let mut state = fanout_state(&scratch, db, snapshot, "http://127.0.0.1:9", None);
    state.clock = Arc::new(move || now);
    state.config.source_receipts =
        SourceReceiptIndex::replay(&fixture.join("live-source.log")).unwrap();
    state.config.paper_log =
        crate::paper_recovery::PaperLog::open(fixture.join("paper.log")).unwrap();
    state.config.journal_path = fixture.join("live.journal");
    let rss_before = proc_status_kib("VmRSS:");
    let started = Instant::now();
    state.config.journal = Arc::new(LiveJournal::open(&state.config.journal_path).unwrap());
    drop(sync_live(&state).unwrap());
    let cold_restart = started.elapsed();
    assert_live(&state, account);
    assert_eq!(&complete_snapshot(&state, account), expected);
    let records = state.config.journal.records_from(0).unwrap().len();
    println!(
        "RESTART process={label} records={records} cold_restart={cold_restart:?} rss_before_kib={rss_before} rss_after_kib={} VmHWM={} cpu={:?} profile={} release_command={:?}",
        proc_status_kib("VmRSS:"),
        proc_status_kib("VmHWM:"),
        cpu(),
        if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        },
        release_command(test),
    );
    verified_recovery_inventory(&state).unwrap()
}

/// PASS: prints local gate/admission, midnight-copy and restart timings for strictly valid settled
/// or open histories, binding the actual paper prefix; every record is applied exactly once.
/// FAIL: an admission closes the view, settlement leaves inventory, or complete restart state drifts.
#[tokio::test]
#[ignore = "release measurement for #808; run with --release --ignored --nocapture"]
async fn live_state_gate_reads_and_cold_restart() {
    let orders = env_usize("BENCH_ORDERS", 50);
    let shape = std::env::var("BENCH_SHAPE").unwrap_or_else(|_| "settled".to_owned());
    assert!(matches!(shape.as_str(), "settled" | "open"));
    let open = if shape == "settled" {
        env_usize("BENCH_OPEN", 10)
    } else {
        orders
    };
    let bench_dir = std::env::var_os("BENCH_DIR").map(std::path::PathBuf::from);
    let dir = if let Some(root) = &bench_dir {
        std::fs::create_dir_all(root).unwrap();
        assert!(
            !root.join("expected.json").exists(),
            "BENCH_DIR already contains a snapshot"
        );
        tempfile::TempDir::with_prefix_in("fixture-", root).unwrap()
    } else {
        tempdir().unwrap()
    };
    let paper_copy = std::env::var_os("BENCH_PAPER_LOG");
    if let Some(source) = &paper_copy {
        // Copy before fanout_state opens PaperLog, so all admissions bind this exact prefix.
        std::fs::copy(source, dir.path().join("paper.log")).unwrap();
    }
    let db = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
    let mut state = fanout_state(&dir, db, armed_snapshot("acct"), "http://127.0.0.1:9", None);
    let account = AccountId::new("acct").unwrap();
    let frames = state.config.paper_log.snapshot().unwrap();
    let now = if paper_copy.is_some() {
        assert!(!frames.is_empty(), "BENCH_PAPER_LOG must contain frames");
        // Read the opened log, including any out-of-order timestamps. No fixture clock can
        // precede an existing paper frame, including the final one.
        frames
            .iter()
            .map(|frame| frame.envelope.received_at.0)
            .max()
            .unwrap()
            + time::Duration::seconds(1)
    } else {
        assert!(frames.is_empty());
        OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap()
    };
    if paper_copy.is_none() {
        bind_empty_live_risk_to_paper_prefix(&state, &mut finality_prepared(), now);
    }
    let paper_frames = state.config.paper_log.snapshot().unwrap().len();
    assert_eq!(
        paper_frames,
        if paper_copy.is_some() {
            frames.len()
        } else {
            1
        }
    );
    state.clock = Arc::new(move || now);
    println!(
        "GENERATED stage=paper shape={shape} orders={orders} open={open} paper_frames={paper_frames} cpu={:?} profile={} release_command={:?}",
        cpu(),
        if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        },
        release_command(GENERATING_TEST),
    );
    large_baseline(&state, &account, now).await;
    let mut prices = BTreeMap::new();
    let generation = Instant::now();
    let separate_conditions = shape == "settled";
    let total = if shape == "settled" {
        orders + open
    } else {
        orders
    };
    for index in 0..total {
        let (prepared, events) = strict_order_events(
            &state,
            &account,
            &prices,
            now,
            now,
            index,
            separate_conditions,
        )
        .await;
        apply_order(&state, &account, events);
        if shape == "settled" && index < orders {
            settle_order(&state, &account, &prepared, now).await;
        } else {
            let market = &prepared.economic.market.market_id;
            prices.insert(market.clone(), retain_price(&state, market, now).await);
        }
    }
    let generated = generation.elapsed();
    assert_eq!(
        live_summary(&state, &account).unwrap().positions.len(),
        if separate_conditions {
            open
        } else {
            usize::from(open > 0)
        }
    );
    assert_eq!(
        state.config.paper_log.snapshot().unwrap().len(),
        paper_frames
    );
    let records = state.config.journal.records_from(0).unwrap().len();
    let journal_bytes = std::fs::metadata(&state.config.journal_path).unwrap().len();
    println!(
        "GENERATED stage=history shape={shape} orders={orders} open={open} paper_frames={paper_frames} records={records} journal_bytes={journal_bytes} elapsed={generated:?}"
    );

    // All source fixtures/indexes and admission envelopes exist before the timing windows.
    let (candidate, candidate_events) = strict_order_events(
        &state,
        &account,
        &prices,
        now,
        now,
        total,
        separate_conditions,
    )
    .await;
    let derived = live_summary(&state, &account).unwrap();
    let receipts = risk_receipts(&derived, &prices);
    let sources = receipts
        .iter()
        .map(|receipt| {
            state
                .config
                .source_receipts
                .source_envelope(*receipt)
                .unwrap()
        })
        .collect::<Vec<_>>();
    let frames = state.config.paper_log.snapshot().unwrap();
    let reads = 200_u32;
    let started = Instant::now();
    for _ in 0..reads {
        std::hint::black_box(live_summary(&state, &account).unwrap());
    }
    let summary_read = started.elapsed() / reads;
    let started = Instant::now();
    for _ in 0..reads {
        std::hint::black_box(
            sync_live(&state)
                .unwrap()
                .prepared_tokens(&account)
                .unwrap(),
        );
    }
    let token_read = started.elapsed() / reads;
    let started = Instant::now();
    for _ in 0..reads {
        let era = paper_era_at_evaluation(&frames, unix_millis_i64(now)).unwrap();
        let mids = replay_live_risk_prices(
            &derived.positions,
            &receipts,
            unix_millis_i64(now),
            &sources,
            Some(&state.config.source_receipts),
        )
        .unwrap();
        let projection = candidate.identity.fill_projection.as_ref().unwrap();
        std::hint::black_box(
            compose_live_risk_audit(
                &derived,
                &era,
                &mids,
                &receipts,
                now,
                LiveRiskProposal {
                    leader_wallet: Some(projection.leader_wallet.clone()),
                    market_id: Some(projection.market_id.clone()),
                    proposed_debit: candidate.economic.balance.worst_case_debit,
                    per_trade_cap_bps: 10_000,
                    drawdown_policy: pe_risk_engine::DrawdownPolicy::KillOnly,
                },
            )
            .unwrap(),
        );
    }
    let risk_compose = started.elapsed() / reads;
    let order_apply = apply_order(&state, &account, candidate_events);
    assert!(
        sync_live(&state)
            .unwrap()
            .view(&account)
            .unwrap()
            .unwrap()
            .timeline
            .wire_two_admissions
            .contains_key(&candidate.identity.dispatch_id),
        "the newest admission's copy deadline was verified"
    );
    let gate_reads = summary_read * 6 + token_read * 2;
    println!(
        "GATES shape={shape} orders={orders} open={open} paper_frames={paper_frames} summary_read={summary_read:?} token_read={token_read:?} six_reads_plus_two_token_reads={gate_reads:?} risk_compose={risk_compose:?} newest_order_apply={order_apply:?} window_local_total={:?}",
        gate_reads + risk_compose * 2 + order_apply,
    );

    let midnight = (now.unix_timestamp().div_euclid(86_400) + 1) * 86_400;
    let evaluated_at = OffsetDateTime::from_unix_timestamp(midnight - 1).unwrap();
    let crossing_stamp = OffsetDateTime::from_unix_timestamp(midnight + 1).unwrap();
    // Refresh every open position's evidence and prepare the order before timing the first
    // crossing record. Its valid risk evaluation is immediately before midnight; the synchronized
    // admission immediately after midnight creates the fold's cutoff copy.
    let derived = live_summary(&state, &account).unwrap();
    prices.clear();
    for position in &derived.positions {
        prices.insert(
            position.market_id.clone(),
            retain_price(&state, &position.market_id, evaluated_at).await,
        );
    }
    let (_, crossing_events) = strict_order_events(
        &state,
        &account,
        &prices,
        evaluated_at,
        crossing_stamp,
        total + 1,
        separate_conditions,
    )
    .await;
    let crossing_apply = apply_order(&state, &account, crossing_events);
    assert!(
        sync_live(&state)
            .unwrap()
            .view(&account)
            .unwrap()
            .unwrap()
            .financial
            .cutoffs
            .contains_key(&midnight)
    );
    println!(
        "CROSSING shape={shape} orders={orders} open={open} paper_frames={paper_frames} cutoff={midnight} crossing_apply={crossing_apply:?}"
    );

    let expected = complete_snapshot(&state, &account);
    let recovery = verified_recovery_inventory(&state).unwrap();
    drop(state);
    wait_for_source_writer(&dir.path().join("live-source.log")).await;
    let restarted_recovery = cold_restart(
        dir.path(),
        &account,
        crossing_stamp,
        &expected,
        "same",
        GENERATING_TEST,
    );
    assert_eq!(restarted_recovery, recovery);
    if let Some(root) = bench_dir {
        let fixture = dir.keep();
        let saved = serde_json::json!({
            "fixture": fixture.canonicalize().unwrap(),
            "now_unix": crossing_stamp.unix_timestamp(),
            "account": account.as_str(),
            "snapshot": expected,
        });
        std::fs::write(
            root.join("expected.json"),
            serde_json::to_vec_pretty(&saved).unwrap(),
        )
        .unwrap();
    }
}

/// PASS: a fresh process opens the retained journal once and publishes the complete expected
/// financial and recovery state, printing cold_restart and process VmHWM without generation.
/// FAIL: saved evidence cannot be reopened or any complete projection/recovery field differs.
#[tokio::test]
#[ignore = "fresh-process AC12 measurement; first generate BENCH_DIR with the companion test"]
async fn live_state_cold_restart() {
    let Some(root) = std::env::var_os("BENCH_DIR") else {
        println!("fresh-process restart requires BENCH_DIR; run the documented two-step command");
        return;
    };
    let saved: serde_json::Value =
        serde_json::from_slice(&std::fs::read(Path::new(&root).join("expected.json")).unwrap())
            .unwrap();
    let fixture = Path::new(saved["fixture"].as_str().unwrap());
    let account = AccountId::new(saved["account"].as_str().unwrap()).unwrap();
    let now = OffsetDateTime::from_unix_timestamp(saved["now_unix"].as_i64().unwrap()).unwrap();
    cold_restart(
        fixture,
        &account,
        now,
        &saved["snapshot"],
        "fresh",
        RESTART_TEST,
    );
}
