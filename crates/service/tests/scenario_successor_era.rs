//! Successor money, generation membership and duplicate ownership on a physically retained log.
#![cfg(feature = "scenario")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::duplicate_mod
)]

#[path = "support/golden.rs"]
mod golden;
mod support;

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use pe_core_types::{
    CollateralAmount, PolymarketConditionId, ReceivedAt, SourceId, SourceTimestamp, WalletAddress,
};
use pe_event_log::{AppendReceipt, ContentType, EnvelopeIn, RetentionAuthority, Scanner, Writer};
use pe_paper_state::{PaperStateDb, WalletHistoryStatusRecord};
use pe_service::config::ServiceConfig;
use pe_service::decision_replay::{WinnerFollowDecisionInputs, replay_decision_pending};
use pe_service::orchestrator_control::OrchestratorControl;
use pe_service::paper_recovery::{
    FinancialPayload, MembershipReason, PaperLog, PaperLogFrame, PaperLogRecord,
    QualificationSealed, SealReason, TailBinding, paper_era, paper_generation, replay_membership,
    scan_paper_log,
};
use pe_service::qualification::{
    FinancialEraCommand, FinancialEraManifest, FinancialEraPreparation, run_financial_era,
};
use pe_service::risk_inputs::SourceReceiptIndex;
use pe_service::runtime_config::AppliedWatchlistCapacity;
use pe_service::source_checkpoint::{
    CheckpointJobSlot, CheckpointOwnerHooks, RetentionContext, RetentionDatabaseInputs,
    RetentionHooks, SourceCheckpointOwner, install_retention_fence,
};
use pe_service::watchlist_admission::AdmissionPreparer;
use pe_service::watchlist_maintenance::{
    MaintenanceConfig, MembershipMode, MembershipPublication, PublicationBinding,
    apply_evictions_and_backfill, decide_evictions,
};
use pe_strategy_winner_follow::cash_fraction_budget;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde_json::{Value, json};
use time::OffsetDateTime;
use tokio::sync::oneshot;

const NOW: i64 = 1_800_000_000;
const OLD: i64 = NOW - 8 * 86_400;
const MIDNIGHT: i64 = (NOW / 86_400 + 1) * 86_400;

fn envelope(source: &str, payload: Vec<u8>, unix: i64) -> EnvelopeIn {
    let clock = OffsetDateTime::from_unix_timestamp(unix).unwrap();
    EnvelopeIn {
        source_id: SourceId(source.into()),
        schema_version: 1,
        parser_version: 1,
        observed_at: SourceTimestamp(clock),
        received_at: ReceivedAt(clock),
        content_type: ContentType::Json,
        payload,
    }
}

fn append_paper(log: &PaperLog, record: PaperLogRecord, unix: i64) -> AppendReceipt {
    let mut input = envelope(
        "pe-service.paper",
        serde_json::to_vec(&record).unwrap(),
        unix,
    );
    input.schema_version = 2;
    log.append_synced(input).unwrap()
}

fn seal_generation(paper: &Path, source: &Path, live: &Path, unix: i64) {
    let log = PaperLog::open(paper).unwrap();
    let (start_receipt, _) = paper_generation(log.snapshot().unwrap()).start.unwrap();
    append_paper(
        &log,
        PaperLogRecord::QualificationSealed(Box::new(QualificationSealed {
            start_receipt,
            source_prefix: TailBinding::from(&Scanner::verify(source).unwrap()),
            financial_prefix: TailBinding::from(&log.verified_tail().unwrap()),
            live_prefix: TailBinding::from(&Scanner::verify(live).unwrap()),
            decision_evidence_digest: "recorded-successor-fixture".into(),
            sealed_cutoff_unix: unix,
            reason: SealReason::InsufficientEvidence("fixture generation is closed".into()),
        })),
        unix,
    );
}

fn knockout(wallet: WalletAddress, last_trade: i64, evaluated: i64) -> Value {
    json!({"wallet": wallet, "evaluated_at_unix": evaluated, "last_trade_unix": last_trade,
        "inactivity_threshold_secs": 259200, "inactivity_hard_cap_secs": 604800,
        "demotion_min_trades": 10, "demotion_cb_alpha": "0.10", "demotion_pnl_window_secs": 2592000,
        "fills": [], "settlements": []})
}

fn admission(state: &PaperStateDb, wallet: WalletAddress) -> Value {
    let history = state.wallet_history_status(&wallet).unwrap().unwrap();
    let coverage = state.wallet_coverage(&wallet).unwrap();
    let anchor = state.position_anchors(&wallet).unwrap().pop().unwrap();
    let validation = state.position_validation(&wallet).unwrap().unwrap();
    json!({"wallet": wallet, "proof": {"membership": [wallet], "proofs": [{"wallet": wallet,
        "history": {"complete": history.complete, "proof_json": history.proof_json, "updated_at_unix": history.updated_at_unix},
        "coverage": {"activity_cutoff_unix": coverage.activity_cutoff_unix, "coverage_generation": coverage.coverage_generation,
            "reanchor_required": coverage.reanchor_required, "anchor_seq": coverage.anchor_seq, "anchored_at_unix": coverage.anchored_at_unix},
        "anchor": {"anchor_seq": anchor.anchor_seq, "anchored_at_unix": anchor.anchored_at_unix,
            "activity_cutoff_unix": anchor.activity_cutoff_unix, "balances_json": anchor.balances_json,
            "ledger_hash_after": anchor.ledger_hash_after, "proof_json": anchor.proof_json},
        "validation": {"ledger_hash": validation.ledger_hash, "positions_proof_hash": validation.positions_proof_hash,
            "activity_bounds_json": validation.activity_bounds_json, "source_log_generation": validation.source_log_generation,
            "proof_json": validation.proof_json, "recorded_at_unix": validation.recorded_at_unix}
    }]}})
}

async fn retention(dir: &Path, state: Arc<PaperStateDb>, index: SourceReceiptIndex, now: i64) {
    // Use the same checkpoint owner and orchestrator commit handler as scenario_source_retention.
    let paper = Arc::new(PaperLog::open(dir.join("paper.log")).unwrap());
    let (_, _, control, actor) =
        support::retention_controls(paper.as_ref().clone(), state.clone(), index.clone());
    let path = dir.join("source.log");
    install_retention_fence(&path).unwrap();
    let first = index
        .receipt_at(pe_core_types::EventSeq(0))
        .unwrap()
        .unwrap()
        .0;
    let (_, end) = pe_event_log::Reader::read_at(
        &path,
        5,
        pe_core_types::EventSeq(0),
        blake3::Hash::from_bytes([0; 32]),
    )
    .unwrap();
    let activation = pe_event_log::LogTailBinding {
        path: path.clone(),
        physical_tail: end,
        last_sequence: Some(first.sequence),
        last_hash: first.this_hash,
    };
    let inputs = state.clone();
    let context = RetentionContext {
        paper_state: state,
        paper_log: paper,
        live_journal_path: None,
        control,
        database_inputs: Arc::new(move || RetentionDatabaseInputs::read(&inputs)),
        hooks: Arc::new(RetentionHooks::default()),
    };
    let mut owner = SourceCheckpointOwner::for_retention_scenario(
        activation,
        index,
        CheckpointJobSlot::default(),
        false,
    )
    .unwrap()
    .with_retention(context)
    .unwrap();
    owner.set_scenario_hooks(Arc::new(CheckpointOwnerHooks {
        clock: Some(Arc::new(move || Ok(u64::try_from(now).unwrap() * 1000))),
        ..Default::default()
    }));
    owner.initialize_for_scenario().await.unwrap();
    owner.retention_for_scenario().await.unwrap();
    drop(owner);
    actor.abort();
    let _ = actor.await;
}

async fn settle(h: &golden::BracketFinancialHarness, market: usize, unix: i64) {
    let condition = format!("0x{market:064x}");
    let mut payload: Value = serde_json::from_slice(include_bytes!(
        "fixtures/golden_stream_v1/clob_resolution.json"
    ))
    .unwrap();
    payload["condition_id"] = condition.clone().into();
    payload["tokens"][0]["token_id"] = (market * 2 + 99).to_string().into();
    payload["tokens"][1]["token_id"] = (market * 2 + 100).to_string().into();
    let mut input = envelope(
        "polymarket.clob.market",
        serde_json::to_vec(&payload).unwrap(),
        unix,
    );
    input.schema_version = pe_source_polymarket_public::CLOB_RESOLUTION_SCHEMA_VERSION;
    input.parser_version = pe_source_polymarket_public::CLOB_RESOLUTION_PARSER_VERSION;
    let receipt = h.source.append(input).await.unwrap();
    h.set_clock(unix);
    let (acknowledged, ack) = oneshot::channel();
    h.control
        .send(OrchestratorControl::ResolutionCandidate {
            condition: PolymarketConditionId(condition),
            payout_by_outcome_index_json: "[\"1\",\"0\"]".into(),
            receipt,
            acknowledged,
        })
        .await
        .unwrap();
    ack.await.unwrap().unwrap();
}

fn successor(
    dir: &Path,
    bankroll: Decimal,
    start_unix: i64,
    runtime: &pe_service::runtime_config::RuntimeConfig,
) -> AppendReceipt {
    let manifest_path = dir.join("financial-era.json");
    let mut manifest: FinancialEraManifest =
        serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
    let predecessor = paper_era(scan_paper_log(&dir.join("paper.log")).unwrap())
        .start
        .unwrap()
        .0;
    manifest.predecessor_start = Some(predecessor);
    manifest.activation_id = "era-successor-scenario".into();
    manifest.state = "prepared".into();
    manifest.membership.clear();
    manifest.schema_version = 3;
    manifest.fresh_bankroll = CollateralAmount::from_decimal_exact(bankroll).unwrap();
    manifest.start_unix = start_unix;
    manifest.preparation = None;
    let config = ServiceConfig {
        event_log_path: dir.join("paper.log"),
        source_event_log_path: dir.join("source.log"),
        paper_state_db_path: dir.join("paper.db"),
        status_path: dir.join("status.json"),
        bankroll_usd: bankroll.to_string(),
        supabase_authoritative: true,
        supabase_url: "https://unused.invalid".into(),
        supabase_secret_key: "fixture-service-role".into(),
        ..Default::default()
    };
    let rows = dir.join("financial-config.json");
    std::fs::write(
        &rows,
        serde_json::to_vec(&golden::golden_config_rows(runtime)).unwrap(),
    )
    .unwrap();
    std::fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();
    let prepared = run_financial_era(
        FinancialEraCommand::Prepare,
        &manifest_path,
        &config,
        Some(&rows),
    )
    .unwrap();
    manifest.preparation =
        Some(serde_json::from_str::<FinancialEraPreparation>(&prepared).unwrap());
    manifest.state = "guarded".into();
    std::fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();
    let first = run_financial_era(
        FinancialEraCommand::Start,
        &manifest_path,
        &config,
        Some(&rows),
    )
    .unwrap();
    assert_eq!(
        run_financial_era(
            FinancialEraCommand::Start,
            &manifest_path,
            &config,
            Some(&rows)
        )
        .unwrap(),
        first
    );
    serde_json::from_str(&first).unwrap()
}

#[tokio::test]
async fn successor_retained_boot_copy_settlement_duplicates_membership_and_midnight() {
    let logs = support::RetentionLogs::default();
    let _subscriber = tracing::subscriber::set_default(logs.subscriber());
    struct FailureLogs(support::RetentionLogs);
    impl Drop for FailureLogs {
        fn drop(&mut self) {
            if std::thread::panicking() {
                eprintln!("{}", self.0.text());
            }
        }
    }
    let _failure_logs = FailureLogs(logs.clone());
    let dir = tempfile::tempdir().unwrap();
    let state = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
    let a = WalletAddress::from_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
    let b = WalletAddress::from_hex("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap();
    let c = WalletAddress::from_hex("0xcccccccccccccccccccccccccccccccccccccccc").unwrap();
    for wallet in [a, b, c] {
        support::install_verified_empty_anchor(&state, wallet, 0);
        state
            .record_reconciled_history_status(&WalletHistoryStatusRecord {
                wallet,
                complete: true,
                proof_json: "{}".into(),
                updated_at_unix: 0,
            })
            .unwrap();
    }
    let mut writer = Writer::open(dir.path().join("source.log")).unwrap();
    writer
        .append_synced(envelope("activation", b"{}".to_vec(), OLD))
        .unwrap();
    drop(writer);
    let old = golden::BracketFinancialHarness::new(dir.path(), state.clone(), &[a, b]).await;
    let initial = old.initial.clone();
    let runtime = old.runtime();
    old.entries_in_one_bucket(&state, a, 100, &[1, 2]).await;
    let archived = state.list_fills().unwrap();
    assert_eq!(archived.len(), 2);
    let terminal_history = state.decision_pending_history().unwrap();
    let gate_history = state.gate_history().unwrap();
    let first_entry_history = archived
        .iter()
        .map(|fill| state.market_history_record(&a, &fill.market_id).unwrap())
        .collect::<Vec<_>>();
    let predecessor = state.financial_start().unwrap().unwrap();
    old.shutdown().await;

    // A survives from the original batch; C replaces B from a later knockout bench.
    let source_path = dir.path().join("source.log");
    let mut writer = Writer::open(&source_path).unwrap();
    let mut noise = Vec::new();
    for block in 0..2048_u64 {
        noise.extend_from_slice(blake3::hash(&block.to_le_bytes()).as_bytes());
    }
    let noise_offset = writer.verified_tail().unwrap().physical_tail;
    writer
        .append_synced(envelope("old-raw", noise, OLD))
        .unwrap();
    let noise_end = writer.verified_tail().unwrap().physical_tail;
    let mut replacement = golden::BracketFinancialHarness::entries(&[c]);
    replacement[0].leader_score_bps = pe_core_types::BasisPoints(900);
    let ranking = writer
        .append_synced(envelope(
            "pe-service.watchlist-ranking",
            serde_json::to_vec(&json!({"batch_id": 546, "entries": replacement})).unwrap(),
            OLD,
        ))
        .unwrap();
    let causal = writer
        .append_synced(envelope(
            "pe-service.watchlist-knockout",
            serde_json::to_vec(&knockout(b, OLD - 259200, OLD)).unwrap(),
            OLD,
        ))
        .unwrap();
    let admitted = writer
        .append_synced(envelope(
            "pe-service.watchlist-admission",
            serde_json::to_vec(&admission(&state, c)).unwrap(),
            OLD,
        ))
        .unwrap();
    writer
        .append_synced(envelope("recent-raw", b"{}".to_vec(), NOW))
        .unwrap();
    drop(writer);
    let paper = PaperLog::open(dir.path().join("paper.log")).unwrap();
    append_paper(
        &paper,
        PaperLogRecord::MembershipChanged {
            reason: MembershipReason::KnockoutInactivity,
            removed: vec![b],
            added: vec![c],
            capacity: 2,
            ranking_batch_id: Some(546),
            evidence: json!({"kind": "knockout_backfill", "ranking_receipt": ranking,
                "evictions": [{"wallet": b, "reason": "knockout_inactivity", "causal_receipt": causal}], "admission_receipts": [{"wallet": c, "receipt": admitted}]}),
        },
        OLD,
    );
    drop(paper);
    seal_generation(
        &dir.path().join("paper.log"),
        &source_path,
        &dir.path().join("live_journal.log"),
        NOW - 1,
    );
    let before = replay_membership(
        &paper_generation(scan_paper_log(&dir.path().join("paper.log")).unwrap()),
        initial.clone(),
        &source_path,
    )
    .unwrap()
    .unwrap();
    assert_eq!(before.last_ranking_batch_id, 546);
    assert_eq!(
        before
            .watchlist
            .entries
            .iter()
            .map(|e| e.wallet)
            .collect::<HashSet<_>>(),
        HashSet::from([a, c])
    );
    retention(
        dir.path(),
        state.clone(),
        SourceReceiptIndex::replay(&source_path).unwrap(),
        NOW,
    )
    .await;
    let authority = RetentionAuthority::load(&source_path)
        .unwrap()
        .unwrap_or_else(|| panic!("{}", logs.text()));
    let bytes = std::fs::read(&source_path).unwrap();
    let first_block = usize::try_from(noise_offset.div_ceil(4096) * 4096).unwrap();
    let last_block = usize::try_from(noise_end / 4096 * 4096).unwrap();
    assert!(last_block > first_block);
    assert!(bytes[first_block..last_block].iter().all(|b| *b == 0));
    for receipt in [ranking, causal, admitted] {
        assert!(authority.pin(receipt.sequence).is_some());
    }

    // Preparation and Start verify a punched prefix through the boot-equivalent index.
    let start = successor(dir.path(), dec!(2000), NOW, &runtime);
    assert_ne!(start, predecessor);
    let state = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
    let fresh = state.financial_snapshot(NOW).unwrap();
    assert_eq!(fresh.cash, dec!(2000));
    assert!(fresh.positions.is_empty());
    assert!(state.list_fills().unwrap().is_empty());
    assert_eq!(state.decision_pending_history().unwrap(), terminal_history);
    assert_eq!(state.gate_history().unwrap(), gate_history);
    assert_eq!(
        archived
            .iter()
            .map(|fill| state.market_history_record(&a, &fill.market_id).unwrap())
            .collect::<Vec<_>>(),
        first_entry_history
    );
    let boot = replay_membership(
        &paper_generation(scan_paper_log(&dir.path().join("paper.log")).unwrap()),
        initial.clone(),
        &source_path,
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        serde_json::to_value(&boot.watchlist.entries).unwrap(),
        serde_json::to_value(&before.watchlist.entries).unwrap()
    );
    let mut h = golden::BracketFinancialHarness::boot_existing(
        dir.path(),
        state.clone(),
        boot.watchlist,
        runtime.clone(),
        false,
        MIDNIGHT + 2,
    )
    .await;
    assert_eq!(h.live.structural_membership(), HashSet::from([a, c]));

    // The first legitimate maintenance tick may remove an inactive carried member.
    let cfg = MaintenanceConfig {
        interval_secs: 600,
        inactivity_threshold_secs: 259200,
        inactivity_hard_cap_secs: 604800,
        demotion_min_trades: 10,
        demotion_cb_alpha: dec!(0.1),
        demotion_pnl_window_secs: 2592000,
        membership_mode: MembershipMode::Knockout,
    };
    let evictions = decide_evictions(
        &h.live.snapshot(),
        &HashMap::new(),
        &HashMap::from([(a, Some(NOW - 259200)), (c, Some(NOW - 1))]),
        &cfg,
        NOW,
    );
    assert_eq!(evictions.len(), 1);
    assert_eq!(evictions[0].wallet, a);
    let receipt = h
        .source
        .append(envelope(
            "pe-service.watchlist-knockout",
            serde_json::to_vec(&knockout(a, NOW - 259200, NOW)).unwrap(),
            NOW,
        ))
        .await
        .unwrap();
    let applied = AppliedWatchlistCapacity::new(2);
    apply_evictions_and_backfill(&h.live, &state, &h.writer_lock,
            &AdmissionPreparer::new(h.control.clone(), state.clone()), MembershipPublication { reason: MembershipReason::KnockoutInactivity, ranking_batch_id: None,
                evidence: json!({"kind": "knockout_backfill", "ranking_receipt": null, "admission_receipts": [], "evictions": [{"wallet": a, "reason": "knockout_inactivity", "causal_receipt": receipt}]}),
                binding: PublicationBinding { structural: h.live.structural_membership(), digests: vec![] } },
            &applied, applied.load(), &HashSet::from([a]), &[], &HashMap::new()).await.unwrap();
    assert_eq!(h.live.structural_membership(), HashSet::from([c]));

    let prepared_before = scan_paper_log(&h.paper_path).unwrap().len();
    h.repeat_entry(a, 100, NOW).await;
    assert_eq!(state.decision_pending_history().unwrap(), terminal_history);
    assert_eq!(state.gate_history().unwrap(), gate_history);
    assert_eq!(
        archived
            .iter()
            .map(|fill| state.market_history_record(&a, &fill.market_id).unwrap())
            .collect::<Vec<_>>(),
        first_entry_history
    );
    assert!(state.list_fills().unwrap().is_empty());
    assert!(!state.dispatch_seeds_exist().unwrap());
    assert_eq!(
        scan_paper_log(&h.paper_path).unwrap().len(),
        prepared_before
    );
    assert!(state.is_seen(&terminal_history[0].source_trade_id).unwrap());

    h.entries_in_one_bucket(&state, c, NOW + 1, &[1]).await;
    let fills = state.list_fills().unwrap();
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].quantity.to_decimal(), dec!(5.04));
    assert_eq!(fills[0].principal.to_decimal(), dec!(2.5));
    assert_eq!(fills[0].fee.to_decimal(), dec!(0.05));
    let holding = state.financial_snapshot(NOW + 1).unwrap();
    assert_eq!(holding.cash, dec!(1997.45));
    assert_eq!(holding.positions.len(), 1);
    assert_eq!(holding.positions[0].long.to_decimal(), dec!(5.04));
    // Market 3 exists only in the archive; market 2 has both retired and new holdings.
    settle(&h, 3, NOW + 2).await;
    assert_eq!(
        state.financial_snapshot(NOW + 2).unwrap().cash,
        dec!(1997.45)
    );
    settle(&h, 2, NOW + 3).await;
    assert_eq!(
        state.financial_snapshot(NOW + 3).unwrap().cash,
        dec!(2002.49)
    );
    assert!(
        state
            .financial_snapshot(NOW + 3)
            .unwrap()
            .positions
            .is_empty()
    );
    let credits = state
        .list_settled_markets()
        .unwrap()
        .into_iter()
        .map(|s| (s.market_id.to_string(), s.credit_applied))
        .collect::<HashMap<_, _>>();
    assert_eq!(
        credits,
        HashMap::from([
            (format!("0x{:064x}", 2), dec!(5.04)),
            (format!("0x{:064x}", 3), Decimal::ZERO)
        ])
    );
    let history_after_fill = state.decision_pending_history().unwrap();
    let admission_after_fill = state.gate_history().unwrap();
    let current_first_entry = state
        .market_history_record(&c, &fills[0].market_id)
        .unwrap();
    let paper_after_fill = scan_paper_log(&h.paper_path).unwrap().len();
    h.repeat_entry(c, NOW + 1, NOW + 4).await;
    assert_eq!(state.list_fills().unwrap(), fills);
    assert_eq!(
        state.decision_pending_history().unwrap(),
        history_after_fill
    );
    assert_eq!(state.gate_history().unwrap(), admission_after_fill);
    assert_eq!(
        state
            .market_history_record(&c, &fills[0].market_id)
            .unwrap(),
        current_first_entry
    );
    assert_eq!(
        scan_paper_log(&h.paper_path).unwrap().len(),
        paper_after_fill
    );
    assert!(!state.dispatch_seeds_exist().unwrap());

    assert_eq!(
        cash_fraction_budget(dec!(0.2), dec!(1400), dec!(2000)).unwrap(),
        Decimal::ZERO
    );
    assert_eq!(
        cash_fraction_budget(dec!(0.2), dec!(1400), dec!(1000)).unwrap(),
        dec!(280)
    );
    let frames = scan_paper_log(&h.paper_path).unwrap();
    let era = paper_era(frames);
    for frame in &era.frames {
        if let PaperLogFrame::Record(PaperLogRecord::FinancialPrepared {
            expected_authority,
            payload,
        }) = &frame.frame
        {
            assert_eq!(expected_authority.qualification_start_receipt, start);
            if let FinancialPayload::Fill { economic, .. } = payload {
                assert_eq!(
                    economic.risk.snapshot.drawdown_policy,
                    pe_risk_engine::DrawdownPolicy::KillOnly
                );
                assert_eq!(
                    economic.risk.snapshot.absolute_pnl_bps,
                    pe_core_types::BasisPoints::ZERO
                );
                assert_eq!(
                    economic.risk.snapshot.intraday_pnl_bps,
                    pe_core_types::BasisPoints::ZERO
                );
            }
        }
    }
    // A stop interval spanning midnight requires the new era's mark at the exact cutoff.
    assert!(
        pe_service::trade_poller::completed_midnight_cutoffs_after(NOW, MIDNIGHT - 1)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        pe_service::trade_poller::completed_midnight_cutoffs_after(NOW, MIDNIGHT).unwrap(),
        vec![MIDNIGHT]
    );
    h.entries_in_one_bucket(&state, c, MIDNIGHT - 1, &[6]).await;
    assert_eq!(state.list_fills().unwrap().len(), 2);
    let before_midnight = paper_era(scan_paper_log(&h.paper_path).unwrap());
    let risk = before_midnight
        .frames
        .iter()
        .rev()
        .find_map(|frame| match &frame.frame {
            PaperLogFrame::Record(PaperLogRecord::FinancialPrepared {
                payload: FinancialPayload::Fill { economic, .. },
                ..
            }) => Some(&economic.risk.snapshot),
            _ => None,
        })
        .unwrap();
    assert_eq!(risk.absolute_pnl_bps, pe_core_types::BasisPoints(12));
    assert_eq!(risk.intraday_pnl_bps, pe_core_types::BasisPoints(12));
    settle(&h, 7, MIDNIGHT - 1).await;
    assert_eq!(
        state.financial_snapshot(MIDNIGHT - 1).unwrap().cash,
        dec!(2004.98)
    );
    h.entries_in_one_bucket(&state, c, MIDNIGHT, &[4]).await;
    assert_eq!(state.list_fills().unwrap().len(), 2);
    let missing = state
        .decision_pending_history()
        .unwrap()
        .into_iter()
        .find(|row| row.source_epoch == MIDNIGHT)
        .unwrap();
    let decision = replay_decision_pending(&missing).unwrap();
    assert!(matches!(
        decision.post_boundary.body.terminal.decline.unwrap().inputs,
        WinnerFollowDecisionInputs::RiskInputsUnavailable {
            cause: pe_service::risk_inputs::RiskInputsUnavailable::MarkMissing,
            ..
        }
    ));
    h.deployed_boundary(MIDNIGHT).await;
    let era = paper_era(scan_paper_log(&h.paper_path).unwrap());
    let mark = era
        .frames
        .iter()
        .find_map(|f| match &f.frame {
            PaperLogFrame::Record(PaperLogRecord::PortfolioMark(mark)) => Some(mark),
            _ => None,
        })
        .unwrap();
    assert_eq!(mark.cutoff_unix, MIDNIGHT);
    assert_eq!(mark.cash, dec!(2004.98));
    assert_eq!(mark.equity, dec!(2004.98));
    assert!(mark.invalid.is_none());
    h.entries_in_one_bucket(&state, c, MIDNIGHT + 1, &[5]).await;
    assert_eq!(state.list_fills().unwrap().len(), 3);
    assert_eq!(
        state.financial_snapshot(MIDNIGHT + 1).unwrap().cash,
        dec!(2002.43)
    );
    let after_midnight = paper_era(scan_paper_log(&h.paper_path).unwrap());
    let risk = after_midnight
        .frames
        .iter()
        .rev()
        .find_map(|frame| match &frame.frame {
            PaperLogFrame::Record(PaperLogRecord::FinancialPrepared {
                payload: FinancialPayload::Fill { economic, .. },
                ..
            }) => Some(&economic.risk.snapshot),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        risk.drawdown_policy,
        pe_risk_engine::DrawdownPolicy::KillOnly
    );
    assert_eq!(risk.absolute_pnl_bps, pe_core_types::BasisPoints(24));
    assert_eq!(risk.intraday_pnl_bps, pe_core_types::BasisPoints::ZERO);
    assert_eq!(
        logs.text()
            .lines()
            .filter(
                |line| line.contains("frame admission ignored") && line.contains("identity_seen")
            )
            .count(),
        2
    );
    let mut cash_fraction = runtime.clone();
    cash_fraction.sizing_mode = pe_strategy_winner_follow::SizingMode::CashFraction {
        fraction: dec!(0.2),
    };
    cash_fraction.per_trade_cap = pe_strategy_winner_follow::PerTradeCap::Unlimited;
    h.set_runtime(cash_fraction.clone());
    let (acknowledged, ack) = oneshot::channel();
    h.control
        .send(OrchestratorControl::SealCheck {
            proposed_economic_hash: cash_fraction.canonical_hash(),
            proposed_financial_semantic_version: 3,
            acknowledged,
        })
        .await
        .unwrap();
    ack.await.unwrap().unwrap();
    h.entries_in_one_bucket(&state, c, MIDNIGHT + 2, &[7, 8])
        .await;
    let after_fractional = paper_era(scan_paper_log(&h.paper_path).unwrap());
    let fractional = after_fractional
        .frames
        .iter()
        .filter_map(|frame| match &frame.frame {
            PaperLogFrame::Record(PaperLogRecord::FinancialPrepared {
                payload: FinancialPayload::Fill { economic, .. },
                ..
            }) if economic.applied_configuration_hash == cash_fraction.canonical_hash() => {
                Some(economic)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        fractional.len(),
        2,
        "{:?}",
        state
            .decision_pending_history()
            .unwrap()
            .iter()
            .filter(|row| row.source_epoch == MIDNIGHT + 2)
            .map(|row| replay_decision_pending(row)
                .unwrap()
                .post_boundary
                .body
                .terminal
                .reason)
            .collect::<Vec<_>>()
    );
    for economic in fractional {
        let before = economic.balance.cash_before.to_decimal();
        let budget = cash_fraction_budget(dec!(0.2), before, dec!(2000)).unwrap();
        assert_eq!(
            economic.sizing.mode,
            pe_execution_core::SizingModeAudit::Dollar { usd: budget }
        );
        assert!(before - economic.balance.worst_case_debit.to_decimal() >= dec!(1400));
    }
    let floor_cash = state.financial_snapshot(MIDNIGHT + 2).unwrap().cash;
    assert!(floor_cash >= dec!(1400));
    assert!(floor_cash < dec!(1403));
    let fills_at_floor = state.list_fills().unwrap().len();
    h.entries_in_one_bucket(&state, c, MIDNIGHT + 3, &[9]).await;
    assert_eq!(
        state.list_fills().unwrap().len(),
        fills_at_floor,
        "new-era floor leaves less than the venue minimum"
    );
    assert_eq!(
        state.financial_snapshot(MIDNIGHT + 3).unwrap().cash,
        floor_cash
    );
    h.shutdown().await;
    // Two restarts after new-era fills and settlements recover the same cash, holdings,
    // frontier and Start.
    let book = state.financial_snapshot(MIDNIGHT + 3).unwrap();
    assert!(book.last_prepared_seq.is_some());
    for _ in 0..2 {
        let membership = replay_membership(
            &paper_generation(scan_paper_log(&dir.path().join("paper.log")).unwrap()),
            initial.clone(),
            &source_path,
        )
        .unwrap()
        .unwrap();
        golden::BracketFinancialHarness::boot_existing(
            dir.path(),
            state.clone(),
            membership.watchlist,
            runtime.clone(),
            false,
            MIDNIGHT + 4,
        )
        .await
        .shutdown()
        .await;
        assert_eq!(state.financial_snapshot(MIDNIGHT + 3).unwrap(), book);
    }
    retention(
        dir.path(),
        state.clone(),
        SourceReceiptIndex::replay(&source_path).unwrap(),
        NOW + 9 * 86_400,
    )
    .await;
    let next = RetentionAuthority::load(&source_path).unwrap().unwrap();
    assert!(next.epoch > authority.epoch);
    assert!(next.boundary.sequence > authority.boundary.sequence);
    assert!(!logs.text().contains("unsealed_start"));
    let frames = scan_paper_log(&dir.path().join("paper.log")).unwrap();
    assert_eq!(
        frames
            .iter()
            .filter(|f| matches!(
                &f.frame,
                PaperLogFrame::Record(PaperLogRecord::QualificationSealed(_))
            ))
            .count(),
        1
    );
    let final_membership = replay_membership(&paper_generation(frames), initial, &source_path)
        .unwrap()
        .unwrap();
    assert_eq!(
        final_membership
            .watchlist
            .entries
            .iter()
            .map(|e| e.wallet)
            .collect::<Vec<_>>(),
        vec![c]
    );
    assert_eq!(state.financial_start().unwrap(), Some(start));
}

/// The PostgreSQL script requests only this fixture setup; all lifecycle commands use its binary.
#[test]
#[ignore = "fixture writer invoked explicitly by test_legacy_to_financial_pg.sh"]
fn postgres_successor_generation_seal_fixture() {
    let dir =
        std::env::var_os("PE_SUCCESSOR_PG_FIXTURE").expect("isolated PostgreSQL fixture directory");
    let dir = Path::new(&dir);
    seal_generation(
        &dir.join("paper.log"),
        &dir.join("source_events.log"),
        &dir.join("live_journal.log"),
        1_700_000_001,
    );
}
