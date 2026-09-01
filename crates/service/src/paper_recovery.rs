//! Startup recovery for the paper trader: reconcile the SQLite mirror against the
//! event log, and rehydrate the in-memory leader `PositionLedger` from the mirror.
//!
//! Lives in the service tier (not in `paper-state`) because both steps need the
//! `PaperFill` / `PositionSnapshot` types from the strategy and signal crates, which
//! `paper-state` deliberately does not depend on.

use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result};
use pe_copy_signal_engine::{PositionSnapshot, PositionState};
use pe_core_types::{MarketOutcomeId, SourceTradeId};
use pe_event_log::Reader;
use pe_paper_state::{FillRecord, FillRow, PaperStateDb};
use pe_position_ledger::PositionLedger;
use pe_strategy_winner_follow::PaperFill;

use crate::bucket_commit::DecisionContinuationV2;
use crate::decision_replay::{
    AuthorityEvidence, DecisionEvidenceAccumulator, TerminalDispositionEvidence,
};
use crate::orchestrator::{pending_terminal, recorded_fill_terminal, render_pending_evidence};
use crate::supabase_sink::supabase_fill_from;

/// Replay event-log fills whose SQLite commit was lost to a crash (those with
/// `seq > last_applied_event_seq`) back into `paper-state`. Returns the count newly
/// applied. No-op when the log does not exist yet (first run). See issue #282 AC5.
pub fn reconcile_paper_state(event_log_path: &Path, paper_state: &PaperStateDb) -> Result<usize> {
    if !event_log_path.exists() {
        return Ok(0);
    }
    let mut applied = 0usize;
    let replay = Reader::replay(event_log_path)
        .with_context(|| format!("open event log {}", event_log_path.display()))?;
    // `reconcile_fill` is itself idempotent (guards on `last_applied_event_seq` and the
    // `fills` PK), so every frame is offered to it; already-mirrored fills are skipped.
    for frame in replay {
        let (seq, envelope) = frame.context("read event-log frame")?;
        let fill: PaperFill = serde_json::from_slice(&envelope.payload)
            .with_context(|| format!("decode PaperFill at seq {}", seq.0))?;
        let record = FillRecord {
            idempotency_key: fill.intent.idempotency_key.clone(),
            market_id: fill.intent.market_id.clone(),
            outcome_id: fill.intent.outcome_id,
            side: fill.intent.side,
            contracts: fill.intent.contracts.0,
            fill_price: fill.simulated_fill_price,
        };
        let source_trade_id = i64::try_from(seq.0)
            .ok()
            .and_then(|event_seq| {
                supabase_fill_from(&FillRow {
                    idempotency_key: record.idempotency_key.clone(),
                    market_id: record.market_id.clone(),
                    outcome_id: record.outcome_id,
                    side: record.side,
                    contracts: record.contracts,
                    fill_price: record.fill_price,
                    event_seq,
                })
            })
            .and_then(|row| row.source_trade_id)
            .map(SourceTradeId);
        let pending_row = match source_trade_id.as_ref() {
            Some(id) => paper_state
                .open_decision_pending()
                .context("load decision_pending for paper-log recovery")?
                .into_iter()
                .find(|row| &row.source_trade_id == id),
            None => None,
        };
        if let (Some(source_trade_id), Some(pending_row)) =
            (source_trade_id.as_ref(), pending_row.as_ref())
        {
            let continuation = DecisionContinuationV2::from_durable(pending_row)
                .context("decode pending paper-log continuation")?;
            let evidence = DecisionEvidenceAccumulator::from_pending_checkpoint(pending_row)
                .context("decode pending paper-log evidence checkpoint")?;
            let leader = paper_state
                .leader_positions()
                .context("load pending paper-log leader mirror")?
                .into_iter()
                .find(|leader| {
                    leader.wallet == continuation.wallet
                        && leader.market_id == continuation.market_id
                        && leader.outcome_id == continuation.outcome_id
                })
                .context("pending paper-log continuation has no leader mirror")?;
            let fill_pending = render_pending_evidence(
                Some(&evidence),
                AuthorityEvidence::local("recovered_from_paper_log"),
                recorded_fill_terminal(&record, seq),
            )
            .context("render recovered paper-log fill evidence")?;
            let settled_pending = render_pending_evidence(
                Some(&evidence),
                AuthorityEvidence::local("recovered_settled_refusal"),
                TerminalDispositionEvidence::settled_refusal(),
            )
            .context("render recovered paper-log refusal evidence")?;
            let outcome = paper_state
                .commit_fill_with_flip_pending(
                    source_trade_id,
                    &leader,
                    &record,
                    seq,
                    None,
                    fill_pending.as_ref().map(pending_terminal),
                    settled_pending.as_ref().map(pending_terminal),
                )
                .context("commit recovered pending paper-log fill")?;
            if matches!(outcome, pe_paper_state::FillCommitOutcome::Applied(_)) {
                applied += 1;
            }
            continue;
        }
        if paper_state.reconcile_fill(&record, seq)? {
            applied += 1;
        }
    }
    Ok(applied)
}

/// Rebuild the leader `PositionLedger` from the `leader_positions` mirror so the
/// first post-restart trade by a leader with an existing position classifies as
/// Add/Trim/Flip, not a fresh Entry (issue #282 AC4).
pub fn build_leader_ledger(paper_state: &PaperStateDb) -> Result<PositionLedger> {
    let mut snapshots: HashMap<_, PositionSnapshot> = HashMap::new();
    for row in paper_state
        .leader_positions()
        .context("read leader positions")?
    {
        let snap = snapshots
            .entry(row.wallet)
            .or_insert_with(|| PositionSnapshot {
                wallet: row.wallet,
                positions: HashMap::new(),
            });
        let key = MarketOutcomeId::new(row.market_id.clone(), row.outcome_id);
        snap.positions.insert(
            key,
            PositionState {
                long_contracts: row.long_contracts,
                short_contracts: row.short_contracts,
            },
        );
    }
    Ok(PositionLedger::from_snapshots(snapshots))
}
