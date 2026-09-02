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
use pe_core_types::{
    MarketId, MarketOutcomeId, OutcomeId, ShareAmount, SourceTimestamp, SourceTradeId,
    VenueMarketId, WalletAddress,
};
use pe_event_log::Reader;
use pe_paper_state::{FillRecord, FillRow, PaperStateDb};
use pe_position_ledger::{LedgerEffect, LedgerEffectDocumentError, LedgerMutation, PositionLedger};
use pe_strategy_winner_follow::PaperFill;

use crate::bucket_commit::DecisionContinuationV2;
use crate::decision_replay::{
    AuthorityEvidence, DecisionEvidenceAccumulator, TerminalDispositionEvidence,
};
use crate::orchestrator::{pending_terminal, recorded_fill_terminal, render_pending_evidence};
use crate::position_seeder::ledger_capture;
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

#[derive(Debug, thiserror::Error)]
pub enum WalletLedgerReplayError {
    #[error("paper-state: {0}")]
    PaperState(#[from] pe_paper_state::PaperStateError),
    #[error("position anchor {anchor_seq} balances are malformed: {source}")]
    AnchorBalances {
        anchor_seq: i64,
        source: serde_json::Error,
    },
    #[error("position anchor sequence expected {expected}, found {actual}")]
    AnchorSequence { expected: i64, actual: i64 },
    #[error("position anchor {anchor_seq} contains duplicate {market_id} outcome {outcome}")]
    DuplicateAnchorBalance {
        anchor_seq: i64,
        market_id: String,
        outcome: u16,
    },
    #[error("position anchor {anchor_seq} ledger hash does not match its balances")]
    AnchorHashMismatch { anchor_seq: i64 },
    #[error("activity group {source_trade_id} effect document: {source}")]
    EffectDocument {
        source_trade_id: SourceTradeId,
        source: LedgerEffectDocumentError,
    },
    #[error("activity group {source_trade_id} revision changed during replay")]
    RevisionMismatch { source_trade_id: SourceTradeId },
    #[error("activity group {source_trade_id} has invalid source epoch {source_epoch}")]
    InvalidSourceEpoch {
        source_trade_id: SourceTradeId,
        source_epoch: i64,
    },
    #[error("activity group {source_trade_id} ledger effect: {message}")]
    Ledger {
        source_trade_id: SourceTradeId,
        message: String,
    },
    #[error("wallet ledger capture failed: {0}")]
    LedgerCapture(String),
}

/// Rebuild one wallet from its append-only anchors and versioned post-cutoff effects.
pub fn replay_wallet_ledger(
    paper_state: &PaperStateDb,
    wallet: WalletAddress,
) -> Result<PositionLedger, WalletLedgerReplayError> {
    let anchors = paper_state.position_anchors(&wallet)?;
    let Some(first_anchor) = anchors.first() else {
        return Ok(PositionLedger::new());
    };
    for (expected, anchor) in anchors.iter().enumerate() {
        let expected = i64::try_from(expected).unwrap_or(i64::MAX);
        if anchor.anchor_seq != expected {
            return Err(WalletLedgerReplayError::AnchorSequence {
                expected,
                actual: anchor.anchor_seq,
            });
        }
    }

    let mut ledger = PositionLedger::new();
    install_replayed_anchor(&mut ledger, paper_state, first_anchor)?;
    let groups = paper_state.activity_groups_after(&wallet, first_anchor.activity_cutoff_unix)?;
    let mut next_group = 0usize;
    for anchor in anchors.iter().skip(1) {
        while let Some(group) = groups.get(next_group) {
            if group.source_epoch > anchor.activity_cutoff_unix {
                break;
            }
            apply_replayed_group(&mut ledger, paper_state, wallet, group)?;
            next_group = next_group.saturating_add(1);
        }
        install_replayed_anchor(&mut ledger, paper_state, anchor)?;
    }
    for group in groups.iter().skip(next_group) {
        apply_replayed_group(&mut ledger, paper_state, wallet, group)?;
    }
    Ok(ledger)
}

fn install_replayed_anchor(
    ledger: &mut PositionLedger,
    paper_state: &PaperStateDb,
    anchor: &pe_paper_state::PositionAnchorRow,
) -> Result<(), WalletLedgerReplayError> {
    let balances: Vec<(String, u16, ShareAmount)> = serde_json::from_str(&anchor.balances_json)
        .map_err(|source| WalletLedgerReplayError::AnchorBalances {
            anchor_seq: anchor.anchor_seq,
            source,
        })?;
    let mut positions = HashMap::new();
    for (market_id, outcome, amount) in balances {
        let key = MarketOutcomeId::new(
            MarketId(VenueMarketId(market_id.clone())),
            OutcomeId(outcome),
        );
        if positions
            .insert(
                key,
                PositionState {
                    long_contracts: amount,
                    short_contracts: ShareAmount::ZERO,
                },
            )
            .is_some()
        {
            return Err(WalletLedgerReplayError::DuplicateAnchorBalance {
                anchor_seq: anchor.anchor_seq,
                market_id,
                outcome,
            });
        }
    }
    ledger.replace_wallet_snapshot(anchor.wallet, positions);
    let capture = ledger_capture(ledger, paper_state, anchor.wallet)
        .map_err(|error| WalletLedgerReplayError::LedgerCapture(error.to_string()))?;
    if capture.hash != anchor.ledger_hash_after {
        return Err(WalletLedgerReplayError::AnchorHashMismatch {
            anchor_seq: anchor.anchor_seq,
        });
    }
    Ok(())
}

fn apply_replayed_group(
    ledger: &mut PositionLedger,
    paper_state: &PaperStateDb,
    wallet: WalletAddress,
    group: &pe_paper_state::ActivityGroupRow,
) -> Result<(), WalletLedgerReplayError> {
    let durable = paper_state.activity_group_state(&group.source_trade_id)?;
    verify_replayed_group_revision(durable.as_ref(), group)?;
    let effect = LedgerEffect::from_document(&group.proof_json).map_err(|source| {
        WalletLedgerReplayError::EffectDocument {
            source_trade_id: group.source_trade_id.clone(),
            source,
        }
    })?;
    if !applied_disposition(&group.disposition) {
        return Ok(());
    }
    let source_time =
        time::OffsetDateTime::from_unix_timestamp(group.source_epoch).map_err(|_| {
            WalletLedgerReplayError::InvalidSourceEpoch {
                source_trade_id: group.source_trade_id.clone(),
                source_epoch: group.source_epoch,
            }
        })?;
    ledger
        .apply(&LedgerMutation {
            source_trade_id: group.source_trade_id.clone(),
            transaction_hash: group.source_trade_id.0.clone(),
            wallet,
            source_time: SourceTimestamp(source_time),
            effect,
        })
        .map_err(|error| WalletLedgerReplayError::Ledger {
            source_trade_id: group.source_trade_id.clone(),
            message: error.to_string(),
        })
}

fn verify_replayed_group_revision(
    durable: Option<&pe_paper_state::ActivityGroupState>,
    group: &pe_paper_state::ActivityGroupRow,
) -> Result<(), WalletLedgerReplayError> {
    if durable.is_none_or(|state| {
        state.semantic_revision != group.semantic_revision
            || state.source_epoch != group.source_epoch
            || state.disposition != group.disposition
    }) {
        return Err(WalletLedgerReplayError::RevisionMismatch {
            source_trade_id: group.source_trade_id.clone(),
        });
    }
    Ok(())
}

fn applied_disposition(disposition: &str) -> bool {
    matches!(
        disposition,
        "applied"
            | "decision_pending"
            | "not_copy_eligible"
            | "not_an_entry"
            | "not_first_entry"
            | "not_buy"
            | "wallet_history_incomplete"
            | "ambiguous_first_entry_same_second"
            | "order_dependent_equal_second_action"
            | "stale_fallback_past_copy_budget"
            | "stale_activity_ws_past_copy_budget"
    )
}

#[cfg(test)]
mod anchor_replay_tests {
    use super::*;

    #[test]
    fn changed_revision_is_a_typed_replay_failure() {
        let source_trade_id = SourceTradeId("g2:revision".to_owned());
        let group = pe_paper_state::ActivityGroupRow {
            source_trade_id: source_trade_id.clone(),
            source_epoch: 100,
            semantic_revision: "revision-a".to_owned(),
            disposition: "applied".to_owned(),
            proof_json: "{}".to_owned(),
        };
        let durable = pe_paper_state::ActivityGroupState {
            transaction_hash: "0xtx".to_owned(),
            semantic_revision: "revision-b".to_owned(),
            source_epoch: 100,
            disposition: "applied".to_owned(),
        };
        assert!(matches!(
            verify_replayed_group_revision(Some(&durable), &group),
            Err(WalletLedgerReplayError::RevisionMismatch {
                source_trade_id: actual
            }) if actual == source_trade_id
        ));
    }
}
