//! Boot resume for staged live dispatch aggregates (#508 Decision 10).
//!
//! A crash can leave a seed `pending_paper` in three distinct situations, each with its own
//! resume rule:
//!
//! 1. **A durable paper fill frame exists** for the seed's `dispatch_id` (crash between the
//!    event-log `sync()` and the local flip transaction): the paper outcome IS durable, so
//!    the seed flips `ready` with outcome `fill`.
//! 2. **No fill frame and no pending redelivery** (the leader's poll cursor advanced past
//!    the frozen signal's observed time — `trade_poller` marks the cursor at fetch, so the
//!    trade will never be re-delivered): the seed is finalized from its own frozen signal
//!    with the typed outcome `no_fill:stuck_seed_boot_finalized`. A `pending_paper` seed
//!    never blocks later `ready` seeds — "strictly in order" governs consumption of ready
//!    seeds, not a global barrier.
//! 3. **A redelivery is still possible** (cursor at/behind the observed time): the seed is
//!    left `pending_paper`; the redelivered trade reuses it and flips it on the normal path.
//!
//! Recovery only ever FLIPS existing staged seeds — it never reconstructs dispatch state
//! from current accounts or configuration (the frozen `signal_json` is the sole identity).

use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result};
use pe_core_types::WalletAddress;
use pe_paper_state::PaperStateDb;
use serde::Deserialize;
use tracing::{info, warn};

use crate::paper_recovery::{
    FinancialPayload, FinancialResult, PaperLogFrame, PaperLogRecord, paper_era, scan_paper_log,
};

/// The subset of the frozen `signal_json` the boot resume needs. The staged JSON is
/// `{"schema_version":1,"signal":{...LeaderSignal...},...}` (see the orchestrator's
/// `stage_dispatch_if_targeted`).
#[derive(Debug, Deserialize)]
struct FrozenSeed {
    signal: FrozenSignal,
}

#[derive(Debug, Deserialize)]
struct FrozenSignal {
    leader: FrozenLeader,
    #[serde(with = "time::serde::rfc3339")]
    observed_at: time::OffsetDateTime,
}

/// `TraderId` serializes as the wallet address string; accept both the transparent string
/// and a one-field tuple form defensively.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum FrozenLeader {
    Wallet(WalletAddress),
}

/// Counters from one boot resume pass, for logging/observability.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct DispatchResume {
    /// Seeds flipped `ready` with outcome `fill` (durable fill frame found).
    pub flipped_fill: usize,
    /// Stuck seeds finalized `ready` with the typed boot no-fill outcome.
    pub finalized_stuck: usize,
    /// Seeds left `pending_paper` awaiting a possible redelivery.
    pub left_pending: usize,
}

/// Typed outcome recorded on a stuck seed finalized at boot.
pub const STUCK_SEED_OUTCOME: &str = "no_fill:stuck_seed_boot_finalized";

/// Resume every `pending_paper` dispatch seed per the module rules. Call at boot AFTER
/// [`crate::paper_recovery::reconcile_paper_state`] so fill accounting is already healed.
pub fn resume_dispatch_seeds(
    event_log_path: &Path,
    paper_state: &PaperStateDb,
) -> Result<DispatchResume> {
    let pending = paper_state
        .pending_dispatch_seeds()
        .context("read pending dispatch seeds")?;
    let mut out = DispatchResume::default();
    if pending.is_empty() {
        return Ok(out);
    }

    // One shared verified paper-log pass: fill idempotency key → terminal frame sequence.
    // In an active era only a matching FinancialFinal is terminal; pre-Start legacy fills
    // retain the old disposition rules.
    let mut logged_keys: HashMap<String, u64> = HashMap::new();
    let mut active_era = false;
    if event_log_path.exists() {
        let era = paper_era(
            scan_paper_log(event_log_path)
                .with_context(|| format!("scan paper log {}", event_log_path.display()))?,
        );
        active_era = era.start.is_some();
        let mut prepared_keys = HashMap::new();
        for frame in era.frames {
            if let Some(fill) = frame.legacy_fill().filter(|_| !active_era) {
                logged_keys.insert(
                    fill.intent.idempotency_key.clone(),
                    frame.receipt.sequence.0,
                );
                continue;
            }
            match frame.frame {
                PaperLogFrame::LegacyFill => {}
                PaperLogFrame::Record(PaperLogRecord::FinancialPrepared {
                    payload:
                        FinancialPayload::Fill {
                            operation,
                            economic,
                        },
                    ..
                }) if active_era => {
                    let key = pe_strategy_winner_follow::evaluate::build_idempotency_key_parts(
                        &operation.leader_wallet.to_string(),
                        &operation.source_trade_id.0,
                        &economic.market.market_id,
                        u16::from(economic.market.outcome_index),
                        economic.market.side,
                        operation.observed_at_bucket,
                    );
                    prepared_keys.insert(frame.receipt.sequence, key);
                }
                PaperLogFrame::Record(PaperLogRecord::FinancialFinal {
                    prepared_receipt,
                    result: FinancialResult::Fill { .. },
                }) if active_era => {
                    if let Some(key) = prepared_keys.get(&prepared_receipt.sequence) {
                        logged_keys.insert(key.clone(), frame.receipt.sequence.0);
                    }
                }
                _ => {}
            }
        }
    }
    let last_applied = paper_state
        .last_applied_event_seq_opt()
        .context("read last_applied")?;

    for seed in pending {
        if let Some(&frame_seq) = logged_keys.get(&seed.dispatch_id) {
            // Rule 1 (#511 disposition-aware): the frame is durable — flip by its
            // DISPOSITION, not by mere existence. A fills row = the fill applied; a
            // row-less frame at or below `last_applied` = the terminal settled refusal;
            // a row-less frame ABOVE `last_applied` has no disposition yet (the boot
            // frame-walk halted before it) — leave it pending for the next pass.
            if active_era {
                paper_state
                    .flip_dispatch_ready(&seed.dispatch_id, "fill")
                    .context("flip recovered finalized fill seed")?;
                out.flipped_fill += 1;
            } else if paper_state
                .fill_exists(&seed.dispatch_id)
                .context("check fill disposition")?
            {
                paper_state
                    .flip_dispatch_ready(&seed.dispatch_id, "fill")
                    .context("flip recovered fill seed")?;
                out.flipped_fill += 1;
            } else if last_applied.is_some_and(|cursor| frame_seq <= cursor.0) {
                paper_state
                    .flip_dispatch_ready(&seed.dispatch_id, "no_fill:market_settled")
                    .context("flip refused seed")?;
                out.finalized_stuck += 1;
            } else {
                out.left_pending += 1;
            }
            continue;
        }
        // Rules 2/3: decide by redelivery possibility from the frozen signal + poll cursor.
        let frozen: FrozenSeed = match serde_json::from_str(&seed.signal_json) {
            Ok(f) => f,
            Err(e) => {
                // A frozen signal that cannot be parsed cannot be recomputed OR redelivered
                // deterministically — finalize with the typed stuck outcome (fail closed for
                // paper; the live fan-out still sees the frozen targets).
                warn!(
                    dispatch_id = %seed.dispatch_id,
                    error = %e,
                    "dispatch resume: frozen signal unparseable; finalizing as stuck"
                );
                paper_state
                    .flip_dispatch_ready(&seed.dispatch_id, STUCK_SEED_OUTCOME)
                    .context("finalize unparseable stuck seed")?;
                out.finalized_stuck += 1;
                continue;
            }
        };
        let FrozenLeader::Wallet(wallet) = frozen.signal.leader;
        let observed_unix = frozen.signal.observed_at.unix_timestamp();
        let cursor = paper_state.cursor(&wallet).context("read poll cursor")?;
        let redelivery_possible = redelivery_is_possible(cursor, observed_unix);
        if redelivery_possible {
            out.left_pending += 1;
        } else {
            paper_state
                .flip_dispatch_ready(&seed.dispatch_id, STUCK_SEED_OUTCOME)
                .context("finalize stuck seed")?;
            out.finalized_stuck += 1;
        }
    }
    info!(
        flipped_fill = out.flipped_fill,
        finalized_stuck = out.finalized_stuck,
        left_pending = out.left_pending,
        "dispatch seeds resumed at boot"
    );
    Ok(out)
}

fn redelivery_is_possible(cursor: Option<i64>, observed_unix: i64) -> bool {
    cursor.is_none_or(|cursor| cursor <= observed_unix)
}

#[cfg(test)]
mod tests {
    use super::redelivery_is_possible;

    #[test]
    fn cursor_equal_to_observed_boundary_remains_redeliverable() {
        assert!(redelivery_is_possible(Some(1_700_000_000), 1_700_000_000));
        assert!(redelivery_is_possible(Some(1_699_999_999), 1_700_000_000));
        assert!(!redelivery_is_possible(Some(1_700_000_001), 1_700_000_000));
    }
}
