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

use std::collections::HashSet;
use std::path::Path;

use anyhow::{Context, Result};
use pe_core_types::WalletAddress;
use pe_event_log::Reader;
use pe_paper_state::PaperStateDb;
use pe_strategy_winner_follow::PaperFill;
use serde::Deserialize;
use tracing::{info, warn};

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

    // One event-log pass: the set of fill idempotency keys that are durably logged.
    let mut logged_keys: HashSet<String> = HashSet::new();
    if event_log_path.exists() {
        let replay = Reader::replay(event_log_path)
            .with_context(|| format!("open event log {}", event_log_path.display()))?;
        for frame in replay {
            let (_seq, envelope) = frame.context("read event-log frame")?;
            let fill: PaperFill =
                serde_json::from_slice(&envelope.payload).context("decode PaperFill")?;
            logged_keys.insert(fill.intent.idempotency_key);
        }
    }

    for seed in pending {
        if logged_keys.contains(&seed.dispatch_id) {
            // Rule 1: the paper fill is durable; only the flip was lost.
            paper_state
                .flip_dispatch_ready(&seed.dispatch_id, "fill")
                .context("flip recovered fill seed")?;
            out.flipped_fill += 1;
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
