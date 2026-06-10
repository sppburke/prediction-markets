//! Offline `(bps × ms × cooldown × top_n)` strategy sweep over a captured run's
//! `raw_ticks` tape (issue #310). Deterministic, no network; runs on a **copy**
//! of a run DB (`--db`), never the live one.
//!
//! Replay is "`drive()` without the network": one streaming decode pass
//! ([`events`]), then a fresh `JoinState` per grid cell fed the cell's top-N
//! venue ticks **in tape order** — the same pure detection path live runs.
//! Books bypass `on_book_update`: fires are sparse, so each emitted market's
//! books are resolved from the per-frame [`book_index`] at the fire's tape
//! position and the observation is rebuilt through the same pure
//! `compute_observation` live uses. Markets register at their **activation id**
//! (first raw CLOB frame referencing either token), reproducing live's
//! registration timing; the reference cell's divergence from the live
//! `observations` table is measured and reported, never assumed away.

mod book_index;
mod events;
mod grid;
mod output;
mod ranking;
mod scorers;

use std::collections::HashMap;
use std::path::PathBuf;

use tracing::{info, warn};

use crate::config::ShadowConfig;
use crate::consensus::ConsensusParams;
use crate::db::{DbError, FRAMES_DROPPED_META_KEYS, ShadowDb};
use crate::error::Error;
use crate::join::{JoinState, compute_observation};
use crate::types::{BtcMarketMeta, ExchangeTick};

pub use events::DecodeStats;
pub use output::{
    BUY_HOLD_FEE_PROVENANCE, CAPTURE_CONFIG_PROVENANCE, CellResult, FidelitySummary,
    ReferenceParams, SweepOutput, TapeValidity,
};

use events::Tape;
use scorers::ReplayFire;

/// The reference cell replays all live venues.
const REFERENCE_TOP_N: usize = 3;

/// `sweep` subcommand arguments (parsed here — no separate `args.rs`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SweepArgs {
    /// Path to a **copy** of a run DB (read-only open).
    pub db: PathBuf,
    /// Optional JSON output path; stdout when omitted.
    pub out: Option<PathBuf>,
}

impl SweepArgs {
    /// Parse the subcommand's trailing args. `--config` pairs are skipped (the
    /// caller already consumed them for the config load).
    pub fn parse(rest: &[String]) -> Result<Self, Error> {
        let mut db = None;
        let mut out = None;
        let mut i = 0;
        while i < rest.len() {
            match rest[i].as_str() {
                "--db" if i + 1 < rest.len() => {
                    db = Some(PathBuf::from(&rest[i + 1]));
                    i += 2;
                }
                "--out" if i + 1 < rest.len() => {
                    out = Some(PathBuf::from(&rest[i + 1]));
                    i += 2;
                }
                "--config" if i + 1 < rest.len() => i += 2,
                other => {
                    return Err(Error::Sweep(format!(
                        "unknown or incomplete sweep arg {other:?} \
                         (usage: sweep --db <path> [--out <json-path>])"
                    )));
                }
            }
        }
        let db = db.ok_or_else(|| Error::Sweep("--db <path> is required".to_string()))?;
        Ok(Self { db, out })
    }
}

/// Run the full sweep: decode once, replay the reference cell + 480 grid cells,
/// assemble the tape-validity block. Pure given the DB contents.
pub fn sweep(config: &ShadowConfig, args: &SweepArgs) -> Result<SweepOutput, Error> {
    let db = ShadowDb::open_readonly(&args.db)?;
    let markets = db.all_markets()?;
    let resolutions = db.all_resolutions_map()?;
    let live_observations = db.all_observations_full()?;

    let tape = events::load_and_decode(&db, &markets)?;
    info!(
        frames = tape.stats.frames_total,
        exchange_ticks = tape.stats.exchange_ticks,
        book_updates = tape.stats.clob_book_updates_indexed,
        clob_decode_errors = tape.stats.clob_decode_errors,
        exchange_decode_errors = tape.stats.exchange_decode_errors,
        capped_tokens = tape.stats.capped_tokens,
        "sweep: tape decoded"
    );

    let markets_by_condition: HashMap<String, BtcMarketMeta> = markets
        .iter()
        .map(|m| (m.condition_id.clone(), m.clone()))
        .collect();
    // Activation order: markets register as the tape crosses their first CLOB
    // frame, regardless of which venue ticks the cell retains.
    let mut activations: Vec<(i64, String)> = tape
        .activation_by_condition
        .iter()
        .map(|(condition, tape_id)| (*tape_id, condition.clone()))
        .collect();
    activations.sort_unstable();

    // Reference cell: trigger params from the sweep invocation's config (the
    // tape does not record the capture run's params — stamped as an assumption).
    let reference_params = config.consensus_params();
    let reference_fires = replay_cell(
        &tape,
        &markets_by_condition,
        &activations,
        reference_params,
        REFERENCE_TOP_N,
    );
    let fidelity = output::fidelity_summary(&live_observations, &reference_fires);
    if fidelity.missing_rows > 0 || fidelity.extra_rows > 0 {
        warn!(
            missing = fidelity.missing_rows,
            extra = fidelity.extra_rows,
            "sweep: reference cell diverges from live observations"
        );
    }

    // frames_dropped_* from meta: present+zero = clean; non-zero = invalid;
    // MISSING = unknown/not-clean (crashed capture never reaches the run-end
    // stamp) — never defaulted to 0. A present-but-unparseable value is corrupt.
    let mut frames_dropped = std::collections::BTreeMap::new();
    for key in FRAMES_DROPPED_META_KEYS {
        let value = match db.get_meta(key)? {
            None => None,
            Some(raw) => Some(
                raw.parse::<u64>()
                    .map_err(|_| Error::Db(DbError::Corrupt(format!("bad meta {key}={raw:?}"))))?,
            ),
        };
        frames_dropped.insert(key.to_string(), value);
    }
    let (valid, frames_dropped_status) = output::frames_dropped_validity(&frames_dropped);
    if !valid {
        warn!(status = %frames_dropped_status, "sweep: TAPE NOT CLEAN — results stamped invalid");
    }

    let mut cells = Vec::with_capacity(480);
    for cell in grid::cells() {
        let fires = replay_cell(
            &tape,
            &markets_by_condition,
            &activations,
            cell.consensus_params(),
            cell.top_n,
        );
        cells.push(CellResult {
            threshold_bps: cell.threshold_bps,
            window_ms: cell.window_ms,
            cooldown_ms: cell.cooldown_ms,
            top_n: cell.top_n,
            fires: fires.len(),
            near_degenerate_window: cell.near_degenerate_window(),
            buy_hold: scorers::buy_hold_groups(&fires, &resolutions),
        });
    }

    Ok(SweepOutput {
        tape_validity: TapeValidity {
            valid,
            frames_dropped_status,
            frames_dropped,
            decode_stats: tape.stats,
            reference_params: ReferenceParams {
                threshold_bps: reference_params.threshold_bps,
                window_ms: reference_params.window_ms,
                cooldown_ms: reference_params.cooldown_ms,
                min_venues: reference_params.min_venues,
                top_n: REFERENCE_TOP_N,
            },
            capture_config_provenance: CAPTURE_CONFIG_PROVENANCE.to_string(),
            fidelity,
            fee_provenance: vec![
                crate::fees::CRYPTO_FEES_V2_PROVENANCE.to_string(),
                BUY_HOLD_FEE_PROVENANCE.to_string(),
            ],
        },
        cells,
    })
}

/// The `sweep` subcommand: parse args, run, print the table, emit JSON.
pub fn sweep_cmd(config: &ShadowConfig, rest: &[String]) -> Result<(), Error> {
    let args = SweepArgs::parse(rest)?;
    let result = sweep(config, &args)?;
    println!("{}", result.to_table());
    let json = result.to_json().map_err(Error::Json)?;
    match &args.out {
        Some(path) => std::fs::write(path, json)?,
        None => println!("{json}"),
    }
    Ok(())
}

/// Replay one cell: fresh `JoinState`, tape-order events, activation-gated
/// `upsert_market`, top-N venue filter, post-fire book resolution through the
/// same pure `compute_observation` live uses.
fn replay_cell(
    tape: &Tape,
    markets_by_condition: &HashMap<String, BtcMarketMeta>,
    activations: &[(i64, String)],
    params: ConsensusParams,
    top_n: usize,
) -> Vec<ReplayFire> {
    let mut state = JoinState::new(Vec::new(), params);
    let venues = ranking::venues_for_top_n(top_n);
    let mut next_activation = 0;
    let mut fires = Vec::new();

    for ev in &tape.events {
        // Register every market whose activation frame precedes this tick —
        // BEFORE the venue filter: registration is tape-position-driven,
        // independent of which venue ticks this cell retains.
        while next_activation < activations.len() && activations[next_activation].0 <= ev.tape_id {
            if let Some(meta) = markets_by_condition.get(&activations[next_activation].1) {
                state.upsert_market(meta.clone());
            }
            next_activation += 1;
        }
        if !venues.contains(&ev.venue) {
            continue;
        }
        let tick = ExchangeTick {
            venue: ev.venue,
            price: ev.price,
            observed_at_ms: ev.observed_at_ms,
        };
        let emitted = state.on_exchange_tick(&tick, ev.received_ms);
        for obs in emitted {
            let Some(meta) = markets_by_condition.get(&obs.condition_id) else {
                continue;
            };
            // Books resolved by tape position (entries <= the fire frame's id),
            // then the observation is rebuilt with identical fields + fee math.
            let yes = tape.books.book_at_tape(&meta.yes_token_id, ev.tape_id);
            let no = tape.books.book_at_tape(&meta.no_token_id, ev.tape_id);
            let rebuilt = compute_observation(
                meta,
                obs.signal_value,
                obs.observed_at_ms,
                ev.received_ms,
                obs.range_start_value,
                yes.as_ref().map(|(book, received_ms)| (book, *received_ms)),
                no.as_ref().map(|(book, received_ms)| (book, *received_ms)),
                obs.move_magnitude_bps,
                obs.move_direction,
            );
            fires.push(ReplayFire {
                obs: rebuilt,
                fire_tape_id: ev.tape_id,
            });
        }
    }
    fires
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn strings(args: &[&str]) -> Vec<String> {
        args.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn sweep_args_parse_db_out_and_skip_config() {
        let args = SweepArgs::parse(&strings(&[
            "--config", "c.toml", "--db", "tape.db", "--out", "g.json",
        ]))
        .unwrap();
        assert_eq!(args.db, PathBuf::from("tape.db"));
        assert_eq!(args.out, Some(PathBuf::from("g.json")));

        let minimal = SweepArgs::parse(&strings(&["--db", "tape.db"])).unwrap();
        assert_eq!(minimal.out, None);
    }

    #[test]
    fn sweep_args_reject_missing_db_and_unknown_flags() {
        assert!(matches!(SweepArgs::parse(&[]), Err(Error::Sweep(_))));
        assert!(matches!(
            SweepArgs::parse(&strings(&["--db", "x", "--frobnicate"])),
            Err(Error::Sweep(_))
        ));
        // Trailing flag without its value is incomplete, not a panic.
        assert!(matches!(
            SweepArgs::parse(&strings(&["--db"])),
            Err(Error::Sweep(_))
        ));
    }
}
