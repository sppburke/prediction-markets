//! Crash-safe SQLite mirror of the paper-trader's runtime state.
//!
//! The BLAKE3 event log is the source of truth for fills; this SQLite database
//! (`paper-state`) is the crash-safe runtime mirror, reconciled to the log by
//! `event_seq`. It holds:
//!
//! - `seen_trades` — permanent input dedup keyed by `source_trade_id`.
//! - `fills` — output dedup keyed by `idempotency_key`, with the originating `event_seq`.
//! - `positions` — our own net paper positions per `(market, outcome)`.
//! - `leader_positions` — scalar mirror of the in-memory leader `PositionLedger`.
//! - `bankroll` — single-row drawdown-aware current bankroll.
//! - `poll_cursors` — per-wallet last-seen `observed_at` unix timestamp.
//! - `meta` — `last_applied_event_seq` reconciliation cursor.
//! - `settled_markets` — durable settled-markets set; the resolution double-credit guard.
//! - `fill_market_snapshots` — best-effort fill-time market-liquidity snapshots (WS2, issue #350).
//!
//! Two write shapes are exposed, each a single transaction:
//! [`PaperStateDb::commit_seen_no_fill`] (a processed trade that produced no order)
//! and [`PaperStateDb::commit_fill`] (a processed trade that produced a paper fill).
//! [`PaperStateDb::reconcile_fill`] replays a logged fill whose SQLite commit was
//! lost to a crash between the event-log `sync()` and the SQLite commit.
//!
//! # Async
//!
//! All methods are synchronous and brief (single-row indexed writes under WAL +
//! `synchronous = NORMAL`). The orchestrator consumes trades sequentially, so a
//! short blocking call on the async worker is acceptable for v1; revisit with
//! `spawn_blocking` if RTDS volume (issue #282 Phase 2) makes it material.

mod migration;
mod schema;

use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::Path;
use std::str::FromStr;
use std::sync::{Mutex, PoisonError};

use rusqlite::{Connection, OpenFlags, OptionalExtension as _, Transaction, params};
use rust_decimal::Decimal;

use pe_core_types::{
    EventSeq, MarketId, OutcomeId, Price, ShareAmount, Side, SourceTradeId,
    SourceTradeIdentityVersion, VenueMarketId, WalletAddress,
};

pub use migration::{
    BoundaryField, BoundaryMismatch, DurableLogBindings, DurableLogName, MigrationMetadata,
    MigrationPhase, MigrationRecord, PaperMainSeal, PaperSideBuildReport, verify_log_bindings,
    verify_side_main_path,
};
pub use schema::SCHEMA_VERSION;
use schema::{
    BANKROLL_ROW_ID, META_LAST_APPLIED_EVENT_SEQ, META_LAST_SUPABASE_APPLIED_EVENT_SEQ, SCHEMA,
};

/// Errors from the paper-state store.
#[derive(Debug, thiserror::Error)]
pub enum PaperStateError {
    /// Underlying SQLite failure.
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    /// On-disk schema version does not match [`SCHEMA_VERSION`].
    #[error(
        "paper-state schema version mismatch: db has user_version {found}, expected {expected}"
    )]
    SchemaVersionMismatch { found: i64, expected: i64 },
    /// A stored value could not be parsed back into its typed form.
    #[error("corrupt stored value: {0}")]
    Corrupt(String),
    /// An internal invariant was violated (e.g. a poisoned lock or arithmetic overflow).
    #[error("internal invariant violated: {0}")]
    Internal(String),
    /// Filesystem synchronization failed while persisting boot metadata.
    #[error("paper-state synchronization failed: {0}")]
    Synchronization(#[from] std::io::Error),
    /// Migration metadata JSON was malformed or could not be encoded.
    #[error("paper-state migration metadata encoding failed: {0}")]
    MigrationEncoding(#[from] serde_json::Error),
    /// A resume attempt addressed a side main other than the exact recorded path.
    #[error(
        "paper-state migration side path mismatch: expected {}, got {}",
        expected.display(),
        actual.display()
    )]
    MigrationSidePathMismatch {
        expected: std::path::PathBuf,
        actual: std::path::PathBuf,
    },
    /// The schema-v1 bootstrap path could not find the existing `meta` owner.
    #[error("paper-state migration meta table is missing")]
    MigrationMetaMissing,
    /// The immutable migration record already exists with different content.
    #[error("paper-state immutable migration record differs from the requested record")]
    MigrationRecordConflict,
    /// A migration record was required but absent.
    #[error("paper-state migration record is missing")]
    MigrationRecordMissing,
    /// Fixed-main and side-main migration records do not agree.
    #[error("paper-state fixed and side migration records diverge")]
    MigrationRecordDivergent,
    /// The requested phase transition is not the next state-machine edge.
    #[error("invalid paper-state migration phase transition from {from} to {to}")]
    MigrationPhaseTransition { from: String, to: String },
    /// A synchronized metadata write could not checkpoint every WAL frame.
    #[error(
        "paper-state migration checkpoint incomplete: busy={busy}, log={log}, checkpointed={checkpointed}"
    )]
    MigrationCheckpointIncomplete {
        busy: i64,
        log: i64,
        checkpointed: i64,
    },
    /// A supplied authoritative replacement contains duplicate position keys.
    #[error("authoritative positions contain duplicate key {market_id}/{outcome_id}")]
    DuplicateAuthoritativePosition { market_id: String, outcome_id: u16 },
    /// A version-two transaction attempted to use a legacy identity shape.
    #[error("paper-state v2 activity key is not a canonical g2 identity: {0}")]
    InvalidVersionTwoIdentity(String),
    /// A previously stored group was presented with different durable semantics.
    #[error("paper-state activity revision conflict for {0}")]
    ActivityRevisionConflict(String),
    /// A candidate anchor would regress the wallet's already-installed activity cutoff.
    #[error(
        "paper-state anchor cutoff regression for {wallet}: stored {stored_cutoff_unix}, candidate {candidate_cutoff_unix}"
    )]
    AnchorCutoffRegression {
        wallet: WalletAddress,
        stored_cutoff_unix: i64,
        candidate_cutoff_unix: i64,
    },
    /// A bucket mixed wallets or epoch seconds and therefore cannot be atomic.
    #[error("paper-state activity bucket identity mismatch")]
    ActivityBucketMismatch,
    /// A terminal continuation was replayed with a different terminal transition.
    #[error("paper-state decision_pending terminal transition conflict for {0}")]
    DecisionPendingConflict(String),
    /// The one-time legacy history input was not a valid sidecar.
    #[error("legacy wallet history import is invalid: {0}")]
    InvalidLegacyHistory(String),
}

/// One leader's net position in a `(market, outcome)`, mirroring the in-memory
/// `PositionState`. The service tier groups these into `PositionSnapshot`s.
/// Typed no-copy disposition for a stale observation from either transport (#530/#546).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NoCopyDisposition {
    /// `"rest_poll"` or `"activity_ws"` (schema CHECK-enforced).
    pub provenance: String,
    /// Observation age at admission (now − trade timestamp), seconds.
    pub age_secs: i64,
    /// Short machine-readable reason, e.g. `"stale_fallback_past_copy_budget"`.
    pub reason: String,
    /// Wall-clock admission time (audit only; never a decision input).
    pub recorded_at_unix: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaderPositionRow {
    pub wallet: WalletAddress,
    pub market_id: MarketId,
    pub outcome_id: OutcomeId,
    pub long_contracts: ShareAmount,
    pub short_contracts: ShareAmount,
}

/// Durable terminal/apply record for one reconciled activity group (#544).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivityDispositionRecord {
    pub source_trade_id: SourceTradeId,
    pub transaction_hash: String,
    pub wallet: WalletAddress,
    pub source_epoch: i64,
    pub semantic_revision: String,
    pub activity_type: String,
    pub disposition: String,
    /// Caller-owned canonical JSON. Versioned effect documents are stored here
    /// unchanged so replay can select and verify the recorded effect schema.
    pub proof_json: String,
    /// Exact typed admission disposition when this group is ledger-only.
    pub no_copy: Option<NoCopyDisposition>,
}

/// Initially applied durable semantics for one reconciled group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivityGroupState {
    pub transaction_hash: String,
    pub semantic_revision: String,
    pub source_epoch: i64,
    pub disposition: String,
}

/// Durable result of applying the first-entry rule to one trade group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntryGateResultRecord {
    pub source_trade_id: SourceTradeId,
    pub wallet: WalletAddress,
    pub market_id: MarketId,
    pub source_epoch: i64,
    pub result: String,
    pub history_consumed: bool,
}

/// One durable market-history effect committed with its wallet-second.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketHistoryRecord {
    pub wallet: WalletAddress,
    pub market_id: MarketId,
    pub first_epoch: i64,
    pub source_trade_id: SourceTradeId,
}

/// Complete-history proof used to fail membership closed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalletHistoryStatusRecord {
    pub wallet: WalletAddress,
    pub complete: bool,
    pub proof_json: String,
    pub updated_at_unix: i64,
}

/// One accepted five-step activity/current-position bracket (#544).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PositionValidationRecord {
    pub wallet: WalletAddress,
    pub ledger_hash: String,
    pub positions_proof_hash: String,
    pub activity_bounds_json: String,
    pub source_log_generation: String,
    pub proof_json: String,
    pub recorded_at_unix: i64,
}

/// One venue-authoritative balance snapshot to install atomically with its
/// accepted activity/positions bracket and wallet coverage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnchorInstallRecord {
    pub wallet: WalletAddress,
    pub balances: Vec<(MarketId, OutcomeId, ShareAmount)>,
    pub activity_cutoff_unix: i64,
    pub anchored_at_unix: i64,
    pub ledger_hash_after: String,
    pub positions_proof_hash: String,
    pub activity_bounds_json: String,
    pub source_log_generation: String,
    pub proof_json: String,
    pub recorded_at_unix: i64,
}

/// Durable coverage state for one wallet's latest installed anchor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalletCoverage {
    pub activity_cutoff_unix: Option<i64>,
    pub coverage_generation: i64,
    pub reanchor_required: bool,
    pub anchor_seq: Option<i64>,
    pub anchored_at_unix: Option<i64>,
}

/// One append-only venue-authoritative position anchor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PositionAnchorRow {
    pub wallet: WalletAddress,
    pub anchor_seq: i64,
    pub anchored_at_unix: i64,
    pub activity_cutoff_unix: i64,
    pub balances_json: String,
    pub ledger_hash_after: String,
    pub proof_json: String,
}

/// One replayable activity group after an anchor cutoff.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivityGroupRow {
    pub source_trade_id: SourceTradeId,
    pub source_epoch: i64,
    pub semantic_revision: String,
    pub disposition: String,
    pub proof_json: String,
}

/// Frozen continuation created in the same transaction as an admitted entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecisionPendingRecord {
    pub source_trade_id: SourceTradeId,
    pub semantic_revision: String,
    pub wallet: WalletAddress,
    pub source_epoch: i64,
    pub frozen_inputs_json: String,
    pub updated_at_unix: i64,
}

/// Open or terminal continuation row reconstructed at boot/replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecisionPendingRow {
    pub source_trade_id: SourceTradeId,
    pub semantic_revision: String,
    pub wallet: WalletAddress,
    pub source_epoch: i64,
    pub frozen_inputs_json: String,
    pub post_commit_inputs_json: String,
    pub state: DecisionPendingState,
    pub terminal_disposition: Option<String>,
    pub updated_at_unix: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecisionPendingState {
    Open,
    Terminal,
}

/// Caller-rendered versioned evidence attached to a terminal pending-decision
/// transition. The paper-state crate owns atomicity; the service tier owns the
/// external-input schema so dependency direction stays intact.
#[derive(Debug, Clone, Copy)]
pub struct PendingTerminalEvidence<'a> {
    pub post_commit_inputs_json: &'a str,
    pub updated_at_unix: i64,
}

/// Monotonic durable wallet fence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalletFenceRecord {
    pub wallet: WalletAddress,
    pub source_trade_id: SourceTradeId,
    pub cause: String,
    pub proof_json: String,
    pub fenced_at_unix: i64,
}

/// Re-anchor state transition committed with its triggering activity group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReanchorRecord {
    pub source_trade_id: SourceTradeId,
    pub reason: String,
}

/// Complete atomic wallet-second commit assembled by the service bucket engine.
#[derive(Debug, Clone)]
pub struct ActivityBucketCommit {
    pub wallet: WalletAddress,
    pub source_epoch: i64,
    pub dispositions: Vec<ActivityDispositionRecord>,
    pub leader_positions: Vec<LeaderPositionRow>,
    pub gate_results: Vec<EntryGateResultRecord>,
    pub history_effects: Vec<MarketHistoryRecord>,
    pub history_status: Option<WalletHistoryStatusRecord>,
    pub pending: Vec<DecisionPendingRecord>,
    pub fence: Option<WalletFenceRecord>,
    pub reanchor: Option<ReanchorRecord>,
    pub advance_cursor: bool,
}

/// Stored proof of the one-time legacy sidecar import.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyHistoryImport {
    pub source_hash: String,
    pub parsed_row_count: u64,
    pub result: String,
    pub imported_at_unix: i64,
    pub already_imported: bool,
}

/// One of our own net paper positions in a `(market, outcome)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaperPositionRow {
    pub market_id: MarketId,
    pub outcome_id: OutcomeId,
    pub long_contracts: u64,
    pub short_contracts: u64,
}

/// Outcome of a local fill commit (#511). Both arms carry the post-commit bankroll.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FillCommitOutcome {
    /// The fill applied (or was an already-applied duplicate key; no double-count).
    Applied(Decimal),
    /// The market is already settled: seen + typed flip written, no fill applied —
    /// a fill here could never be credited (`settle_and_credit` retries credit zero).
    RefusedSettled(Decimal),
}

/// A paper fill to record. Built by the service tier from the executed `OrderIntent`
/// and the `PaperFill` returned by the executor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FillRecord {
    pub idempotency_key: String,
    pub market_id: MarketId,
    pub outcome_id: OutcomeId,
    pub side: Side,
    pub contracts: u64,
    pub fill_price: Price,
}

/// A recorded paper fill, read back from the `fills` table for inspection
/// (dashboard, `/paper/fills`). The `idempotency_key` still encodes the leader,
/// source trade id and observed-at bucket; callers parse it for those fields.
#[derive(Debug, Clone)]
pub struct FillRow {
    pub idempotency_key: String,
    pub market_id: MarketId,
    pub outcome_id: OutcomeId,
    pub side: Side,
    pub contracts: u64,
    pub fill_price: Price,
    pub event_seq: i64,
}

/// One settled market, read back from the `settled_markets` table to hydrate the
/// resolution double-credit guard (issue #343 step 0). `outcome_prices_json` is the
/// caller-owned JSON-encoded vector of text decimals, returned verbatim — this crate
/// does not interpret it. `credit_applied` follows the crate's text-decimal convention.
#[derive(Debug, Clone)]
pub struct SettledMarketRow {
    pub market_id: MarketId,
    pub outcome_prices_json: String,
    pub credit_applied: Decimal,
    pub settled_at_unix: i64,
}

/// A fill-time market-liquidity snapshot row in the `fill_market_snapshots` table
/// (WS2 of issue #350): one best-effort row per BUY fill, keyed by the fill's
/// `idempotency_key`. `liquidity`/`volume` are the Gamma scalars; the CLOB-derived
/// `absorbable_usd_100bps` and raw `ask_levels_json` are `None` on a `/book` failure
/// (a partial, Gamma-only row). Serves as both the
/// [`upsert`](PaperStateDb::upsert_fill_market_snapshot) input and the
/// [`list`](PaperStateDb::list_fill_snapshots) read-back row. Decimals follow the
/// crate's text-decimal convention; `ask_levels_json` is the caller-owned raw `/book`
/// ask side, stored verbatim — this crate does not interpret it.
#[derive(Debug, Clone)]
pub struct FillMarketSnapshot {
    pub idempotency_key: String,
    pub liquidity: Option<Decimal>,
    pub volume: Option<Decimal>,
    pub absorbable_usd_100bps: Option<Decimal>,
    pub ask_levels_json: Option<String>,
    pub captured_at_unix: i64,
}

/// Flip instruction folded into a paper-outcome commit (#508 Decision 10): the staged
/// dispatch seed `dispatch_id` flips `pending_paper → ready` with this typed
/// `paper_outcome` (`"fill"` or `"no_fill:<reason>"`) in the same transaction.
#[derive(Debug, Clone, Copy)]
pub struct DispatchFlip<'a> {
    pub dispatch_id: &'a str,
    pub paper_outcome: &'a str,
}

/// One frozen live target inside a staged dispatch seed (#508). Order in
/// [`DispatchSeedRecord::targets`] IS the frozen execution order (primary first, then
/// `(execution_order, account_id)`); `exec_rank` is assigned from that order at staging.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DispatchTargetSeed {
    pub account_id: String,
    pub credential_bundle_version: i64,
    pub credential_key_id: String,
}

/// A complete dispatch aggregate to stage (#508 Decision 10): the frozen normalized
/// signal + configuration/decision identity (`signal_json`, caller-owned JSON stored
/// verbatim) and the frozen ordered target list. Staged BEFORE the paper outcome can
/// become durable; idempotent on `dispatch_id` (redelivery reuses the staged seed).
#[derive(Debug, Clone)]
pub struct DispatchSeedRecord {
    pub dispatch_id: String,
    pub signal_json: String,
    pub source_trade_id: String,
    pub created_at_unix: i64,
    pub targets: Vec<DispatchTargetSeed>,
}

/// A staged dispatch aggregate read back from `dispatch_seeds`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DispatchSeedRow {
    pub dispatch_id: String,
    pub state: String,
    pub signal_json: String,
    pub paper_outcome: Option<String>,
    pub source_trade_id: String,
    pub created_at_unix: i64,
    pub finalized_at_unix: Option<i64>,
}

/// A frozen dispatch target read back from `dispatch_targets`, in `exec_rank` order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DispatchTargetRow {
    pub dispatch_id: String,
    pub account_id: String,
    pub exec_rank: i64,
    pub credential_bundle_version: i64,
    pub credential_key_id: String,
    pub state: String,
    pub terminal_reason: Option<String>,
    pub updated_at_unix: i64,
}

/// Crash-safe SQLite mirror. Cheap to share behind an `Arc`; all methods take `&self`.
pub struct PaperStateDb {
    conn: Mutex<Connection>,
}

impl PaperStateDb {
    /// Open or create the database at `path`, running idempotent DDL.
    ///
    /// A freshly created database (or one predating versioning, `user_version == 0`)
    /// is stamped with [`SCHEMA_VERSION`]. An existing database whose `user_version`
    /// differs from [`SCHEMA_VERSION`] is rejected with
    /// [`PaperStateError::SchemaVersionMismatch`] rather than silently mis-read.
    pub fn open(path: &Path) -> Result<Self, PaperStateError> {
        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        )?;
        // Wait rather than fail immediately on a transient lock (other readers).
        conn.busy_timeout(std::time::Duration::from_secs(5))?;

        let found: i64 = conn.pragma_query_value(None, "user_version", |row| row.get(0))?;
        if found != 0 && found != SCHEMA_VERSION {
            return Err(PaperStateError::SchemaVersionMismatch {
                found,
                expected: SCHEMA_VERSION,
            });
        }
        conn.execute_batch(SCHEMA)?;
        if found == 0 {
            conn.pragma_update(None, "user_version", SCHEMA_VERSION)?;
        }
        // Additive guarded migrations: CREATE IF NOT EXISTS above cannot add columns
        // to the existing wallet cursor owner. Each ALTER is idempotent across reopens.
        for (column, alter) in [
            (
                "last_activity_unix",
                "ALTER TABLE poll_cursors ADD COLUMN last_activity_unix INTEGER",
            ),
            (
                "activity_cutoff_unix",
                "ALTER TABLE poll_cursors ADD COLUMN activity_cutoff_unix INTEGER",
            ),
            (
                "coverage_generation",
                "ALTER TABLE poll_cursors ADD COLUMN coverage_generation INTEGER NOT NULL DEFAULT 0",
            ),
            (
                "reanchor_required",
                "ALTER TABLE poll_cursors ADD COLUMN reanchor_required INTEGER NOT NULL DEFAULT 0",
            ),
        ] {
            let has_column: bool = conn
                .prepare("SELECT 1 FROM pragma_table_info('poll_cursors') WHERE name = ?1")?
                .exists(params![column])?;
            if !has_column {
                conn.execute(alter, [])?;
            }
        }

        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Lock the connection, recovering the guard if a previous holder panicked.
    /// rusqlite operations are transactional (an uncommitted `Transaction` rolls
    /// back on drop), so a recovered guard never exposes a half-applied write.
    fn lock(&self) -> std::sync::MutexGuard<'_, Connection> {
        self.conn.lock().unwrap_or_else(PoisonError::into_inner)
    }

    // ── Input dedup ────────────────────────────────────────────────────────────

    /// Whether this `source_trade_id` has already been processed. Checked as the
    /// first action in the trade handler, before any ledger mutation.
    pub fn is_seen(&self, source_trade_id: &SourceTradeId) -> Result<bool, PaperStateError> {
        let conn = self.lock();
        let found: Option<i64> = conn
            .query_row(
                "SELECT 1 FROM seen_trades WHERE source_trade_id = ?1",
                params![source_trade_id.0],
                |row| row.get(0),
            )
            .optional()?;
        Ok(found.is_some())
    }

    /// Read the immutable initially applied semantics for a reconciled group.
    pub fn activity_group_state(
        &self,
        source_trade_id: &SourceTradeId,
    ) -> Result<Option<ActivityGroupState>, PaperStateError> {
        let conn = self.lock();
        conn.query_row(
            "SELECT transaction_hash, semantic_revision, source_epoch, disposition \
             FROM activity_groups WHERE source_trade_id = ?1",
            params![source_trade_id.0],
            |row| {
                Ok(ActivityGroupState {
                    transaction_hash: row.get(0)?,
                    semantic_revision: row.get(1)?,
                    source_epoch: row.get(2)?,
                    disposition: row.get(3)?,
                })
            },
        )
        .optional()
        .map_err(PaperStateError::from)
    }

    /// Greatest fully committed version-two bucket epoch for one wallet.
    pub fn last_activity_group_epoch(
        &self,
        wallet: &WalletAddress,
    ) -> Result<Option<i64>, PaperStateError> {
        let conn = self.lock();
        conn.query_row(
            "SELECT MAX(source_epoch) FROM activity_groups WHERE wallet_hex = ?1",
            params![wallet.to_string()],
            |row| row.get(0),
        )
        .map_err(PaperStateError::from)
    }

    /// Replayable activity groups strictly after `cutoff_unix`, in causal order.
    pub fn activity_groups_after(
        &self,
        wallet: &WalletAddress,
        cutoff_unix: i64,
    ) -> Result<Vec<ActivityGroupRow>, PaperStateError> {
        let conn = self.lock();
        let mut statement = conn.prepare(
            "SELECT source_trade_id, source_epoch, semantic_revision, disposition, proof_json \
             FROM activity_groups WHERE wallet_hex = ?1 AND source_epoch > ?2 \
             ORDER BY source_epoch, source_trade_id",
        )?;
        let rows = statement.query_map(params![wallet.to_string(), cutoff_unix], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
            ))
        })?;
        let mut groups = Vec::new();
        for row in rows {
            let (source_trade_id, source_epoch, semantic_revision, disposition, proof_json) = row?;
            groups.push(ActivityGroupRow {
                source_trade_id: SourceTradeId(source_trade_id),
                source_epoch,
                semantic_revision,
                disposition,
                proof_json,
            });
        }
        Ok(groups)
    }

    // ── Write shapes ─────────────────────────────────────────────────────────

    /// Commit a processed trade that produced **no** order (classify-`None`,
    /// `NoEdge`, `Shadow`, a dispatch failure, or a live fill). One transaction:
    /// `mark_seen` + `upsert_leader_position`. No event-log frame was written, so
    /// there is no `event_seq` to advance.
    pub fn commit_seen_no_fill(
        &self,
        source_trade_id: &SourceTradeId,
        leader: &LeaderPositionRow,
    ) -> Result<(), PaperStateError> {
        self.commit_seen_no_fill_with_flip(source_trade_id, leader, None)
    }

    /// [`Self::commit_seen_no_fill`] that also flips a staged dispatch seed
    /// `pending_paper → ready` with a **typed** no-fill outcome in the same transaction
    /// (#508 Decision 10). With `flip = None` the behavior is byte-identical to the
    /// plain no-fill commit (the zero-live-target path stays untyped and untouched).
    pub fn commit_seen_no_fill_with_flip(
        &self,
        source_trade_id: &SourceTradeId,
        leader: &LeaderPositionRow,
        flip: Option<DispatchFlip<'_>>,
    ) -> Result<(), PaperStateError> {
        self.commit_seen_no_fill_with_flip_pending(source_trade_id, leader, flip, None)
    }

    pub fn commit_seen_no_fill_with_flip_pending(
        &self,
        source_trade_id: &SourceTradeId,
        leader: &LeaderPositionRow,
        flip: Option<DispatchFlip<'_>>,
        pending: Option<PendingTerminalEvidence<'_>>,
    ) -> Result<(), PaperStateError> {
        let mut conn = self.lock();
        let tx = conn.transaction()?;
        tx_mark_seen(&tx, source_trade_id, None)?;
        tx_upsert_leader(&tx, leader)?;
        if let Some(flip) = flip {
            tx_flip_dispatch_ready(&tx, flip)?;
        }
        tx_terminalize_pending(&tx, source_trade_id, "no_fill", pending)?;
        tx.commit()?;
        Ok(())
    }

    /// Commit a stale REST-fallback trade with its typed no-copy disposition (#530):
    /// `mark_seen` + `upsert_leader_position` + the disposition row, one transaction.
    /// The trade is fully admitted for bookkeeping — the held delivery cursor advances
    /// through `is_seen` exactly as for any processed trade — but no copy is staged.
    pub fn commit_seen_no_copy(
        &self,
        source_trade_id: &SourceTradeId,
        leader: &LeaderPositionRow,
        disposition: &NoCopyDisposition,
    ) -> Result<(), PaperStateError> {
        self.commit_seen_no_copy_with_pending(source_trade_id, leader, disposition, None)
    }

    pub fn commit_seen_no_copy_with_pending(
        &self,
        source_trade_id: &SourceTradeId,
        leader: &LeaderPositionRow,
        disposition: &NoCopyDisposition,
        pending: Option<PendingTerminalEvidence<'_>>,
    ) -> Result<(), PaperStateError> {
        let mut conn = self.lock();
        let tx = conn.transaction()?;
        tx_mark_seen(&tx, source_trade_id, None)?;
        tx_upsert_leader(&tx, leader)?;
        tx_record_no_copy_disposition(&tx, source_trade_id, disposition)?;
        tx_terminalize_pending(
            &tx,
            source_trade_id,
            &format!("no_copy:{}", disposition.reason),
            pending,
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Read back a no-copy disposition (audit/scenario surface).
    pub fn no_copy_disposition(
        &self,
        source_trade_id: &SourceTradeId,
    ) -> Result<Option<(String, i64, String)>, PaperStateError> {
        let conn = self.lock();
        let mut stmt = conn.prepare(
            "SELECT provenance, age_secs, reason FROM no_copy_dispositions
              WHERE source_trade_id = ?1",
        )?;
        let mut rows = stmt.query(params![source_trade_id.0])?;
        match rows.next()? {
            Some(row) => Ok(Some((row.get(0)?, row.get(1)?, row.get(2)?))),
            None => Ok(None),
        }
    }

    /// Commit one complete wallet epoch-second atomically (#544).
    ///
    /// The caller supplies the post-bucket exact leader rows and every durable
    /// group/gate/history/pending/fence disposition. The delivery cursor advances
    /// only after all of them have been written in this same transaction.
    pub fn commit_activity_bucket(
        &self,
        bucket: &ActivityBucketCommit,
    ) -> Result<(), PaperStateError> {
        validate_activity_bucket(bucket)?;
        let mut conn = self.lock();
        let tx = conn.transaction()?;
        let is_revision_fence = bucket
            .fence
            .as_ref()
            .is_some_and(|fence| fence.cause == "revised_applied_aggregate");
        let wallet_already_fenced = tx
            .query_row(
                "SELECT 1 FROM wallet_fences WHERE wallet_hex = ?1",
                params![bucket.wallet.to_string()],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
            .is_some();
        let mut invalidates_position_validation = false;
        let mut inserts_reanchor_trigger = false;

        for record in &bucket.dispositions {
            let existing: Option<(String, String, String, String, i64, String, String)> = tx
                .query_row(
                    "SELECT transaction_hash, semantic_revision, disposition, wallet_hex, \
                            source_epoch, activity_type, proof_json \
                     FROM activity_groups WHERE source_trade_id = ?1",
                    params![record.source_trade_id.0],
                    |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?,
                            row.get(5)?,
                            row.get(6)?,
                        ))
                    },
                )
                .optional()?;
            if let Some((
                transaction_hash,
                revision,
                disposition,
                wallet_hex,
                source_epoch,
                activity_type,
                proof_json,
            )) = existing
            {
                let exact_retry = transaction_hash == record.transaction_hash
                    && revision == record.semantic_revision
                    && disposition == record.disposition
                    && wallet_hex == record.wallet.to_string()
                    && source_epoch == record.source_epoch
                    && activity_type == record.activity_type
                    && proof_json == record.proof_json;
                let retained_revision = transaction_hash == record.transaction_hash
                    && revision != record.semantic_revision
                    && (is_revision_fence || wallet_already_fenced);
                if !exact_retry && !retained_revision {
                    return Err(PaperStateError::ActivityRevisionConflict(
                        record.source_trade_id.0.clone(),
                    ));
                }
                invalidates_position_validation |= retained_revision;
            } else {
                invalidates_position_validation = true;
                inserts_reanchor_trigger |= bucket
                    .reanchor
                    .as_ref()
                    .is_some_and(|reanchor| reanchor.source_trade_id == record.source_trade_id);
                tx.execute(
                    "INSERT INTO activity_groups \
                         (source_trade_id, transaction_hash, wallet_hex, source_epoch, \
                          semantic_revision, activity_type, disposition, proof_json) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                    params![
                        record.source_trade_id.0,
                        record.transaction_hash,
                        record.wallet.to_string(),
                        record.source_epoch,
                        record.semantic_revision,
                        record.activity_type,
                        record.disposition,
                        record.proof_json,
                    ],
                )?;
            }
            let durable_revision: Option<(String, String, String)> = tx
                .query_row(
                    "SELECT transaction_hash, disposition, proof_json \
                     FROM activity_group_revisions \
                     WHERE source_trade_id = ?1 AND semantic_revision = ?2",
                    params![record.source_trade_id.0, record.semantic_revision],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .optional()?;
            let requested_revision = (
                record.transaction_hash.clone(),
                record.disposition.clone(),
                record.proof_json.clone(),
            );
            if let Some(durable_revision) = durable_revision {
                if durable_revision != requested_revision {
                    return Err(PaperStateError::ActivityRevisionConflict(
                        record.source_trade_id.0.clone(),
                    ));
                }
            } else {
                tx.execute(
                    "INSERT INTO activity_group_revisions \
                         (source_trade_id, semantic_revision, transaction_hash, disposition, \
                          proof_json, recorded_at_unix) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![
                        record.source_trade_id.0,
                        record.semantic_revision,
                        requested_revision.0,
                        requested_revision.1,
                        requested_revision.2,
                        bucket
                            .fence
                            .as_ref()
                            .map_or(record.source_epoch, |fence| fence.fenced_at_unix),
                    ],
                )?;
            }
            tx_mark_seen(&tx, &record.source_trade_id, Some(&record.transaction_hash))?;
            if let Some(disposition) = &record.no_copy {
                tx_record_no_copy_disposition(&tx, &record.source_trade_id, disposition)?;
            } else if record.disposition != "applied"
                && record.disposition != "decision_pending"
                && record.disposition != "raw_only"
            {
                tx_record_no_copy_disposition(
                    &tx,
                    &record.source_trade_id,
                    &NoCopyDisposition {
                        provenance: "reconciled_rest".to_owned(),
                        age_secs: 0,
                        reason: record.disposition.clone(),
                        recorded_at_unix: record.source_epoch,
                    },
                )?;
            }
        }

        if inserts_reanchor_trigger {
            let reanchor = bucket.reanchor.as_ref().ok_or_else(|| {
                PaperStateError::Internal("missing activity re-anchor marker".to_owned())
            })?;
            let updated = tx.execute(
                "UPDATE poll_cursors SET reanchor_required = 1, \
                     coverage_generation = coverage_generation + 1 WHERE wallet_hex = ?1",
                params![bucket.wallet.to_string()],
            )?;
            if updated != 1 {
                return Err(PaperStateError::Internal(format!(
                    "reanchor mark for {} ({}) requires an existing wallet cursor for {}",
                    reanchor.source_trade_id, reanchor.reason, bucket.wallet
                )));
            }
            invalidates_position_validation = true;
        }

        for leader in &bucket.leader_positions {
            tx_upsert_leader(&tx, leader)?;
        }
        for gate in &bucket.gate_results {
            let durable: Option<(String, String, i64, String, i64)> = tx
                .query_row(
                    "SELECT wallet_hex, market_id, source_epoch, result, history_consumed \
                     FROM entry_gate_results WHERE source_trade_id = ?1",
                    params![gate.source_trade_id.0],
                    |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?,
                        ))
                    },
                )
                .optional()?;
            let requested = (
                gate.wallet.to_string(),
                gate.market_id.to_string(),
                gate.source_epoch,
                gate.result.clone(),
                i64::from(gate.history_consumed),
            );
            if let Some(durable) = durable {
                if durable != requested {
                    return Err(PaperStateError::ActivityRevisionConflict(
                        gate.source_trade_id.0.clone(),
                    ));
                }
            } else {
                tx.execute(
                    "INSERT INTO entry_gate_results \
                         (source_trade_id, wallet_hex, market_id, source_epoch, result, history_consumed) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![
                        gate.source_trade_id.0,
                        requested.0,
                        requested.1,
                        requested.2,
                        requested.3,
                        requested.4,
                    ],
                )?;
            }
        }
        for history in &bucket.history_effects {
            tx.execute(
                "INSERT OR IGNORE INTO wallet_market_history_v2 \
                     (wallet_hex, market_id, first_epoch, source_trade_id, origin) \
                 VALUES (?1, ?2, ?3, ?4, 'activity_v2')",
                params![
                    history.wallet.to_string(),
                    history.market_id.to_string(),
                    history.first_epoch,
                    history.source_trade_id.0,
                ],
            )?;
        }
        if let Some(status) = &bucket.history_status {
            tx.execute(
                "INSERT INTO wallet_history_status_v2 \
                     (wallet_hex, complete, proof_json, updated_at_unix) VALUES (?1, ?2, ?3, ?4) \
                 ON CONFLICT(wallet_hex) DO UPDATE SET \
                     complete = excluded.complete, proof_json = excluded.proof_json, \
                     updated_at_unix = excluded.updated_at_unix",
                params![
                    status.wallet.to_string(),
                    i64::from(status.complete),
                    status.proof_json,
                    status.updated_at_unix,
                ],
            )?;
        }
        for pending in &bucket.pending {
            let durable: Option<(String, String, i64, String)> = tx
                .query_row(
                    "SELECT semantic_revision, wallet_hex, source_epoch, frozen_inputs_json \
                     FROM decision_pending WHERE source_trade_id = ?1",
                    params![pending.source_trade_id.0],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .optional()?;
            let requested = (
                pending.semantic_revision.clone(),
                pending.wallet.to_string(),
                pending.source_epoch,
                pending.frozen_inputs_json.clone(),
            );
            if let Some(durable) = durable {
                if durable != requested {
                    return Err(PaperStateError::DecisionPendingConflict(
                        pending.source_trade_id.0.clone(),
                    ));
                }
            } else {
                tx.execute(
                    "INSERT INTO decision_pending \
                         (source_trade_id, semantic_revision, wallet_hex, source_epoch, \
                          frozen_inputs_json, post_commit_inputs_json, state, terminal_disposition, \
                          updated_at_unix) \
                     VALUES (?1, ?2, ?3, ?4, ?5, '[]', 'open', NULL, ?6)",
                    params![
                        pending.source_trade_id.0,
                        requested.0,
                        requested.1,
                        requested.2,
                        requested.3,
                        pending.updated_at_unix,
                    ],
                )?;
            }
        }
        if let Some(fence) = &bucket.fence {
            let durable: Option<(String, String, String, i64)> = tx
                .query_row(
                    "SELECT source_trade_id, cause, proof_json, fenced_at_unix \
                     FROM wallet_fences WHERE wallet_hex = ?1",
                    params![fence.wallet.to_string()],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .optional()?;
            let requested = (
                fence.source_trade_id.0.clone(),
                fence.cause.clone(),
                fence.proof_json.clone(),
                fence.fenced_at_unix,
            );
            if let Some(durable) = durable {
                if durable != requested {
                    return Err(PaperStateError::ActivityRevisionConflict(
                        fence.source_trade_id.0.clone(),
                    ));
                }
            } else {
                tx.execute(
                    "INSERT INTO wallet_fences \
                         (wallet_hex, source_trade_id, cause, proof_json, fenced_at_unix) \
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![
                        fence.wallet.to_string(),
                        requested.0,
                        requested.1,
                        requested.2,
                        requested.3,
                    ],
                )?;
            }
        }
        if bucket.advance_cursor {
            tx.execute(
                "INSERT INTO poll_cursors (wallet_hex, last_ts_unix, last_activity_unix) \
                 VALUES (?1, ?2, ?2) \
                 ON CONFLICT(wallet_hex) DO UPDATE SET \
                    last_ts_unix = MAX(poll_cursors.last_ts_unix, excluded.last_ts_unix), \
                    last_activity_unix = MAX(COALESCE(poll_cursors.last_activity_unix, 0), \
                                             excluded.last_activity_unix)",
                params![bucket.wallet.to_string(), bucket.source_epoch],
            )?;
        }
        if invalidates_position_validation {
            tx.execute(
                "DELETE FROM position_validations WHERE wallet_hex = ?1",
                params![bucket.wallet.to_string()],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Rebuild the in-memory first-entry projection from its durable owner.
    pub fn gate_history(
        &self,
    ) -> Result<HashMap<WalletAddress, HashSet<MarketId>>, PaperStateError> {
        let conn = self.lock();
        let mut statement = conn.prepare(
            "SELECT wallet_hex, market_id FROM wallet_market_history_v2 \
             ORDER BY wallet_hex, market_id",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        let mut history: HashMap<WalletAddress, HashSet<MarketId>> = HashMap::new();
        for row in rows {
            let (wallet, market) = row?;
            history
                .entry(parse_wallet(&wallet)?)
                .or_default()
                .insert(MarketId(VenueMarketId(market)));
        }
        let mut completed = conn.prepare(
            "SELECT wallet_hex FROM wallet_history_status_v2 WHERE complete = 1 \
             ORDER BY wallet_hex",
        )?;
        let wallets = completed.query_map([], |row| row.get::<_, String>(0))?;
        for wallet in wallets {
            history.entry(parse_wallet(&wallet?)?).or_default();
        }
        Ok(history)
    }

    /// Whether complete reconciled history permits this wallet's membership publication.
    pub fn wallet_history_complete(&self, wallet: &WalletAddress) -> Result<bool, PaperStateError> {
        let conn = self.lock();
        let complete: Option<i64> = conn
            .query_row(
                "SELECT complete FROM wallet_history_status_v2 WHERE wallet_hex = ?1",
                params![wallet.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        Ok(complete == Some(1))
    }

    /// Persist the result of a complete reconciled-history obligation (#544).
    ///
    /// Source ingestion owns the proof; membership only consumes this durable status.
    pub fn record_reconciled_history_status(
        &self,
        status: &WalletHistoryStatusRecord,
    ) -> Result<(), PaperStateError> {
        serde_json::from_str::<serde_json::Value>(&status.proof_json)?;
        let conn = self.lock();
        conn.execute(
            "INSERT INTO wallet_history_status_v2 \
                 (wallet_hex, complete, proof_json, updated_at_unix) VALUES (?1, ?2, ?3, ?4) \
             ON CONFLICT(wallet_hex) DO UPDATE SET complete = excluded.complete, \
                 proof_json = excluded.proof_json, updated_at_unix = excluded.updated_at_unix",
            params![
                status.wallet.to_string(),
                i64::from(status.complete),
                status.proof_json,
                status.updated_at_unix,
            ],
        )?;
        Ok(())
    }

    /// Promote SEEDED history rows to complete for wallets whose causal bracket
    /// was just accepted (#544 activation fix). The bracket's complete fixed-end
    /// activity catch-up is the reconciliation the plan requires to extend and
    /// validate the one-time sidecar seed; a wallet with NO seed row stays
    /// incomplete (fail-closed — runtime admission owns unseeded wallets).
    pub fn mark_seeded_history_validated(
        &self,
        wallets: &[WalletAddress],
        proof_json: &str,
        validated_at_unix: i64,
    ) -> Result<usize, PaperStateError> {
        serde_json::from_str::<serde_json::Value>(proof_json)?;
        let mut conn = self.lock();
        let tx = conn.transaction()?;
        let mut promoted = 0usize;
        for wallet in wallets {
            promoted += tx.execute(
                "UPDATE wallet_history_status_v2                      SET complete = 1, proof_json = ?2, updated_at_unix = ?3                  WHERE wallet_hex = ?1 AND complete = 0",
                params![wallet.to_string(), proof_json, validated_at_unix],
            )?;
        }
        tx.commit()?;
        Ok(promoted)
    }

    /// Install one all-or-nothing batch of venue-authoritative position anchors.
    pub fn install_anchors(&self, installs: &[AnchorInstallRecord]) -> Result<(), PaperStateError> {
        self.install_anchors_inner(installs, false)
    }

    fn install_anchors_inner(
        &self,
        installs: &[AnchorInstallRecord],
        fail_before_commit: bool,
    ) -> Result<(), PaperStateError> {
        let mut prepared = Vec::with_capacity(installs.len());
        for install in installs {
            serde_json::from_str::<serde_json::Value>(&install.activity_bounds_json)?;
            serde_json::from_str::<serde_json::Value>(&install.proof_json)?;
            let mut balances = install
                .balances
                .iter()
                .map(|(market_id, outcome_id, long)| (market_id.to_string(), outcome_id.0, *long))
                .collect::<Vec<_>>();
            balances
                .sort_by(|left, right| (left.0.as_str(), left.1).cmp(&(right.0.as_str(), right.1)));
            for duplicate in balances.windows(2) {
                if duplicate[0].0 == duplicate[1].0 && duplicate[0].1 == duplicate[1].1 {
                    return Err(PaperStateError::DuplicateAuthoritativePosition {
                        market_id: duplicate[0].0.clone(),
                        outcome_id: duplicate[0].1,
                    });
                }
            }
            let balances_json = serde_json::to_string(&balances)?;
            prepared.push((install, balances, balances_json));
        }

        let mut conn = self.lock();
        let tx = conn.transaction()?;
        for (install, balances, balances_json) in prepared {
            let wallet_hex = install.wallet.to_string();
            let stored_cutoff: Option<Option<i64>> = tx
                .query_row(
                    "SELECT activity_cutoff_unix FROM poll_cursors WHERE wallet_hex = ?1",
                    params![wallet_hex],
                    |row| row.get(0),
                )
                .optional()?;
            let stored_cutoff = stored_cutoff.ok_or_else(|| {
                PaperStateError::Internal(format!(
                    "anchor install requires an existing wallet cursor for {}",
                    install.wallet
                ))
            })?;
            if let Some(stored_cutoff_unix) = stored_cutoff
                && stored_cutoff_unix > install.activity_cutoff_unix
            {
                return Err(PaperStateError::AnchorCutoffRegression {
                    wallet: install.wallet,
                    stored_cutoff_unix,
                    candidate_cutoff_unix: install.activity_cutoff_unix,
                });
            }

            let previous_anchor_seq: Option<i64> = tx.query_row(
                "SELECT MAX(anchor_seq) FROM position_anchors WHERE wallet_hex = ?1",
                params![wallet_hex],
                |row| row.get(0),
            )?;
            let anchor_seq = match previous_anchor_seq {
                Some(previous) => previous.checked_add(1).ok_or_else(|| {
                    PaperStateError::Internal(format!(
                        "anchor sequence overflow for {}",
                        install.wallet
                    ))
                })?,
                None => 0,
            };
            tx.execute(
                "INSERT INTO position_anchors \
                     (wallet_hex, anchor_seq, anchored_at_unix, activity_cutoff_unix, \
                      balances_json, ledger_hash_after, proof_json) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    wallet_hex,
                    anchor_seq,
                    install.anchored_at_unix,
                    install.activity_cutoff_unix,
                    balances_json,
                    install.ledger_hash_after,
                    install.proof_json,
                ],
            )?;

            tx.execute(
                "DELETE FROM leader_positions WHERE wallet_hex = ?1",
                params![wallet_hex],
            )?;
            {
                let mut statement = tx.prepare(
                    "INSERT INTO leader_positions \
                         (wallet_hex, market_id, outcome_id, long_amount_str, short_amount_str) \
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                )?;
                for (market_id, outcome_id, long) in balances {
                    statement.execute(params![
                        wallet_hex,
                        market_id,
                        i64::from(outcome_id),
                        long.to_decimal().to_string(),
                        ShareAmount::ZERO.to_decimal().to_string(),
                    ])?;
                }
            }

            let validation = PositionValidationRecord {
                wallet: install.wallet,
                ledger_hash: install.ledger_hash_after.clone(),
                positions_proof_hash: install.positions_proof_hash.clone(),
                activity_bounds_json: install.activity_bounds_json.clone(),
                source_log_generation: install.source_log_generation.clone(),
                proof_json: install.proof_json.clone(),
                recorded_at_unix: install.recorded_at_unix,
            };
            tx_upsert_position_validation(&tx, &validation)?;
            tx.execute(
                "UPDATE poll_cursors SET activity_cutoff_unix = ?2, reanchor_required = 0 \
                 WHERE wallet_hex = ?1",
                params![wallet_hex, install.activity_cutoff_unix],
            )?;
        }
        if fail_before_commit {
            return Err(PaperStateError::Internal(
                "injected anchor install failure before commit".to_owned(),
            ));
        }
        tx.commit()?;
        Ok(())
    }

    /// Return the currently accepted bracket, if no later activity invalidated it.
    pub fn position_validation(
        &self,
        wallet: &WalletAddress,
    ) -> Result<Option<PositionValidationRecord>, PaperStateError> {
        let conn = self.lock();
        conn.query_row(
            "SELECT ledger_hash, positions_proof_hash, activity_bounds_json, \
                    source_log_generation, proof_json, recorded_at_unix \
             FROM position_validations WHERE wallet_hex = ?1",
            params![wallet.to_string()],
            |row| {
                Ok(PositionValidationRecord {
                    wallet: *wallet,
                    ledger_hash: row.get(0)?,
                    positions_proof_hash: row.get(1)?,
                    activity_bounds_json: row.get(2)?,
                    source_log_generation: row.get(3)?,
                    proof_json: row.get(4)?,
                    recorded_at_unix: row.get(5)?,
                })
            },
        )
        .optional()
        .map_err(PaperStateError::from)
    }

    /// Whether a causal position bracket is still current for publication.
    pub fn position_validation_current(
        &self,
        wallet: &WalletAddress,
    ) -> Result<bool, PaperStateError> {
        Ok(self.position_validation(wallet)?.is_some())
    }

    /// Read the latest installed anchor and coverage state for one wallet.
    pub fn wallet_coverage(
        &self,
        wallet: &WalletAddress,
    ) -> Result<WalletCoverage, PaperStateError> {
        type RawCoverageRow = (Option<i64>, i64, i64, Option<i64>, Option<i64>);

        let conn = self.lock();
        let row: Option<RawCoverageRow> = conn
            .query_row(
                "SELECT activity_cutoff_unix, coverage_generation, reanchor_required, \
                        (SELECT MAX(anchor_seq) FROM position_anchors \
                         WHERE wallet_hex = poll_cursors.wallet_hex), \
                        (SELECT anchored_at_unix FROM position_anchors \
                         WHERE wallet_hex = poll_cursors.wallet_hex \
                         ORDER BY anchor_seq DESC LIMIT 1) \
                 FROM poll_cursors WHERE wallet_hex = ?1",
                params![wallet.to_string()],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .optional()?;
        let Some((
            activity_cutoff_unix,
            coverage_generation,
            reanchor_required,
            anchor_seq,
            anchored_at_unix,
        )) = row
        else {
            return Ok(WalletCoverage {
                activity_cutoff_unix: None,
                coverage_generation: 0,
                reanchor_required: false,
                anchor_seq: None,
                anchored_at_unix: None,
            });
        };
        Ok(WalletCoverage {
            activity_cutoff_unix,
            coverage_generation,
            reanchor_required: parse_bool_flag(reanchor_required, "reanchor_required")?,
            anchor_seq,
            anchored_at_unix,
        })
    }

    /// Age in seconds of the oldest live wallet's latest anchor, or `None` when
    /// no live wallet has an anchor.
    pub fn oldest_anchor_age(
        &self,
        wallets: &[WalletAddress],
        now_unix: i64,
    ) -> Result<Option<i64>, PaperStateError> {
        let conn = self.lock();
        let mut statement = conn.prepare(
            "SELECT anchored_at_unix FROM position_anchors WHERE wallet_hex = ?1 \
             ORDER BY anchor_seq DESC LIMIT 1",
        )?;
        let mut anchored_at_unix = None;
        for wallet in wallets {
            let latest = statement
                .query_row(params![wallet.to_string()], |row| row.get::<_, i64>(0))
                .optional()?;
            if let Some(latest) = latest {
                anchored_at_unix =
                    Some(anchored_at_unix.map_or(latest, |oldest: i64| oldest.min(latest)));
            }
        }
        Ok(anchored_at_unix.map(|anchored| now_unix.saturating_sub(anchored).max(0)))
    }

    /// All position anchors for one wallet in append order.
    pub fn position_anchors(
        &self,
        wallet: &WalletAddress,
    ) -> Result<Vec<PositionAnchorRow>, PaperStateError> {
        let conn = self.lock();
        let mut statement = conn.prepare(
            "SELECT anchor_seq, anchored_at_unix, activity_cutoff_unix, balances_json, \
                    ledger_hash_after, proof_json FROM position_anchors \
             WHERE wallet_hex = ?1 ORDER BY anchor_seq",
        )?;
        let rows = statement.query_map(params![wallet.to_string()], |row| {
            Ok(PositionAnchorRow {
                wallet: *wallet,
                anchor_seq: row.get(0)?,
                anchored_at_unix: row.get(1)?,
                activity_cutoff_unix: row.get(2)?,
                balances_json: row.get(3)?,
                ledger_hash_after: row.get(4)?,
                proof_json: row.get(5)?,
            })
        })?;
        let mut anchors = Vec::new();
        for row in rows {
            anchors.push(row?);
        }
        Ok(anchors)
    }

    /// Complete-history wallet set used to initialize the bucket gate owner.
    pub fn complete_history_wallets(&self) -> Result<HashSet<WalletAddress>, PaperStateError> {
        let conn = self.lock();
        let mut statement = conn.prepare(
            "SELECT wallet_hex FROM wallet_history_status_v2 WHERE complete = 1 \
             ORDER BY wallet_hex",
        )?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        let mut wallets = HashSet::new();
        for wallet in rows {
            wallets.insert(parse_wallet(&wallet?)?);
        }
        Ok(wallets)
    }

    /// Load the monotonic fence set before membership/producers/replay.
    pub fn wallet_fences(&self) -> Result<Vec<WalletFenceRecord>, PaperStateError> {
        let conn = self.lock();
        let mut statement = conn.prepare(
            "SELECT wallet_hex, source_trade_id, cause, proof_json, fenced_at_unix \
             FROM wallet_fences ORDER BY wallet_hex",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, i64>(4)?,
            ))
        })?;
        let mut fences = Vec::new();
        for row in rows {
            let (wallet, source_trade_id, cause, proof_json, fenced_at_unix) = row?;
            fences.push(WalletFenceRecord {
                wallet: parse_wallet(&wallet)?,
                source_trade_id: SourceTradeId(source_trade_id),
                cause,
                proof_json,
                fenced_at_unix,
            });
        }
        Ok(fences)
    }

    /// Replace the pre-activation census while the side main is still the sole
    /// write target. Canonical JSON and its BLAKE3 bind source bounds, cursors,
    /// obligations, fences, wallet identities, and the binary identity (#544).
    pub fn record_migration_activation_facts(
        &self,
        facts: &serde_json::Value,
        binary_identity: &str,
    ) -> Result<String, PaperStateError> {
        let rendered = serde_json::to_string(facts)?;
        let hash = blake3::hash(rendered.as_bytes()).to_hex().to_string();
        let conn = self.lock();
        conn.execute(
            "INSERT INTO migration_activation_facts_v2
                 (singleton, facts_json, facts_blake3, binary_identity)
             VALUES (1, ?1, ?2, ?3)
             ON CONFLICT(singleton) DO UPDATE SET
                 facts_json = excluded.facts_json,
                 facts_blake3 = excluded.facts_blake3,
                 binary_identity = excluded.binary_identity",
            params![rendered, hash, binary_identity],
        )?;
        Ok(hash)
    }

    pub fn is_wallet_fenced(&self, wallet: &WalletAddress) -> Result<bool, PaperStateError> {
        let conn = self.lock();
        let exists = conn
            .query_row(
                "SELECT 1 FROM wallet_fences WHERE wallet_hex = ?1",
                params![wallet.to_string()],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
            .is_some();
        Ok(exists)
    }

    /// Open production continuations, in causal order, for boot recovery before producers.
    pub fn open_decision_pending(&self) -> Result<Vec<DecisionPendingRow>, PaperStateError> {
        let conn = self.lock();
        let mut statement = conn.prepare(
            "SELECT source_trade_id, semantic_revision, wallet_hex, source_epoch, \
                    frozen_inputs_json, post_commit_inputs_json, state, terminal_disposition, \
                    updated_at_unix \
             FROM decision_pending WHERE state = 'open' \
             ORDER BY source_epoch, source_trade_id",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, Option<String>>(7)?,
                row.get::<_, i64>(8)?,
            ))
        })?;
        let mut pending = Vec::new();
        for row in rows {
            let (id, revision, wallet, epoch, frozen, post, state, terminal, updated) = row?;
            pending.push(DecisionPendingRow {
                source_trade_id: SourceTradeId(id),
                semantic_revision: revision,
                wallet: parse_wallet(&wallet)?,
                source_epoch: epoch,
                frozen_inputs_json: frozen,
                post_commit_inputs_json: post,
                state: parse_pending_state(&state)?,
                terminal_disposition: terminal,
                updated_at_unix: updated,
            });
        }
        Ok(pending)
    }

    /// Complete continuation history for deterministic offline replay. Unlike
    /// production boot recovery this is read-only and includes terminal rows;
    /// callers consume the recorded terminal transition without executing it.
    pub fn decision_pending_history(&self) -> Result<Vec<DecisionPendingRow>, PaperStateError> {
        let conn = self.lock();
        let mut statement = conn.prepare(
            "SELECT source_trade_id, semantic_revision, wallet_hex, source_epoch, \
                    frozen_inputs_json, post_commit_inputs_json, state, terminal_disposition, \
                    updated_at_unix \
             FROM decision_pending ORDER BY source_epoch, source_trade_id",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, Option<String>>(7)?,
                row.get::<_, i64>(8)?,
            ))
        })?;
        let mut history = Vec::new();
        for row in rows {
            let (id, revision, wallet, epoch, frozen, post, state, terminal, updated) = row?;
            history.push(DecisionPendingRow {
                source_trade_id: SourceTradeId(id),
                semantic_revision: revision,
                wallet: parse_wallet(&wallet)?,
                source_epoch: epoch,
                frozen_inputs_json: frozen,
                post_commit_inputs_json: post,
                state: parse_pending_state(&state)?,
                terminal_disposition: terminal,
                updated_at_unix: updated,
            });
        }
        Ok(history)
    }

    /// Whether one bucket-applied delivery still owns an open continuation.
    pub fn is_decision_pending_open(
        &self,
        source_trade_id: &SourceTradeId,
    ) -> Result<bool, PaperStateError> {
        let conn = self.lock();
        let open = conn
            .query_row(
                "SELECT 1 FROM decision_pending \
                 WHERE source_trade_id = ?1 AND state = 'open'",
                params![source_trade_id.0],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
            .is_some();
        Ok(open)
    }

    /// Persist the complete post-boundary decision inputs before a paper-log or
    /// external-authority side effect. Terminal rows are immutable and make a
    /// repeated checkpoint a no-op; a missing row is an invariant conflict.
    pub fn checkpoint_decision_pending(
        &self,
        source_trade_id: &SourceTradeId,
        post_commit_inputs_json: &str,
        updated_at_unix: i64,
    ) -> Result<(), PaperStateError> {
        serde_json::from_str::<serde_json::Value>(post_commit_inputs_json)?;
        let mut conn = self.lock();
        let tx = conn.transaction()?;
        let state: Option<String> = tx
            .query_row(
                "SELECT state FROM decision_pending WHERE source_trade_id = ?1",
                params![source_trade_id.0],
                |row| row.get(0),
            )
            .optional()?;
        match state.as_deref() {
            Some("open") => {
                tx.execute(
                    "UPDATE decision_pending SET post_commit_inputs_json = ?2, \
                         updated_at_unix = ?3 WHERE source_trade_id = ?1 AND state = 'open'",
                    params![source_trade_id.0, post_commit_inputs_json, updated_at_unix],
                )?;
            }
            Some("terminal") => {}
            _ => {
                return Err(PaperStateError::DecisionPendingConflict(
                    source_trade_id.0.clone(),
                ));
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Record the post-commit inputs and terminal transition. A same-value retry
    /// is idempotent; a different terminal replay fails closed (#544 revision 3).
    pub fn close_decision_pending(
        &self,
        source_trade_id: &SourceTradeId,
        post_commit_inputs_json: &str,
        terminal_disposition: &str,
        updated_at_unix: i64,
    ) -> Result<(), PaperStateError> {
        serde_json::from_str::<serde_json::Value>(post_commit_inputs_json)?;
        let mut conn = self.lock();
        let tx = conn.transaction()?;
        let current: Option<(String, Option<String>, String)> = tx
            .query_row(
                "SELECT state, terminal_disposition, post_commit_inputs_json \
                 FROM decision_pending WHERE source_trade_id = ?1",
                params![source_trade_id.0],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        match current {
            Some((state, terminal, inputs)) if state == "terminal" => {
                if terminal.as_deref() != Some(terminal_disposition)
                    || inputs != post_commit_inputs_json
                {
                    return Err(PaperStateError::DecisionPendingConflict(
                        source_trade_id.0.clone(),
                    ));
                }
            }
            Some(_) => {
                tx.execute(
                    "UPDATE decision_pending SET state = 'terminal', \
                         post_commit_inputs_json = ?2, terminal_disposition = ?3, \
                         updated_at_unix = ?4 WHERE source_trade_id = ?1 AND state = 'open'",
                    params![
                        source_trade_id.0,
                        post_commit_inputs_json,
                        terminal_disposition,
                        updated_at_unix,
                    ],
                )?;
            }
            None => {
                return Err(PaperStateError::DecisionPendingConflict(
                    source_trade_id.0.clone(),
                ));
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Import a captured legacy `wallet_market_history.json` exactly once.
    /// A later call returns the stored proof without reading the changed payload.
    pub fn import_legacy_wallet_history(
        &self,
        bytes: &[u8],
        imported_at_unix: i64,
    ) -> Result<LegacyHistoryImport, PaperStateError> {
        const IMPORT_NAME: &str = "wallet_market_history_v1";
        let mut conn = self.lock();
        let tx = conn.transaction()?;
        let existing: Option<(String, i64, String, i64)> = tx
            .query_row(
                "SELECT source_hash, parsed_row_count, result, imported_at_unix \
                 FROM legacy_history_imports WHERE import_name = ?1",
                params![IMPORT_NAME],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()?;
        if let Some((source_hash, count, result, imported_at_unix)) = existing {
            let existing = LegacyHistoryImport {
                source_hash,
                parsed_row_count: parse_u64(count)?,
                result,
                imported_at_unix,
                already_imported: true,
            };
            tx.commit()?;
            return Ok(existing);
        }

        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Sidecar {
            wallets: Vec<Entry>,
        }
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Entry {
            wallet: WalletAddress,
            markets: Vec<MarketId>,
        }

        let sidecar: Sidecar = serde_json::from_slice(bytes)
            .map_err(|error| PaperStateError::InvalidLegacyHistory(error.to_string()))?;
        let parsed_row_count = u64::try_from(sidecar.wallets.len())
            .map_err(|_| PaperStateError::InvalidLegacyHistory("row count overflow".to_owned()))?;
        let source_hash = blake3::hash(bytes).to_hex().to_string();
        let source_trade_id = format!("legacy:v1:{source_hash}");
        let mut merged: HashMap<WalletAddress, HashSet<MarketId>> = HashMap::new();
        for entry in sidecar.wallets {
            merged
                .entry(entry.wallet)
                .or_default()
                .extend(entry.markets);
        }

        let expected_entries = merged
            .iter()
            .flat_map(|(wallet, markets)| {
                markets.iter().map(move |market| (*wallet, market.clone()))
            })
            .collect::<Vec<_>>();
        for (wallet, markets) in merged {
            for market in markets {
                tx.execute(
                    "INSERT OR IGNORE INTO wallet_market_history_v2 \
                         (wallet_hex, market_id, first_epoch, source_trade_id, origin) \
                     VALUES (?1, ?2, 0, ?3, 'legacy_seed_v1')",
                    params![wallet.to_string(), market.to_string(), source_trade_id],
                )?;
            }
            tx.execute(
                "INSERT OR IGNORE INTO wallet_history_status_v2 \
                     (wallet_hex, complete, proof_json, updated_at_unix) \
                 VALUES (?1, 0, ?2, ?3)",
                params![
                    wallet.to_string(),
                    format!("{{\"legacy_source_hash\":\"{source_hash}\"}}"),
                    imported_at_unix,
                ],
            )?;
        }
        for (wallet, market) in &expected_entries {
            let present: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM wallet_market_history_v2 \
                 WHERE wallet_hex = ?1 AND market_id = ?2 AND source_trade_id = ?3 \
                       AND origin = 'legacy_seed_v1')",
                params![wallet.to_string(), market.to_string(), source_trade_id],
                |row| row.get(0),
            )?;
            if !present {
                return Err(PaperStateError::InvalidLegacyHistory(format!(
                    "import proof omitted {wallet}/{market}"
                )));
            }
        }
        tx.execute(
            "INSERT INTO legacy_history_imports \
                 (import_name, source_hash, parsed_row_count, result, imported_at_unix) \
             VALUES (?1, ?2, ?3, 'imported', ?4)",
            params![
                IMPORT_NAME,
                source_hash,
                to_i64(parsed_row_count)?,
                imported_at_unix,
            ],
        )?;
        tx.commit()?;
        Ok(LegacyHistoryImport {
            source_hash,
            parsed_row_count,
            result: "imported".to_owned(),
            imported_at_unix,
            already_imported: false,
        })
    }

    /// Commit a processed trade that produced a paper fill. One transaction:
    /// `mark_seen` + `upsert_leader_position` + `record_fill` + `update_position`
    /// + `update_bankroll` + `set_last_applied_event_seq(fill_seq)`.
    ///
    /// The fill is recorded with `INSERT OR IGNORE` on `idempotency_key`: if a
    /// fill with that key already exists (the 1-second idempotency-bucket collision
    /// of issue #282 Open risk #6), the position and bankroll are **not** applied
    /// twice. `mark_seen`/`upsert_leader` still run, since they concern this trade.
    ///
    /// Returns the commit outcome; both arms carry the post-commit bankroll so the
    /// caller can keep its in-memory copy in sync (#511: `RefusedSettled` = the market
    /// already settled — seen + typed flip written, no fill applied).
    pub fn commit_fill(
        &self,
        source_trade_id: &SourceTradeId,
        leader: &LeaderPositionRow,
        fill: &FillRecord,
        fill_seq: EventSeq,
    ) -> Result<FillCommitOutcome, PaperStateError> {
        self.commit_fill_with_flip(source_trade_id, leader, fill, fill_seq, None)
    }

    /// [`Self::commit_fill`] that also flips a staged dispatch seed `pending_paper → ready`
    /// in the same transaction (#508 Decision 10), recording the `fill` outcome on the
    /// aggregate row. With `flip = None` the behavior is byte-identical to the plain fill
    /// commit.
    pub fn commit_fill_with_flip(
        &self,
        source_trade_id: &SourceTradeId,
        leader: &LeaderPositionRow,
        fill: &FillRecord,
        fill_seq: EventSeq,
        flip: Option<DispatchFlip<'_>>,
    ) -> Result<FillCommitOutcome, PaperStateError> {
        self.commit_fill_with_flip_pending(
            source_trade_id,
            leader,
            fill,
            fill_seq,
            flip,
            None,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn commit_fill_with_flip_pending(
        &self,
        source_trade_id: &SourceTradeId,
        leader: &LeaderPositionRow,
        fill: &FillRecord,
        fill_seq: EventSeq,
        flip: Option<DispatchFlip<'_>>,
        fill_pending: Option<PendingTerminalEvidence<'_>>,
        settled_pending: Option<PendingTerminalEvidence<'_>>,
    ) -> Result<FillCommitOutcome, PaperStateError> {
        let mut conn = self.lock();
        let tx = conn.transaction()?;
        tx_mark_seen(&tx, source_trade_id, None)?;
        tx_upsert_leader(&tx, leader)?;
        // #511: no fill may enter a settled market — the resolution for it already ran
        // and can never credit the position (`settle_and_credit` retries credit zero).
        // Checked INSIDE the transaction: the single-connection mutex serializes this
        // against the local settle txn, so there is no check-then-commit window.
        if tx_is_settled(&tx, &fill.market_id)? {
            if let Some(flip) = flip {
                tx_flip_dispatch_ready(
                    &tx,
                    DispatchFlip {
                        dispatch_id: flip.dispatch_id,
                        paper_outcome: "no_fill:market_settled",
                    },
                )?;
            }
            tx_set_last_applied_max(&tx, fill_seq)?;
            tx_terminalize_pending(
                &tx,
                source_trade_id,
                "no_fill:market_settled",
                settled_pending,
            )?;
            let bankroll = tx_read_bankroll(&tx)?;
            tx.commit()?;
            return Ok(FillCommitOutcome::RefusedSettled(bankroll));
        }
        let inserted = tx_record_fill(&tx, fill, fill_seq)?;
        let bankroll = if inserted {
            tx_apply_our_position(&tx, fill)?;
            let after = tx_apply_bankroll(&tx, fill)?;
            tx_set_last_applied(&tx, fill_seq)?;
            after
        } else {
            // Duplicate idempotency key: fill already applied; do not double-count.
            tx_read_bankroll(&tx)?
        };
        if let Some(flip) = flip {
            tx_flip_dispatch_ready(&tx, flip)?;
        }
        tx_terminalize_pending(&tx, source_trade_id, "fill", fill_pending)?;
        tx.commit()?;
        Ok(FillCommitOutcome::Applied(bankroll))
    }

    /// Terminal disposition for a frame the authority refused (settled market) or a
    /// runtime settled-refusal (#511): seen + optional leader mirror + typed dispatch
    /// flip + `last_applied = max(·, seq)` — no fills row, no money. Boot replay then
    /// early-skips the frame forever, and the Supabase watermark successor-advances
    /// over it. `leader` is `None` on the boot path (the frame does not carry the
    /// long/short mirror; it refreshes on the wallet's next processed trade).
    pub fn commit_refused_fill(
        &self,
        source_trade_id: &SourceTradeId,
        leader: Option<&LeaderPositionRow>,
        seq: EventSeq,
        flip: Option<DispatchFlip<'_>>,
    ) -> Result<(), PaperStateError> {
        self.commit_refused_fill_pending(source_trade_id, leader, seq, flip, None)
    }

    pub fn commit_refused_fill_pending(
        &self,
        source_trade_id: &SourceTradeId,
        leader: Option<&LeaderPositionRow>,
        seq: EventSeq,
        flip: Option<DispatchFlip<'_>>,
        pending: Option<PendingTerminalEvidence<'_>>,
    ) -> Result<(), PaperStateError> {
        let mut conn = self.lock();
        let tx = conn.transaction()?;
        tx_mark_seen(&tx, source_trade_id, None)?;
        if let Some(leader) = leader {
            tx_upsert_leader(&tx, leader)?;
        }
        if let Some(flip) = flip {
            tx_flip_dispatch_ready(&tx, flip)?;
        }
        tx_set_last_applied_max(&tx, seq)?;
        tx_terminalize_pending(&tx, source_trade_id, "no_fill:market_settled", pending)?;
        tx.commit()?;
        Ok(())
    }

    /// Convergence transaction for an authority-confirmed fill (#511): mirror the
    /// CANONICAL row the `commit_fill_v2` RPC returned — on `existing`, the canonical
    /// `event_seq` differs from the current frame's — and SET the bankroll to the
    /// authority-returned value (never re-derived locally). One transaction: seen +
    /// optional leader mirror + canonical fill row (INSERT OR IGNORE by key, position
    /// applied only when newly inserted) + bankroll SET + dispatch flip +
    /// `last_applied = max(·, current_seq)`.
    #[allow(clippy::too_many_arguments)]
    pub fn commit_fill_canonical(
        &self,
        source_trade_id: Option<&SourceTradeId>,
        leader: Option<&LeaderPositionRow>,
        fill: &FillRecord,
        canonical_seq: EventSeq,
        current_seq: EventSeq,
        canonical_bankroll: Decimal,
        flip: Option<DispatchFlip<'_>>,
    ) -> Result<(), PaperStateError> {
        self.commit_fill_canonical_pending(
            source_trade_id,
            leader,
            fill,
            canonical_seq,
            current_seq,
            canonical_bankroll,
            flip,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn commit_fill_canonical_pending(
        &self,
        source_trade_id: Option<&SourceTradeId>,
        leader: Option<&LeaderPositionRow>,
        fill: &FillRecord,
        canonical_seq: EventSeq,
        current_seq: EventSeq,
        canonical_bankroll: Decimal,
        flip: Option<DispatchFlip<'_>>,
        pending: Option<PendingTerminalEvidence<'_>>,
    ) -> Result<(), PaperStateError> {
        let mut conn = self.lock();
        let tx = conn.transaction()?;
        if let Some(id) = source_trade_id {
            tx_mark_seen(&tx, id, None)?;
        }
        if let Some(leader) = leader {
            tx_upsert_leader(&tx, leader)?;
        }
        let inserted = tx_record_fill(&tx, fill, canonical_seq)?;
        if inserted {
            tx_apply_our_position(&tx, fill)?;
        }
        tx_set_bankroll(&tx, canonical_bankroll)?;
        if let Some(flip) = flip {
            tx_flip_dispatch_ready(&tx, flip)?;
        }
        tx_set_last_applied_max(&tx, current_seq)?;
        if let Some(id) = source_trade_id {
            tx_terminalize_pending(&tx, id, "fill", pending)?;
        }
        tx.commit()?;
        Ok(())
    }

    /// `true` when a fill row exists for `idempotency_key` (#511: dispatch recovery
    /// flips by disposition — fills row = "fill"; disposed row-less frame = refused).
    pub fn fill_exists(&self, idempotency_key: &str) -> Result<bool, PaperStateError> {
        let conn = self.lock();
        let found: Option<i64> = conn
            .query_row(
                "SELECT 1 FROM fills WHERE idempotency_key = ?1",
                params![idempotency_key],
                |row| row.get(0),
            )
            .optional()?;
        Ok(found.is_some())
    }

    /// Replay a logged fill whose SQLite commit was lost to a crash between the
    /// event-log `sync()` and the `commit_fill` transaction. Applies
    /// `record_fill` + `update_position` + `update_bankroll` + `set_last_applied`
    /// in one transaction, guarded so no fill is applied twice:
    ///
    /// - if `fill_seq <= last_applied_event_seq`, the fill is already mirrored — skip;
    /// - otherwise `INSERT OR IGNORE` on `idempotency_key` — a duplicate key means
    ///   the position/bankroll were already applied, so only the cursor advances.
    ///
    /// `seen_trades`/`leader_positions` are **not** restored here (they are never
    /// written to the event log); a mid-crash trade's leader-ledger effect and
    /// seen-mark self-heal when the poll backstop re-delivers it (the `fills`
    /// PK then blocks a duplicate fill). Since #511 that redelivery is GUARANTEED:
    /// the poll cursor never advances past an unseen trade (#282 Open risk #7, closed).
    ///
    /// Returns `true` if this call newly applied the fill.
    pub fn reconcile_fill(
        &self,
        fill: &FillRecord,
        fill_seq: EventSeq,
    ) -> Result<bool, PaperStateError> {
        let mut conn = self.lock();
        let tx = conn.transaction()?;
        // `last_applied` is `None` until the first fill is mirrored, so a fill at the
        // 0-based first event-log seq is not mistaken for "already applied".
        if let Some(last_applied) = tx_last_applied(&tx)?
            && fill_seq.0 <= last_applied
        {
            tx.commit()?;
            return Ok(false);
        }
        // #511: never resurrect a fill into a settled market — the resolution already ran
        // and can never credit it. Advance the cursor (terminal disposition) without
        // applying. Covers legacy replay and `--rebuild-state` (which restores
        // `settled_markets` from its backup before replaying).
        if tx_is_settled(&tx, &fill.market_id)? {
            tx_set_last_applied(&tx, fill_seq)?;
            tx.commit()?;
            return Ok(false);
        }
        let inserted = tx_record_fill(&tx, fill, fill_seq)?;
        if inserted {
            tx_apply_our_position(&tx, fill)?;
            tx_apply_bankroll(&tx, fill)?;
        }
        tx_set_last_applied(&tx, fill_seq)?;
        tx.commit()?;
        Ok(inserted)
    }

    // ── Live dispatch aggregate (#508 Decision 10) ───────────────────────────

    /// Durably stage a dispatch aggregate (seed + frozen ordered targets) in one
    /// transaction, state `pending_paper`. Idempotent: a seed already staged under this
    /// `dispatch_id` is left untouched (redelivery reuses it and never recomputes targets
    /// from current configuration) and `false` is returned.
    pub fn stage_dispatch_seed(&self, seed: &DispatchSeedRecord) -> Result<bool, PaperStateError> {
        self.stage_dispatch_seed_pending(seed, None)
    }

    pub fn stage_dispatch_seed_pending(
        &self,
        seed: &DispatchSeedRecord,
        pending: Option<PendingTerminalEvidence<'_>>,
    ) -> Result<bool, PaperStateError> {
        let mut conn = self.lock();
        let tx = conn.transaction()?;
        let inserted = tx.execute(
            "INSERT OR IGNORE INTO dispatch_seeds
                 (dispatch_id, state, signal_json, paper_outcome, source_trade_id,
                  created_at_unix, finalized_at_unix)
             VALUES (?1, 'pending_paper', ?2, NULL, ?3, ?4, NULL)",
            params![
                seed.dispatch_id,
                seed.signal_json,
                seed.source_trade_id,
                seed.created_at_unix
            ],
        )? == 1;
        if inserted {
            for (rank, target) in seed.targets.iter().enumerate() {
                let rank = i64::try_from(rank)
                    .map_err(|_| PaperStateError::Internal("target rank overflow".into()))?;
                tx.execute(
                    "INSERT INTO dispatch_targets
                         (dispatch_id, account_id, exec_rank, credential_bundle_version,
                          credential_key_id, state, terminal_reason, updated_at_unix)
                     VALUES (?1, ?2, ?3, ?4, ?5, 'pending', NULL, ?6)",
                    params![
                        seed.dispatch_id,
                        target.account_id,
                        rank,
                        target.credential_bundle_version,
                        target.credential_key_id,
                        seed.created_at_unix
                    ],
                )?;
            }
        }
        tx_terminalize_pending(
            &tx,
            &SourceTradeId(seed.source_trade_id.clone()),
            "dispatch_staged",
            pending,
        )?;
        tx.commit()?;
        Ok(inserted)
    }

    /// Standalone `pending_paper → ready` flip (boot stuck-seed finalization, #508
    /// Decision 10). The transactional trade-path flip rides
    /// [`Self::commit_fill_with_flip`] / [`Self::commit_seen_no_fill_with_flip`] instead.
    /// Returns `true` when this call performed the flip; `false` when the seed was
    /// already `ready` (idempotent). A missing seed is an invariant breach.
    pub fn flip_dispatch_ready(
        &self,
        dispatch_id: &str,
        paper_outcome: &str,
    ) -> Result<bool, PaperStateError> {
        let mut conn = self.lock();
        let tx = conn.transaction()?;
        let flipped = tx_flip_dispatch_ready(
            &tx,
            DispatchFlip {
                dispatch_id,
                paper_outcome,
            },
        )?;
        tx.commit()?;
        Ok(flipped)
    }

    /// Read one dispatch aggregate row, or `None`.
    pub fn dispatch_seed(
        &self,
        dispatch_id: &str,
    ) -> Result<Option<DispatchSeedRow>, PaperStateError> {
        let conn = self.lock();
        conn.query_row(
            "SELECT dispatch_id, state, signal_json, paper_outcome, source_trade_id,
                    created_at_unix, finalized_at_unix
             FROM dispatch_seeds WHERE dispatch_id = ?1",
            params![dispatch_id],
            row_to_dispatch_seed,
        )
        .optional()
        .map_err(Into::into)
    }

    /// Every `pending_paper` seed, oldest first (boot stuck-seed finalization input).
    pub fn pending_dispatch_seeds(&self) -> Result<Vec<DispatchSeedRow>, PaperStateError> {
        self.list_dispatch_seeds("state = 'pending_paper'")
    }

    /// Every `ready`, not-yet-finalized seed, oldest first (the live fan-out consumes
    /// these strictly in order; boot resumes incomplete aggregates oldest-first).
    pub fn unfinalized_ready_dispatch_seeds(
        &self,
    ) -> Result<Vec<DispatchSeedRow>, PaperStateError> {
        self.list_dispatch_seeds("state = 'ready' AND finalized_at_unix IS NULL")
    }

    fn list_dispatch_seeds(
        &self,
        predicate: &str,
    ) -> Result<Vec<DispatchSeedRow>, PaperStateError> {
        let conn = self.lock();
        let sql = format!(
            "SELECT dispatch_id, state, signal_json, paper_outcome, source_trade_id,
                    created_at_unix, finalized_at_unix
             FROM dispatch_seeds WHERE {predicate}
             ORDER BY created_at_unix ASC, dispatch_id ASC"
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt
            .query_map([], row_to_dispatch_seed)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// The frozen targets of one aggregate, in execution order.
    pub fn dispatch_targets(
        &self,
        dispatch_id: &str,
    ) -> Result<Vec<DispatchTargetRow>, PaperStateError> {
        let conn = self.lock();
        let mut stmt = conn.prepare(
            "SELECT dispatch_id, account_id, exec_rank, credential_bundle_version,
                    credential_key_id, state, terminal_reason, updated_at_unix
             FROM dispatch_targets WHERE dispatch_id = ?1 ORDER BY exec_rank ASC",
        )?;
        let rows = stmt
            .query_map(params![dispatch_id], |row| {
                Ok(DispatchTargetRow {
                    dispatch_id: row.get(0)?,
                    account_id: row.get(1)?,
                    exec_rank: row.get(2)?,
                    credential_bundle_version: row.get(3)?,
                    credential_key_id: row.get(4)?,
                    state: row.get(5)?,
                    terminal_reason: row.get(6)?,
                    updated_at_unix: row.get(7)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Durably transition one target's lifecycle state (`pending` / `submitted` /
    /// `ambiguous` / `terminal`, with the typed detail in `terminal_reason`).
    pub fn set_dispatch_target_state(
        &self,
        dispatch_id: &str,
        account_id: &str,
        state: &str,
        terminal_reason: Option<&str>,
        now_unix: i64,
    ) -> Result<(), PaperStateError> {
        let conn = self.lock();
        let updated = conn.execute(
            "UPDATE dispatch_targets
             SET state = ?3, terminal_reason = ?4, updated_at_unix = ?5
             WHERE dispatch_id = ?1 AND account_id = ?2",
            params![dispatch_id, account_id, state, terminal_reason, now_unix],
        )?;
        if updated != 1 {
            return Err(PaperStateError::Internal(format!(
                "dispatch target {dispatch_id}/{account_id} not found"
            )));
        }
        Ok(())
    }

    /// Stamp `finalized_at_unix` when every target of a `ready` aggregate is terminal
    /// (also finalizes a zero-live-outcome seed whose targets were all terminalized at
    /// boot). Returns `true` when this call performed the finalization.
    pub fn finalize_dispatch_if_terminal(
        &self,
        dispatch_id: &str,
        now_unix: i64,
    ) -> Result<bool, PaperStateError> {
        let conn = self.lock();
        let updated = conn.execute(
            "UPDATE dispatch_seeds SET finalized_at_unix = ?2
             WHERE dispatch_id = ?1 AND finalized_at_unix IS NULL
               AND NOT EXISTS (
                   SELECT 1 FROM dispatch_targets
                   WHERE dispatch_targets.dispatch_id = dispatch_seeds.dispatch_id
                     AND dispatch_targets.state != 'terminal'
               )",
            params![dispatch_id, now_unix],
        )?;
        Ok(updated == 1)
    }

    /// Delete terminal aggregates finalized more than `retention_secs` ago (#508:
    /// `dispatch_seed_retention_days`, `_GLOSSARY.md`). Returns the pruned seed count.
    pub fn prune_terminal_dispatch(
        &self,
        now_unix: i64,
        retention_secs: i64,
    ) -> Result<usize, PaperStateError> {
        let mut conn = self.lock();
        let tx = conn.transaction()?;
        let cutoff = now_unix.saturating_sub(retention_secs);
        tx.execute(
            "DELETE FROM dispatch_targets WHERE dispatch_id IN (
                 SELECT dispatch_id FROM dispatch_seeds
                 WHERE finalized_at_unix IS NOT NULL AND finalized_at_unix < ?1
             )",
            params![cutoff],
        )?;
        let pruned = tx.execute(
            "DELETE FROM dispatch_seeds
             WHERE finalized_at_unix IS NOT NULL AND finalized_at_unix < ?1",
            params![cutoff],
        )?;
        tx.commit()?;
        Ok(pruned)
    }

    /// The Phase-D executor's first-boot instant (#508 Decision 8 arming fence),
    /// recorded once and immutable thereafter: promotion records/requests predating it
    /// are never honored, so the executor binary provably ships dark. Returns the fence
    /// (existing or newly recorded at `now_unix`).
    pub fn record_live_executor_first_boot(&self, now_unix: i64) -> Result<i64, PaperStateError> {
        let conn = self.lock();
        conn.execute(
            "INSERT OR IGNORE INTO meta (key, value) VALUES ('live_executor_first_boot_unix', ?1)",
            params![now_unix],
        )?;
        conn.query_row(
            "SELECT value FROM meta WHERE key = 'live_executor_first_boot_unix'",
            [],
            |row| row.get(0),
        )
        .map_err(Into::into)
    }

    /// Read the arming fence, or `None` before the executor's first boot.
    pub fn live_executor_first_boot(&self) -> Result<Option<i64>, PaperStateError> {
        let conn = self.lock();
        conn.query_row(
            "SELECT value FROM meta WHERE key = 'live_executor_first_boot_unix'",
            [],
            |row| row.get(0),
        )
        .optional()
        .map_err(Into::into)
    }

    // ── Bankroll ─────────────────────────────────────────────────────────────

    /// Initialise the bankroll to `initial` if not already present, then return
    /// the current (possibly already drawn-down) bankroll. Idempotent across runs.
    pub fn init_bankroll(&self, initial: Decimal) -> Result<Decimal, PaperStateError> {
        let conn = self.lock();
        conn.execute(
            "INSERT OR IGNORE INTO bankroll (id, bankroll_str) VALUES (?1, ?2)",
            params![BANKROLL_ROW_ID, initial.to_string()],
        )?;
        read_bankroll(&conn)
    }

    /// Current bankroll, or `None` if it has not been initialised.
    pub fn bankroll(&self) -> Result<Option<Decimal>, PaperStateError> {
        let conn = self.lock();
        let raw: Option<String> = conn
            .query_row(
                "SELECT bankroll_str FROM bankroll WHERE id = ?1",
                params![BANKROLL_ROW_ID],
                |row| row.get(0),
            )
            .optional()?;
        raw.map(|s| parse_decimal(&s)).transpose()
    }

    /// Credit `amount` to the bankroll (e.g. on market resolution payout).
    ///
    /// # Precondition
    /// Idempotency is the caller's responsibility: use [`ResolutionStore`] to ensure
    /// this is called at most once per market. `credit_bankroll` itself does not track
    /// which markets have been settled.
    pub fn credit_bankroll(&self, amount: Decimal) -> Result<Decimal, PaperStateError> {
        let conn = self.lock();
        let current = read_bankroll(&conn)?;
        let new = current
            .checked_add(amount)
            .ok_or_else(|| PaperStateError::Internal("bankroll credit overflow".to_string()))?;
        conn.execute(
            "INSERT INTO bankroll (id, bankroll_str) VALUES (?1, ?2) \
             ON CONFLICT(id) DO UPDATE SET bankroll_str = excluded.bankroll_str",
            params![BANKROLL_ROW_ID, new.to_string()],
        )?;
        Ok(new)
    }

    /// Overwrite the bankroll to `value` unconditionally (issue #397 boot pull): the
    /// authoritative Supabase value becomes the local-cache value. Unlike
    /// [`credit_bankroll`](Self::credit_bankroll) (a delta) or
    /// [`init_bankroll`](Self::init_bankroll) (insert-or-ignore), this is a direct setter
    /// for mirroring the authoritative store into SQLite. Not used on the non-authoritative
    /// path, where the bankroll only ever moves through `commit_fill`/`credit_bankroll`.
    pub fn set_bankroll(&self, value: Decimal) -> Result<(), PaperStateError> {
        let conn = self.lock();
        conn.execute(
            "INSERT INTO bankroll (id, bankroll_str) VALUES (?1, ?2) \
             ON CONFLICT(id) DO UPDATE SET bankroll_str = excluded.bankroll_str",
            params![BANKROLL_ROW_ID, value.to_string()],
        )?;
        Ok(())
    }

    /// Number of fills recorded in the `fills` table.
    pub fn fills_count(&self) -> Result<usize, PaperStateError> {
        let conn = self.lock();
        let n: i64 = conn.query_row("SELECT COUNT(*) FROM fills", [], |row| row.get(0))?;
        usize::try_from(n)
            .map_err(|_| PaperStateError::Internal(format!("fills_count {n} exceeds usize::MAX")))
    }

    /// Number of genuinely OPEN positions: net-nonzero rows whose market has not settled
    /// (#516) — the same semantic as `/paper/positions` and portfolio valuation. The
    /// `positions` table itself is cumulative by design: settlement credits the bankroll
    /// and records `settled_markets` but never zeroes/deletes position rows, because
    /// `apply_resolution_v2` computes the credit FROM those rows at settlement time.
    pub fn positions_count(&self) -> Result<usize, PaperStateError> {
        let conn = self.lock();
        let n: i64 = conn.query_row(
            "SELECT COUNT(*) FROM positions p \
             WHERE (p.long_contracts > 0 OR p.short_contracts > 0) \
               AND NOT EXISTS (SELECT 1 FROM settled_markets s WHERE s.market_id = p.market_id)",
            [],
            |row| row.get(0),
        )?;
        usize::try_from(n).map_err(|_| {
            PaperStateError::Internal(format!("positions_count {n} exceeds usize::MAX"))
        })
    }

    /// Number of settled-market rows. Cheap `COUNT(*)` for the status snapshot.
    pub fn settled_count(&self) -> Result<usize, PaperStateError> {
        let conn = self.lock();
        let n: i64 =
            conn.query_row("SELECT COUNT(*) FROM settled_markets", [], |row| row.get(0))?;
        usize::try_from(n)
            .map_err(|_| PaperStateError::Internal(format!("settled_count {n} exceeds usize::MAX")))
    }

    /// All recorded fills in chronological (event-log) order, newest last.
    pub fn list_fills(&self) -> Result<Vec<FillRow>, PaperStateError> {
        let conn = self.lock();
        let mut stmt = conn.prepare(
            "SELECT idempotency_key, market_id, outcome_id, side, contracts, fill_price_str, \
             event_seq FROM fills ORDER BY event_seq ASC",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, i64>(6)?,
            ))
        })?;
        let mut out = Vec::new();
        for row in rows {
            let (key, market, outcome, side, contracts, price_str, event_seq) = row?;
            out.push(FillRow {
                idempotency_key: key,
                market_id: MarketId(VenueMarketId(market)),
                outcome_id: OutcomeId(parse_u16(outcome)?),
                side: parse_side(&side)?,
                contracts: parse_u64(contracts)?,
                fill_price: Price(parse_decimal(&price_str)?),
                event_seq,
            });
        }
        Ok(out)
    }

    // ── Reconciliation cursor ────────────────────────────────────────────────

    /// The highest event-log `seq` whose fill has been mirrored into SQLite.
    /// Defaults to `EventSeq(0)` before any fill has been committed.
    pub fn last_applied_event_seq(&self) -> Result<EventSeq, PaperStateError> {
        let conn = self.lock();
        Ok(EventSeq(read_last_applied(&conn)?.unwrap_or(0)))
    }

    /// Absent-vs-zero variant of [`last_applied_event_seq`](Self::last_applied_event_seq)
    /// (#511): `None` = no disposition has ever advanced the cursor (fresh DB), which is
    /// distinct from `Some(0)` = seq 0 disposed. Dispatch recovery uses this to avoid
    /// misreading an undisposed seq-0 frame as a settled refusal.
    pub fn last_applied_event_seq_opt(&self) -> Result<Option<EventSeq>, PaperStateError> {
        let conn = self.lock();
        Ok(read_last_applied(&conn)?.map(EventSeq))
    }

    /// The highest event-log `seq` whose fill has been applied to the **authoritative
    /// Supabase** `commit_fill` RPC (issue #397). `None` = the meta row is ABSENT — no
    /// fill has ever been confirmed (fresh DB, or after a SQLite loss, forcing a safe
    /// full idempotent replay that INCLUDES seq 0). `Some(EventSeq(0))` is distinct:
    /// seq 0 itself is confirmed (#510 — the absent-vs-zero split lets catch-up and the
    /// runtime successor gate treat the first frame correctly). Kept separate from
    /// [`last_applied_event_seq`](Self::last_applied_event_seq) so a local-only
    /// reconcile never advances the Supabase catch-up cursor.
    pub fn last_supabase_applied_event_seq(&self) -> Result<Option<EventSeq>, PaperStateError> {
        let conn = self.lock();
        let raw: Option<i64> = conn
            .query_row(
                "SELECT value FROM meta WHERE key = ?1",
                params![META_LAST_SUPABASE_APPLIED_EVENT_SEQ],
                |row| row.get(0),
            )
            .optional()?;
        Ok(raw.map(parse_u64).transpose()?.map(EventSeq))
    }

    /// Persist the Supabase authoritative catch-up watermark (issue #397). Set to the
    /// event-log head at cutover (so the first authoritative boot's catch-up is a no-op),
    /// advanced as boot catch-up confirms each fill against Supabase, and advanced at
    /// runtime by `commit_fill_authoritative` on each confirmed successor fill (#510).
    pub fn set_supabase_applied_event_seq(&self, seq: EventSeq) -> Result<(), PaperStateError> {
        let conn = self.lock();
        conn.execute(
            "INSERT INTO meta (key, value) VALUES (?1, ?2) \
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![META_LAST_SUPABASE_APPLIED_EVENT_SEQ, to_i64(seq.0)?],
        )?;
        Ok(())
    }

    // ── Restore readers ──────────────────────────────────────────────────────

    /// All leader position rows, for rehydrating the in-memory `PositionLedger`.
    pub fn leader_positions(&self) -> Result<Vec<LeaderPositionRow>, PaperStateError> {
        let conn = self.lock();
        let mut stmt = conn.prepare(
            "SELECT wallet_hex, market_id, outcome_id, long_amount_str, short_amount_str \
             FROM leader_positions",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
            ))
        })?;
        let mut out = Vec::new();
        for row in rows {
            let (wallet_hex, market, outcome, long, short) = row?;
            out.push(LeaderPositionRow {
                wallet: parse_wallet(&wallet_hex)?,
                market_id: MarketId(VenueMarketId(market)),
                outcome_id: OutcomeId(parse_u16(outcome)?),
                long_contracts: parse_share_amount(&long)?,
                short_contracts: parse_share_amount(&short)?,
            });
        }
        Ok(out)
    }

    /// All of our own paper position rows, for restoring open positions on restart.
    pub fn paper_positions(&self) -> Result<Vec<PaperPositionRow>, PaperStateError> {
        let conn = self.lock();
        let mut stmt = conn.prepare(
            "SELECT market_id, outcome_id, long_contracts, short_contracts FROM positions",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })?;
        let mut out = Vec::new();
        for row in rows {
            let (market, outcome, long, short) = row?;
            out.push(PaperPositionRow {
                market_id: MarketId(VenueMarketId(market)),
                outcome_id: OutcomeId(parse_u16(outcome)?),
                long_contracts: parse_u64(long)?,
                short_contracts: parse_u64(short)?,
            });
        }
        Ok(out)
    }

    /// Upsert one of our own net paper positions (issue #397 boot pull): mirror an
    /// authoritative Supabase `paper_positions` row into the local cache. On the
    /// non-authoritative path positions only move through `commit_fill`; this setter
    /// exists solely so the boot pull can overwrite the cache with the Supabase value.
    pub fn upsert_position(
        &self,
        market_id: &MarketId,
        outcome_id: OutcomeId,
        long_contracts: u64,
        short_contracts: u64,
    ) -> Result<(), PaperStateError> {
        let conn = self.lock();
        conn.execute(
            "INSERT INTO positions (market_id, outcome_id, long_contracts, short_contracts) \
             VALUES (?1, ?2, ?3, ?4) \
             ON CONFLICT(market_id, outcome_id) DO UPDATE SET \
                long_contracts = excluded.long_contracts, \
                short_contracts = excluded.short_contracts",
            params![
                market_id.to_string(),
                i64::from(outcome_id.0),
                to_i64(long_contracts)?,
                to_i64(short_contracts)?,
            ],
        )?;
        Ok(())
    }

    /// Atomically replace the complete authoritative bankroll/positions snapshot (#544).
    ///
    /// All keys and integer conversions are validated before the transaction begins. The single
    /// transaction then replaces the bankroll, deletes every prior position, and inserts the
    /// complete fetched set, so a failed pull or failed replacement cannot expose a partial page.
    pub fn replace_authoritative_state(
        &self,
        bankroll: Decimal,
        positions: &[PaperPositionRow],
    ) -> Result<(), PaperStateError> {
        self.replace_authoritative_state_inner(bankroll, positions, None)
    }

    /// Scenario seam for proving rollback after an injected mid-replacement failure (#544).
    #[cfg(feature = "scenario")]
    pub fn replace_authoritative_state_failing_after(
        &self,
        bankroll: Decimal,
        positions: &[PaperPositionRow],
        inserted_rows: usize,
    ) -> Result<(), PaperStateError> {
        self.replace_authoritative_state_inner(bankroll, positions, Some(inserted_rows))
    }

    fn replace_authoritative_state_inner(
        &self,
        bankroll: Decimal,
        positions: &[PaperPositionRow],
        fail_after: Option<usize>,
    ) -> Result<(), PaperStateError> {
        let mut keys = BTreeSet::new();
        let mut encoded = Vec::with_capacity(positions.len());
        for position in positions {
            let key = (position.market_id.to_string(), position.outcome_id.0);
            if !keys.insert(key.clone()) {
                return Err(PaperStateError::DuplicateAuthoritativePosition {
                    market_id: key.0,
                    outcome_id: key.1,
                });
            }
            encoded.push((
                key.0,
                i64::from(position.outcome_id.0),
                to_i64(position.long_contracts)?,
                to_i64(position.short_contracts)?,
            ));
        }

        let mut conn = self.lock();
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT INTO bankroll (id, bankroll_str) VALUES (?1, ?2) \
             ON CONFLICT(id) DO UPDATE SET bankroll_str = excluded.bankroll_str",
            params![BANKROLL_ROW_ID, bankroll.to_string()],
        )?;
        tx.execute("DELETE FROM positions", [])?;
        {
            let mut statement = tx.prepare(
                "INSERT INTO positions \
                 (market_id, outcome_id, long_contracts, short_contracts) \
                 VALUES (?1, ?2, ?3, ?4)",
            )?;
            for (index, (market_id, outcome_id, long_contracts, short_contracts)) in
                encoded.into_iter().enumerate()
            {
                if fail_after == Some(index) {
                    return Err(PaperStateError::Internal(
                        "injected authoritative replacement failure".to_owned(),
                    ));
                }
                statement.execute(params![
                    market_id,
                    outcome_id,
                    long_contracts,
                    short_contracts
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    // ── Poll cursors ─────────────────────────────────────────────────────────

    /// Last-seen `observed_at` unix timestamp for `wallet`, or `None`.
    pub fn cursor(&self, wallet: &WalletAddress) -> Result<Option<i64>, PaperStateError> {
        let conn = self.lock();
        let ts: Option<i64> = conn
            .query_row(
                "SELECT last_ts_unix FROM poll_cursors WHERE wallet_hex = ?1",
                params![wallet.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        Ok(ts)
    }

    /// Advance `wallet`'s poll cursor to `ts_unix`.
    pub fn set_cursor(&self, wallet: &WalletAddress, ts_unix: i64) -> Result<(), PaperStateError> {
        let conn = self.lock();
        conn.execute(
            "INSERT INTO poll_cursors (wallet_hex, last_ts_unix) VALUES (?1, ?2) \
             ON CONFLICT(wallet_hex) DO UPDATE SET \
                last_ts_unix = MAX(poll_cursors.last_ts_unix, excluded.last_ts_unix)",
            params![wallet.to_string(), ts_unix],
        )?;
        Ok(())
    }

    /// Atomically advance several poll cursors in one SQLite transaction.
    ///
    /// Membership transitions use this before publishing newly admitted wallets, so either every
    /// admission has a bounded activity cursor or none of the cursor batch is committed.
    pub fn set_cursors(&self, cursors: &[(WalletAddress, i64)]) -> Result<(), PaperStateError> {
        let mut conn = self.lock();
        let tx = conn.transaction()?;
        {
            let mut statement = tx.prepare(
                "INSERT INTO poll_cursors (wallet_hex, last_ts_unix) VALUES (?1, ?2) \
                 ON CONFLICT(wallet_hex) DO UPDATE SET \
                    last_ts_unix = MAX(poll_cursors.last_ts_unix, excluded.last_ts_unix)",
            )?;
            for (wallet, ts_unix) in cursors {
                statement.execute(params![wallet.to_string(), ts_unix])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Newest trade timestamp ever observed for `wallet` (#511 activity clock), or
    /// `None` (no row / unmigrated / never observed). Feeds the inactivity knockout;
    /// callers fall back to [`cursor`](Self::cursor) on `None`.
    pub fn activity(&self, wallet: &WalletAddress) -> Result<Option<i64>, PaperStateError> {
        let conn = self.lock();
        let ts: Option<Option<i64>> = conn
            .query_row(
                "SELECT last_activity_unix FROM poll_cursors WHERE wallet_hex = ?1",
                params![wallet.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        Ok(ts.flatten())
    }

    /// MAX-advance `wallet`'s activity clock (#511). UPDATE-only: a wallet with no
    /// cursor row keeps none (a fabricated delivery cursor would unhold an unbounded
    /// first fetch); the knockout treats a missing row as just-admitted, which is safe.
    pub fn set_activity(
        &self,
        wallet: &WalletAddress,
        ts_unix: i64,
    ) -> Result<(), PaperStateError> {
        let conn = self.lock();
        conn.execute(
            "UPDATE poll_cursors              SET last_activity_unix = MAX(COALESCE(last_activity_unix, 0), ?2)              WHERE wallet_hex = ?1",
            params![wallet.to_string(), ts_unix],
        )?;
        Ok(())
    }

    /// Seed `wallet`'s delivery cursor ONLY when it has none (#511): an existing cursor
    /// — held or not — is already a valid lower bound and must never be jumped by a
    /// startup/admission seed. Seeds the activity clock too (MAX via the insert value).
    pub fn seed_cursor_if_absent(
        &self,
        wallet: &WalletAddress,
        ts_unix: i64,
    ) -> Result<(), PaperStateError> {
        let conn = self.lock();
        conn.execute(
            "INSERT INTO poll_cursors (wallet_hex, last_ts_unix, last_activity_unix)              VALUES (?1, ?2, ?2)              ON CONFLICT(wallet_hex) DO UPDATE SET                 last_activity_unix = MAX(COALESCE(poll_cursors.last_activity_unix, 0),                                          excluded.last_activity_unix)",
            params![wallet.to_string(), ts_unix],
        )?;
        Ok(())
    }

    /// Batch [`seed_cursor_if_absent`](Self::seed_cursor_if_absent) in one transaction
    /// (membership admission publishes all-or-none, mirroring `set_cursors`).
    pub fn seed_cursors_if_absent(
        &self,
        cursors: &[(WalletAddress, i64)],
    ) -> Result<(), PaperStateError> {
        let mut conn = self.lock();
        let tx = conn.transaction()?;
        {
            let mut statement = tx.prepare(
                "INSERT INTO poll_cursors (wallet_hex, last_ts_unix, last_activity_unix)                  VALUES (?1, ?2, ?2)                  ON CONFLICT(wallet_hex) DO UPDATE SET                     last_activity_unix = MAX(COALESCE(poll_cursors.last_activity_unix, 0),                                              excluded.last_activity_unix)",
            )?;
            for (wallet, ts_unix) in cursors {
                statement.execute(params![wallet.to_string(), ts_unix])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    // ── Settled markets (resolution double-credit guard) ──────────────────────

    /// Record a settled market, **idempotent** on `market_id` (`ON CONFLICT DO NOTHING`):
    /// a second call for the same market is a no-op and never overwrites the first
    /// settlement. `outcome_prices_json` is stored verbatim (the caller owns the
    /// encoding); `credit_applied` is persisted as text per the crate's decimal convention.
    pub fn record_settled_market(
        &self,
        market_id: &MarketId,
        outcome_prices_json: &str,
        credit_applied: Decimal,
        settled_at_unix: i64,
    ) -> Result<(), PaperStateError> {
        let conn = self.lock();
        conn.execute(
            "INSERT INTO settled_markets \
                (market_id, outcome_prices, credit_applied, settled_at_unix) \
             VALUES (?1, ?2, ?3, ?4) \
             ON CONFLICT(market_id) DO NOTHING",
            params![
                market_id.to_string(),
                outcome_prices_json,
                credit_applied.to_string(),
                settled_at_unix,
            ],
        )?;
        Ok(())
    }

    /// Atomically record a settled market (guard-insert) and, **only when the row is
    /// newly inserted**, credit the bankroll — one transaction, mirroring the Supabase
    /// `apply_resolution` RPC (issue #397). Returns the resulting bankroll (unchanged
    /// when the market was already settled, so a retry credits **zero**).
    ///
    /// This is the local mirror of the authoritative `apply_resolution`: the resolution
    /// tick calls the RPC first, then this. Gating the credit on the settled-row insert
    /// (not just `ON CONFLICT DO NOTHING` on the marker) is what makes a reload-then-retry
    /// idempotent — the marker no-ops but the credit must not re-run.
    /// Copy the `settled_markets` rows from another paper-state DB (#511 rebuild): the
    /// rebuild wipes the DB before replaying, which would discard the very authority that
    /// lets replay refuse fills into settled markets. Restores rows verbatim (idempotent).
    pub fn restore_settled_markets_from(&self, other: &Path) -> Result<usize, PaperStateError> {
        let src = Connection::open_with_flags(other, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        let mut rows = Vec::new();
        {
            let mut statement = src.prepare(
                "SELECT market_id, outcome_prices, credit_applied, settled_at_unix \
                 FROM settled_markets",
            )?;
            let mapped = statement.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            })?;
            for r in mapped {
                rows.push(r?);
            }
        }
        let n = rows.len();
        let conn = self.lock();
        for (market, prices, credit, at) in rows {
            conn.execute(
                "INSERT INTO settled_markets \
                    (market_id, outcome_prices, credit_applied, settled_at_unix) \
                 VALUES (?1, ?2, ?3, ?4) ON CONFLICT(market_id) DO NOTHING",
                params![market, prices, credit, at],
            )?;
        }
        Ok(n)
    }

    /// Legacy-mode settle (#511): compute the credit **inside** the settle transaction
    /// from freshly-read positions, closing the read-then-settle TOCTOU (a fill
    /// committing between an outside read and the settle could never be credited).
    /// `credit_fn` is pure (e.g. `PnlLedger::resolution_credit`); it sees the market's
    /// positions as of this transaction. Returns `(credit_applied, bankroll)`;
    /// `(0, bankroll)` when the market was already settled.
    pub fn settle_and_credit_from_positions<F>(
        &self,
        market_id: &MarketId,
        outcome_prices_json: &str,
        settled_at_unix: i64,
        credit_fn: F,
    ) -> Result<(Decimal, Decimal), PaperStateError>
    where
        F: FnOnce(&[PaperPositionRow]) -> Decimal,
    {
        let mut conn = self.lock();
        let tx = conn.transaction()?;
        if tx_is_settled(&tx, market_id)? {
            let bankroll = tx_read_bankroll(&tx)?;
            tx.commit()?;
            return Ok((Decimal::ZERO, bankroll));
        }
        let positions = tx_positions_for_market(&tx, market_id)?;
        let credit = credit_fn(&positions);
        tx.execute(
            "INSERT INTO settled_markets \
                (market_id, outcome_prices, credit_applied, settled_at_unix) \
             VALUES (?1, ?2, ?3, ?4) \
             ON CONFLICT(market_id) DO NOTHING",
            params![
                market_id.to_string(),
                outcome_prices_json,
                credit.to_string(),
                settled_at_unix,
            ],
        )?;
        let current = tx_read_bankroll(&tx)?;
        let new = current
            .checked_add(credit)
            .ok_or_else(|| PaperStateError::Internal("bankroll credit overflow".to_string()))?;
        tx_set_bankroll(&tx, new)?;
        tx.commit()?;
        Ok((credit, new))
    }

    pub fn settle_and_credit(
        &self,
        market_id: &MarketId,
        outcome_prices_json: &str,
        credit: Decimal,
        settled_at_unix: i64,
    ) -> Result<Decimal, PaperStateError> {
        let mut conn = self.lock();
        let tx = conn.transaction()?;
        let inserted = tx.execute(
            "INSERT INTO settled_markets \
                (market_id, outcome_prices, credit_applied, settled_at_unix) \
             VALUES (?1, ?2, ?3, ?4) \
             ON CONFLICT(market_id) DO NOTHING",
            params![
                market_id.to_string(),
                outcome_prices_json,
                credit.to_string(),
                settled_at_unix,
            ],
        )? > 0;
        let bankroll = if inserted {
            let current = tx_read_bankroll(&tx)?;
            let new = current
                .checked_add(credit)
                .ok_or_else(|| PaperStateError::Internal("bankroll credit overflow".to_string()))?;
            tx.execute(
                "INSERT INTO bankroll (id, bankroll_str) VALUES (?1, ?2) \
                 ON CONFLICT(id) DO UPDATE SET bankroll_str = excluded.bankroll_str",
                params![BANKROLL_ROW_ID, new.to_string()],
            )?;
            new
        } else {
            // Already settled: no double-credit; return the existing bankroll.
            tx_read_bankroll(&tx)?
        };
        tx.commit()?;
        Ok(bankroll)
    }

    /// All settled markets, for hydrating the resolution store's double-credit guard
    /// on restart. Unordered (the guard is a set lookup, not a sequence).
    pub fn list_settled_markets(&self) -> Result<Vec<SettledMarketRow>, PaperStateError> {
        let conn = self.lock();
        let mut stmt = conn.prepare(
            "SELECT market_id, outcome_prices, credit_applied, settled_at_unix \
             FROM settled_markets",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })?;
        let mut out = Vec::new();
        for row in rows {
            let (market, prices_json, credit_str, settled_at_unix) = row?;
            out.push(SettledMarketRow {
                market_id: MarketId(VenueMarketId(market)),
                outcome_prices_json: prices_json,
                credit_applied: parse_decimal(&credit_str)?,
                settled_at_unix,
            });
        }
        Ok(out)
    }

    /// Upsert a fill-time market-liquidity snapshot, keyed by `idempotency_key`
    /// (`ON CONFLICT DO UPDATE`: a later snapshot for the same fill — e.g. a `/book`
    /// retry that fills in a previously-partial row — replaces the earlier one).
    /// `liquidity`/`volume` and the CLOB-derived `absorbable_usd_100bps`/`ask_levels_json`
    /// are each optional; `None` is stored as SQL `NULL` (a partial row when `/book`
    /// failed). Decimals are persisted as text per the crate's exactness convention.
    ///
    /// # Precondition
    /// `snapshot.idempotency_key` should reference a committed `fills` row. The mirror
    /// is documentary: `fill_market_snapshots` declares no SQL foreign key (the bundled
    /// SQLite enforces FKs by default, and this write must never fail on referential
    /// grounds), so referential integrity is the caller's responsibility, not a checked
    /// invariant.
    pub fn upsert_fill_market_snapshot(
        &self,
        snapshot: &FillMarketSnapshot,
    ) -> Result<(), PaperStateError> {
        let conn = self.lock();
        let liquidity = snapshot.liquidity.map(|d| d.to_string());
        let volume = snapshot.volume.map(|d| d.to_string());
        let absorbable = snapshot.absorbable_usd_100bps.map(|d| d.to_string());
        conn.execute(
            "INSERT INTO fill_market_snapshots \
                (idempotency_key, liquidity, volume, absorbable_usd_100bps, \
                 ask_levels_json, captured_at_unix) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6) \
             ON CONFLICT(idempotency_key) DO UPDATE SET \
                liquidity = excluded.liquidity, \
                volume = excluded.volume, \
                absorbable_usd_100bps = excluded.absorbable_usd_100bps, \
                ask_levels_json = excluded.ask_levels_json, \
                captured_at_unix = excluded.captured_at_unix",
            params![
                snapshot.idempotency_key,
                liquidity,
                volume,
                absorbable,
                snapshot.ask_levels_json,
                snapshot.captured_at_unix,
            ],
        )?;
        Ok(())
    }

    /// All fill-market snapshots, unordered (keyed lookups and capacity analysis, not
    /// a sequence). Optional Gamma/CLOB scalars surface as `None` when the stored cell
    /// is `NULL` (a partial row).
    pub fn list_fill_snapshots(&self) -> Result<Vec<FillMarketSnapshot>, PaperStateError> {
        let conn = self.lock();
        let mut stmt = conn.prepare(
            "SELECT idempotency_key, liquidity, volume, absorbable_usd_100bps, \
             ask_levels_json, captured_at_unix FROM fill_market_snapshots",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, i64>(5)?,
            ))
        })?;
        let mut out = Vec::new();
        for row in rows {
            let (key, liquidity, volume, absorbable, ask_levels_json, captured_at_unix) = row?;
            out.push(FillMarketSnapshot {
                idempotency_key: key,
                liquidity: liquidity.map(|s| parse_decimal(&s)).transpose()?,
                volume: volume.map(|s| parse_decimal(&s)).transpose()?,
                absorbable_usd_100bps: absorbable.map(|s| parse_decimal(&s)).transpose()?,
                ask_levels_json,
                captured_at_unix,
            });
        }
        Ok(out)
    }
}

// ── Transaction-scoped helpers ──────────────────────────────────────────────

fn tx_upsert_position_validation(
    tx: &Transaction<'_>,
    validation: &PositionValidationRecord,
) -> Result<(), PaperStateError> {
    tx.execute(
        "INSERT INTO position_validations \
             (wallet_hex, ledger_hash, positions_proof_hash, activity_bounds_json, \
              source_log_generation, proof_json, recorded_at_unix) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) \
         ON CONFLICT(wallet_hex) DO UPDATE SET \
             ledger_hash = excluded.ledger_hash, \
             positions_proof_hash = excluded.positions_proof_hash, \
             activity_bounds_json = excluded.activity_bounds_json, \
             source_log_generation = excluded.source_log_generation, \
             proof_json = excluded.proof_json, \
             recorded_at_unix = excluded.recorded_at_unix",
        params![
            validation.wallet.to_string(),
            validation.ledger_hash,
            validation.positions_proof_hash,
            validation.activity_bounds_json,
            validation.source_log_generation,
            validation.proof_json,
            validation.recorded_at_unix,
        ],
    )?;
    Ok(())
}

fn validate_activity_bucket(bucket: &ActivityBucketCommit) -> Result<(), PaperStateError> {
    let mut group_ids = BTreeSet::new();
    for record in &bucket.dispositions {
        if record.wallet != bucket.wallet || record.source_epoch != bucket.source_epoch {
            return Err(PaperStateError::ActivityBucketMismatch);
        }
        if !record.source_trade_id.is_reconciled_v2() {
            return Err(PaperStateError::InvalidVersionTwoIdentity(
                record.source_trade_id.0.clone(),
            ));
        }
        if !group_ids.insert(record.source_trade_id.0.clone()) {
            return Err(PaperStateError::ActivityRevisionConflict(
                record.source_trade_id.0.clone(),
            ));
        }
        serde_json::from_str::<serde_json::Value>(&record.proof_json)?;
    }
    for leader in &bucket.leader_positions {
        if leader.wallet != bucket.wallet {
            return Err(PaperStateError::ActivityBucketMismatch);
        }
    }
    for gate in &bucket.gate_results {
        if gate.wallet != bucket.wallet
            || gate.source_epoch != bucket.source_epoch
            || !group_ids.contains(&gate.source_trade_id.0)
        {
            return Err(PaperStateError::ActivityBucketMismatch);
        }
    }
    for history in &bucket.history_effects {
        if history.wallet != bucket.wallet
            || history.first_epoch != bucket.source_epoch
            || !group_ids.contains(&history.source_trade_id.0)
        {
            return Err(PaperStateError::ActivityBucketMismatch);
        }
    }
    if let Some(status) = &bucket.history_status {
        if status.wallet != bucket.wallet {
            return Err(PaperStateError::ActivityBucketMismatch);
        }
        serde_json::from_str::<serde_json::Value>(&status.proof_json)?;
    }
    for pending in &bucket.pending {
        if pending.wallet != bucket.wallet
            || pending.source_epoch != bucket.source_epoch
            || !group_ids.contains(&pending.source_trade_id.0)
        {
            return Err(PaperStateError::ActivityBucketMismatch);
        }
        serde_json::from_str::<serde_json::Value>(&pending.frozen_inputs_json)?;
    }
    if let Some(fence) = &bucket.fence {
        if fence.wallet != bucket.wallet || !group_ids.contains(&fence.source_trade_id.0) {
            return Err(PaperStateError::ActivityBucketMismatch);
        }
        serde_json::from_str::<serde_json::Value>(&fence.proof_json)?;
    }
    if let Some(reanchor) = &bucket.reanchor
        && !group_ids.contains(&reanchor.source_trade_id.0)
    {
        return Err(PaperStateError::ActivityBucketMismatch);
    }
    Ok(())
}

/// Map a `dispatch_seeds` row into [`DispatchSeedRow`].
fn row_to_dispatch_seed(row: &rusqlite::Row<'_>) -> rusqlite::Result<DispatchSeedRow> {
    Ok(DispatchSeedRow {
        dispatch_id: row.get(0)?,
        state: row.get(1)?,
        signal_json: row.get(2)?,
        paper_outcome: row.get(3)?,
        source_trade_id: row.get(4)?,
        created_at_unix: row.get(5)?,
        finalized_at_unix: row.get(6)?,
    })
}

/// Flip a staged dispatch seed `pending_paper → ready` inside the caller's transaction
/// (#508 Decision 10). `false` = the seed was already `ready` (an idempotent redelivery
/// re-commit); a missing seed is an invariant breach — the flip may only ever target an
/// existing staged seed (recovery never reconstructs dispatch state).
fn tx_flip_dispatch_ready(
    tx: &Transaction<'_>,
    flip: DispatchFlip<'_>,
) -> Result<bool, PaperStateError> {
    let flipped = tx.execute(
        "UPDATE dispatch_seeds SET state = 'ready', paper_outcome = ?2
         WHERE dispatch_id = ?1 AND state = 'pending_paper'",
        params![flip.dispatch_id, flip.paper_outcome],
    )? == 1;
    if !flipped {
        let exists: Option<i64> = tx
            .query_row(
                "SELECT 1 FROM dispatch_seeds WHERE dispatch_id = ?1",
                params![flip.dispatch_id],
                |row| row.get(0),
            )
            .optional()?;
        if exists.is_none() {
            return Err(PaperStateError::Internal(format!(
                "dispatch flip targeted a seed that was never staged: {}",
                flip.dispatch_id
            )));
        }
    }
    Ok(flipped)
}

fn tx_record_no_copy_disposition(
    tx: &Transaction<'_>,
    source_trade_id: &SourceTradeId,
    d: &NoCopyDisposition,
) -> Result<(), PaperStateError> {
    tx.execute(
        "INSERT OR IGNORE INTO no_copy_dispositions
             (source_trade_id, provenance, age_secs, reason, recorded_at_unix)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            source_trade_id.0,
            d.provenance,
            d.age_secs,
            d.reason,
            d.recorded_at_unix
        ],
    )?;
    Ok(())
}

/// Close a bucket-applied continuation in the caller's terminal transaction.
fn tx_terminalize_pending(
    tx: &Transaction<'_>,
    source_trade_id: &SourceTradeId,
    terminal_disposition: &str,
    pending: Option<PendingTerminalEvidence<'_>>,
) -> Result<(), PaperStateError> {
    let current: Option<(String, String, Option<String>)> = tx
        .query_row(
            "SELECT state, post_commit_inputs_json, terminal_disposition \
             FROM decision_pending WHERE source_trade_id = ?1",
            params![source_trade_id.0],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let Some((state, _durable_json, _durable_disposition)) = current else {
        return Ok(());
    };
    if state == "terminal" {
        // `dispatch_staged` is a legitimate first terminal boundary. The later paper
        // outcome still commits through this transaction, but must not rewrite that
        // already-durable terminal evidence. Explicit offline-close retries retain the
        // stricter same-value check in `close_decision_pending`.
        return Ok(());
    }
    let evidence = pending.ok_or_else(|| {
        PaperStateError::Internal(format!(
            "terminal transition for open decision_pending {} omitted post-boundary evidence",
            source_trade_id.0
        ))
    })?;
    serde_json::from_str::<serde_json::Value>(evidence.post_commit_inputs_json)?;
    tx.execute(
        "UPDATE decision_pending SET state = 'terminal', \
             post_commit_inputs_json = ?2, terminal_disposition = ?3, updated_at_unix = ?4 \
         WHERE source_trade_id = ?1 AND state = 'open'",
        params![
            source_trade_id.0,
            evidence.post_commit_inputs_json,
            terminal_disposition,
            evidence.updated_at_unix
        ],
    )?;
    Ok(())
}

fn tx_mark_seen(
    tx: &Transaction<'_>,
    source_trade_id: &SourceTradeId,
    transaction_hash: Option<&str>,
) -> Result<(), PaperStateError> {
    let identity_version = match source_trade_id.identity_version() {
        SourceTradeIdentityVersion::TransactionHashV1 => 1_i64,
        SourceTradeIdentityVersion::ReconciledGroupV2 => 2_i64,
    };
    tx.execute(
        "INSERT OR IGNORE INTO seen_trades \
             (source_trade_id, identity_version, transaction_hash) VALUES (?1, ?2, ?3)",
        params![source_trade_id.0, identity_version, transaction_hash],
    )?;
    Ok(())
}

fn tx_upsert_leader(
    tx: &Transaction<'_>,
    leader: &LeaderPositionRow,
) -> Result<(), PaperStateError> {
    tx.execute(
        "INSERT INTO leader_positions \
            (wallet_hex, market_id, outcome_id, long_amount_str, short_amount_str) \
         VALUES (?1, ?2, ?3, ?4, ?5) \
         ON CONFLICT(wallet_hex, market_id, outcome_id) DO UPDATE SET \
            long_amount_str = excluded.long_amount_str, \
            short_amount_str = excluded.short_amount_str",
        params![
            leader.wallet.to_string(),
            leader.market_id.to_string(),
            i64::from(leader.outcome_id.0),
            leader.long_contracts.to_decimal().to_string(),
            leader.short_contracts.to_decimal().to_string(),
        ],
    )?;
    Ok(())
}

/// `INSERT OR IGNORE` a fill; returns `true` if a new row was inserted.
fn tx_record_fill(
    tx: &Transaction<'_>,
    fill: &FillRecord,
    fill_seq: EventSeq,
) -> Result<bool, PaperStateError> {
    let affected = tx.execute(
        "INSERT OR IGNORE INTO fills \
            (idempotency_key, market_id, outcome_id, side, contracts, fill_price_str, event_seq) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            fill.idempotency_key,
            fill.market_id.to_string(),
            i64::from(fill.outcome_id.0),
            side_str(fill.side),
            to_i64(fill.contracts)?,
            fill.fill_price.0.to_string(),
            to_i64(fill_seq.0)?,
        ],
    )?;
    Ok(affected > 0)
}

/// Apply a fill to our own net position for its `(market, outcome)`.
fn tx_apply_our_position(tx: &Transaction<'_>, fill: &FillRecord) -> Result<(), PaperStateError> {
    let market = fill.market_id.to_string();
    let outcome = i64::from(fill.outcome_id.0);
    let current: Option<(i64, i64)> = tx
        .query_row(
            "SELECT long_contracts, short_contracts FROM positions \
             WHERE market_id = ?1 AND outcome_id = ?2",
            params![market, outcome],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let (long, short) = match current {
        Some((l, s)) => (parse_u64(l)?, parse_u64(s)?),
        None => (0, 0),
    };
    let (new_long, new_short) = apply_fill_to_net(long, short, fill.side, fill.contracts);
    tx.execute(
        "INSERT INTO positions (market_id, outcome_id, long_contracts, short_contracts) \
         VALUES (?1, ?2, ?3, ?4) \
         ON CONFLICT(market_id, outcome_id) DO UPDATE SET \
            long_contracts = excluded.long_contracts, \
            short_contracts = excluded.short_contracts",
        params![market, outcome, to_i64(new_long)?, to_i64(new_short)?],
    )?;
    Ok(())
}

/// Apply a fill's cash flow to the bankroll and persist it. BUY debits
/// `fill_price × contracts` (clamped at zero so the bankroll never goes negative);
/// SELL credits the same proceeds. Returns the new bankroll.
fn tx_apply_bankroll(tx: &Transaction<'_>, fill: &FillRecord) -> Result<Decimal, PaperStateError> {
    let current = tx_read_bankroll(tx)?;
    let qty = Decimal::from(fill.contracts);
    let notional = fill
        .fill_price
        .0
        .checked_mul(qty)
        .ok_or_else(|| PaperStateError::Internal("fill notional overflow".to_string()))?;
    let new = match fill.side {
        Side::Buy => current
            .checked_sub(notional)
            .ok_or_else(|| PaperStateError::Internal("bankroll debit overflow".to_string()))?
            .max(Decimal::ZERO),
        Side::Sell => current
            .checked_add(notional)
            .ok_or_else(|| PaperStateError::Internal("bankroll credit overflow".to_string()))?,
    };
    tx.execute(
        "INSERT INTO bankroll (id, bankroll_str) VALUES (?1, ?2) \
         ON CONFLICT(id) DO UPDATE SET bankroll_str = excluded.bankroll_str",
        params![BANKROLL_ROW_ID, new.to_string()],
    )?;
    Ok(new)
}

fn tx_read_bankroll(tx: &Transaction<'_>) -> Result<Decimal, PaperStateError> {
    let raw: Option<String> = tx
        .query_row(
            "SELECT bankroll_str FROM bankroll WHERE id = ?1",
            params![BANKROLL_ROW_ID],
            |row| row.get(0),
        )
        .optional()?;
    match raw {
        Some(s) => parse_decimal(&s),
        None => Ok(Decimal::ZERO),
    }
}

/// `last_applied = max(last_applied, seq)` — monotone disposition advance (#511):
/// canonical/refused dispositions must never regress the replay cursor.
fn tx_set_last_applied_max(tx: &Transaction<'_>, seq: EventSeq) -> Result<(), PaperStateError> {
    let current = tx_last_applied(tx)?.unwrap_or(0);
    let target = seq.0.max(current);
    tx.execute(
        "INSERT INTO meta (key, value) VALUES (?1, ?2) \
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        params![META_LAST_APPLIED_EVENT_SEQ, to_i64(target)?],
    )?;
    Ok(())
}

/// SET the bankroll to an authority-provided canonical value (#511) — never derived.
fn tx_set_bankroll(tx: &Transaction<'_>, bankroll: Decimal) -> Result<(), PaperStateError> {
    tx.execute(
        "INSERT INTO bankroll (id, bankroll_str) VALUES (?1, ?2) \
         ON CONFLICT(id) DO UPDATE SET bankroll_str = excluded.bankroll_str",
        params![BANKROLL_ROW_ID, bankroll.to_string()],
    )?;
    Ok(())
}

/// `true` when `market_id` is in `settled_markets` — the #511 fill-refusal guard,
/// transaction-scoped so the single-connection mutex serializes it with settles.
fn tx_is_settled(tx: &Transaction<'_>, market_id: &MarketId) -> Result<bool, PaperStateError> {
    let found: Option<i64> = tx
        .query_row(
            "SELECT 1 FROM settled_markets WHERE market_id = ?1",
            params![market_id.to_string()],
            |row| row.get(0),
        )
        .optional()?;
    Ok(found.is_some())
}

/// The market's position rows as of this transaction (#511 in-txn resolution credit).
fn tx_positions_for_market(
    tx: &Transaction<'_>,
    market_id: &MarketId,
) -> Result<Vec<PaperPositionRow>, PaperStateError> {
    let mut statement = tx.prepare(
        "SELECT market_id, outcome_id, long_contracts, short_contracts \
         FROM positions WHERE market_id = ?1",
    )?;
    let rows = statement.query_map(params![market_id.to_string()], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, i64>(3)?,
        ))
    })?;
    let mut out = Vec::new();
    for row in rows {
        let (market, outcome, long, short) = row?;
        out.push(PaperPositionRow {
            market_id: MarketId(pe_core_types::VenueMarketId(market)),
            outcome_id: OutcomeId(
                u16::try_from(outcome)
                    .map_err(|_| PaperStateError::Internal(format!("outcome_id {outcome}")))?,
            ),
            long_contracts: parse_u64(long)?,
            short_contracts: parse_u64(short)?,
        });
    }
    Ok(out)
}

fn tx_set_last_applied(tx: &Transaction<'_>, seq: EventSeq) -> Result<(), PaperStateError> {
    tx.execute(
        "INSERT INTO meta (key, value) VALUES (?1, ?2) \
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        params![META_LAST_APPLIED_EVENT_SEQ, to_i64(seq.0)?],
    )?;
    Ok(())
}

/// `None` when no fill has been mirrored yet; `Some(seq)` for the highest applied seq.
fn tx_last_applied(tx: &Transaction<'_>) -> Result<Option<u64>, PaperStateError> {
    let raw: Option<i64> = tx
        .query_row(
            "SELECT value FROM meta WHERE key = ?1",
            params![META_LAST_APPLIED_EVENT_SEQ],
            |row| row.get(0),
        )
        .optional()?;
    raw.map(parse_u64).transpose()
}

// ── Connection-scoped read helpers ──────────────────────────────────────────

fn read_bankroll(conn: &Connection) -> Result<Decimal, PaperStateError> {
    let raw: Option<String> = conn
        .query_row(
            "SELECT bankroll_str FROM bankroll WHERE id = ?1",
            params![BANKROLL_ROW_ID],
            |row| row.get(0),
        )
        .optional()?;
    match raw {
        Some(s) => parse_decimal(&s),
        None => Ok(Decimal::ZERO),
    }
}

fn read_last_applied(conn: &Connection) -> Result<Option<u64>, PaperStateError> {
    let raw: Option<i64> = conn
        .query_row(
            "SELECT value FROM meta WHERE key = ?1",
            params![META_LAST_APPLIED_EVENT_SEQ],
            |row| row.get(0),
        )
        .optional()?;
    raw.map(parse_u64).transpose()
}

// ── Pure helpers ────────────────────────────────────────────────────────────

/// Net-position update, identical to `PositionLedger::ingest`: a BUY covers shorts
/// first then adds to long; a SELL trims longs first then adds to short.
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

fn side_str(side: Side) -> &'static str {
    match side {
        Side::Buy => "buy",
        Side::Sell => "sell",
    }
}

fn parse_side(s: &str) -> Result<Side, PaperStateError> {
    match s {
        "buy" => Ok(Side::Buy),
        "sell" => Ok(Side::Sell),
        other => Err(PaperStateError::Corrupt(format!("bad side {other:?}"))),
    }
}

fn to_i64(v: u64) -> Result<i64, PaperStateError> {
    i64::try_from(v).map_err(|_| PaperStateError::Internal(format!("u64 {v} exceeds i64::MAX")))
}

fn parse_u64(v: i64) -> Result<u64, PaperStateError> {
    u64::try_from(v).map_err(|_| PaperStateError::Corrupt(format!("negative count {v}")))
}

fn parse_u16(v: i64) -> Result<u16, PaperStateError> {
    u16::try_from(v).map_err(|_| PaperStateError::Corrupt(format!("outcome_id {v} out of range")))
}

fn parse_bool_flag(value: i64, field: &str) -> Result<bool, PaperStateError> {
    match value {
        0 => Ok(false),
        1 => Ok(true),
        other => Err(PaperStateError::Corrupt(format!(
            "bad {field} flag {other}"
        ))),
    }
}

fn parse_decimal(s: &str) -> Result<Decimal, PaperStateError> {
    Decimal::from_str(s).map_err(|_| PaperStateError::Corrupt(format!("bad decimal {s:?}")))
}

fn parse_share_amount(s: &str) -> Result<ShareAmount, PaperStateError> {
    let decimal = parse_decimal(s)?;
    ShareAmount::from_decimal_exact(decimal)
        .map_err(|error| PaperStateError::Corrupt(format!("bad share amount {s:?}: {error}")))
}

fn parse_wallet(s: &str) -> Result<WalletAddress, PaperStateError> {
    WalletAddress::from_hex(s).map_err(|_| PaperStateError::Corrupt(format!("bad wallet {s:?}")))
}

fn parse_pending_state(s: &str) -> Result<DecisionPendingState, PaperStateError> {
    match s {
        "open" => Ok(DecisionPendingState::Open),
        "terminal" => Ok(DecisionPendingState::Terminal),
        other => Err(PaperStateError::Corrupt(format!(
            "bad decision_pending state {other:?}"
        ))),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn db() -> (tempfile::TempDir, PaperStateDb) {
        let dir = tempfile::tempdir().unwrap();
        let db = PaperStateDb::open(&dir.path().join("paper_state.db")).unwrap();
        (dir, db)
    }

    fn wallet() -> WalletAddress {
        WalletAddress::from_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap()
    }

    fn market() -> MarketId {
        MarketId(VenueMarketId("0xmarket1".to_string()))
    }

    fn other_wallet() -> WalletAddress {
        WalletAddress::from_hex("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap()
    }

    fn named_market(name: &str) -> MarketId {
        MarketId(VenueMarketId(name.to_owned()))
    }

    fn anchor_install(
        wallet: WalletAddress,
        balances: Vec<(MarketId, OutcomeId, ShareAmount)>,
        activity_cutoff_unix: i64,
        anchored_at_unix: i64,
        ledger_hash_after: &str,
    ) -> AnchorInstallRecord {
        AnchorInstallRecord {
            wallet,
            balances,
            activity_cutoff_unix,
            anchored_at_unix,
            ledger_hash_after: ledger_hash_after.to_owned(),
            positions_proof_hash: format!("positions-{ledger_hash_after}"),
            activity_bounds_json: format!("{{\"cutoff\":{activity_cutoff_unix}}}"),
            source_log_generation: format!("generation-{ledger_hash_after}"),
            proof_json: format!("{{\"anchor\":\"{ledger_hash_after}\"}}"),
            recorded_at_unix: anchored_at_unix,
        }
    }

    fn leader_projection(db: &PaperStateDb) -> Vec<(String, String, u16, u64, u64)> {
        let mut rows = db
            .leader_positions()
            .unwrap()
            .into_iter()
            .map(|row| {
                (
                    row.wallet.to_string(),
                    row.market_id.to_string(),
                    row.outcome_id.0,
                    row.long_contracts.atomic(),
                    row.short_contracts.atomic(),
                )
            })
            .collect::<Vec<_>>();
        rows.sort();
        rows
    }

    fn group_id(suffix: char) -> SourceTradeId {
        SourceTradeId(format!("g2:{}", suffix.to_string().repeat(64)))
    }

    fn record_activity_group(
        db: &PaperStateDb,
        wallet: WalletAddress,
        source_epoch: i64,
        suffix: char,
        proof_json: &str,
    ) {
        db.commit_activity_bucket(&ActivityBucketCommit {
            wallet,
            source_epoch,
            dispositions: vec![ActivityDispositionRecord {
                source_trade_id: group_id(suffix),
                transaction_hash: format!("transaction-{suffix}"),
                wallet,
                source_epoch,
                semantic_revision: format!("revision-{suffix}"),
                activity_type: "TRADE".to_owned(),
                disposition: "applied".to_owned(),
                proof_json: proof_json.to_owned(),
                no_copy: None,
            }],
            leader_positions: Vec::new(),
            gate_results: Vec::new(),
            history_effects: Vec::new(),
            history_status: None,
            pending: Vec::new(),
            fence: None,
            reanchor: None,
            advance_cursor: false,
        })
        .unwrap();
    }

    fn leader(long: u64, short: u64) -> LeaderPositionRow {
        LeaderPositionRow {
            wallet: wallet(),
            market_id: market(),
            outcome_id: OutcomeId(0),
            long_contracts: ShareAmount::from_whole(long).unwrap(),
            short_contracts: ShareAmount::from_whole(short).unwrap(),
        }
    }

    fn fill(key: &str, side: Side, contracts: u64, price: Decimal) -> FillRecord {
        FillRecord {
            idempotency_key: key.to_string(),
            market_id: market(),
            outcome_id: OutcomeId(0),
            side,
            contracts,
            fill_price: Price(price),
        }
    }

    #[test]
    fn fresh_db_stamps_schema_version() {
        let (dir, _db) = db();
        let conn = Connection::open(dir.path().join("paper_state.db")).unwrap();
        let v: i64 = conn
            .pragma_query_value(None, "user_version", |r| r.get(0))
            .unwrap();
        assert_eq!(v, SCHEMA_VERSION);
    }

    #[test]
    fn mismatched_schema_version_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("paper_state.db");
        {
            let db = PaperStateDb::open(&path).unwrap();
            drop(db);
        }
        // Tamper with the on-disk version.
        let conn = Connection::open(&path).unwrap();
        conn.pragma_update(None, "user_version", 999_i64).unwrap();
        drop(conn);
        let result = PaperStateDb::open(&path);
        assert!(matches!(
            result,
            Err(PaperStateError::SchemaVersionMismatch {
                found: 999,
                expected: SCHEMA_VERSION
            })
        ));
    }

    #[test]
    fn pre_anchor_schema_reopens_with_guarded_columns_and_table_once() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("paper_state.db");
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(
                "PRAGMA user_version = 2;
                 CREATE TABLE poll_cursors (
                     wallet_hex TEXT PRIMARY KEY NOT NULL,
                     last_ts_unix INTEGER NOT NULL,
                     last_activity_unix INTEGER
                 );
                 INSERT INTO poll_cursors (wallet_hex, last_ts_unix, last_activity_unix)
                 VALUES ('0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 42, 43);",
            )
            .unwrap();
        }

        for reopen in 0..2 {
            let db = PaperStateDb::open(&path).unwrap();
            assert_eq!(
                db.wallet_coverage(&wallet()).unwrap(),
                WalletCoverage {
                    activity_cutoff_unix: None,
                    coverage_generation: 0,
                    reanchor_required: false,
                    anchor_seq: None,
                    anchored_at_unix: None,
                }
            );
            assert_eq!(db.oldest_anchor_age(&[wallet()], 100).unwrap(), None);
            drop(db);

            let conn = Connection::open(&path).unwrap();
            let added_columns: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM pragma_table_info('poll_cursors')
                     WHERE name IN ('activity_cutoff_unix', 'coverage_generation',
                                    'reanchor_required')",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(added_columns, 3, "reopen {reopen}");
            let anchor_tables: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master
                     WHERE type = 'table' AND name = 'position_anchors'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(anchor_tables, 1, "reopen {reopen}");
        }
    }

    #[test]
    fn install_anchors_batch_replaces_mirror_validation_and_coverage() {
        let (_dir, db) = db();
        let first = wallet();
        let second = other_wallet();
        db.seed_cursors_if_absent(&[(first, 10), (second, 20)])
            .unwrap();
        let market_a = named_market("0xanchor-a");
        let market_b = named_market("0xanchor-b");
        let market_c = named_market("0xanchor-c");
        let first_install = anchor_install(
            first,
            vec![
                (market_b.clone(), OutcomeId(1), ShareAmount::from_atomic(8)),
                (market_a.clone(), OutcomeId(0), ShareAmount::from_atomic(7)),
            ],
            100,
            200,
            "ledger-first",
        );
        let second_install = anchor_install(
            second,
            vec![(market_c.clone(), OutcomeId(2), ShareAmount::from_atomic(9))],
            110,
            220,
            "ledger-second",
        );
        db.install_anchors(&[first_install.clone(), second_install.clone()])
            .unwrap();

        let first_anchors = db.position_anchors(&first).unwrap();
        assert_eq!(first_anchors.len(), 1);
        assert_eq!(first_anchors[0].anchor_seq, 0);
        assert_eq!(first_anchors[0].ledger_hash_after, "ledger-first");
        let canonical_balances: Vec<(String, u16, ShareAmount)> =
            serde_json::from_str(&first_anchors[0].balances_json).unwrap();
        assert_eq!(
            canonical_balances,
            vec![
                ("0xanchor-a".to_owned(), 0, ShareAmount::from_atomic(7)),
                ("0xanchor-b".to_owned(), 1, ShareAmount::from_atomic(8)),
            ]
        );
        assert_eq!(db.position_anchors(&second).unwrap()[0].anchor_seq, 0);
        assert_eq!(
            db.position_validation(&first).unwrap().unwrap(),
            PositionValidationRecord {
                wallet: first,
                ledger_hash: first_install.ledger_hash_after.clone(),
                positions_proof_hash: first_install.positions_proof_hash.clone(),
                activity_bounds_json: first_install.activity_bounds_json.clone(),
                source_log_generation: first_install.source_log_generation.clone(),
                proof_json: first_install.proof_json.clone(),
                recorded_at_unix: first_install.recorded_at_unix,
            }
        );
        assert_eq!(
            db.wallet_coverage(&first).unwrap(),
            WalletCoverage {
                activity_cutoff_unix: Some(100),
                coverage_generation: 0,
                reanchor_required: false,
                anchor_seq: Some(0),
                anchored_at_unix: Some(200),
            }
        );
        assert_eq!(
            db.oldest_anchor_age(&[first, second], 250).unwrap(),
            Some(50)
        );
        assert_eq!(db.cursor(&first).unwrap(), Some(10));
        assert_eq!(db.activity(&first).unwrap(), Some(10));

        db.install_anchors(&[anchor_install(
            first,
            vec![(market_b.clone(), OutcomeId(1), ShareAmount::from_atomic(12))],
            120,
            300,
            "ledger-first-next",
        )])
        .unwrap();
        let first_anchors = db.position_anchors(&first).unwrap();
        assert_eq!(
            first_anchors
                .iter()
                .map(|anchor| anchor.anchor_seq)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
        assert_eq!(
            leader_projection(&db)
                .into_iter()
                .filter(|row| row.0 == first.to_string())
                .collect::<Vec<_>>(),
            vec![(first.to_string(), market_b.to_string(), 1, 12, 0,)],
            "balances absent from the new anchor are deleted"
        );
        assert_eq!(db.cursor(&first).unwrap(), Some(10));
        assert_eq!(db.activity(&first).unwrap(), Some(10));
        assert_eq!(
            db.oldest_anchor_age(&[first, second], 350).unwrap(),
            Some(130),
            "age uses each wallet's latest anchor"
        );
    }

    #[test]
    fn injected_anchor_failure_before_commit_rolls_back_all_four_surfaces() {
        let (_dir, db) = db();
        let first = wallet();
        let second = other_wallet();
        db.seed_cursors_if_absent(&[(first, 10), (second, 20)])
            .unwrap();
        db.install_anchors(&[
            anchor_install(
                first,
                vec![(market(), OutcomeId(0), ShareAmount::from_atomic(1))],
                100,
                200,
                "baseline-first",
            ),
            anchor_install(
                second,
                vec![(
                    named_market("0xbaseline-second"),
                    OutcomeId(1),
                    ShareAmount::from_atomic(2),
                )],
                100,
                210,
                "baseline-second",
            ),
        ])
        .unwrap();
        let before_anchors = (
            db.position_anchors(&first).unwrap(),
            db.position_anchors(&second).unwrap(),
        );
        let before_mirror = leader_projection(&db);
        let before_validations = (
            db.position_validation(&first).unwrap(),
            db.position_validation(&second).unwrap(),
        );
        let before_coverage = (
            db.wallet_coverage(&first).unwrap(),
            db.wallet_coverage(&second).unwrap(),
        );

        let result = db.install_anchors_inner(
            &[
                anchor_install(
                    first,
                    vec![(
                        named_market("0xfailing-first"),
                        OutcomeId(0),
                        ShareAmount::from_atomic(3),
                    )],
                    120,
                    300,
                    "failing-first",
                ),
                anchor_install(
                    second,
                    vec![(
                        named_market("0xfailing-second"),
                        OutcomeId(1),
                        ShareAmount::from_atomic(4),
                    )],
                    120,
                    310,
                    "failing-second",
                ),
            ],
            true,
        );
        assert!(matches!(result, Err(PaperStateError::Internal(_))));
        assert_eq!(
            (
                db.position_anchors(&first).unwrap(),
                db.position_anchors(&second).unwrap(),
            ),
            before_anchors
        );
        assert_eq!(leader_projection(&db), before_mirror);
        assert_eq!(
            (
                db.position_validation(&first).unwrap(),
                db.position_validation(&second).unwrap(),
            ),
            before_validations
        );
        assert_eq!(
            (
                db.wallet_coverage(&first).unwrap(),
                db.wallet_coverage(&second).unwrap(),
            ),
            before_coverage
        );
    }

    #[test]
    fn cutoff_regression_rolls_back_the_whole_anchor_batch() {
        let (_dir, db) = db();
        let first = wallet();
        let second = other_wallet();
        db.seed_cursors_if_absent(&[(first, 10), (second, 20)])
            .unwrap();
        db.install_anchors(&[
            anchor_install(first, Vec::new(), 100, 200, "baseline-first"),
            anchor_install(second, Vec::new(), 100, 210, "baseline-second"),
        ])
        .unwrap();
        let before_first = db.position_anchors(&first).unwrap();
        let before_second = db.position_anchors(&second).unwrap();
        let before_validations = (
            db.position_validation(&first).unwrap(),
            db.position_validation(&second).unwrap(),
        );

        let result = db.install_anchors(&[
            anchor_install(
                first,
                vec![(market(), OutcomeId(0), ShareAmount::from_atomic(5))],
                120,
                300,
                "candidate-first",
            ),
            anchor_install(second, Vec::new(), 99, 310, "candidate-second"),
        ]);
        assert!(matches!(
            result,
            Err(PaperStateError::AnchorCutoffRegression {
                wallet: regressed,
                stored_cutoff_unix: 100,
                candidate_cutoff_unix: 99,
            }) if regressed == second
        ));
        assert_eq!(db.position_anchors(&first).unwrap(), before_first);
        assert_eq!(db.position_anchors(&second).unwrap(), before_second);
        assert_eq!(
            (
                db.position_validation(&first).unwrap(),
                db.position_validation(&second).unwrap(),
            ),
            before_validations
        );
        assert!(leader_projection(&db).is_empty());
        assert_eq!(
            db.wallet_coverage(&first).unwrap().activity_cutoff_unix,
            Some(100)
        );
        assert_eq!(
            db.wallet_coverage(&second).unwrap().activity_cutoff_unix,
            Some(100)
        );
    }

    #[test]
    fn reanchor_group_failure_rolls_back_and_retry_repairs_coverage_atomically() {
        let (dir, db) = db();
        let wallet = wallet();
        db.seed_cursor_if_absent(&wallet, 10).unwrap();
        db.install_anchors(&[anchor_install(
            wallet,
            vec![(market(), OutcomeId(0), ShareAmount::from_atomic(1))],
            100,
            200,
            "baseline",
        )])
        .unwrap();
        assert!(db.position_validation(&wallet).unwrap().is_some());

        let bucket = ActivityBucketCommit {
            wallet,
            source_epoch: 101,
            dispositions: vec![ActivityDispositionRecord {
                source_trade_id: group_id('d'),
                transaction_hash: "transaction-d".to_owned(),
                wallet,
                source_epoch: 101,
                semantic_revision: "revision-d".to_owned(),
                activity_type: "TRADE".to_owned(),
                disposition: "anchor_covered_late".to_owned(),
                proof_json: "{}".to_owned(),
                no_copy: None,
            }],
            leader_positions: Vec::new(),
            gate_results: Vec::new(),
            history_effects: Vec::new(),
            history_status: None,
            pending: Vec::new(),
            fence: None,
            reanchor: Some(ReanchorRecord {
                source_trade_id: group_id('d'),
                reason: "anchor_covered_late".to_owned(),
            }),
            advance_cursor: true,
        };
        let trigger = Connection::open(dir.path().join("paper_state.db")).unwrap();
        trigger
            .execute_batch(
                "CREATE TRIGGER fail_reanchor_update
                 BEFORE UPDATE OF reanchor_required ON poll_cursors
                 BEGIN SELECT RAISE(FAIL, 'injected reanchor failure'); END;",
            )
            .unwrap();
        assert!(matches!(
            db.commit_activity_bucket(&bucket),
            Err(PaperStateError::Sqlite(_))
        ));
        assert!(db.activity_group_state(&group_id('d')).unwrap().is_none());
        assert_eq!(db.wallet_coverage(&wallet).unwrap().coverage_generation, 0);
        assert!(!db.wallet_coverage(&wallet).unwrap().reanchor_required);
        assert!(db.position_validation(&wallet).unwrap().is_some());

        trigger
            .execute_batch("DROP TRIGGER fail_reanchor_update;")
            .unwrap();
        db.commit_activity_bucket(&bucket).unwrap();
        assert_eq!(
            db.wallet_coverage(&wallet).unwrap(),
            WalletCoverage {
                activity_cutoff_unix: Some(100),
                coverage_generation: 1,
                reanchor_required: true,
                anchor_seq: Some(0),
                anchored_at_unix: Some(200),
            }
        );
        assert!(db.position_validation(&wallet).unwrap().is_none());

        db.commit_activity_bucket(&bucket).unwrap();
        assert_eq!(db.wallet_coverage(&wallet).unwrap().coverage_generation, 1);

        db.install_anchors(&[anchor_install(
            wallet,
            vec![(market(), OutcomeId(0), ShareAmount::from_atomic(2))],
            120,
            300,
            "refreshed",
        )])
        .unwrap();
        let coverage = db.wallet_coverage(&wallet).unwrap();
        assert_eq!(coverage.coverage_generation, 1);
        assert!(!coverage.reanchor_required);
        assert_eq!(coverage.anchor_seq, Some(1));
        assert!(db.position_validation(&wallet).unwrap().is_some());
    }

    #[test]
    fn activity_groups_after_is_cutoff_exclusive_and_causally_ordered() {
        let (_dir, db) = db();
        let wallet = wallet();
        let proof_b = "{\"version\":1,\"effect\":{\"Trade\":{\"amount\":\"2\"}}}";
        let proof_a = "{\"version\":1,\"effect\":{\"Trade\":{\"amount\":\"1\"}}}";
        record_activity_group(&db, wallet, 101, 'b', proof_b);
        record_activity_group(
            &db,
            wallet,
            100,
            'c',
            "{\"version\":1,\"effect\":\"RawOnly\"}",
        );
        record_activity_group(&db, wallet, 101, 'a', proof_a);

        let groups = db.activity_groups_after(&wallet, 100).unwrap();
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].source_trade_id, group_id('a'));
        assert_eq!(groups[1].source_trade_id, group_id('b'));
        assert!(groups.iter().all(|group| group.source_epoch == 101));
        assert_eq!(groups[0].proof_json, proof_a);
        assert_eq!(groups[1].proof_json, proof_b);
        assert!(db.activity_groups_after(&wallet, 101).unwrap().is_empty());
    }

    #[test]
    fn pending_checkpoint_is_durable_while_open_and_cannot_rewrite_terminal_evidence() {
        let (_dir, db) = db();
        let source_trade_id = SourceTradeId("g2:checkpoint".to_owned());
        {
            let conn = db.lock();
            conn.execute(
                "INSERT INTO decision_pending \
                     (source_trade_id, semantic_revision, wallet_hex, source_epoch, \
                      frozen_inputs_json, post_commit_inputs_json, state, terminal_disposition, \
                      updated_at_unix) \
                 VALUES (?1, 'revision', ?2, 1, '{}', '[]', 'open', NULL, 1)",
                params![source_trade_id.0, wallet().to_string()],
            )
            .unwrap();
        }

        db.checkpoint_decision_pending(&source_trade_id, "{\"checkpoint\":1}", 2)
            .unwrap();
        let open = db.open_decision_pending().unwrap().remove(0);
        assert_eq!(open.post_commit_inputs_json, "{\"checkpoint\":1}");
        assert_eq!(open.updated_at_unix, 2);

        db.close_decision_pending(&source_trade_id, "{\"terminal\":1}", "fill", 3)
            .unwrap();
        db.checkpoint_decision_pending(&source_trade_id, "{\"checkpoint\":2}", 4)
            .unwrap();
        let terminal = db.decision_pending_history().unwrap().remove(0);
        assert_eq!(terminal.post_commit_inputs_json, "{\"terminal\":1}");
        assert_eq!(terminal.terminal_disposition.as_deref(), Some("fill"));
        assert_eq!(terminal.updated_at_unix, 3);
    }

    #[test]
    fn commit_seen_no_fill_marks_seen() {
        let (_dir, db) = db();
        let id = SourceTradeId("tx1".to_string());
        assert!(!db.is_seen(&id).unwrap());
        db.commit_seen_no_fill(&id, &leader(5, 0)).unwrap();
        assert!(db.is_seen(&id).unwrap());
        // Leader position mirrored.
        let rows = db.leader_positions().unwrap();
        assert_eq!(rows, vec![leader(5, 0)]);
    }

    #[test]
    fn commit_fill_debits_bankroll_and_opens_position() {
        let (_dir, db) = db();
        db.init_bankroll(dec!(1000)).unwrap();
        let id = SourceTradeId("tx1".to_string());
        let new = db
            .commit_fill(
                &id,
                &leader(10, 0),
                &fill("k1", Side::Buy, 10, dec!(0.40)),
                EventSeq(7),
            )
            .unwrap();
        // 1000 - (0.40 * 10) = 996
        assert_eq!(new, FillCommitOutcome::Applied(dec!(996.0)));
        assert_eq!(db.bankroll().unwrap(), Some(dec!(996.0)));
        assert!(db.is_seen(&id).unwrap());
        assert_eq!(db.last_applied_event_seq().unwrap(), EventSeq(7));
        let pos = db.paper_positions().unwrap();
        assert_eq!(pos.len(), 1);
        assert_eq!(pos[0].long_contracts, 10);
        assert_eq!(pos[0].short_contracts, 0);
    }

    #[test]
    fn sell_credits_bankroll_and_trims_position() {
        let (_dir, db) = db();
        db.init_bankroll(dec!(100)).unwrap();
        db.commit_fill(
            &SourceTradeId("buy".to_string()),
            &leader(10, 0),
            &fill("k_buy", Side::Buy, 10, dec!(0.50)),
            EventSeq(1),
        )
        .unwrap();
        // 100 - 5 = 95, then sell 4 @ 0.60 credits 2.40 -> 97.40
        let new = db
            .commit_fill(
                &SourceTradeId("sell".to_string()),
                &leader(6, 0),
                &fill("k_sell", Side::Sell, 4, dec!(0.60)),
                EventSeq(2),
            )
            .unwrap();
        assert_eq!(new, FillCommitOutcome::Applied(dec!(97.40)));
        let pos = db.paper_positions().unwrap();
        assert_eq!(pos[0].long_contracts, 6);
    }

    #[test]
    fn bankroll_never_goes_negative() {
        let (_dir, db) = db();
        db.init_bankroll(dec!(10)).unwrap();
        // BUY notional 100 * 0.50 = 50 > 10 -> clamps to 0, not negative.
        let new = db
            .commit_fill(
                &SourceTradeId("tx".to_string()),
                &leader(100, 0),
                &fill("k", Side::Buy, 100, dec!(0.50)),
                EventSeq(1),
            )
            .unwrap();
        assert_eq!(new, FillCommitOutcome::Applied(Decimal::ZERO));
        assert_eq!(db.bankroll().unwrap(), Some(Decimal::ZERO));
    }

    #[test]
    fn duplicate_idempotency_key_does_not_double_apply() {
        let (_dir, db) = db();
        db.init_bankroll(dec!(1000)).unwrap();
        let f = fill("dup", Side::Buy, 10, dec!(0.50));
        db.commit_fill(
            &SourceTradeId("a".to_string()),
            &leader(10, 0),
            &f,
            EventSeq(1),
        )
        .unwrap();
        // Same idempotency key again (e.g. 1s-bucket collision): position/bankroll unchanged.
        let new = db
            .commit_fill(
                &SourceTradeId("b".to_string()),
                &leader(20, 0),
                &f,
                EventSeq(2),
            )
            .unwrap();
        assert_eq!(new, FillCommitOutcome::Applied(dec!(995.0)));
        assert_eq!(db.bankroll().unwrap(), Some(dec!(995.0)));
        let pos = db.paper_positions().unwrap();
        assert_eq!(pos[0].long_contracts, 10);
    }

    #[test]
    fn reconcile_replays_missing_fill_once() {
        let (_dir, db) = db();
        db.init_bankroll(dec!(1000)).unwrap();
        let f = fill("k", Side::Buy, 10, dec!(0.50));
        // Simulate crash: fill never committed, so last_applied stays 0.
        assert_eq!(db.last_applied_event_seq().unwrap(), EventSeq(0));
        assert!(db.reconcile_fill(&f, EventSeq(3)).unwrap());
        assert_eq!(db.bankroll().unwrap(), Some(dec!(995.0)));
        assert_eq!(db.last_applied_event_seq().unwrap(), EventSeq(3));
        // Replaying the same (or earlier) seq is a no-op.
        assert!(!db.reconcile_fill(&f, EventSeq(3)).unwrap());
        assert!(
            !db.reconcile_fill(&fill("k2", Side::Buy, 10, dec!(0.50)), EventSeq(2))
                .unwrap()
        );
        assert_eq!(db.bankroll().unwrap(), Some(dec!(995.0)));
    }

    #[test]
    fn committed_fill_is_not_reconciled_again() {
        let (_dir, db) = db();
        db.init_bankroll(dec!(1000)).unwrap();
        let f = fill("k", Side::Buy, 10, dec!(0.50));
        db.commit_fill(
            &SourceTradeId("a".to_string()),
            &leader(10, 0),
            &f,
            EventSeq(4),
        )
        .unwrap();
        // last_applied advanced to 4 by commit_fill, so reconcile finds nothing to do.
        assert!(!db.reconcile_fill(&f, EventSeq(4)).unwrap());
        assert_eq!(db.bankroll().unwrap(), Some(dec!(995.0)));
    }

    #[test]
    fn seed_if_absent_preserves_held_cursor_and_max_seeds_activity() {
        let (_dir, db) = db();
        let w = wallet();
        // Vacancy: seed lands as both cursor and activity.
        db.seed_cursor_if_absent(&w, 100).unwrap();
        assert_eq!(db.cursor(&w).unwrap(), Some(100));
        assert_eq!(db.activity(&w).unwrap(), Some(100));
        // A held cursor is NEVER jumped by a later (higher) seed — activity still MAXes.
        db.seed_cursor_if_absent(&w, 500).unwrap();
        assert_eq!(
            db.cursor(&w).unwrap(),
            Some(100),
            "delivery cursor preserved (#511)"
        );
        assert_eq!(db.activity(&w).unwrap(), Some(500), "activity MAX-seeded");
        // Activity is MAX-only and UPDATE-only (no row → no fabricated cursor).
        db.set_activity(&w, 400).unwrap();
        assert_eq!(db.activity(&w).unwrap(), Some(500));
        let other = WalletAddress::from_hex("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap();
        db.set_activity(&other, 999).unwrap();
        assert_eq!(
            db.cursor(&other).unwrap(),
            None,
            "no delivery cursor fabricated"
        );
        assert_eq!(db.activity(&other).unwrap(), None);
    }

    #[test]
    fn cursor_round_trips() {
        let (_dir, db) = db();
        let w = wallet();
        assert_eq!(db.cursor(&w).unwrap(), None);
        db.set_cursor(&w, 1_700_000_000).unwrap();
        assert_eq!(db.cursor(&w).unwrap(), Some(1_700_000_000));
        db.set_cursor(&w, 1_700_000_500).unwrap();
        assert_eq!(db.cursor(&w).unwrap(), Some(1_700_000_500));
        db.set_cursor(&w, 1_699_999_999).unwrap();
        assert_eq!(
            db.cursor(&w).unwrap(),
            Some(1_700_000_500),
            "a stale poll must not regress the activity cursor"
        );
    }

    #[test]
    fn cursor_batch_commits_every_wallet() {
        let (_dir, db) = db();
        let first = wallet();
        let mut bytes = first.0;
        bytes[19] = bytes[19].wrapping_add(1);
        let second = WalletAddress(bytes);
        db.set_cursors(&[(first, 1_700_000_001), (second, 1_700_000_002)])
            .unwrap();
        assert_eq!(db.cursor(&first).unwrap(), Some(1_700_000_001));
        assert_eq!(db.cursor(&second).unwrap(), Some(1_700_000_002));
        db.set_cursors(&[(first, 1_600_000_000), (second, 1_800_000_000)])
            .unwrap();
        assert_eq!(db.cursor(&first).unwrap(), Some(1_700_000_001));
        assert_eq!(db.cursor(&second).unwrap(), Some(1_800_000_000));
    }

    #[test]
    fn credit_bankroll_adds_to_balance() {
        let (_dir, db) = db();
        db.init_bankroll(dec!(100)).unwrap();
        let new = db.credit_bankroll(dec!(25)).unwrap();
        assert_eq!(new, dec!(125));
        assert_eq!(db.bankroll().unwrap(), Some(dec!(125)));
    }

    #[test]
    fn fills_count_increments_per_commit() {
        let (_dir, db) = db();
        db.init_bankroll(dec!(1000)).unwrap();
        assert_eq!(db.fills_count().unwrap(), 0);
        db.commit_fill(
            &SourceTradeId("a".to_string()),
            &leader(10, 0),
            &fill("k1", Side::Buy, 10, dec!(0.50)),
            EventSeq(1),
        )
        .unwrap();
        assert_eq!(db.fills_count().unwrap(), 1);
    }

    #[test]
    fn positions_and_settled_counts() {
        let (_dir, db) = db();
        db.init_bankroll(dec!(1000)).unwrap();
        assert_eq!(db.positions_count().unwrap(), 0);
        assert_eq!(db.settled_count().unwrap(), 0);
        db.commit_fill(
            &SourceTradeId("a".to_string()),
            &leader(10, 0),
            &fill("k1", Side::Buy, 10, dec!(0.50)),
            EventSeq(1),
        )
        .unwrap();
        // Open before settlement (#516: the count is open-only, not all-time rows)…
        assert_eq!(db.positions_count().unwrap(), 1);
        db.record_settled_market(&market(), "[\"1\",\"0\"]", dec!(5), 1_700_000_000)
            .unwrap();
        // …and no longer counted once its market settles, while the row itself is
        // preserved (cumulative table — the hold-gate/settlement loader still sees it).
        assert_eq!(db.positions_count().unwrap(), 0);
        assert_eq!(db.settled_count().unwrap(), 1);
        assert_eq!(db.paper_positions().unwrap().len(), 1);

        // A second, unsettled market stays counted; an unsettled zero-net row does not.
        db.upsert_position(
            &MarketId(VenueMarketId("0xopen".to_string())),
            OutcomeId(0),
            3,
            0,
        )
        .unwrap();
        db.upsert_position(
            &MarketId(VenueMarketId("0xflat".to_string())),
            OutcomeId(0),
            0,
            0,
        )
        .unwrap();
        assert_eq!(db.positions_count().unwrap(), 1);
        assert_eq!(db.paper_positions().unwrap().len(), 3);
    }

    #[test]
    fn init_bankroll_is_idempotent_across_runs() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("paper_state.db");
        {
            let db = PaperStateDb::open(&path).unwrap();
            assert_eq!(db.init_bankroll(dec!(5000)).unwrap(), dec!(5000));
            db.commit_fill(
                &SourceTradeId("a".to_string()),
                &leader(10, 0),
                &fill("k", Side::Buy, 10, dec!(0.50)),
                EventSeq(1),
            )
            .unwrap();
        }
        // Reopen: init must NOT reset the drawn-down bankroll.
        let db = PaperStateDb::open(&path).unwrap();
        assert_eq!(db.init_bankroll(dec!(5000)).unwrap(), dec!(4995.0));
    }

    #[test]
    fn settled_markets_round_trip_and_idempotent() {
        let (_dir, db) = db();
        assert!(db.list_settled_markets().unwrap().is_empty());

        db.record_settled_market(&market(), "[\"1\",\"0\"]", dec!(12.50), 1_700_000_000)
            .unwrap();
        let rows = db.list_settled_markets().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].market_id, market());
        assert_eq!(rows[0].outcome_prices_json, "[\"1\",\"0\"]");
        assert_eq!(rows[0].credit_applied, dec!(12.50));
        assert_eq!(rows[0].settled_at_unix, 1_700_000_000);

        // ON CONFLICT(market_id) DO NOTHING: a re-record never overwrites the first.
        db.record_settled_market(&market(), "[\"0\",\"1\"]", dec!(99), 1_700_000_999)
            .unwrap();
        let rows = db.list_settled_markets().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].outcome_prices_json, "[\"1\",\"0\"]");
        assert_eq!(rows[0].credit_applied, dec!(12.50));
    }

    #[test]
    fn settled_markets_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("paper_state.db");
        {
            let db = PaperStateDb::open(&path).unwrap();
            db.record_settled_market(&market(), "[\"1\",\"0\"]", dec!(5), 1_700_000_000)
                .unwrap();
        }
        // Additive table materialises on reopen; the row persists.
        let db = PaperStateDb::open(&path).unwrap();
        let rows = db.list_settled_markets().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].market_id, market());
    }

    #[test]
    fn set_bankroll_overwrites_unconditionally() {
        // Boot pull (issue #397): the authoritative value overwrites the local cache.
        let (_dir, db) = db();
        db.init_bankroll(dec!(1000)).unwrap();
        db.set_bankroll(dec!(4242.50)).unwrap();
        assert_eq!(db.bankroll().unwrap(), Some(dec!(4242.50)));
        // A second set overwrites again (unlike init_bankroll's insert-or-ignore).
        db.set_bankroll(dec!(7)).unwrap();
        assert_eq!(db.bankroll().unwrap(), Some(dec!(7)));
    }

    #[test]
    fn upsert_position_round_trips_and_overwrites() {
        let (_dir, db) = db();
        db.upsert_position(&market(), OutcomeId(0), 10, 3).unwrap();
        let pos = db.paper_positions().unwrap();
        assert_eq!(pos.len(), 1);
        assert_eq!(pos[0].long_contracts, 10);
        assert_eq!(pos[0].short_contracts, 3);
        // Same key overwrites (ON CONFLICT DO UPDATE).
        db.upsert_position(&market(), OutcomeId(0), 0, 5).unwrap();
        let pos = db.paper_positions().unwrap();
        assert_eq!(pos.len(), 1);
        assert_eq!(pos[0].long_contracts, 0);
        assert_eq!(pos[0].short_contracts, 5);
    }

    #[test]
    fn authoritative_replacement_deletes_stale_rows_atomically() {
        let (_dir, db) = db();
        db.set_bankroll(dec!(10)).unwrap();
        db.upsert_position(&market(), OutcomeId(0), 5, 0).unwrap();
        let replacement = PaperPositionRow {
            market_id: MarketId(VenueMarketId("0xfresh".to_owned())),
            outcome_id: OutcomeId(1),
            long_contracts: 7,
            short_contracts: 2,
        };

        db.replace_authoritative_state(dec!(42.5), std::slice::from_ref(&replacement))
            .unwrap();

        assert_eq!(db.bankroll().unwrap(), Some(dec!(42.5)));
        assert_eq!(db.paper_positions().unwrap(), vec![replacement]);
    }

    #[test]
    fn invalid_authoritative_replacement_leaves_prior_state_untouched() {
        let (_dir, db) = db();
        db.set_bankroll(dec!(10)).unwrap();
        db.upsert_position(&market(), OutcomeId(0), 5, 0).unwrap();
        let duplicate = PaperPositionRow {
            market_id: market(),
            outcome_id: OutcomeId(1),
            long_contracts: 7,
            short_contracts: 2,
        };
        let result = db.replace_authoritative_state(dec!(99), &[duplicate.clone(), duplicate]);

        assert!(matches!(
            result,
            Err(PaperStateError::DuplicateAuthoritativePosition { .. })
        ));
        assert_eq!(db.bankroll().unwrap(), Some(dec!(10)));
        assert_eq!(db.paper_positions().unwrap()[0].long_contracts, 5);
    }

    #[cfg(feature = "scenario")]
    #[test]
    fn mid_transaction_authoritative_replacement_failure_rolls_back_every_write() {
        let (_dir, db) = db();
        db.set_bankroll(dec!(10)).unwrap();
        db.upsert_position(&market(), OutcomeId(0), 5, 0).unwrap();
        let replacements = [
            PaperPositionRow {
                market_id: MarketId(VenueMarketId("0xfresh-1".to_owned())),
                outcome_id: OutcomeId(0),
                long_contracts: 7,
                short_contracts: 2,
            },
            PaperPositionRow {
                market_id: MarketId(VenueMarketId("0xfresh-2".to_owned())),
                outcome_id: OutcomeId(1),
                long_contracts: 8,
                short_contracts: 3,
            },
        ];

        assert!(
            db.replace_authoritative_state_failing_after(dec!(99), &replacements, 1)
                .is_err()
        );
        assert_eq!(db.bankroll().unwrap(), Some(dec!(10)));
        let prior = db.paper_positions().unwrap();
        assert_eq!(prior.len(), 1);
        assert_eq!(prior[0].market_id, market());
        assert_eq!(prior[0].long_contracts, 5);
    }

    #[test]
    fn settle_and_credit_is_idempotent_credit_once() {
        // Mirrors the apply_resolution RPC: the credit applies only on the first
        // (newly-inserted) settlement; a retry returns the existing bankroll, no double-credit.
        let (_dir, db) = db();
        db.init_bankroll(dec!(100)).unwrap();
        let b1 = db
            .settle_and_credit(&market(), "[\"1\",\"0\"]", dec!(25.50), 1_700_000_000)
            .unwrap();
        assert_eq!(b1, dec!(125.50));
        assert_eq!(db.bankroll().unwrap(), Some(dec!(125.50)));
        // Re-settle the same market: no credit, returns the current bankroll.
        let b2 = db
            .settle_and_credit(&market(), "[\"1\",\"0\"]", dec!(25.50), 1_700_000_999)
            .unwrap();
        assert_eq!(b2, dec!(125.50));
        assert_eq!(db.bankroll().unwrap(), Some(dec!(125.50)));
        // The first settlement row is preserved (DO NOTHING).
        let rows = db.list_settled_markets().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].settled_at_unix, 1_700_000_000);
    }

    #[test]
    fn supabase_watermark_round_trips_and_absent_is_none() {
        let (_dir, db) = db();
        // Absent row = None (include seq 0 in catch-up); Some(0) is a distinct state (#510).
        assert_eq!(db.last_supabase_applied_event_seq().unwrap(), None);
        db.set_supabase_applied_event_seq(EventSeq(0)).unwrap();
        assert_eq!(
            db.last_supabase_applied_event_seq().unwrap(),
            Some(EventSeq(0))
        );
        db.set_supabase_applied_event_seq(EventSeq(42)).unwrap();
        assert_eq!(
            db.last_supabase_applied_event_seq().unwrap(),
            Some(EventSeq(42))
        );
        // Independent of the local reconciliation cursor.
        assert_eq!(db.last_applied_event_seq().unwrap(), EventSeq(0));
    }

    fn snapshot(key: &str) -> FillMarketSnapshot {
        FillMarketSnapshot {
            idempotency_key: key.to_string(),
            liquidity: Some(dec!(6434.84)),
            volume: Some(dec!(120000)),
            absorbable_usd_100bps: Some(dec!(512.25)),
            ask_levels_json: Some("[[\"0.51\",\"100\"],[\"0.52\",\"900\"]]".to_string()),
            captured_at_unix: 1_700_000_000,
        }
    }

    #[test]
    fn fill_snapshot_round_trip() {
        let (_dir, db) = db();
        assert!(db.list_fill_snapshots().unwrap().is_empty());

        db.upsert_fill_market_snapshot(&snapshot("k1")).unwrap();
        let rows = db.list_fill_snapshots().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].idempotency_key, "k1");
        assert_eq!(rows[0].liquidity, Some(dec!(6434.84)));
        assert_eq!(rows[0].volume, Some(dec!(120000)));
        assert_eq!(rows[0].absorbable_usd_100bps, Some(dec!(512.25)));
        assert_eq!(
            rows[0].ask_levels_json.as_deref(),
            Some("[[\"0.51\",\"100\"],[\"0.52\",\"900\"]]")
        );
        assert_eq!(rows[0].captured_at_unix, 1_700_000_000);
    }

    #[test]
    fn fill_snapshot_partial_row_keeps_gamma_scalars_only() {
        // A `/book` failure: Gamma scalars present, CLOB-derived fields NULL.
        let (_dir, db) = db();
        let partial = FillMarketSnapshot {
            idempotency_key: "k2".to_string(),
            liquidity: Some(dec!(42)),
            volume: Some(dec!(7)),
            absorbable_usd_100bps: None,
            ask_levels_json: None,
            captured_at_unix: 1_700_000_111,
        };
        db.upsert_fill_market_snapshot(&partial).unwrap();
        let rows = db.list_fill_snapshots().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].liquidity, Some(dec!(42)));
        assert_eq!(rows[0].volume, Some(dec!(7)));
        assert_eq!(rows[0].absorbable_usd_100bps, None);
        assert_eq!(rows[0].ask_levels_json, None);
    }

    #[test]
    fn fill_snapshot_upsert_overwrites_partial_with_full() {
        // DO UPDATE: a later `/book`-success snapshot replaces the earlier partial row.
        let (_dir, db) = db();
        let partial = FillMarketSnapshot {
            idempotency_key: "k3".to_string(),
            liquidity: Some(dec!(1)),
            volume: None,
            absorbable_usd_100bps: None,
            ask_levels_json: None,
            captured_at_unix: 1,
        };
        db.upsert_fill_market_snapshot(&partial).unwrap();
        db.upsert_fill_market_snapshot(&snapshot("k3")).unwrap();
        let rows = db.list_fill_snapshots().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].liquidity, Some(dec!(6434.84)));
        assert_eq!(rows[0].absorbable_usd_100bps, Some(dec!(512.25)));
        assert_eq!(rows[0].captured_at_unix, 1_700_000_000);
    }

    #[test]
    fn fill_snapshot_does_not_require_a_fills_row() {
        // `fill_market_snapshots` declares no SQL foreign key (bundled SQLite enforces
        // FKs by default, so the column is left unconstrained), so a snapshot for a key
        // with no committed fill still inserts — the documentary mirror is best-effort.
        let (_dir, db) = db();
        db.upsert_fill_market_snapshot(&snapshot("orphan")).unwrap();
        assert_eq!(db.list_fill_snapshots().unwrap().len(), 1);
    }

    #[test]
    fn fill_snapshots_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("paper_state.db");
        {
            let db = PaperStateDb::open(&path).unwrap();
            db.upsert_fill_market_snapshot(&snapshot("k4")).unwrap();
        }
        // Additive table materialises on reopen; the row persists.
        let db = PaperStateDb::open(&path).unwrap();
        let rows = db.list_fill_snapshots().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].idempotency_key, "k4");
    }
    // ── Live dispatch aggregate (#508 Decision 10) ───────────────────────────

    fn seed(id: &str, targets: &[&str]) -> DispatchSeedRecord {
        DispatchSeedRecord {
            dispatch_id: id.to_string(),
            signal_json: format!("{{\"frozen\":\"{id}\"}}"),
            source_trade_id: format!("src-{id}"),
            created_at_unix: 1_000,
            targets: targets
                .iter()
                .map(|a| DispatchTargetSeed {
                    account_id: (*a).to_string(),
                    credential_bundle_version: 1,
                    credential_key_id: "key-1".to_string(),
                })
                .collect(),
        }
    }

    #[test]
    fn stage_is_idempotent_and_freezes_targets() {
        let (_dir, db) = db();
        assert!(
            db.stage_dispatch_seed(&seed("d1", &["primary", "partner"]))
                .unwrap()
        );
        // Redelivery with a DIFFERENT target list must reuse the frozen seed untouched.
        assert!(!db.stage_dispatch_seed(&seed("d1", &["other"])).unwrap());
        let targets = db.dispatch_targets("d1").unwrap();
        assert_eq!(
            targets
                .iter()
                .map(|t| t.account_id.as_str())
                .collect::<Vec<_>>(),
            vec!["primary", "partner"],
            "frozen order preserved; redelivery never recomputes targets"
        );
        assert_eq!(targets[0].exec_rank, 0);
        assert_eq!(targets[1].exec_rank, 1);
        let row = db.dispatch_seed("d1").unwrap().unwrap();
        assert_eq!(row.state, "pending_paper");
        assert_eq!(row.paper_outcome, None);
    }

    #[test]
    fn commit_fill_with_flip_flips_in_the_same_transaction() {
        let (_dir, db) = db();
        db.init_bankroll(dec!(1000)).unwrap();
        db.stage_dispatch_seed(&seed("d2", &["primary"])).unwrap();
        let src = SourceTradeId("src-d2".to_string());
        db.commit_fill_with_flip(
            &src,
            &leader(10, 0),
            &fill("d2", Side::Buy, 10, dec!(0.50)),
            EventSeq(1),
            Some(DispatchFlip {
                dispatch_id: "d2",
                paper_outcome: "fill",
            }),
        )
        .unwrap();
        let row = db.dispatch_seed("d2").unwrap().unwrap();
        assert_eq!(row.state, "ready");
        assert_eq!(row.paper_outcome.as_deref(), Some("fill"));
        assert!(
            db.is_seen(&src).unwrap(),
            "seen-mark and flip share one transaction"
        );
        // The ready, unfinalized seed is visible to the fan-out consumer, oldest first.
        let ready = db.unfinalized_ready_dispatch_seeds().unwrap();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].dispatch_id, "d2");
    }

    #[test]
    fn commit_no_fill_with_flip_records_the_typed_outcome() {
        let (_dir, db) = db();
        db.stage_dispatch_seed(&seed("d3", &["primary"])).unwrap();
        db.commit_seen_no_fill_with_flip(
            &SourceTradeId("src-d3".to_string()),
            &leader(0, 0),
            Some(DispatchFlip {
                dispatch_id: "d3",
                paper_outcome: "no_fill:no_edge",
            }),
        )
        .unwrap();
        let row = db.dispatch_seed("d3").unwrap().unwrap();
        assert_eq!(row.state, "ready");
        assert_eq!(row.paper_outcome.as_deref(), Some("no_fill:no_edge"));
    }

    #[test]
    fn flip_is_idempotent_and_a_missing_seed_is_an_invariant_breach() {
        let (_dir, db) = db();
        db.stage_dispatch_seed(&seed("d4", &["primary"])).unwrap();
        assert!(db.flip_dispatch_ready("d4", "fill").unwrap());
        // Second flip: already ready → no-op, not an error (idempotent redelivery).
        assert!(!db.flip_dispatch_ready("d4", "fill").unwrap());
        // The outcome recorded by the FIRST flip is retained.
        assert_eq!(
            db.dispatch_seed("d4")
                .unwrap()
                .unwrap()
                .paper_outcome
                .as_deref(),
            Some("fill")
        );
        // A flip against a never-staged seed is an invariant breach.
        assert!(matches!(
            db.flip_dispatch_ready("ghost", "fill"),
            Err(PaperStateError::Internal(_))
        ));
    }

    #[test]
    fn finalize_requires_every_target_terminal_and_prune_respects_retention() {
        let (_dir, db) = db();
        db.stage_dispatch_seed(&seed("d5", &["a", "b"])).unwrap();
        db.flip_dispatch_ready("d5", "fill").unwrap();
        db.set_dispatch_target_state("d5", "a", "terminal", Some("filled"), 2_000)
            .unwrap();
        assert!(
            !db.finalize_dispatch_if_terminal("d5", 2_000).unwrap(),
            "one non-terminal target must block finalization"
        );
        db.set_dispatch_target_state("d5", "b", "terminal", Some("killed"), 2_100)
            .unwrap();
        assert!(db.finalize_dispatch_if_terminal("d5", 2_100).unwrap());
        assert!(
            !db.finalize_dispatch_if_terminal("d5", 2_200).unwrap(),
            "idempotent"
        );
        assert!(db.unfinalized_ready_dispatch_seeds().unwrap().is_empty());
        // Retention: not yet pruned inside the window, pruned past it (targets cascade).
        assert_eq!(db.prune_terminal_dispatch(2_500, 1_000).unwrap(), 0);
        assert_eq!(db.prune_terminal_dispatch(5_000, 1_000).unwrap(), 1);
        assert!(db.dispatch_seed("d5").unwrap().is_none());
        assert!(db.dispatch_targets("d5").unwrap().is_empty());
    }

    #[test]
    fn dispatch_state_survives_reopen_and_migration_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("paper_state.db");
        {
            let db = PaperStateDb::open(&path).unwrap();
            db.stage_dispatch_seed(&seed("d6", &["primary"])).unwrap();
        }
        // Reopen runs the additive DDL again (idempotent) and the staged seed persists.
        let db = PaperStateDb::open(&path).unwrap();
        let row = db.dispatch_seed("d6").unwrap().unwrap();
        assert_eq!(row.state, "pending_paper");
        assert_eq!(db.pending_dispatch_seeds().unwrap().len(), 1);
    }

    #[test]
    fn set_target_state_rejects_unknown_targets() {
        let (_dir, db) = db();
        db.stage_dispatch_seed(&seed("d7", &["a"])).unwrap();
        assert!(matches!(
            db.set_dispatch_target_state("d7", "ghost", "terminal", None, 1),
            Err(PaperStateError::Internal(_))
        ));
        db.set_dispatch_target_state("d7", "a", "submitted", None, 1)
            .unwrap();
        assert_eq!(db.dispatch_targets("d7").unwrap()[0].state, "submitted");
    }

    #[test]
    fn seeded_history_promotes_only_existing_incomplete_rows() {
        // #544 activation fix: an accepted bracket promotes the conservative
        // sidecar seed to complete; an unseeded wallet gains no row at all.
        let dir = tempfile::tempdir().unwrap();
        let db = PaperStateDb::open(&dir.path().join("p.db")).unwrap();
        let seeded = WalletAddress::from_hex(&format!("0x{}", "a".repeat(40))).unwrap();
        let unseeded = WalletAddress::from_hex(&format!("0x{}", "b".repeat(40))).unwrap();
        db.record_reconciled_history_status(&WalletHistoryStatusRecord {
            wallet: seeded,
            complete: false,
            proof_json: "{\"seed\":true}".to_owned(),
            updated_at_unix: 1,
        })
        .unwrap();
        let promoted = db
            .mark_seeded_history_validated(&[seeded, unseeded], "{\"t\":1}", 2)
            .unwrap();
        assert_eq!(promoted, 1);
        assert!(db.wallet_history_complete(&seeded).unwrap());
        assert!(!db.wallet_history_complete(&unseeded).unwrap());
        // Idempotent: a second promotion changes nothing.
        assert_eq!(
            db.mark_seeded_history_validated(&[seeded], "{\"t\":2}", 3)
                .unwrap(),
            0
        );
    }
}
