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

mod schema;

use std::path::Path;
use std::str::FromStr;
use std::sync::{Mutex, PoisonError};

use rusqlite::{Connection, OpenFlags, OptionalExtension as _, Transaction, params};
use rust_decimal::Decimal;

use pe_core_types::{
    EventSeq, MarketId, OutcomeId, Price, Side, SourceTradeId, VenueMarketId, WalletAddress,
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
}

/// One leader's net position in a `(market, outcome)`, mirroring the in-memory
/// `PositionState`. The service tier groups these into `PositionSnapshot`s.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaderPositionRow {
    pub wallet: WalletAddress,
    pub market_id: MarketId,
    pub outcome_id: OutcomeId,
    pub long_contracts: u64,
    pub short_contracts: u64,
}

/// One of our own net paper positions in a `(market, outcome)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaperPositionRow {
    pub market_id: MarketId,
    pub outcome_id: OutcomeId,
    pub long_contracts: u64,
    pub short_contracts: u64,
}

/// A paper fill to record. Built by the service tier from the executed `OrderIntent`
/// and the `PaperFill` returned by the executor.
#[derive(Debug, Clone)]
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
        let mut conn = self.lock();
        let tx = conn.transaction()?;
        tx_mark_seen(&tx, source_trade_id)?;
        tx_upsert_leader(&tx, leader)?;
        tx.commit()?;
        Ok(())
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
    /// Returns the post-fill bankroll so the caller can keep its in-memory copy in sync.
    pub fn commit_fill(
        &self,
        source_trade_id: &SourceTradeId,
        leader: &LeaderPositionRow,
        fill: &FillRecord,
        fill_seq: EventSeq,
    ) -> Result<Decimal, PaperStateError> {
        let mut conn = self.lock();
        let tx = conn.transaction()?;
        tx_mark_seen(&tx, source_trade_id)?;
        tx_upsert_leader(&tx, leader)?;
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
        tx.commit()?;
        Ok(bankroll)
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
    /// PK then blocks a duplicate fill). See issue #282 Open risk #7.
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
        let inserted = tx_record_fill(&tx, fill, fill_seq)?;
        if inserted {
            tx_apply_our_position(&tx, fill)?;
            tx_apply_bankroll(&tx, fill)?;
        }
        tx_set_last_applied(&tx, fill_seq)?;
        tx.commit()?;
        Ok(inserted)
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

    /// Number of open net-position rows. Cheap `COUNT(*)` for the status snapshot, avoiding
    /// loading every row via [`paper_positions`](Self::paper_positions).
    pub fn positions_count(&self) -> Result<usize, PaperStateError> {
        let conn = self.lock();
        let n: i64 = conn.query_row("SELECT COUNT(*) FROM positions", [], |row| row.get(0))?;
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

    /// The highest event-log `seq` whose fill has been applied to the **authoritative
    /// Supabase** `commit_fill` RPC (issue #397). Defaults to `EventSeq(0)` before any
    /// fill has been applied (or after a SQLite loss, forcing a safe full idempotent
    /// replay). Kept separate from [`last_applied_event_seq`](Self::last_applied_event_seq)
    /// so a local-only reconcile never advances the Supabase catch-up cursor.
    pub fn last_supabase_applied_event_seq(&self) -> Result<EventSeq, PaperStateError> {
        let conn = self.lock();
        let raw: Option<i64> = conn
            .query_row(
                "SELECT value FROM meta WHERE key = ?1",
                params![META_LAST_SUPABASE_APPLIED_EVENT_SEQ],
                |row| row.get(0),
            )
            .optional()?;
        Ok(EventSeq(raw.map(parse_u64).transpose()?.unwrap_or(0)))
    }

    /// Persist the Supabase authoritative catch-up watermark (issue #397). Set to the
    /// event-log head at cutover (so the first authoritative boot's catch-up is a no-op)
    /// and advanced as boot catch-up confirms each fill against Supabase.
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
            "SELECT wallet_hex, market_id, outcome_id, long_contracts, short_contracts \
             FROM leader_positions",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(4)?,
            ))
        })?;
        let mut out = Vec::new();
        for row in rows {
            let (wallet_hex, market, outcome, long, short) = row?;
            out.push(LeaderPositionRow {
                wallet: parse_wallet(&wallet_hex)?,
                market_id: MarketId(VenueMarketId(market)),
                outcome_id: OutcomeId(parse_u16(outcome)?),
                long_contracts: parse_u64(long)?,
                short_contracts: parse_u64(short)?,
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

fn tx_mark_seen(
    tx: &Transaction<'_>,
    source_trade_id: &SourceTradeId,
) -> Result<(), PaperStateError> {
    tx.execute(
        "INSERT OR IGNORE INTO seen_trades (source_trade_id) VALUES (?1)",
        params![source_trade_id.0],
    )?;
    Ok(())
}

fn tx_upsert_leader(
    tx: &Transaction<'_>,
    leader: &LeaderPositionRow,
) -> Result<(), PaperStateError> {
    tx.execute(
        "INSERT INTO leader_positions \
            (wallet_hex, market_id, outcome_id, long_contracts, short_contracts) \
         VALUES (?1, ?2, ?3, ?4, ?5) \
         ON CONFLICT(wallet_hex, market_id, outcome_id) DO UPDATE SET \
            long_contracts = excluded.long_contracts, \
            short_contracts = excluded.short_contracts",
        params![
            leader.wallet.to_string(),
            leader.market_id.to_string(),
            i64::from(leader.outcome_id.0),
            to_i64(leader.long_contracts)?,
            to_i64(leader.short_contracts)?,
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

fn parse_decimal(s: &str) -> Result<Decimal, PaperStateError> {
    Decimal::from_str(s).map_err(|_| PaperStateError::Corrupt(format!("bad decimal {s:?}")))
}

fn parse_wallet(s: &str) -> Result<WalletAddress, PaperStateError> {
    WalletAddress::from_hex(s).map_err(|_| PaperStateError::Corrupt(format!("bad wallet {s:?}")))
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

    fn leader(long: u64, short: u64) -> LeaderPositionRow {
        LeaderPositionRow {
            wallet: wallet(),
            market_id: market(),
            outcome_id: OutcomeId(0),
            long_contracts: long,
            short_contracts: short,
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
                expected: 1
            })
        ));
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
        assert_eq!(new, dec!(996.0));
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
        assert_eq!(new, dec!(97.40));
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
        assert_eq!(new, Decimal::ZERO);
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
        assert_eq!(new, dec!(995.0));
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
        db.record_settled_market(&market(), "[\"1\",\"0\"]", dec!(5), 1_700_000_000)
            .unwrap();
        assert_eq!(db.positions_count().unwrap(), 1);
        assert_eq!(db.settled_count().unwrap(), 1);
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
    fn supabase_watermark_round_trips_and_defaults_zero() {
        let (_dir, db) = db();
        // Defaults to 0 before any set (or after a SQLite loss → safe full replay).
        assert_eq!(db.last_supabase_applied_event_seq().unwrap(), EventSeq(0));
        db.set_supabase_applied_event_seq(EventSeq(42)).unwrap();
        assert_eq!(db.last_supabase_applied_event_seq().unwrap(), EventSeq(42));
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
}
