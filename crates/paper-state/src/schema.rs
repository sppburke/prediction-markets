//! SQLite schema for the paper-trader crash-safe state mirror.
//!
//! `journal_mode = WAL` + `synchronous = NORMAL` gives per-commit durability
//! without full-fsync cost (matching `bootstrap`'s wallet cache). The schema is
//! versioned via `PRAGMA user_version`; see [`SCHEMA_VERSION`].

/// Current on-disk schema version, written to `PRAGMA user_version` on create
/// and checked on open. Bump when the table layout changes incompatibly.
pub const SCHEMA_VERSION: i64 = 1;

/// `meta` key under which the event-log reconciliation cursor is stored.
pub(crate) const META_LAST_APPLIED_EVENT_SEQ: &str = "last_applied_event_seq";

/// Single-row `bankroll` table primary key.
pub(crate) const BANKROLL_ROW_ID: i64 = 0;

/// DDL run on every open (idempotent via `IF NOT EXISTS`).
pub(crate) const SCHEMA: &str = "
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

-- Input dedup: every watchlisted trade we have processed, keyed by the venue's
-- tx-hash trade id. Permanent (no TTL); see issue #282 Open risk #7.
CREATE TABLE IF NOT EXISTS seen_trades (
    source_trade_id TEXT PRIMARY KEY NOT NULL
);

-- Output dedup + replay anchor: one row per recorded paper fill, keyed by the
-- strategy idempotency key. `event_seq` ties the row to its event-log frame.
CREATE TABLE IF NOT EXISTS fills (
    idempotency_key TEXT    PRIMARY KEY NOT NULL,
    market_id       TEXT    NOT NULL,
    outcome_id      INTEGER NOT NULL,
    side            TEXT    NOT NULL CHECK(side IN ('buy', 'sell')),
    contracts       INTEGER NOT NULL,
    fill_price_str  TEXT    NOT NULL,
    event_seq       INTEGER NOT NULL
);

-- Our own net paper positions per (market, outcome).
CREATE TABLE IF NOT EXISTS positions (
    market_id       TEXT    NOT NULL,
    outcome_id      INTEGER NOT NULL,
    long_contracts  INTEGER NOT NULL,
    short_contracts INTEGER NOT NULL,
    PRIMARY KEY (market_id, outcome_id)
);

-- Mirror of the in-memory leader PositionLedger, as net long/short scalars per
-- (wallet, market, outcome). Rehydrated into a PositionLedger at the service tier.
CREATE TABLE IF NOT EXISTS leader_positions (
    wallet_hex      TEXT    NOT NULL,
    market_id       TEXT    NOT NULL,
    outcome_id      INTEGER NOT NULL,
    long_contracts  INTEGER NOT NULL,
    short_contracts INTEGER NOT NULL,
    PRIMARY KEY (wallet_hex, market_id, outcome_id)
);

-- Single-row current bankroll (decimal stored as text for exactness).
CREATE TABLE IF NOT EXISTS bankroll (
    id           INTEGER PRIMARY KEY CHECK(id = 0),
    bankroll_str TEXT NOT NULL
);

-- Per-wallet poll cursor: the newest observed_at unix second seen. The poller
-- fetches from `start = cursor - 1` because the `/activity` `start` bound is
-- exclusive (`timestamp > start`), so the boundary second is re-included and deduped.
CREATE TABLE IF NOT EXISTS poll_cursors (
    wallet_hex   TEXT    PRIMARY KEY NOT NULL,
    last_ts_unix INTEGER NOT NULL
);

-- Key/value scalars (currently: last_applied_event_seq reconciliation cursor).
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT    PRIMARY KEY NOT NULL,
    value INTEGER NOT NULL
);

-- Durable settled-markets set: the double-credit guard for resolution crediting
-- (issue #343 step 0). Mirrors paper-pnl's `SettledMarket`. `outcome_prices` is a
-- caller-owned JSON-encoded vector of text decimals (this crate stores it verbatim);
-- `credit_applied` follows the existing text-decimal convention. Additive table —
-- materialises on the live v1 DB via `IF NOT EXISTS` with SCHEMA_VERSION held at 1.
CREATE TABLE IF NOT EXISTS settled_markets (
    market_id       TEXT    PRIMARY KEY NOT NULL,
    outcome_prices  TEXT    NOT NULL,
    credit_applied  TEXT    NOT NULL,
    settled_at_unix INTEGER NOT NULL
);

-- Fill-time market-liquidity snapshot (WS2 of issue #350): one best-effort row per
-- BUY fill, written off the hot path by the snapshot worker after the fill commits.
-- `liquidity`/`volume` are the Gamma scalars (text decimals, this crate's convention);
-- `absorbable_usd_100bps` and the raw `ask_levels_json` come from the CLOB `/book`
-- call and are NULL on a `/book` failure (a partial, Gamma-only row).
--
-- `idempotency_key` mirrors `fills(idempotency_key)` but is deliberately NOT declared
-- as a SQL foreign key. Although schema.rs sets no `PRAGMA foreign_keys`, the bundled
-- SQLite (libsqlite3-sys, `SQLITE_DEFAULT_FOREIGN_KEYS=1`) enforces FK constraints by
-- default — verified at runtime: a `REFERENCES` clause raises SQLITE_CONSTRAINT_FOREIGNKEY
-- for an orphan key, it is NOT inert. The snapshot write is best-effort and must never
-- fail on referential grounds, so the relationship is documentary (column unconstrained);
-- `fills` is append-only so there is nothing to cascade regardless. Additive table —
-- materialises on the live DB via `IF NOT EXISTS` with SCHEMA_VERSION held at 1.
CREATE TABLE IF NOT EXISTS fill_market_snapshots (
    idempotency_key       TEXT    PRIMARY KEY NOT NULL,
    liquidity             TEXT,
    volume                TEXT,
    absorbable_usd_100bps TEXT,
    ask_levels_json       TEXT,
    captured_at_unix      INTEGER NOT NULL
);
";
