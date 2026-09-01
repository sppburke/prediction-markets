PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

-- Exact #544-base definitions used by the old binary's mandatory write path.
CREATE TABLE seen_trades (
    source_trade_id TEXT PRIMARY KEY NOT NULL
);

CREATE TABLE leader_positions (
    wallet_hex      TEXT    NOT NULL,
    market_id       TEXT    NOT NULL,
    outcome_id      INTEGER NOT NULL,
    long_contracts  INTEGER NOT NULL,
    short_contracts INTEGER NOT NULL,
    PRIMARY KEY (wallet_hex, market_id, outcome_id)
);

CREATE TABLE poll_cursors (
    wallet_hex           TEXT    PRIMARY KEY NOT NULL,
    last_ts_unix         INTEGER NOT NULL,
    last_activity_unix   INTEGER
);

CREATE TABLE meta (
    key   TEXT    PRIMARY KEY NOT NULL,
    value INTEGER NOT NULL
);

PRAGMA user_version = 1;

INSERT INTO seen_trades (source_trade_id) VALUES ('0xlegacy');
INSERT INTO leader_positions
    (wallet_hex, market_id, outcome_id, long_contracts, short_contracts)
VALUES ('0x1111111111111111111111111111111111111111', 'm', 0, 4, 0);
INSERT INTO poll_cursors (wallet_hex, last_ts_unix, last_activity_unix)
VALUES ('0x1111111111111111111111111111111111111111', 100, 100);
