#!/usr/bin/env python3
"""Filtered rebuild: write a pruned copy of wallet_cache.db keeping only
active_tradeable wallets (is_active=1 AND is_infra=0) + their associated data,
copying shared market tables whole. NON-DESTRUCTIVE: reads the internal source,
writes a new DB on the external drive. The original is never touched here; the
swap is a separate, explicit step gated on the verification this prints.

exFAT-safe (journal_mode=DELETE, no WAL). Reads stream from the fast internal
NVMe; only the kept subset (~200 GB) is written to the slow external drive.
"""
import sqlite3, sys, time
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
SRC = str(_REPO / "data/wallet_cache.db")
DST = "/media/sean/CORSAIR/db_update/wallet_cache_pruned.db"
KEEP_WHERE = "is_active = 1 AND is_infra = 0"

WALLET_TABLES = ["trades", "wallet_features", "wallets", "delta_audit",
                 "funder_lookup_done", "leaderboard_snapshots"]
EDGE_TABLES = {"counterparty_edges": ("maker_hex", "taker_hex"),
               "funder_edges": ("funder_hex", "funded_hex")}
COPY_WHOLE = ["market_events", "market_fees", "market_liquidity",
              "market_resolutions", "market_schedules", "token_conditions",
              "source_cursor"]
DROP = {"first_mover_rank_cache"}


def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def main():
    t0 = time.time()
    con = sqlite3.connect(DST)
    con.execute("PRAGMA journal_mode=DELETE")     # exFAT: no WAL
    con.execute("PRAGMA synchronous=OFF")          # offline build; verified after
    con.execute("PRAGMA cache_size=-2000000")      # 2GB page cache for speed
    con.execute(f"ATTACH 'file:{SRC}?mode=ro' AS src")

    # keep-set temp table (indexed) for fast joins
    con.execute("CREATE TABLE _keep(hex TEXT PRIMARY KEY)")
    con.execute(f"INSERT INTO _keep SELECT wallet_hex FROM src.wallets WHERE {KEEP_WHERE}")
    nkeep = con.execute("SELECT COUNT(*) FROM _keep").fetchone()[0]
    log(f"keep-set: {nkeep:,} active_tradeable wallets")

    # table CREATE statements from source (skip DROP set)
    schema = {n: sql for (n, sql) in con.execute(
        "SELECT name, sql FROM src.sqlite_master WHERE type='table' AND sql NOT NULL").fetchall()
        if n not in DROP and not n.startswith("sqlite_")}
    idx = [(n, sql) for (n, sql) in con.execute(
        "SELECT name, sql FROM src.sqlite_master WHERE type='index' AND sql NOT NULL").fetchall()]

    counts = {}
    for name, create_sql in schema.items():
        con.execute(create_sql)
        if name in WALLET_TABLES:
            con.execute(f"INSERT INTO main.{name} SELECT t.* FROM src.{name} t "
                        f"JOIN _keep k ON k.hex = t.wallet_hex")
        elif name in EDGE_TABLES:
            a, b = EDGE_TABLES[name]
            # two indexed passes; OR-dedup via INSERT OR IGNORE on the table PK
            con.execute(f"INSERT OR IGNORE INTO main.{name} SELECT e.* FROM src.{name} e "
                        f"JOIN _keep k ON k.hex = e.{a}")
            con.execute(f"INSERT OR IGNORE INTO main.{name} SELECT e.* FROM src.{name} e "
                        f"JOIN _keep k ON k.hex = e.{b}")
        elif name in COPY_WHOLE:
            con.execute(f"INSERT INTO main.{name} SELECT * FROM src.{name}")
        else:
            log(f"WARN: unclassified table {name} — copying WHOLE (safe default)")
            con.execute(f"INSERT INTO main.{name} SELECT * FROM src.{name}")
        con.commit()
        counts[name] = con.execute(f"SELECT COUNT(*) FROM main.{name}").fetchone()[0]
        log(f"  {name}: {counts[name]:,} rows ({time.time()-t0:.0f}s elapsed)")

    # rebuild indexes (after bulk insert = faster)
    con.execute("DROP TABLE _keep")
    for n, sql in idx:
        try:
            con.execute(sql); log(f"  index {n} built")
        except sqlite3.OperationalError as e:
            log(f"  index {n} skip: {e}")
    con.commit()

    log("running integrity_check ...")
    ic = con.execute("PRAGMA integrity_check").fetchone()[0]
    log(f"integrity_check: {ic}")
    con.close()
    log(f"DONE in {(time.time()-t0)/60:.1f} min. pruned DB at {DST}")
    print("VERIFY_SUMMARY " + " ".join(f"{k}={v}" for k, v in counts.items()), flush=True)
    print(f"INTEGRITY={ic}", flush=True)


if __name__ == "__main__":
    main()
