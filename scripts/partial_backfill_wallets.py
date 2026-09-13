"""Schema-one quarantine and its active, non-infrastructure retry subset.

Shared by analytics, publication and shell guards; deliberately standard-library
only so a pure re-push never needs NumPy or Pandas. Reads never migrate a cache.
"""

import sqlite3


def partial_backfill_wallets(connection: sqlite3.Connection,
                             retryable_only: bool = False) -> set[str]:
    if int(connection.execute("PRAGMA user_version").fetchone()[0]) >= 2:
        return set()
    columns = {row[1] for row in connection.execute("PRAGMA table_info(wallets)")}
    if "backfill_partial" not in columns:
        return set()
    predicate = "backfill_partial = 1"
    if retryable_only:
        predicate += " AND is_active = 1 AND is_infra = 0"
    return {str(row[0]).lower() for row in connection.execute(
        f"SELECT wallet_hex FROM wallets WHERE {predicate}"
    )}
