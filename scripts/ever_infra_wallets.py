"""Wallets that ever carried an infra mark (#826): never collected, ranked or published.

The cache's infra tombstones and live flags, plus the committed supplement for marks no cache
still holds (deploy/forge/ever-infra-wallets.txt, the file the binary embeds). Standard library
only, like partial_backfill_wallets; caches without the exclusion tables (trade-only research
fixtures) contribute no marks, but the supplement always applies.
"""

import re
import sqlite3
from pathlib import Path

SUPPLEMENT = Path(__file__).resolve().parent.parent / "deploy" / "forge" / "ever-infra-wallets.txt"
_WALLET = re.compile(r"0x[0-9a-f]{40}")


def parse_supplement(text: str) -> set[str]:
    """One lowercase 0x-prefixed 40-hex wallet per line; '#' comment lines. Anything else refuses.

    Lines split like Rust's `str::lines` (at `\n`, dropping one `\r` before it), so this reader and
    the binary's embedded copy accept exactly the same file.
    """
    wallets: set[str] = set()
    lines = text.split("\n")
    for line in [line.removesuffix("\r") for line in lines[:-1]] + lines[-1:]:
        if not line or line.startswith("#"):
            continue
        if not _WALLET.fullmatch(line) or line in wallets:
            raise ValueError(f"ever-infra supplement line is not a new canonical wallet: {line!r}")
        wallets.add(line)
    return wallets


def ever_infra_wallets(connection: sqlite3.Connection) -> set[str]:
    wallets = parse_supplement(SUPPLEMENT.read_bytes().decode("utf-8"))
    tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
    if "purged_wallets" in tables:
        wallets |= {str(row[0]).lower() for row in connection.execute(
            "SELECT wallet_hex FROM purged_wallets WHERE reason = 'infra'")}
    if "wallets" in tables and "is_infra" in {row[1] for row in connection.execute("PRAGMA table_info(wallets)")}:
        wallets |= {str(row[0]).lower() for row in connection.execute(
            "SELECT wallet_hex FROM wallets WHERE is_infra = 1")}
    return wallets
