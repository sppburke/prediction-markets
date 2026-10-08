#!/usr/bin/env python3
"""Freeze and compare the purge-free Forge source watermark (#544)."""

import argparse
import hashlib
import json
import os
import re
import sqlite3
import stat
import tempfile
from pathlib import Path

from partial_backfill_wallets import partial_backfill_wallets

MANIFEST_VERSION = 1
# Payout, finalize, export, both ranking passes and re-finalize after a completed
# collection; a head that cannot also cover them takes the cycle's one top-up
# (`candidate_top_up_reserve_hours` in docs/_GLOSSARY.md).
TOP_UP_RESERVE_HOURS = 7


def _one(connection: sqlite3.Connection, query: str, args=()):
    row = connection.execute(query, args).fetchone()
    return None if row is None else row[0]


# A wallet's newest trade in the latest completed activity generation: one
# descending walk of `idx_activity_groups_v2_wallet_time` that stops at the
# first match instead of reading the wallet's history.
WALLET_NEWEST_TRADE_V2 = (
    "SELECT g.source_time_unix FROM activity_groups_v2 g "
    "WHERE g.wallet_hex = {wallet} AND g.activity_type = 'TRADE' "
    "AND g.coverage_generation = (SELECT MAX(generation) FROM activity_coverage_manifests_v2) "
    "ORDER BY g.source_time_unix DESC LIMIT 1"
)

CERTIFICATE_COLUMNS = (
    "wallet_hex", "generation", "newest_source_unix", "newest_trade_unix",
    "aggregate_count", "source_row_count", "ordered_digest", "scope_drops_json",
)
DROP_CAUSES = {"conversion", "order_dependent", "overflow", "underflow",
               "unknown_condition", "unknown_type", "unmapped"}


def certificate_digest(rows) -> str:
    digest = hashlib.sha256(b"[")
    for index, row in enumerate(rows):
        if index:
            digest.update(b",")
        digest.update(json.dumps(row, sort_keys=True, separators=(",", ":"),
                                 ensure_ascii=False, allow_nan=False).encode())
    digest.update(b"]")
    return digest.hexdigest()


def decode_scope_drops(raw: str) -> list[dict]:
    drops = json.loads(raw)
    if not isinstance(drops, list):
        raise ValueError("invalid certificate scope drops")
    previous = None
    for drop in drops:
        if (not isinstance(drop, dict)
                or set(drop) != {"cause", "dropped_at_unix", "scope_id", "scope_kind"}
                or drop["cause"] not in DROP_CAUSES
                or drop["scope_kind"] not in ("event", "market")
                or not isinstance(drop["scope_id"], str)
                or re.fullmatch(r"0x[0-9a-f]+", drop["scope_id"]) is None
                or type(drop["dropped_at_unix"]) is not int):
            raise ValueError("invalid certificate scope drop")
        scope = (drop["scope_kind"], drop["scope_id"])
        if previous is not None and scope <= previous:
            raise ValueError("certificate scope drops are not sorted and unique")
        previous = scope
    if json.dumps(drops, sort_keys=True, separators=(",", ":"), ensure_ascii=False) != raw:
        raise ValueError("certificate scope drops are not canonical")
    return drops


def finalized_certificates(connection: sqlite3.Connection) -> dict | None:
    """Verify all certificates, retaining only this finalized pass's wallets."""
    columns = {row[1] for row in connection.execute("PRAGMA table_info(cache_v2_migration_state)")}
    if "fresh_collection_json" not in columns:
        return None
    raw = _one(connection, "SELECT fresh_collection_json FROM cache_v2_migration_state WHERE singleton = 1")
    if raw is None or json.loads(raw).get("version") in (None, 1, 2, 3):
        return None
    identity = _fresh_identity(raw)
    phase, classifier, inputs_raw = connection.execute(
        "SELECT phase, ranker_classifier_version, ranker_projection_inputs_json "
        "FROM cache_v2_migration_state WHERE singleton = 1").fetchone()
    inputs = json.loads(inputs_raw) if inputs_raw is not None else {}
    generation = identity["generation"]
    if (phase != "finalized" or classifier != 6 or type(inputs.get("oracle_version")) is not int
            or inputs["oracle_version"] != 6
            or inputs.get("activity_generation") != generation
            or _one(connection, "SELECT MAX(generation) FROM activity_coverage_manifests_v2") != generation):
        raise ValueError("format-three certificates require the finalized head")
    current = {}
    count = source_rows = 0
    newest = None

    def rows():
        nonlocal count, source_rows, newest
        for values in connection.execute(
                f"SELECT {', '.join(CERTIFICATE_COLUMNS)} FROM activity_wallet_history_v3 ORDER BY wallet_hex"):
            row = dict(zip(CERTIFICATE_COLUMNS, values, strict=True))
            yield row
            if row["generation"] == generation:
                current[row["wallet_hex"]] = row
                count += row["aggregate_count"]
                source_rows += row["source_row_count"]
                trade = row["newest_trade_unix"]
                if trade is not None:
                    newest = trade if newest is None else max(newest, trade)

    if certificate_digest(rows()) != inputs.get("certificate_digest"):
        raise ValueError("activity_wallet_history_v3 certificate digest mismatch")
    for row in current.values():
        decode_scope_drops(row["scope_drops_json"])
    return {"generation": generation, "classifier_version": classifier,
            "wallets": current, "count": count, "source_row_count": source_rows,
            "newest_trade_unix": newest}


def newest_trade_unix(connection: sqlite3.Connection) -> int | None:
    """The cache's newest trade, the watermark the publisher's freshness gate reads.

    Schema one reads ``trades``. Schema two reads only ``TRADE`` groups of its
    latest completed generation (#544). Certification admits that generation's
    rows only under its wallet receipts with aggregates, so the newest of those
    wallets' newest trades is the generation's newest trade. Receipts are
    retained in the staging table, or embedded in an authentic legacy manifest.
    """
    schema = int(_one(connection, "PRAGMA user_version") or 0)
    if schema == -2:
        raise ValueError("unfinished bulk root (schema -2); resume cache-populate-activity-v2 --bulk-root before any other command")
    if schema >= 2:
        certificates = finalized_certificates(connection)
        if certificates is not None:
            return certificates["newest_trade_unix"]
        value = _one(
            connection,
            "WITH latest AS (SELECT generation, cursors_json FROM activity_coverage_manifests_v2 "
            "ORDER BY generation DESC LIMIT 1), "
            "wallets(wallet_hex) AS ("
            "SELECT r.wallet_hex FROM activity_wallet_coverage_staging_v2 r, latest "
            "WHERE r.generation = latest.generation AND r.aggregate_count > 0 "
            "UNION ALL SELECT json_extract(e.value, '$.wallet_hex') FROM latest, json_each(latest.cursors_json) e "
            "WHERE json_type(latest.cursors_json) = 'array' "
            "AND json_extract(e.value, '$.aggregate_count') > 0) "
            "SELECT MAX((" + WALLET_NEWEST_TRADE_V2.format(wallet="wallets.wallet_hex") + ")) FROM wallets",
        )
    else:
        value = _one(connection, "SELECT MAX(timestamp_unix) FROM trades")
    return None if value is None else int(value)


def _wallet_universe(connection: sqlite3.Connection) -> dict:
    wallets = [
        str(row[0]).lower()
        for row in connection.execute(
            "SELECT wallet_hex FROM active_tradeable_wallets ORDER BY wallet_hex"
        )
    ]
    rendered = "\n".join(wallets).encode()
    return {
        "active_tradeable_count": len(wallets),
        "active_tradeable_sha256": hashlib.sha256(rendered).hexdigest(),
    }


def snapshot(db_path: Path, day_utc: str, versions: dict, configuration: dict) -> dict:
    # Imported here: the reranker pulls in NumPy, and the publisher's pure re-push
    # imports this module for `newest_trade_unix` without analytics installed.
    from latency_shift_rerank import ORACLE_VERSION

    versions = {**versions, "ranker": ORACLE_VERSION}
    connection = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        connection.execute("BEGIN")
        schema = int(_one(connection, "PRAGMA user_version") or 0)
        if schema == -2:
            raise ValueError("unfinished bulk root (schema -2); resume cache-populate-activity-v2 --bulk-root before any other command")
        universe = _wallet_universe(connection)
        if schema >= 2:
            certificates = finalized_certificates(connection)
            activity_generation = _one(
                connection,
                "SELECT MAX(generation) FROM activity_coverage_manifests_v2",
            )
            payout_generation = _one(
                connection,
                "SELECT MAX(generation) FROM clob_payout_coverage_manifests_v2",
            )
            # This snapshot runs inside the publication freshness window (#588).
            # Finalize certified the latest generation's row count as its
            # manifest's group count. A finalized fresh identity of that generation
            # proves no row has moved since: fresh admission clears `finalized`
            # before any carry, and the frozen collector refuses a fresh identity.
            # Any other cache, including one from before #588 without the identity
            # column, counts its rows; the newest trade is read per wallet.
            certified = any(
                row[1] == "fresh_collection_json"
                for row in connection.execute("PRAGMA table_info(cache_v2_migration_state)")
            ) and _one(
                connection,
                "SELECT phase = 'finalized' AND json_extract(fresh_collection_json, '$.generation') = ? "
                "FROM cache_v2_migration_state WHERE singleton = 1",
                (activity_generation,),
            )
            activity = {
                "generation": activity_generation,
                "count": certificates["count"] if certificates is not None else _one(
                    connection,
                    "SELECT group_count FROM activity_coverage_manifests_v2 WHERE generation = ?"
                    if certified
                    else "SELECT COUNT(*) FROM activity_groups_v2 WHERE coverage_generation = ?",
                    (activity_generation,),
                ),
                "newest_source_unix": (certificates["newest_trade_unix"] if certificates is not None
                                       else newest_trade_unix(connection)),
                "cursor": _one(
                    connection,
                    "SELECT cursors_json FROM activity_coverage_manifests_v2 "
                    "ORDER BY generation DESC LIMIT 1",
                ),
                "completed_at_unix": _one(
                    connection,
                    "SELECT completed_at_unix FROM activity_coverage_manifests_v2 "
                    "ORDER BY generation DESC LIMIT 1",
                ),
                "reference_sha256": _one(
                    connection,
                    "SELECT reference_sha256 FROM activity_coverage_manifests_v2 "
                    "WHERE generation = ?",
                    (activity_generation,),
                ),
                "wallet_count": _one(
                    connection,
                    "SELECT wallet_count FROM activity_coverage_manifests_v2 "
                    "WHERE generation = ?",
                    (activity_generation,),
                ),
                "receipt_set_digest": _one(
                    connection,
                    "SELECT receipt_set_digest FROM activity_coverage_manifests_v2 "
                    "WHERE generation = ?",
                    (activity_generation,),
                ),
                "aggregate_digest": _one(
                    connection,
                    "SELECT aggregate_digest FROM activity_coverage_manifests_v2 "
                    "WHERE generation = ?",
                    (activity_generation,),
                ),
                "source_row_count": certificates["source_row_count"] if certificates is not None else _one(
                    connection,
                    "SELECT source_row_count FROM activity_coverage_manifests_v2 "
                    "WHERE generation = ?",
                    (activity_generation,),
                ),
                "ranker_projection": {
                    "count": _one(
                        connection,
                        "SELECT ranker_projection_count FROM cache_v2_migration_state "
                        "WHERE singleton = 1",
                    ),
                    "digest": _one(
                        connection,
                        "SELECT ranker_projection_digest FROM cache_v2_migration_state "
                        "WHERE singleton = 1",
                    ),
                    "classifier_version": _one(
                        connection,
                        "SELECT ranker_classifier_version FROM cache_v2_migration_state "
                        "WHERE singleton = 1",
                    ),
                },
            }
            resolution = {
                "generation": payout_generation,
                "count": _one(
                    connection,
                    "SELECT COUNT(*) FROM clob_payout_evidence_v2 "
                    "WHERE coverage_generation = ?",
                    (payout_generation,),
                ),
                "newest_fetch_unix": _one(
                    connection,
                    "SELECT MAX(fetched_at_unix) FROM clob_payout_evidence_v2 "
                    "WHERE coverage_generation = ?",
                    (payout_generation,),
                ),
                "completed_at_unix": _one(
                    connection,
                    "SELECT completed_at_unix FROM clob_payout_coverage_manifests_v2 "
                    "WHERE generation = ?",
                    (payout_generation,),
                ),
                "terminal_kind": _one(
                    connection,
                    "SELECT terminal_kind FROM clob_payout_coverage_manifests_v2 "
                    "WHERE generation = ?",
                    (payout_generation,),
                ),
                "manifest_json": _one(
                    connection,
                    "SELECT manifest_json "
                    "FROM clob_payout_coverage_manifests_v2 WHERE generation = ?",
                    (payout_generation,),
                ),
                "terminal_page_sha256": _one(
                    connection,
                    "SELECT terminal_page_sha256 FROM clob_payout_coverage_manifests_v2 "
                    "WHERE generation = ?",
                    (payout_generation,),
                ),
            }
        else:
            activity = {
                "generation": 1,
                "count": _one(connection, "SELECT COUNT(*) FROM trades"),
                "newest_source_unix": newest_trade_unix(connection),
                "cursor": None,
            }
            cursor = connection.execute(
                "SELECT value, updated_at FROM source_cursor WHERE key = 'clob_closed'"
            ).fetchone()
            resolution = {
                "generation": 1,
                "count": _one(connection, "SELECT COUNT(*) FROM market_resolutions"),
                "newest_fetch_unix": _one(
                    connection, "SELECT MAX(fetched_at_unix) FROM market_resolutions"
                ),
                "cursor": None
                if cursor is None
                else {"value": str(cursor[0]), "updated_at": int(cursor[1])},
            }
        universe["backfill_partial_wallets"] = sorted(partial_backfill_wallets(connection))
        body = {
            "version": MANIFEST_VERSION,
            "day_utc": day_utc,
            "cache_schema": schema,
            "universe": universe,
            "source_watermark": {"activity": activity, "resolution": resolution},
            "versions": versions,
            "configuration": configuration,
        }
        fingerprint_body = dict(body)
        fingerprint_body.pop("day_utc")
        body["fingerprint_sha256"] = hashlib.sha256(
            json.dumps(fingerprint_body, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        return body
    finally:
        connection.close()


def atomic_write(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n"
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(rendered)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def latest_accepted(root: Path, day_utc: str) -> dict | None:
    for path in sorted(root.glob("cron-*/accepted_cycle_manifest.json"), reverse=True):
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if value.get("version") == MANIFEST_VERSION and value.get("day_utc") == day_utc:
            return value
    return None


CYCLE_PATTERN = r"cron-[0-9]{8}T[0-9]{6}Z"


def accepted_candidate_cycle(root: Path, out: Path | None = None) -> Path | None:
    """The named, else the newest, cycle whose verified candidate-lane publication
    wrote its accepted record (written only after that publication)."""
    candidates = [out] if out is not None else sorted(root.glob("cron-*"), reverse=True)
    for candidate in candidates:
        if (candidate.is_symlink() or not candidate.is_dir()
                or ".." in candidate.parts or not re.fullmatch(CYCLE_PATTERN, candidate.name)
                or candidate.resolve().parent != root.resolve()):
            continue
        accepted = candidate / "accepted_cycle_manifest.json"
        if not accepted.is_file() or accepted.is_symlink():
            continue
        manifest = json.loads(accepted.read_text(encoding="utf-8"))
        if (manifest.get("version") == MANIFEST_VERSION
                and manifest.get("configuration", {}).get("cache_lane") == "fresh_v2"):
            return candidate
    return None


def retire_completed_cycle(root: Path, out: Path | None = None) -> int:
    """Resume retention under the wrapper's run lock: 0 done, 2 held, 3 absent.

    The durable accepted record is written only after verified publication.
    It and the unchanged request/staging evidence remain the cleanup obligation,
    including when both pointers and every eligible cache file are already gone.
    """
    out = accepted_candidate_cycle(root, out)
    if out is None:
        return 3

    guards = [root / name for name in (
        "rank_and_push.cycle", "rank_and_push.pending", ".forge_pause.json")]

    def held():
        return any(os.path.lexists(path) for path in guards)

    if held():
        print("   [cache-retention] nothing deleted: recovery pointer or Forge pause record remains")
        return 2
    request_path = out / "ranking_publish_request.json"
    if not request_path.is_file() or request_path.is_symlink():
        raise ValueError("completed cycle omitted its regular publication request")
    from push_ranking_to_supabase import load_publish_request

    request = load_publish_request(str(request_path))
    activation = request.get("cache_activation")
    if activation is None:
        raise ValueError("completed candidate cycle omitted its activation binding")
    fixed = Path(activation["fixed_path"])
    side = Path(activation["side_path"])
    if ".." in fixed.parts or ".." in side.parts:
        raise ValueError("unsafe completed cycle cache path")
    fixed = fixed.resolve(strict=True)
    if (not fixed.is_file() or side.name != f"wallet_cache.{out.name}.side.db"
            or side.resolve().parent != fixed.parent):
        raise ValueError("unrecognized completed cycle or fixed file")

    print(f"RANK_AND_PUSH_RETENTION_CYCLE={out}")
    # Make pointer clearings durable before retiring rollback files.
    directory = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    pattern = re.compile(r"wallet_cache\.(" + CYCLE_PATTERN + r")\.(?:"
                         r"(?:prior|side|displaced)\.db(?:-wal|-shm)?|side\.db\.projection-v3\.jsonl)")
    deleted = 0
    deferred = False
    for path in sorted(fixed.parent.iterdir()):
        match = pattern.fullmatch(path.name)
        if not match or match[1] > out.name:
            continue
        metadata = path.lstat()
        # Never follow a symlink, touch the installed inode, or delete an alias.
        if not stat.S_ISREG(metadata.st_mode) or path.samefile(fixed):
            continue
        if held():
            print("   [cache-retention] stopped: recovery pointer or Forge pause record appeared")
            deferred = True
            break
        path.unlink()
        deleted += 1
        print(f"   [cache-retention] deleted {path} freed_size_bytes={metadata.st_size}")
    # Unconditional: a previous process may have died after the final unlink,
    # before syncing it. An empty directory scan is not durability evidence.
    directory = os.open(fixed.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    if not deleted:
        print("   [cache-retention] nothing deleted: no eligible cycle files")
    return 2 if deferred else 0


def _fresh_identity(raw: str | None) -> dict | None:
    if raw is None:
        return None
    identity = json.loads(raw)
    if not isinstance(identity, dict):
        raise ValueError("malformed candidate activity identity")
    version = identity.get("version")
    if type(version) is not int:
        raise ValueError("unsupported candidate activity identity version")
    expected = {"version", "generation", "fixed_end_unix", "wallets", "digest"}
    if version in (2, 3, 4):
        expected |= {"base_generation", "base_manifest_sha256", "start_exclusive", "full_read_wallets"}
        if version >= 3:
            expected |= {"deferred_wallets", "quiet_after_secs", "repoll_period_secs"}
        if version == 4:
            expected |= {"repair_wallets", "certified_digest"}
    elif version != 1:
        raise ValueError("unsupported candidate activity identity version")
    if set(identity) != expected:
        raise ValueError("malformed candidate activity identity")
    content = {key: value for key, value in identity.items() if key != "digest"}
    digest = hashlib.sha256(json.dumps(content, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()
    if identity["digest"] != digest:
        raise ValueError("candidate activity identity digest mismatch")
    if type(identity["generation"]) is not int or not 0 < identity["generation"] <= 2**63 - 1:
        raise ValueError("invalid candidate generation")
    if type(identity["fixed_end_unix"]) is not int or identity["fixed_end_unix"] <= 0:
        raise ValueError("invalid candidate fixed end")
    if version == 4:
        wallets = identity["wallets"]
        full = identity["full_read_wallets"]
        deferred = identity["deferred_wallets"]
        repair = identity["repair_wallets"]
        for values in (wallets, full, deferred, repair):
            if (not isinstance(values, list)
                    or any(not isinstance(wallet, str) or re.fullmatch(r"0x[0-9a-f]{40}", wallet) is None
                           for wallet in values)
                    or values != sorted(set(values))):
                raise ValueError("invalid format-three wallet list")
        if (not set(full) <= set(wallets) or not set(deferred) <= set(wallets)
                or set(full) & set(deferred) or not set(repair) <= set(full)):
            raise ValueError("invalid format-three wallet subsets")
        start = identity["start_exclusive"]
        base = identity["base_generation"]
        base_hash = identity["base_manifest_sha256"]
        if (type(start) is not int or not 0 <= start < identity["fixed_end_unix"]
                or identity["fixed_end_unix"] > 253402300799
                or type(identity["quiet_after_secs"]) is not int
                or identity["quiet_after_secs"] != 2_592_000
                or type(identity["repoll_period_secs"]) is not int
                or identity["repoll_period_secs"] != 604_800
                or not isinstance(identity["certified_digest"], str)
                or re.fullmatch(r"[0-9a-f]{64}", identity["certified_digest"]) is None):
            raise ValueError("invalid format-three identity bounds or certificate digest")
        if base is None:
            if base_hash is not None or start != 0 or full != wallets or deferred:
                raise ValueError("invalid format-three root identity")
        elif (type(base) is not int or not 0 < base < identity["generation"] or start == 0
              or not isinstance(base_hash, str) or re.fullmatch(r"[0-9a-f]{64}", base_hash) is None):
            raise ValueError("invalid format-three predecessor binding")
    return identity


def _bulk_root_eligible(side: sqlite3.Connection, schema: int, head: dict | None,
                        generation: int) -> bool:
    """Mirror Rust's begin_or_resume_fresh_collection/require_bulk_root_state.

    This is routing from durable state, not admission proof. Rust still validates
    paths, identity/content, wallet union, temporary storage and the writer lock.
    Completed roots use ordinary collection even though Rust permits a bulk retry.
    """
    if generation != 1 or schema not in (2, -2):
        return False
    if schema == -2:
        if head is None or head["version"] not in (2, 4) or head["generation"] != 1 or head["base_generation"] is not None:
            return False
    elif head is not None:
        # An ordinary collection, even an empty interrupted one, cannot convert.
        return False
    state = side.execute("SELECT * FROM cache_v2_migration_state WHERE singleton = 1")
    row = state.fetchone()
    if row is None:
        return False
    state = dict(zip((column[0] for column in state.description), row))
    if state["phase"] != "schema_sealed" or any(state.get(column) is not None for column in (
        "ranker_projection_count", "ranker_projection_digest", "ranker_classifier_version",
        "ranker_projection_inputs_json",
    )):
        return False
    if not _one(side, """SELECT
        NOT EXISTS(SELECT 1 FROM activity_coverage_manifests_v2)
        AND NOT EXISTS(SELECT 1 FROM cache_frozen_payload_verifications)
        AND NOT EXISTS(SELECT 1 FROM ranker_entries_v2)"""):
        return False
    if schema == -2:
        return True  # Committed wallet rows/receipts are the bulk resume state.
    return bool(_one(side, """SELECT
        NOT EXISTS(SELECT 1 FROM activity_groups_v2)
        AND NOT EXISTS(SELECT 1 FROM activity_wallet_coverage_staging_v2)
        AND NOT EXISTS(SELECT 1 FROM pragma_table_info('activity_groups_v2') WHERE pk != 0)
        AND EXISTS(
            SELECT 1 FROM pragma_index_list('activity_groups_v2') i
            WHERE i.name = 'idx_activity_groups_v2_source_trade_id'
              AND i.\"unique\" = 1 AND i.partial = 0
              AND (SELECT COUNT(*) FROM pragma_index_xinfo(i.name) WHERE key = 1) = 1
              AND EXISTS(SELECT 1 FROM pragma_index_xinfo(i.name)
                  WHERE key = 1 AND cid >= 0 AND name = 'source_trade_id'
                    AND coll = 'BINARY' AND desc = 0))"""))


def read_staging_baseline(side_path: Path, expected_sha256: str | None = None) -> dict:
    """Read the Rust-owned immutable baseline, including while fixed is absent."""
    raw = side_path.with_suffix(".stage.json").read_bytes()
    if expected_sha256 is not None and hashlib.sha256(raw).hexdigest() != expected_sha256:
        raise ValueError("stage evidence differs from prepared publication request")
    evidence = json.loads(raw)
    if evidence.get("version") != 1 or Path(evidence["side_path"]).resolve() != side_path.resolve():
        raise ValueError("invalid staging baseline version or candidate binding")
    digest = evidence.get("source_sha256")
    if not isinstance(digest, str) or len(digest) != 64 or any(ch not in "0123456789abcdef" for ch in digest):
        raise ValueError("invalid staging baseline hash")
    if Path(evidence["prior_path"]).exists():
        raise ValueError("cycle has both prior and two-file staging evidence")
    return evidence


def candidate_targets(prior_path: Path | None, side_path: Path, *, after_collection=False,
                      now: int | None = None, max_staleness_hours: int | None = None,
                      include_bulk_root=False) -> tuple[int, int, int] | tuple[int, int, int, int]:
    """Select this cycle's initial head or its one linked successor, read-only.

    Rust certifies content and linkage on every collection call. This owner only
    establishes cycle membership and the restart-safe one-top-up allowance.
    The optional fourth and fifth integers report bulk routing and completed
    activity; the default three values remain unchanged for existing callers.
    """
    if side_path.with_suffix(".stage.json").exists():
        baseline = read_staging_baseline(side_path)
        previous = int(baseline["activity_generation"])
        payout = int(baseline["payout_generation"])
        prior_identity = _fresh_identity(json.dumps(baseline["fresh_identity"])) if baseline["fresh_identity"] is not None else None
    else:
        if prior_path is None:
            raise ValueError("legacy candidate requires --prior; new candidate requires staging evidence")
        with sqlite3.connect(f"file:{prior_path}?mode=ro&immutable=1", uri=True) as prior:
            schema = int(_one(prior, "PRAGMA user_version") or 0)
            if schema == -2:
                raise ValueError("unfinished bulk root (schema -2); resume cache-populate-activity-v2 --bulk-root before any other command")
            previous = 0 if schema < 2 else int(_one(prior, "SELECT COALESCE(MAX(generation), 0) FROM activity_coverage_manifests_v2"))
            prior_identity = None
            if schema == 2 and any(row[1] == "fresh_collection_json" for row in prior.execute("PRAGMA table_info(cache_v2_migration_state)")):
                prior_identity = _fresh_identity(_one(prior, "SELECT fresh_collection_json FROM cache_v2_migration_state WHERE singleton = 1"))
            active = _one(prior, "SELECT generation FROM clob_payout_walk_state_v2 WHERE singleton = 1")
            payout = int(active) if active is not None else int(_one(prior, "SELECT COALESCE(MAX(generation), 0) FROM clob_payout_coverage_manifests_v2")) + 1
    initial = previous + 1
    with sqlite3.connect(f"file:{side_path}?mode=ro", uri=True) as side:
        side.execute("BEGIN")
        side_schema = int(_one(side, "PRAGMA user_version") or 0)
        if side_schema == -2 and (not include_bulk_root or after_collection):
            raise ValueError("unfinished bulk root (schema -2); resume cache-populate-activity-v2 --bulk-root before any other command")
        head = _fresh_identity(_one(side, "SELECT fresh_collection_json FROM cache_v2_migration_state WHERE singleton = 1"))

        def targets(generation):
            if after_collection:
                latest = side.execute("SELECT generation, completed_at_unix FROM clob_payout_coverage_manifests_v2 ORDER BY generation DESC LIMIT 1").fetchone()
                payout_done = int(generation == head["generation"] and latest is not None
                                  and latest[0] >= payout and latest[1] >= head["fixed_end_unix"])
            else:
                payout_done = int(_one(side, "SELECT EXISTS(SELECT 1 FROM clob_payout_coverage_manifests_v2 WHERE generation = ?)", (payout,)))
            values = (generation, payout, payout_done)
            if not include_bulk_root:
                return values
            bulk = not after_collection and _bulk_root_eligible(side, side_schema, head, generation)
            if side_schema == -2 and not bulk:
                raise ValueError("invalid unfinished bulk root; resume cache-populate-activity-v2 --bulk-root to diagnose the candidate")
            activity_complete = (side_schema == 2 and head is not None
                                 and generation == head["generation"]
                                 and _one(side, "SELECT reference_sha256 FROM activity_coverage_manifests_v2 WHERE generation = ?", (generation,)) == head["digest"])
            return (*values, int(bulk), int(activity_complete))

        if head is None or (head == prior_identity and head["generation"] == previous):
            if after_collection:
                raise ValueError("activity collection did not complete this cycle's head")
            return targets(initial)
        generation = head["generation"]
        top_up_used = generation != initial
        initial_identity = head
        if top_up_used:
            if generation < initial or head["version"] < 2 or head.get("base_generation") != initial:
                raise ValueError("candidate head is outside this cycle's single top-up allowance")
            initial_identity = _fresh_identity(_one(side, "SELECT collection_identity_json FROM activity_coverage_manifests_v2 WHERE generation = ?", (initial,)))
            if initial_identity is None or initial_identity["generation"] != initial:
                raise ValueError("top-up omitted this cycle's initial identity")
        if initial_identity["version"] >= 2 and initial_identity["base_generation"] != (previous if prior_identity else None):
            raise ValueError("initial activity head does not belong to this staged cycle")
        if after_collection:
            reference = _one(side, "SELECT reference_sha256 FROM activity_coverage_manifests_v2 WHERE generation = ?", (generation,))
            if reference != head["digest"]:
                raise ValueError("activity head has no matching completed manifest")
            if now is None:
                import time
                now = int(time.time())
            if max_staleness_hours is None:
                from push_ranking_to_supabase import build_parser
                max_staleness_hours = build_parser().get_default("max_cache_staleness_hours")
            age = now - head["fixed_end_unix"]
            if age < 0:
                raise ValueError("activity head fixed end is in the future")
            if top_up_used:
                # The last allowed head proceeds while it can still pass the publisher.
                if age > max_staleness_hours * 3600:
                    raise ValueError("activity top-up is stale; preserve the cycle, no second top-up is permitted")
            elif head["version"] < 4 or age + TOP_UP_RESERVE_HOURS * 3600 > max_staleness_hours * 3600:
                generation = initial + 1
        return targets(generation)


def parse_args():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    capture = subparsers.add_parser("capture")
    capture.add_argument("--db", required=True, type=Path)
    capture.add_argument("--day-utc", required=True)
    capture.add_argument("--versions-file", required=True, type=Path)
    capture.add_argument("--configuration-file", required=True, type=Path)
    capture.add_argument("--output", required=True, type=Path)
    compare = subparsers.add_parser("unchanged")
    compare.add_argument("--db", required=True, type=Path)
    compare.add_argument("--current", required=True, type=Path)
    compare.add_argument("--root", required=True, type=Path)
    retire = subparsers.add_parser("retire-completed")
    retire.add_argument("--root", required=True, type=Path)
    retire.add_argument("--out-dir", type=Path)
    installed = subparsers.add_parser("installed-request",
                                      help="print the newest accepted candidate cycle's request, if any")
    installed.add_argument("--root", required=True, type=Path)
    targets = subparsers.add_parser("candidate-targets")
    targets.add_argument("--prior", type=Path, help="legacy immutable prior; new cycles read staging evidence")
    targets.add_argument("--side", type=Path, required=True)
    targets.add_argument("--after-collection", action="store_true")
    targets.add_argument("--max-staleness-hours", type=int)
    targets.add_argument("--include-bulk-root", action="store_true",
                         help="append bulk eligibility and activity completion (0/1); default output stays three lines")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.command == "retire-completed":
        return retire_completed_cycle(args.root, args.out_dir)
    if args.command == "installed-request":
        cycle = accepted_candidate_cycle(args.root)
        if cycle is not None and (cycle / "ranking_publish_request.json").is_file():
            print(cycle / "ranking_publish_request.json")
        return 0
    if args.command == "candidate-targets":
        for value in candidate_targets(args.prior, args.side, after_collection=args.after_collection,
                                       max_staleness_hours=args.max_staleness_hours,
                                       include_bulk_root=args.include_bulk_root):
            print(value)
        return 0
    if args.command == "capture":
        versions = json.loads(args.versions_file.read_text(encoding="utf-8"))
        configuration = json.loads(args.configuration_file.read_text(encoding="utf-8"))
        atomic_write(args.output, snapshot(args.db, args.day_utc, versions, configuration))
        return 0
    current = json.loads(args.current.read_text(encoding="utf-8"))
    # The wrapper must retry a same-day partial walk even if no new rows landed.
    connection = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)
    try:
        if partial_backfill_wallets(connection, retryable_only=True):
            return 1
    finally:
        connection.close()
    accepted = latest_accepted(args.root, current["day_utc"])
    if accepted is not None and accepted.get("fingerprint_sha256") == current.get(
        "fingerprint_sha256"
    ):
        print(accepted["fingerprint_sha256"])
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
