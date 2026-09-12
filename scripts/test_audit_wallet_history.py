#!/usr/bin/env python3
"""Writer/auditor parity and fail-closed acquisition scenarios for #608/#609."""
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import audit_wallet_history as audit


def row(tid="t", timestamp=1000, **changes):
    return dict(transactionHash=tid, conditionId="market", side="BUY", size="1",
                price="0.5", timestamp=timestamp, outcomeIndex=0, **changes)


def converted(rows):
    return [value for r in audit.parse_page(json.dumps(rows))
            if (value := audit.convert_row(r)) is not None]


class AuditHistoryTest(unittest.TestCase):
    def test_dto_validation_precedes_conversion(self):
        for key, value in [("price", "broken"), ("size", "broken"), ("timestamp", "1000"),
                           ("timestamp", 2**63), ("outcomeIndex", 65536), ("side", None)]:
            bad = {**row(), key: value}
            with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                audit.parse_page(json.dumps([row("valid"), bad]))

    def test_each_conversion_rejection_and_normalization(self):
        for key, value in [("price", "-0.1"), ("price", "1.1"), ("size", "0"),
                           ("size", "-1"), ("size", str(2**64)), ("side", "UNKNOWN"),
                           ("timestamp", -(2**63)), ("timestamp", 2**63-1)]:
            with self.subTest(key=key, value=value):
                self.assertEqual(converted([{**row(), key: value}]), [])
        for timestamp in [1_704_067_200, 1_704_067_200_000]:
            value = converted([{**row(timestamp=timestamp), "size": "0.25", "outcomeIndex": None}])[0]
            self.assertEqual(value["timestamp_unix"], 1_704_067_200)
            self.assertEqual(value["contracts"], 1)
            self.assertEqual(value["outcome_id"], 0)
        self.assertEqual(converted([{**row(), "size": "3.9"}])[0]["contracts"], 3)
        self.assertEqual(converted([{**row(), "size": str(2**64-1)}])[0]["contracts"], 2**64-1)
        # A valid Decimal above u64 is a row rejection, not a DTO-page error.
        self.assertEqual(converted([{**row(), "size": str(2**96-1)}]), [])
        for price in ["0", "1"]:
            self.assertEqual(len(converted([{**row(), "price": price}])), 1)
        self.assertEqual(converted([{**row(), "size": "0." + "0" * 28 + "1"}]), [])
        self.assertEqual(converted([{**row(), "size": "0." + "0" * 28 + "5"}])[0]["contracts"], 1)
        with self.assertRaises(ValueError):
            audit.parse_page(json.dumps([{**row(), "size": "1e-29"}]))

    def test_writer_lexicon_parity(self):
        # A repeated field cannot be expressed with a dict literal, so this
        # fixture is raw JSON. json.loads would silently keep "0.5".
        valid = ('"transactionHash":"t","conditionId":"market","side":"BUY","size":"1",'
                 '"price":"0.5","timestamp":1000,"outcomeIndex":0')
        # A repeated field cannot be expressed with a dict literal, so these are
        # raw JSON. json.loads would silently keep the last value.
        with self.assertRaises(ValueError):
            audit.parse_page('[{"transactionHash":"t","conditionId":"market","side":"BUY",'
                             '"size":"1","price":"malformed","price":"0.5",'
                             '"timestamp":1000,"outcomeIndex":0}]')
        # serde tracks repeats only for fields it knows; a repeated ignored field
        # is consumed by IgnoredAny, so the writer accepts these pages.
        for ignored in ['"title":"a","title":"b"', '"usdcSize":"1","usdcSize":"2"']:
            with self.subTest(ignored=ignored):
                self.assertEqual(len(audit.parse_page("[{" + valid + "," + ignored + "}]")), 1)
        # serde_json has no NaN/Infinity literals and fails the whole page on one,
        # even inside an ignored field. Python's json accepts all three.
        for constant in ["NaN", "Infinity", "-Infinity"]:
            with self.subTest(constant=constant), self.assertRaises(ValueError):
                audit.parse_page("[{" + valid + ',"usdcSize":' + constant + "}]")
        # Every one of these is accepted by Python's Decimal and rejected by the
        # writer, which fails the whole page: surrounding whitespace, a leading
        # underscore, Unicode digits, and a separator inside the exponent.
        for value in [" 0 ", " 0", "0 ", "\t0", "0\n", " 1e2 ", "_1",
                      "\u0663", "\uff11", "1e1_0"]:
            for field in ("size", "price"):
                with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                    audit.parse_page(json.dumps([{**row(), field: value}]))
        # Forms the writer does accept must keep parsing.
        for value in ["1_0", ".5", "1.", "+1", "-1", "1e2", "1E2", "1e+2", "1.5e3"]:
            with self.subTest(value=value):
                self.assertEqual(len(audit.parse_page(json.dumps([{**row(), "size": value}]))), 1)

    def test_collisions_are_reported_before_id_deduplication(self):
        venue = converted([row(), {**row(), "outcomeIndex": 1}, row()])
        result = audit.compare(venue, venue[:1])
        self.assertEqual(result["missing_ids"], [])
        self.assertEqual(result["extra_ids"], [])
        self.assertFalse(result["ok"])
        collision = result["collisions"][0]
        self.assertEqual(len(collision["normalized_rows"]), 2)
        self.assertEqual(collision["stored_representative"], venue[0])
        self.assertTrue(audit.compare([venue[0], venue[0]], venue[:1])["ok"])

    def venue(self, rows, fail_offset=None):
        calls = []
        def fetch(url):
            query = parse_qs(urlsplit(url).query)
            start, end, offset = (int(query[k][0]) for k in ("start", "end", "offset"))
            calls.append((start, end, offset))
            self.assertEqual(query["sortDirection"], ["DESC"])
            if fail_offset == offset:
                raise OSError("injected later-page failure")
            selected = sorted([r for r in rows if start <= r["timestamp"] <= end],
                              key=lambda r: -r["timestamp"])
            return json.dumps(selected[offset:offset+500]).encode()
        return fetch, calls

    def test_boundary_501_sibling_is_detected_and_complete_cache_passes(self):
        rows = [row(f"t{i}") for i in range(501)] + [row("older", 999)]
        fetch, calls = self.venue(rows)
        acquired = audit.fetch_all_activity("wallet", 1000, fetch=fetch)
        self.assertEqual(calls, [(1,1000,0), (1000,1000,0), (1000,1000,500), (1,999,0)])
        missing = audit.compare(acquired, converted(rows[:500] + rows[501:]))
        self.assertEqual(missing["missing_ids"], ["t500"])
        self.assertTrue(audit.compare(acquired, converted(rows))["ok"])

    def test_saturation_and_later_failure_refuse_partial_comparison(self):
        fetch, _ = self.venue([row(f"t{i}") for i in range(5500)])
        with self.assertRaisesRegex(ValueError, "saturated second"):
            audit.fetch_all_activity("wallet", 1000, fetch=fetch)
        fetch, _ = self.venue([row(f"t{i}") for i in range(501)], fail_offset=500)
        with self.assertRaises(OSError):
            audit.fetch_all_activity("wallet", 1000, fetch=fetch)

    def test_read_only_consistent_marker_frontier_preconditions(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "cache.db"
            with sqlite3.connect(db) as con:
                con.executescript("CREATE TABLE wallets(wallet_hex TEXT, backfill_partial INTEGER, forward_frontier_unix INTEGER);"
                                  "CREATE TABLE trades(source_trade_id TEXT, wallet_hex TEXT, market_id TEXT, outcome_id INTEGER, side TEXT, price_str TEXT, contracts INTEGER, timestamp_unix INTEGER);"
                                  "INSERT INTO wallets VALUES ('wallet', 1, 1000);")
            for partial, frontier, cutoff, valid in [(1,1000,900,False), (0,None,900,False),
                                                    (0,1000,1001,False), (0,1000,1000,True)]:
                with sqlite3.connect(db) as con:
                    con.execute("UPDATE wallets SET backfill_partial=?, forward_frontier_unix=?", (partial, frontier))
                if valid:
                    self.assertEqual(audit.load_cache(db,"wallet",cutoff), [])
                else:
                    with self.assertRaises(ValueError):
                        audit.load_cache(db,"wallet",cutoff)
            with self.assertRaises(sqlite3.OperationalError):
                audit.load_cache(Path(temp)/"absent.db","wallet",900)
            self.assertFalse((Path(temp)/"absent.db").exists())


if __name__ == "__main__":
    unittest.main(verbosity=2)
