#!/usr/bin/env python3
"""Deterministic guard for the reusable pure logic in `scripts/probe_gamma_ua.py` (issue #382 Phase 0).

The probe itself makes live Gamma/CLOB calls and is never run in CI. This test covers only its
network-free helpers — `build_url` (the repeat-key batch URL shape the Rust `GammaMarketsClient`
must reproduce byte-for-byte) and `demux` (index-by-conditionId). No network, deterministic.

Importing `probe_gamma_ua` runs only module-level constants/defs — no live calls fire.

Run: `python3 scripts/test_probe_gamma_ua.py`
  or: `pytest scripts/test_probe_gamma_ua.py -v`
"""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import probe_gamma_ua as p  # noqa: E402


class TestBuildUrl(unittest.TestCase):
    def test_repeat_key_closed(self):
        url = p.build_url("https://g/markets", ["0xA", "0xB"], closed=True, limit=500)
        self.assertEqual(url, "https://g/markets?condition_ids=0xA&condition_ids=0xB&closed=true&limit=500")
        print("PASS: repeat-key &closed=true URL matches the proven backfill_end_dates.py:40 shape")

    def test_repeat_key_plain_omits_closed(self):
        url = p.build_url("https://g/markets", ["0xA", "0xB"], closed=False, limit=500)
        self.assertNotIn("closed=true", url)
        self.assertEqual(url, "https://g/markets?condition_ids=0xA&condition_ids=0xB&limit=500")
        print("PASS: plain (open) URL omits &closed=true")

    def test_param_order_condition_ids_then_closed_then_limit(self):
        url = p.build_url("https://g/markets", ["0xA"], closed=True, limit=200)
        self.assertLess(url.index("condition_ids="), url.index("closed=true"))
        self.assertLess(url.index("closed=true"), url.index("limit=200"))
        print("PASS: query-param order is condition_ids → closed → limit (deterministic for fixture keying)")

    def test_single_id(self):
        url = p.build_url("https://g/markets", ["0xA"], closed=True, limit=500)
        self.assertEqual(url.count("condition_ids="), 1)
        print("PASS: single-id URL has exactly one condition_ids key")

    def test_comma_form_is_distinct_and_single_key(self):
        url = p.build_url("https://g/markets", ["0xA", "0xB"], closed=True, limit=500, comma=True)
        self.assertEqual(url.count("condition_ids="), 1)
        self.assertIn("condition_ids=0xA,0xB", url)
        print("PASS: comma form joins ids under one key (the known-broken shape the probe disproves)")

    def test_input_order_preserved(self):
        ids = ["0xC", "0xA", "0xB"]
        url = p.build_url("https://g/markets", ids, closed=False, limit=500)
        keys = [seg.split("=")[1] for seg in url.split("?")[1].split("&") if seg.startswith("condition_ids=")]
        self.assertEqual(keys, ids)
        print("PASS: input id order is preserved (no sort) — batch URLs are deterministic")


class TestDemux(unittest.TestCase):
    def test_indexes_by_condition_id(self):
        data = [{"conditionId": "0xA", "endDate": "x"}, {"conditionId": "0xB", "endDate": "y"}]
        out = p.demux(data)
        self.assertEqual(set(out.keys()), {"0xA", "0xB"})
        self.assertEqual(out["0xA"]["endDate"], "x")
        print("PASS: demux indexes each market by conditionId")

    def test_skips_entries_without_condition_id(self):
        data = [{"endDate": "x"}, {"conditionId": "0xB"}]
        out = p.demux(data)
        self.assertEqual(set(out.keys()), {"0xB"})
        print("PASS: demux drops rows with no conditionId")

    def test_missing_id_absent_from_map(self):
        out = p.demux([{"conditionId": "0xA"}])
        self.assertIsNone(out.get("0xZZZ"))  # caller stores NULL for ids Gamma doesn't return
        print("PASS: ids Gamma omits are absent from the map (caller → NULL)")


if __name__ == "__main__":
    unittest.main(verbosity=2)
