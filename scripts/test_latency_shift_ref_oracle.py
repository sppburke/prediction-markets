#!/usr/bin/env python3
"""Deterministic first-entry taker-print, horizon and survival tests (#757)."""
from __future__ import annotations

import csv
import json
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import duckdb

import latency_shift_rerank as ranker
from test_ranker_duck_parity import all_markets_scope, write_market_scope

WALLET = '0x' + '1' * 40
DAY = 1_776_384_000 // 86400 * 86400
FIELDS = ['wallet', 'market_id', 'outcome_id', 'entry_ts', 'ttr_secs', 'resolved_at', 'payoff']


def position(market, entry=DAY + 100, *, marker=DAY, payoff=1):
    return (WALLET, market, '0', entry, marker - entry, marker, payoff)


def first_entry(row, px=.4, *, fee=True, wallet=None, second=None):
    return (wallet or row[0], row[1], int(row[2]), second if second is not None else row[3],
            '10', str(px * 10), str(px * 10 + (.01 if fee else 0)))


def args(root, **overrides):
    result = SimpleNamespace(positions_csv=str(root / 'positions.csv'), min_trl=0,
                             market_scope=str(root / 'market_scope.json'),
                             min_ttr_secs=60, ttr_max_secs=86400, latency_shift_secs=2.,
                             fill_window_secs=120., slip_cents=.5, price_min=.15, price_max=.65,
                             floor_tstat=3., half_life_days=0., schema_two=True)
    result.__dict__.update(overrides)
    return result


class PrintOracle(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.con = duckdb.connect()
        self.con.execute('SET threads=4')
        self.con.execute('CREATE TABLE entries (wallet_hex VARCHAR, condition_id VARCHAR, outcome_id INTEGER, '
                         'source_time_unix BIGINT, share_amount_str VARCHAR, '
                         'price_weighted_share_amount_str VARCHAR, source_usdc_amount_str VARCHAR)')
        self.con.execute('CREATE VIEW projection AS SELECT * FROM entries')

    def tearDown(self):
        self.con.close()
        self.tmp.cleanup()

    def write_positions(self, positions, scope=None):
        with open(self.root / 'positions.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(FIELDS)
            writer.writerows(positions)
        write_market_scope(self.root / 'market_scope.json',
                           scope if scope is not None else {row[1] for row in positions})

    def run_pipeline(self, positions, prints, **overrides):
        self.write_positions(positions)
        self.con.executemany('INSERT INTO entries VALUES (?,?,?,?,?,?,?)',
                             [first_entry(row) for row in positions] + prints)
        a = args(self.root, **overrides)
        ranker.load_positions(self.con, a)
        ranker.reprice_positions(self.con, a)
        ranker.score_wallets(self.con, a, DAY + 86400)
        return a

    def outcomes(self):
        return {row[0]: row[1:] for row in self.con.execute(
            'SELECT market_id, outcome, sample_t, sample_price, net FROM positions').fetchall()}

    def test_first_second_max_fee_print_and_window_boundaries(self):
        positions = [position(m) for m in ('two', 'last', 'late', 'tie', 'maker', 'own')]
        prints = []
        for row in positions:
            name = row[1]
            if name == 'own':
                continue  # The candidate's own first entry is outside its lookup window.
            offset = {'two': 2, 'last': 120, 'late': 121, 'tie': 2, 'maker': 2}[name]
            prints.append(first_entry(row, .4, wallet='other', second=row[3] + offset,
                                      fee=name != 'maker'))
            if name == 'tie':
                prints.extend([first_entry(row, .6, wallet='higher', second=row[3] + 2),
                               first_entry(row, .64, wallet='later', second=row[3] + 3),
                               first_entry(row, .9, wallet='maker', second=row[3] + 2, fee=False),
                               first_entry(row, .2, wallet='early', second=row[3] + 1)])
        self.run_pipeline(positions, prints)
        results = self.outcomes()
        self.assertEqual(results['two'][0:2], ('repriced', DAY + 102))
        self.assertEqual(results['last'][0:2], ('repriced', DAY + 220))
        for market in ('late', 'maker', 'own'):
            self.assertEqual(results[market], ('no_print', None, None, None))
        self.assertEqual(results['tie'][0:3], ('repriced', DAY + 102, .6))
        eff = .605
        self.assertAlmostEqual(results['tie'][3], 1 / (eff * (1 + .07 * (1 - eff))) - 1)

    def test_fee_threshold_is_strict_and_does_not_stop_the_first_print_search(self):
        positions = [position('a'), position('b')]
        prints = [first_entry(row, wallet='equal', second=row[3] + 2) for row in positions]
        self.con.executemany('INSERT INTO entries VALUES (?,?,?,?,?,?,?)',
                             [(*row[:-1], '4.002') for row in prints])
        self.run_pipeline(positions, [(*first_entry(row, wallet='over', second=row[3] + 3)[:-1], '4.002001')
                                      for row in positions])
        self.assertTrue(all(value[0:2] == ('repriced', DAY + 103) for value in self.outcomes().values()))

    def test_future_entry_and_print_tail_are_independent_of_decay_anchor(self):
        positions = [position('a', DAY + 1000), position('b', DAY + 2000)]
        prints = [first_entry(row, wallet='other', second=row[3] + 120) for row in positions]
        self.run_pipeline(positions, prints)
        self.assertEqual(self.con.execute("SELECT COUNT(*) FROM positions WHERE outcome='repriced'").fetchone()[0], 2)
        # Entries and prints after the anchor still score (decay clips future ages).
        self.con.execute('DROP VIEW ranked')
        self.con.execute('DROP TABLE scores')
        ranker.score_wallets(self.con, args(self.root), DAY)
        self.assertEqual(next(ranker.ranked_rows(self.con))['n_filled'], '2')

    def test_effective_price_band_and_payoff(self):
        positions = [position(m, payoff=.5) for m in ('floor', 'below', 'ceiling', 'inside')]
        prices = [.145, .144, .645, .644]
        self.run_pipeline(positions, [first_entry(row, px, wallet='other', second=row[3] + 2)
                                      for row, px in zip(positions, prices)])
        results = self.outcomes()
        self.assertEqual({key: value[0] for key, value in results.items()},
                         {'floor': 'repriced', 'below': 'price_band', 'ceiling': 'price_band', 'inside': 'repriced'})
        self.assertAlmostEqual(results['floor'][3], .5 / (.15 * (1 + .07 * .85)) - 1)
        row = next(ranker.ranked_rows(self.con))
        self.assertEqual(row['fill_rate'], '1.0')
        self.assertEqual(row['n_total'], 4)
        self.assertEqual(row['n_filled'], '2')

    def test_maker_denominator_precedes_scope_price_pair_and_pricing_filters(self):
        positions = [position('priced'), position('missing'), position('band'),
                     position('horizon', DAY + 86400 - 61)]
        self.write_positions(positions)
        entries = [first_entry(row, fee=index < 2) for index, row in enumerate(positions)]
        entries += [first_entry(positions[0], wallet='other', second=DAY + 102),
                    first_entry(positions[2], .9, wallet='other', second=DAY + 102),
                    first_entry(positions[0], fee=False, wallet=WALLET, second=DAY + 86400 + 1)]
        self.con.executemany('INSERT INTO entries VALUES (?,?,?,?,?,?,?)', entries)
        a = args(self.root)
        ranker.load_positions(self.con, a)
        self.assertEqual(self.con.execute('SELECT maker_share FROM maker_shares').fetchone()[0], .5)
        ranker.reprice_positions(self.con, a)
        self.assertEqual(self.outcomes()['horizon'][0], 'scheduled_horizon')
        ranker.score_wallets(self.con, a, DAY + 86400)
        row = next(ranker.ranked_rows(self.con))
        self.assertEqual(row['maker_share'], '0.5')
        self.assertFalse(row['eligible'])
        self.assertEqual(row['fill_rate'], '0.5')

    def test_survival_gates_and_coverage_is_reported_without_a_gate(self):
        # Ten entry days: survival's t is clustered by day, so one day alone cannot survive.
        positions = [position(str(index), DAY + index % 10 * 86400 + 100, marker=DAY + index % 10 * 86400)
                     for index in range(61)]
        self.run_pipeline(positions, [first_entry(row, .3 + index / 1000, wallet='other', second=row[3] + 2)
                                      for index, row in enumerate(positions[:20])], min_trl=20)
        row = next(ranker.ranked_rows(self.con))
        self.assertTrue(row['survives'])
        self.assertEqual(row['n_filled'], '20')
        self.assertEqual(row['fill_rate'], str(round(20 / 61, 4)))
        for n, maker, t, mean, days, eligible, survives in (
            (20, .499, 3., .1, 10, True, True), (20, .5, 4., .1, 10, False, False),
            (19, 0., 4., .1, 10, False, False), (1, 0., 4., .1, 10, False, False),
            (20, 0., 2.999, .1, 10, True, False), (20, 0., 3., 0., 10, True, False),
            (20, 0., 4., .1, 9, False, False),
        ):
            with self.subTest(n=n, maker=maker, t=t, mean=mean, days=days):
                self.con.execute('DROP VIEW ranked'); self.con.execute('DROP TABLE scores')
                self.con.execute("UPDATE positions SET outcome=CASE WHEN ordinal <= ? THEN 'repriced' ELSE 'no_print' END", [n])
                self.con.execute('UPDATE maker_shares SET maker_share=?', [maker])
                # The unclustered t (99) is ignored: survival reads the day-clustered t.
                with mock.patch.object(ranker, 'weighted_stats', return_value=(mean, 1., n, 99.)), \
                        mock.patch.object(ranker, 'day_clustered_tstat', return_value=(t, days)):
                    ranker.score_wallets(self.con, args(self.root, min_trl=20), DAY + 86400)
                result = next(ranker.ranked_rows(self.con))
                self.assertEqual((result['eligible'], result['survives']), (eligible, survives))

    def test_manifest_identity_and_removed_flags(self):
        with mock.patch.object(sys, 'argv', ['rank', '--ranked-csv', 'r', '--positions-csv', 'p',
                                             '--market-scope', 's']):
            a = ranker.parse_args()
        self.assertEqual((a.slip_cents, a.floor_tstat, a.min_ttr_secs, a.ttr_max_secs, a.price_max),
                         (.5, 3., 60, 86400, .65))
        self.assertEqual((ranker.ORACLE_NAME, ranker.ORACLE_VERSION, ranker.TAKER_FEE_RATE, ranker.MAKER_SHARE_CAP,
                          ranker.MIN_ACTIVE_DAYS), ('first-entry-taker-print', 8, .07, .5, 10))
        self.assertIn('maker_share', ranker.RANKED_FIELDS)
        for removed in ('emit_targets', 'min_fill_rate', 'min_active_months', 'min_avg_per_month'):
            self.assertFalse(hasattr(a, removed))

    def test_every_pass_one_row_must_be_in_the_scope_it_covers(self):
        other = '0x' + '2' * 40
        # The out-of-scope row belongs to a wallet that could never survive (one entry, MinTRL 20).
        positions = [position('in'), (other, 'out', '0', DAY + 100, -100, DAY, 1)]
        self.write_positions(positions, scope=['in'])
        with self.assertRaisesRegex(ValueError, '1 pass-one market\\(s\\) outside the market scope'):
            ranker.load_positions(self.con, args(self.root, min_trl=20))
        for floor, completed in ((DAY + 100 - 86_399, 2**62), (0, DAY + 99)):
            with self.subTest(floor=floor, completed=completed):
                self.write_positions(positions[:1])
                write_market_scope(self.root / 'market_scope.json', ['in'],
                                   end_date_min=floor, completed_at=completed)
                with self.assertRaisesRegex(ValueError, 'does not cover'):
                    ranker.load_positions(duckdb.connect(), args(self.root))


class ManifestRun(unittest.TestCase):
    def test_real_export_manifest_identity_hashes_and_spill_retry(self):
        import hashlib
        import os
        from test_ranker_duck_parity import build_format_three_cache, _export_certified_cache
        from ranker_duck import get_engine, duck_extract_positions_v2

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            db = root / 'cache.db'
            build_format_three_cache(str(db))
            parquet = root / 'parquet'
            projection = _export_certified_cache(str(db), str(parquet))
            self.assertEqual(projection['oracle_version'], 6)
            positions = root / 'positions.csv'
            engine = get_engine(force='duck', parquet_dir=str(parquet), schema_version=2)
            with positions.open('w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(['wallet','market_id','outcome_id','entry_ts','ttr_secs','price','contracts','payoff','resolved_at'])
                for chunk in duck_extract_positions_v2(engine, ['0x' + 'a' * 40, '0x' + 'b' * 40], 0, 2**62):
                    writer.writerows(chunk.itertuples(index=False, name=None))
            engine.close()
            ranked = root / 'ranked.csv'; ranked.write_text('wallet\n')
            before = root / 'before.json'; before.write_text('[]')
            cycle = root / 'cycle.json'; cycle.write_text('{}')
            stage = root / 'stage.json'; stage.write_text('{}')
            versions = root / 'versions.json'
            versions.write_text(json.dumps(dict(source='polymarket-public-activity', activity_schema=2,
                                                activity_parser=2, clob_resolution_schema=2,
                                                clob_resolution_parser=2, cache_schema=2, configuration=1)))
            out = root / 'out'; out.mkdir()
            unrelated = out / 'pass2-unrelated'; unrelated.mkdir(); (unrelated / 'keep').write_text('retained')
            scope = all_markets_scope(str(db), root / 'scope.json')
            argv = ['rank', '--db', str(db), '--ranked-csv', str(ranked), '--positions-csv', str(positions),
                    '--out-dir', str(out), '--as-of', '2026-04-01', '--before-ranking-json', str(before),
                    '--cycle-manifest-file', str(cycle), '--cache-stage-record', str(stage),
                    '--pipeline-versions-file', str(versions), '--market-scope', scope]
            manifests = []
            for _ in range(2):
                spill = out / 'pass2-spill'; spill.mkdir(); (spill / 'stale').write_text('old')
                with mock.patch.dict(os.environ, {'PE_RANKER_PARQUET_DIR': str(parquet)}), mock.patch.object(sys, 'argv', argv):
                    self.assertEqual(ranker.main(), 0)
                self.assertFalse(spill.exists())
                self.assertEqual((unrelated / 'keep').read_text(), 'retained')
                manifests.append((out / 'oracle_manifest.json').read_bytes())
            self.assertEqual(manifests[0], manifests[1])
            manifest = json.loads(manifests[0])
            self.assertEqual((manifest['oracle'], manifest['oracle_version'], manifest['slip'],
                              manifest['fee_rate'], manifest['fill_window_secs'], manifest['maker_share_cap'],
                              manifest['min_active_days']),
                             ('first-entry-taker-print', 8, .005, .07, 120., .5, 10))
            scope_bytes = Path(scope).read_bytes()
            self.assertEqual(manifest['inputs']['market_scope_sha256'], hashlib.sha256(scope_bytes).hexdigest())
            self.assertEqual(manifest['market_scope'], {key: json.loads(scope_bytes)[key] for key in
                             ('recurrence', 'count', 'digest', 'end_date_min_unix', 'completed_at_unix')})
            for key in ('fidelity_minutes','parser_version','lookup','min_coverage','min_active_months','min_avg_per_month'):
                self.assertNotIn(key, manifest)
            self.assertNotIn('oracle_parser', manifest['versions'])
            self.assertNotIn('oracle_targets_sha256', manifest['inputs'])
            for filename, key in (('latency_shift_ranked.csv','latency_shift_ranked_sha256'),
                                  ('oracle_outcomes.csv','oracle_outcomes_sha256')):
                self.assertEqual(manifest['outputs'][key], hashlib.sha256((out / filename).read_bytes()).hexdigest())
            self.assertEqual(manifest['price_band']['maximum_exclusive'], .65)
            self.assertEqual(manifest['scheduled_horizon']['maximum_secs_exclusive'], 86400)
            # A damaged certified export fails before writing another result.
            with (parquet / 'projection.parquet').open('ab') as file:
                file.write(b'changed')
            with mock.patch.dict(os.environ, {'PE_RANKER_PARQUET_DIR': str(parquet)}), mock.patch.object(sys, 'argv', argv), \
                    self.assertRaisesRegex(RuntimeError, 'Parquet hash mismatch'):
                ranker.main()
            self.assertFalse((out / 'pass2-spill').exists())


class Horizon(unittest.TestCase):
    def test_non_positive_shift_or_window_is_refused(self):
        # A zero shift would let a wallet's own first entry be its copy print.
        for flag in ('--latency-shift-secs', '--fill-window-secs'):
            argv = ['latency_shift_rerank.py', '--ranked-csv', 'unused.csv', '--positions-csv', 'unused.csv',
                    '--market-scope', 'unused.json', flag, '0']
            with self.subTest(flag=flag), mock.patch.object(sys, 'argv', argv):
                self.assertEqual(ranker.main(), 1)

    def test_day_markers_floor_and_exclusive_ceiling(self):
        a = SimpleNamespace(min_ttr_secs=60, ttr_max_secs=86400, latency_shift_secs=2)
        for marker in ('2026-04-17T00:00:00+00:00', '2026-04-17T12:00:00+00:00',
                       '2026-04-17T23:00:00+05:00'):
            epoch = int(datetime.fromisoformat(marker).timestamp())
            day_end = (epoch // 86400 + 1) * 86400
            for remaining, expected in ((59, False), (60, True), (86399, True), (86400, False)):
                entry = day_end - remaining - 2
                with self.subTest(marker=marker, remaining=remaining):
                    self.assertEqual(ranker.in_horizon(entry, epoch - entry, a), expected)

    def test_zero_bounds_and_overflow_fail_closed(self):
        a = SimpleNamespace(min_ttr_secs=0, ttr_max_secs=0, latency_shift_secs=2)
        self.assertTrue(ranker.in_horizon(DAY + 86400, -86400, a))
        self.assertTrue(ranker.in_horizon(DAY - 86400, 86400, a))
        self.assertFalse(ranker.in_horizon(2**63 - 1, 0, a))
        self.assertFalse(ranker.in_horizon(0, 2**63, a))
        a.latency_shift_secs = 0
        self.assertFalse(ranker.in_horizon(-2**63, 2**63 - 1, a))


if __name__ == '__main__':
    unittest.main()
