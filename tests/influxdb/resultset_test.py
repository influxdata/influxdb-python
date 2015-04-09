# -*- coding: utf-8 -*-

import unittest

from influxdb.resultset import ResultSet


class TestResultSet(unittest.TestCase):

    def setUp(self):
        self.query_response = {
            "results": [
                {"series": [{"name": "cpu_load_short",
                             "tags": {"host": "server01",
                                      "region": "us-west"},
                             "columns": ["time", "value"],
                            "values": [
                                ["2015-01-29T21:51:28.968422294Z", 0.64]
                             ]},
                            {"name": "cpu_load_short",
                             "tags": {"host": "server02",
                                      "region": "us-west"},
                             "columns": ["time", "value"],
                            "values": [
                                ["2015-01-29T21:51:28.968422294Z", 0.64]
                             ]},
                            {"name": "other_serie",
                             "tags": {"host": "server01",
                                      "region": "us-west"},
                             "columns": ["time", "value"],
                            "values": [
                                ["2015-01-29T21:51:28.968422294Z", 0.64]
                             ]}]}
            ]
        }
        self.rs = ResultSet(self.query_response)

    def test_filter_by_name(self):
        self.assertEqual(
            list(self.rs['cpu_load_short']),
            [
                {'value': 0.64, 'time': '2015-01-29T21:51:28.968422294Z'},
                {'value': 0.64, 'time': '2015-01-29T21:51:28.968422294Z'}
            ]
        )

    def test_filter_by_tags(self):
        self.assertEqual(
            list(self.rs[('cpu_load_short', {"host": "server01"})]),
            [{'time': '2015-01-29T21:51:28.968422294Z', 'value': 0.64}]
        )

        self.assertEqual(
            list(self.rs[('cpu_load_short', {"region": "us-west"})]),
            [
                {'value': 0.64, 'time': '2015-01-29T21:51:28.968422294Z'},
                {'value': 0.64, 'time': '2015-01-29T21:51:28.968422294Z'}
            ]
        )

    def test_keys(self):
        self.assertEqual(
            self.rs.keys(),
            [
                ('cpu_load_short', {'host': 'server01', 'region': 'us-west'}),
                ('cpu_load_short', {'host': 'server02', 'region': 'us-west'}),
                ('other_serie', {'host': 'server01', 'region': 'us-west'})
            ]
        )

    def test_len(self):
        self.assertEqual(
            len(self.rs),
            3
        )

    def test_items(self):
        items = list(self.rs.items())
        items_lists = [(item[0], list(item[1])) for item in items]

        self.assertEqual(
            items_lists,
            [
                (
                    ('cpu_load_short',
                     {'host': 'server01', 'region': 'us-west'}),
                    [{'value': 0.64, 'time': '2015-01-29T21:51:28.968422294Z'}]
                ),
                (
                    ('cpu_load_short',
                     {'host': 'server02', 'region': 'us-west'}),
                    [{'value': 0.64, 'time': '2015-01-29T21:51:28.968422294Z'}]
                ),
                (
                    ('other_serie',
                     {'host': 'server01', 'region': 'us-west'}),
                    [{'value': 0.64, 'time': '2015-01-29T21:51:28.968422294Z'}]
                )
            ]
        )

    def test_point_from_cols_vals(self):
        cols = ['col1', 'col2']
        vals = [1, '2']

        point = ResultSet.point_from_cols_vals(cols, vals)
        self.assertDictEqual(
            point,
            {'col1': 1, 'col2': '2'}
        )

    def test_system_query(self):
        rs = ResultSet(
            {'results': [
                {'series': [
                    {'values': [['another', '48h0m0s', 3, False],
                                ['default', '0', 1, False],
                                ['somename', '24h0m0s', 4, True]],
                     'columns': ['name', 'duration',
                                 'replicaN', 'default']}]}
            ]
            }
        )

        self.assertEqual(
            rs.keys(),
            [('results', None)]
        )

        self.assertEqual(
            list(rs['results']),
            [
                {'duration': '48h0m0s', 'default': False, 'replicaN': 3,
                 'name': 'another'},
                {'duration': '0', 'default': False, 'replicaN': 1,
                 'name': 'default'},
                {'duration': '24h0m0s', 'default': True, 'replicaN': 4,
                 'name': 'somename'}
            ]
        )
