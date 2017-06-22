# -*- coding: utf-8 -*-
"""Define the resultset test package."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unittest

from influxdb.exceptions import InfluxDBClientError
from influxdb.resultset import ResultSet


class TestResultSet(unittest.TestCase):
    """Define the ResultSet test object."""

    def setUp(self):
        """Set up an instance of TestResultSet."""
        self.query_response = {
            "results": [
                {"series": [{"measurement": "cpu_load_short",
                             "tags": {"host": "server01",
                                      "region": "us-west"},
                             "columns": ["time", "value"],
                            "values": [
                                ["2015-01-29T21:51:28.968422294Z", 0.64]
                             ]},
                            {"measurement": "cpu_load_short",
                             "tags": {"host": "server02",
                                      "region": "us-west"},
                             "columns": ["time", "value"],
                            "values": [
                                ["2015-01-29T21:51:28.968422294Z", 0.65]
                             ]},
                            {"measurement": "other_serie",
                             "tags": {"host": "server01",
                                      "region": "us-west"},
                             "columns": ["time", "value"],
                            "values": [
                                ["2015-01-29T21:51:28.968422294Z", 0.66]
                             ]}]}
            ]
        }

        self.rs = ResultSet(self.query_response['results'][0])

    def test_filter_by_name(self):
        """Test filtering by name in TestResultSet object."""
        expected = [
            {'value': 0.64, 'time': '2015-01-29T21:51:28.968422294Z'},
            {'value': 0.65, 'time': '2015-01-29T21:51:28.968422294Z'}
        ]

        self.assertEqual(expected, list(self.rs['cpu_load_short']))
        self.assertEqual(expected,
                         list(self.rs.get_points(
                             measurement='cpu_load_short')))

    def test_filter_by_tags(self):
        """Test filter by tags in TestResultSet object."""
        expected = [
            {'time': '2015-01-29T21:51:28.968422294Z', 'value': 0.64},
            {'time': '2015-01-29T21:51:28.968422294Z', 'value': 0.66}
        ]

        self.assertEqual(
            expected,
            list(self.rs[{"host": "server01"}])
        )

        self.assertEqual(
            expected,
            list(self.rs.get_points(tags={'host': 'server01'}))
        )

    def test_filter_by_name_and_tags(self):
        """Test filter by name and tags in TestResultSet object."""
        self.assertEqual(
            list(self.rs[('cpu_load_short', {"host": "server01"})]),
            [{'time': '2015-01-29T21:51:28.968422294Z', 'value': 0.64}]
        )

        self.assertEqual(
            list(self.rs[('cpu_load_short', {"region": "us-west"})]),
            [
                {'value': 0.64, 'time': '2015-01-29T21:51:28.968422294Z'},
                {'value': 0.65, 'time': '2015-01-29T21:51:28.968422294Z'}
            ]
        )

    def test_keys(self):
        """Test keys in TestResultSet object."""
        self.assertEqual(
            self.rs.keys(),
            [
                ('cpu_load_short', {'host': 'server01', 'region': 'us-west'}),
                ('cpu_load_short', {'host': 'server02', 'region': 'us-west'}),
                ('other_serie', {'host': 'server01', 'region': 'us-west'})
            ]
        )

    def test_len(self):
        """Test length in TestResultSet object."""
        self.assertEqual(
            len(self.rs),
            3
        )

    def test_items(self):
        """Test items in TestResultSet object."""
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
                    [{'value': 0.65, 'time': '2015-01-29T21:51:28.968422294Z'}]
                ),
                (
                    ('other_serie',
                     {'host': 'server01', 'region': 'us-west'}),
                    [{'value': 0.66, 'time': '2015-01-29T21:51:28.968422294Z'}]
                )
            ]
        )

    def test_point_from_cols_vals(self):
        """Test points from columns in TestResultSet object."""
        cols = ['col1', 'col2']
        vals = [1, '2']

        point = ResultSet.point_from_cols_vals(cols, vals)
        self.assertDictEqual(
            point,
            {'col1': 1, 'col2': '2'}
        )

    def test_system_query(self):
        """Test system query capabilities in TestResultSet object."""
        rs = ResultSet(
            {'series': [
                {'values': [['another', '48h0m0s', 3, False],
                            ['default', '0', 1, False],
                            ['somename', '24h0m0s', 4, True]],
                 'columns': ['name', 'duration',
                             'replicaN', 'default']}]}
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

    def test_resultset_error(self):
        """Test returning error in TestResultSet object."""
        with self.assertRaises(InfluxDBClientError):
            ResultSet({
                "series": [],
                "error": "Big error, many problems."
            })
