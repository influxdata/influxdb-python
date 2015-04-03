# -*- coding: utf-8 -*-

import unittest

from influxdb.resultset import ResultSet
from influxdb.point import Point


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
                Point("cpu_load_short", ["time", "value"],
                      ["2015-01-29T21:51:28.968422294Z", 0.64],
                      tags={"host": "server01", "region": "us-west"}),
                Point("cpu_load_short", ["time", "value"],
                      ["2015-01-29T21:51:28.968422294Z", 0.64],
                      tags={"host": "server02", "region": "us-west"})
            ]
        )

    def test_filter_by_tags(self):
        self.assertEqual(
            list(self.rs[('cpu_load_short', {"host": "server01"})]),
            [
                Point(
                    "cpu_load_short", ["time", "value"],
                    ["2015-01-29T21:51:28.968422294Z", 0.64],
                    tags={"host": "server01", "region": "us-west"}
                )
            ]
        )

        self.assertEqual(
            list(self.rs[('cpu_load_short', {"region": "us-west"})]),
            [
                Point("cpu_load_short", ["time", "value"],
                      ["2015-01-29T21:51:28.968422294Z", 0.64],
                      tags={"host": "server01", "region": "us-west"}),
                Point("cpu_load_short", ["time", "value"],
                      ["2015-01-29T21:51:28.968422294Z", 0.64],
                      tags={"host": "server02", "region": "us-west"}),
            ]
        )
