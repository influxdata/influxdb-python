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
        self.assertItemsEqual(
            self.rs['cpu_load_short'],
            [
                {
                    "tags": {"host": "server01", "region": "us-west"},
                    "points": [
                        {"time": "2015-01-29T21:51:28.968422294Z",
                         "value": 0.64}
                    ]
                },
                {
                    "tags": {"host": "server02", "region": "us-west"},
                    "points": [
                        {"time": "2015-01-29T21:51:28.968422294Z",
                         "value": 0.64}
                    ]
                }
            ]
        )

    def test_filter_by_tags(self):
        self.assertItemsEqual(
            self.rs[('cpu_load_short', {"host": "server01"})],
            [
                {
                    "tags": {"host": "server01", "region": "us-west"},
                    "points": [
                        {"time": "2015-01-29T21:51:28.968422294Z",
                         "value": 0.64}
                    ]
                }
            ]
        )

        self.assertItemsEqual(
            self.rs[('cpu_load_short', {"region": "us-west"})],
            [
                {
                    "tags": {"host": "server01", "region": "us-west"},
                    "points": [
                        {"time": "2015-01-29T21:51:28.968422294Z",
                         "value": 0.64}
                    ]
                },
                {
                    "tags": {"host": "server02", "region": "us-west"},
                    "points": [
                        {"time": "2015-01-29T21:51:28.968422294Z",
                         "value": 0.64}
                    ]
                }
            ]
        )

