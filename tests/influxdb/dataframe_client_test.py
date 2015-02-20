# -*- coding: utf-8 -*-
"""
unit tests for misc module
"""
from .client_test import _mocked_session

import unittest
import json
import requests_mock
from nose.tools import raises
from datetime import timedelta
from tests import skipIfPYpy, using_pypy
import copy
import warnings

if not using_pypy:
    import pandas as pd
    from pandas.util.testing import assert_frame_equal
    from influxdb import DataFrameClient


@unittest.skip('Not updated for 0.9')
@skipIfPYpy
class TestDataFrameClient(unittest.TestCase):

    def setUp(self):
        # By default, raise exceptions on warnings
        warnings.simplefilter('error', FutureWarning)

    def test_write_points_from_dataframe(self):
        now = pd.Timestamp('1970-01-01 00:00+00:00')
        dataframe = pd.DataFrame(data=[["1", 1, 1.0], ["2", 2, 2.0]],
                                 index=[now, now + timedelta(hours=1)],
                                 columns=["column_one", "column_two",
                                          "column_three"])
        points = [
            {
                "points": [
                    ["1", 1, 1.0, 0],
                    ["2", 2, 2.0, 3600]
                ],
                "name": "foo",
                "columns": ["column_one", "column_two", "column_three", "time"]
            }
        ]

        with requests_mock.Mocker() as m:
            m.register_uri(requests_mock.POST,
                           "http://localhost:8086/db/db/series")

            cli = DataFrameClient(database='db')
            cli.write_points({"foo": dataframe})

            self.assertListEqual(json.loads(m.last_request.body), points)

    def test_write_points_from_dataframe_in_batches(self):
        now = pd.Timestamp('1970-01-01 00:00+00:00')
        dataframe = pd.DataFrame(data=[["1", 1, 1.0], ["2", 2, 2.0]],
                                 index=[now, now + timedelta(hours=1)],
                                 columns=["column_one", "column_two",
                                          "column_three"])
        with requests_mock.Mocker() as m:
            m.register_uri(requests_mock.POST,
                           "http://localhost:8086/db/db/series")

            cli = DataFrameClient(database='db')
            assert cli.write_points({"foo": dataframe},
                                    batch_size=1) is True

    def test_write_points_from_dataframe_with_numeric_column_names(self):
        now = pd.Timestamp('1970-01-01 00:00+00:00')
        # df with numeric column names
        dataframe = pd.DataFrame(data=[["1", 1, 1.0], ["2", 2, 2.0]],
                                 index=[now, now + timedelta(hours=1)])
        points = [
            {
                "points": [
                    ["1", 1, 1.0, 0],
                    ["2", 2, 2.0, 3600]
                ],
                "name": "foo",
                "columns": ['0', '1', '2', "time"]
            }
        ]

        with requests_mock.Mocker() as m:
            m.register_uri(requests_mock.POST,
                           "http://localhost:8086/db/db/series")

            cli = DataFrameClient(database='db')
            cli.write_points({"foo": dataframe})

            self.assertListEqual(json.loads(m.last_request.body), points)

    def test_write_points_from_dataframe_with_period_index(self):
        dataframe = pd.DataFrame(data=[["1", 1, 1.0], ["2", 2, 2.0]],
                                 index=[pd.Period('1970-01-01'),
                                        pd.Period('1970-01-02')],
                                 columns=["column_one", "column_two",
                                          "column_three"])
        points = [
            {
                "points": [
                    ["1", 1, 1.0, 0],
                    ["2", 2, 2.0, 86400]
                ],
                "name": "foo",
                "columns": ["column_one", "column_two", "column_three", "time"]
            }
        ]

        with requests_mock.Mocker() as m:
            m.register_uri(requests_mock.POST,
                           "http://localhost:8086/db/db/series")

            cli = DataFrameClient(database='db')
            cli.write_points({"foo": dataframe})

            self.assertListEqual(json.loads(m.last_request.body), points)

    def test_write_points_from_dataframe_with_time_precision(self):
        now = pd.Timestamp('1970-01-01 00:00+00:00')
        dataframe = pd.DataFrame(data=[["1", 1, 1.0], ["2", 2, 2.0]],
                                 index=[now, now + timedelta(hours=1)],
                                 columns=["column_one", "column_two",
                                          "column_three"])
        points = [
            {
                "points": [
                    ["1", 1, 1.0, 0],
                    ["2", 2, 2.0, 3600]
                ],
                "name": "foo",
                "columns": ["column_one", "column_two", "column_three", "time"]
            }
        ]

        points_ms = copy.deepcopy(points)
        points_ms[0]["points"][1][-1] = 3600 * 1000

        points_us = copy.deepcopy(points)
        points_us[0]["points"][1][-1] = 3600 * 1000000

        with requests_mock.Mocker() as m:
            m.register_uri(requests_mock.POST,
                           "http://localhost:8086/db/db/series")

            cli = DataFrameClient(database='db')

            cli.write_points({"foo": dataframe}, time_precision='s')
            self.assertListEqual(json.loads(m.last_request.body), points)

            cli.write_points({"foo": dataframe}, time_precision='m')
            self.assertListEqual(json.loads(m.last_request.body), points_ms)

            cli.write_points({"foo": dataframe}, time_precision='u')
            self.assertListEqual(json.loads(m.last_request.body), points_us)

    @raises(TypeError)
    def test_write_points_from_dataframe_fails_without_time_index(self):
        dataframe = pd.DataFrame(data=[["1", 1, 1.0], ["2", 2, 2.0]],
                                 columns=["column_one", "column_two",
                                          "column_three"])

        with requests_mock.Mocker() as m:
            m.register_uri(requests_mock.POST,
                           "http://localhost:8086/db/db/series")

            cli = DataFrameClient(database='db')
            cli.write_points({"foo": dataframe})

    @raises(TypeError)
    def test_write_points_from_dataframe_fails_with_series(self):
        now = pd.Timestamp('1970-01-01 00:00+00:00')
        dataframe = pd.Series(data=[1.0, 2.0],
                              index=[now, now + timedelta(hours=1)])

        with requests_mock.Mocker() as m:
            m.register_uri(requests_mock.POST,
                           "http://localhost:8086/db/db/series")

            cli = DataFrameClient(database='db')
            cli.write_points({"foo": dataframe})

    def test_query_into_dataframe(self):
        data = [
            {
                "name": "foo",
                "columns": ["time", "sequence_number", "column_one"],
                "points": [
                    [3600, 16, 2], [3600, 15, 1],
                    [0, 14, 2], [0, 13, 1]
                ]
            }
        ]
        # dataframe sorted ascending by time first, then sequence_number
        dataframe = pd.DataFrame(data=[[13, 1], [14, 2], [15, 1], [16, 2]],
                                 index=pd.to_datetime([0, 0,
                                                      3600, 3600],
                                                      unit='s', utc=True),
                                 columns=['sequence_number', 'column_one'])
        with _mocked_session('get', 200, data):
            cli = DataFrameClient('host', 8086, 'username', 'password', 'db')
            result = cli.query('select column_one from foo;')
            assert_frame_equal(dataframe, result)

    def test_query_with_empty_result(self):
        with _mocked_session('get', 200, []):
            cli = DataFrameClient('host', 8086, 'username', 'password', 'db')
            result = cli.query('select column_one from foo;')
            assert result == []

    def test_list_series(self):
        response = [
            {
                'columns': ['time', 'name'],
                'name': 'list_series_result',
                'points': [[0, 'seriesA'], [0, 'seriesB']]
            }
        ]
        with _mocked_session('get', 200, response):
            cli = DataFrameClient('host', 8086, 'username', 'password', 'db')
            series_list = cli.get_list_series()
            assert series_list == ['seriesA', 'seriesB']
