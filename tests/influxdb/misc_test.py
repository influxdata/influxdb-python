# -*- coding: utf-8 -*-
"""
unit tests for misc module
"""
import unittest
import json
import requests_mock
from nose.tools import raises
from datetime import datetime, timedelta
import time
import pandas as pd
from pandas.util.testing import assert_frame_equal

from influxdb.misc import DataFrameClient
from .client_test import _mocked_session


class TestDataFrameClient(unittest.TestCase):

    def test_write_points_from_dataframe(self):
        now = datetime(2014, 11, 15, 15, 42, 44, 543)
        dataframe = pd.DataFrame(data=[["1", 1, 1.0], ["2", 2, 2.0]],
                                 index=[now, now + timedelta(hours=1)],
                                 columns=["column_one", "column_two",
                                          "column_three"])
        points = [
            {
                "points": [
                    ["1", 1, 1.0, time.mktime(now.timetuple())],
                    ["2", 2, 2.0, time.mktime((now + timedelta(hours=1))
                                              .timetuple())]
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

    def test_write_points_from_dataframe_with_numeric_column_names(self):
        now = datetime(2014, 11, 15, 15, 42, 44, 543)
        # df with numeric column names
        dataframe = pd.DataFrame(data=[["1", 1, 1.0], ["2", 2, 2.0]],
                                 index=[now, now + timedelta(hours=1)])
        points = [
            {
                "points": [
                    ["1", 1, 1.0, time.mktime(now.timetuple())],
                    ["2", 2, 2.0, time.mktime((now + timedelta(hours=1))
                                              .timetuple())]
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
        now = datetime(2014, 11, 16)
        dataframe = pd.DataFrame(data=[["1", 1, 1.0], ["2", 2, 2.0]],
                                 index=[pd.Period('2014-11-16'),
                                        pd.Period('2014-11-17')],
                                 columns=["column_one", "column_two",
                                          "column_three"])
        points = [
            {
                "points": [
                    ["1", 1, 1.0, time.mktime(now.timetuple())],
                    ["2", 2, 2.0, time.mktime((now + timedelta(hours=24))
                                              .timetuple())]
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
        now = datetime(2014, 11, 16)
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
                    [1383876043, 16, 2], [1383876043, 15, 1],
                    [1383876035, 14, 2], [1383876035, 13, 1]
                ]
            }
        ]
        dataframe = pd.DataFrame(data=[[16, 2], [15, 1], [14, 2], [13, 1]],
                                 index=pd.to_datetime([1383876043, 1383876043,
                                                      1383876035, 1383876035],
                                                      unit='s', utc=True),
                                 columns=['sequence_number', 'column_one'])
        with _mocked_session('get', 200, data):
            cli = DataFrameClient('host', 8086, 'username', 'password', 'db')
            result = cli.query('select column_one from foo;')
            assert_frame_equal(dataframe, result)
