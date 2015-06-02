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
import warnings

if not using_pypy:
    import pandas as pd
    from pandas.util.testing import assert_frame_equal
    from influxdb import DataFrameClient


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
        expected = {
            'database': 'db',
            'points': [
                {'time': '1970-01-01T00:00:00+00:00',
                 'fields': {
                     'column_two': 1,
                     'column_three': 1.0,
                     'column_one': '1'},
                 'tags': {},
                 'measurement': 'foo'},
                {'time': '1970-01-01T01:00:00+00:00',
                 'fields': {
                     'column_two': 2,
                     'column_three': 2.0,
                     'column_one': '2'},
                 'tags': {},
                 'measurement': 'foo'}]
        }

        with requests_mock.Mocker() as m:
            m.register_uri(requests_mock.POST,
                           "http://localhost:8086/write",
                           status_code=204)

            cli = DataFrameClient(database='db')

            cli.write_points(dataframe, 'foo')
            self.assertEqual(json.loads(m.last_request.body), expected)

            cli.write_points(dataframe, 'foo', tags=None)
            self.assertEqual(json.loads(m.last_request.body), expected)

    def test_write_points_from_dataframe_in_batches(self):
        now = pd.Timestamp('1970-01-01 00:00+00:00')
        dataframe = pd.DataFrame(data=[["1", 1, 1.0], ["2", 2, 2.0]],
                                 index=[now, now + timedelta(hours=1)],
                                 columns=["column_one", "column_two",
                                          "column_three"])
        with requests_mock.Mocker() as m:
            m.register_uri(requests_mock.POST,
                           "http://localhost:8086/write",
                           status_code=204)

            cli = DataFrameClient(database='db')
            self.assertTrue(cli.write_points(dataframe, "foo", batch_size=1))

    def test_write_points_from_dataframe_with_numeric_column_names(self):
        now = pd.Timestamp('1970-01-01 00:00+00:00')
        # df with numeric column names
        dataframe = pd.DataFrame(data=[["1", 1, 1.0], ["2", 2, 2.0]],
                                 index=[now, now + timedelta(hours=1)])

        expected = {
            'database': 'db',
            'points': [
                {'fields': {
                    '0': '1',
                    '1': 1,
                    '2': 1.0},
                 'tags': {'hello': 'there'},
                 'time': '1970-01-01T00:00:00+00:00',
                 'measurement': 'foo'},
                {'fields': {
                    '0': '2',
                    '1': 2,
                    '2': 2.0},
                 'tags': {'hello': 'there'},
                 'time': '1970-01-01T01:00:00+00:00',
                 'measurement': 'foo'}],
        }

        with requests_mock.Mocker() as m:
            m.register_uri(requests_mock.POST,
                           "http://localhost:8086/write",
                           status_code=204)

            cli = DataFrameClient(database='db')
            cli.write_points(dataframe, "foo", {"hello": "there"})

            self.assertEqual(json.loads(m.last_request.body), expected)

    def test_write_points_from_dataframe_with_period_index(self):
        dataframe = pd.DataFrame(data=[["1", 1, 1.0], ["2", 2, 2.0]],
                                 index=[pd.Period('1970-01-01'),
                                        pd.Period('1970-01-02')],
                                 columns=["column_one", "column_two",
                                          "column_three"])
        expected = {
            'points': [
                {'measurement': 'foo',
                 'tags': {},
                 'fields': {
                     'column_one': '1',
                     'column_two': 1,
                     'column_three': 1.0},
                 'time': '1970-01-01T00:00:00+00:00'},
                {'measurement': 'foo',
                 'tags': {},
                 'fields': {
                     'column_one': '2',
                     'column_two': 2,
                     'column_three': 2.0},
                 'time': '1970-01-02T00:00:00+00:00'}],
            'database': 'db',
        }

        with requests_mock.Mocker() as m:
            m.register_uri(requests_mock.POST,
                           "http://localhost:8086/write",
                           status_code=204)

            cli = DataFrameClient(database='db')
            cli.write_points(dataframe, "foo")

            self.assertEqual(json.loads(m.last_request.body), expected)

    def test_write_points_from_dataframe_with_time_precision(self):
        now = pd.Timestamp('1970-01-01 00:00+00:00')
        dataframe = pd.DataFrame(data=[["1", 1, 1.0], ["2", 2, 2.0]],
                                 index=[now, now + timedelta(hours=1)],
                                 columns=["column_one", "column_two",
                                          "column_three"])

        with requests_mock.Mocker() as m:
            m.register_uri(requests_mock.POST,
                           "http://localhost:8086/write",
                           status_code=204)

            points = {
                'database': 'db',
                'points': [
                    {'time': '1970-01-01T00:00:00+00:00',
                     'fields': {
                         'column_one': '1',
                         'column_three': 1.0,
                         'column_two': 1},
                     'tags': {},
                     'measurement': 'foo'},
                    {'time': '1970-01-01T01:00:00+00:00',
                     'fields': {
                         'column_one': '2',
                         'column_three': 2.0,
                         'column_two': 2},
                     'tags': {},
                     'measurement': 'foo'}]
            }

            cli = DataFrameClient(database='db')
            measurement = "foo"

            cli.write_points(dataframe, measurement, time_precision='s')
            points.update(precision='s')
            self.assertEqual(json.loads(m.last_request.body), points)

            cli.write_points(dataframe, measurement, time_precision='m')
            points.update(precision='m')
            self.assertEqual(json.loads(m.last_request.body), points)

            cli.write_points(dataframe, measurement, time_precision='u')
            points.update(precision='u')
            self.assertEqual(json.loads(m.last_request.body), points)

    @raises(TypeError)
    def test_write_points_from_dataframe_fails_without_time_index(self):
        dataframe = pd.DataFrame(data=[["1", 1, 1.0], ["2", 2, 2.0]],
                                 columns=["column_one", "column_two",
                                          "column_three"])

        with requests_mock.Mocker() as m:
            m.register_uri(requests_mock.POST,
                           "http://localhost:8086/db/db/series",
                           status_code=204)

            cli = DataFrameClient(database='db')
            cli.write_points(dataframe, "foo")

    @raises(TypeError)
    def test_write_points_from_dataframe_fails_with_series(self):
        now = pd.Timestamp('1970-01-01 00:00+00:00')
        dataframe = pd.Series(data=[1.0, 2.0],
                              index=[now, now + timedelta(hours=1)])

        with requests_mock.Mocker() as m:
            m.register_uri(requests_mock.POST,
                           "http://localhost:8086/db/db/series",
                           status_code=204)

            cli = DataFrameClient(database='db')
            cli.write_points(dataframe, "foo")

    def test_query_into_dataframe(self):
        data = {
            "results": [{
                "series": [
                    {"measurement": "network",
                     "tags": {"direction": ""},
                     "columns": ["time", "value"],
                     "values":[["2009-11-10T23:00:00Z", 23422]]
                     },
                    {"measurement": "network",
                     "tags": {"direction": "in"},
                     "columns": ["time", "value"],
                     "values": [["2009-11-10T23:00:00Z", 23422],
                                ["2009-11-10T23:00:00Z", 23422],
                                ["2009-11-10T23:00:00Z", 23422]]
                     }
                ]
            }]
        }

        pd1 = pd.DataFrame(
            [[23422]], columns=['value'],
            index=pd.to_datetime(["2009-11-10T23:00:00Z"]))
        pd1.index = pd1.index.tz_localize('UTC')
        pd2 = pd.DataFrame(
            [[23422], [23422], [23422]], columns=['value'],
            index=pd.to_datetime(["2009-11-10T23:00:00Z",
                                  "2009-11-10T23:00:00Z",
                                  "2009-11-10T23:00:00Z"]))
        pd2.index = pd2.index.tz_localize('UTC')
        expected = {
            ('network', (('direction', ''),)): pd1,
            ('network', (('direction', 'in'),)): pd2
        }

        cli = DataFrameClient('host', 8086, 'username', 'password', 'db')
        with _mocked_session(cli, 'GET', 200, data):
            result = cli.query('select value from network group by direction;')
            for k in expected:
                assert_frame_equal(expected[k], result[k])

    def test_query_with_empty_result(self):
        cli = DataFrameClient('host', 8086, 'username', 'password', 'db')
        with _mocked_session(cli, 'GET', 200, {"results": [{}]}):
            result = cli.query('select column_one from foo;')
            self.assertEqual(result, {})

    def test_list_series(self):
        response = {
            'results': [
                {'series': [
                    {
                        'columns': ['host'],
                        'measurement': 'cpu',
                        'values': [
                            ['server01']]
                    },
                    {
                        'columns': [
                            'host',
                            'region'
                        ],
                        'measurement': 'network',
                        'values': [
                            [
                                'server01',
                                'us-west'
                            ],
                            [
                                'server01',
                                'us-east'
                            ]
                        ]
                    }
                ]}
            ]
        }

        expected = {
            'cpu': pd.DataFrame([['server01']], columns=['host']),
            'network': pd.DataFrame(
                [['server01', 'us-west'], ['server01', 'us-east']],
                columns=['host', 'region'])}

        cli = DataFrameClient('host', 8086, 'username', 'password', 'db')
        with _mocked_session(cli, 'GET', 200, response):
            series = cli.get_list_series()
            assert_frame_equal(series['cpu'], expected['cpu'])
            assert_frame_equal(series['network'], expected['network'])

    def test_get_list_database(self):
        data = {'results': [
            {'series': [
                {'measurement': 'databases',
                 'values': [
                     ['new_db_1'],
                     ['new_db_2']],
                 'columns': ['name']}]}
        ]}

        cli = DataFrameClient('host', 8086, 'username', 'password', 'db')
        with _mocked_session(cli, 'get', 200, json.dumps(data)):
            self.assertListEqual(
                cli.get_list_database(),
                [{'name': 'new_db_1'}, {'name': 'new_db_2'}]
            )

    def test_datetime_to_epoch(self):
        timestamp = pd.Timestamp('2013-01-01 00:00:00.000+00:00')
        cli = DataFrameClient('host', 8086, 'username', 'password', 'db')

        self.assertEqual(
            cli._datetime_to_epoch(timestamp),
            1356998400.0
        )
        self.assertEqual(
            cli._datetime_to_epoch(timestamp, time_precision='s'),
            1356998400.0
        )
        self.assertEqual(
            cli._datetime_to_epoch(timestamp, time_precision='ms'),
            1356998400000.0
        )
        self.assertEqual(
            cli._datetime_to_epoch(timestamp, time_precision='u'),
            1356998400000000.0
        )
        self.assertEqual(
            cli._datetime_to_epoch(timestamp, time_precision='n'),
            1356998400000000000.0
        )
