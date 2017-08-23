# -*- coding: utf-8 -*-
"""Unit tests for misc module."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from datetime import timedelta

import json
import unittest
import warnings
import requests_mock

from influxdb.tests import skipIfPYpy, using_pypy
from nose.tools import raises

from .client_test import _mocked_session

if not using_pypy:
    import pandas as pd
    from pandas.util.testing import assert_frame_equal
    from influxdb import DataFrameClient


@skipIfPYpy
class TestDataFrameClient(unittest.TestCase):
    """Set up a test DataFrameClient object."""

    def setUp(self):
        """Instantiate a TestDataFrameClient object."""
        # By default, raise exceptions on warnings
        warnings.simplefilter('error', FutureWarning)

    def test_write_points_from_dataframe(self):
        """Test write points from df in TestDataFrameClient object."""
        now = pd.Timestamp('1970-01-01 00:00+00:00')
        dataframe = pd.DataFrame(data=[["1", 1, 1.0], ["2", 2, 2.0]],
                                 index=[now, now + timedelta(hours=1)],
                                 columns=["column_one", "column_two",
                                          "column_three"])
        expected = (
            b"foo column_one=\"1\",column_two=1i,column_three=1.0 0\n"
            b"foo column_one=\"2\",column_two=2i,column_three=2.0 "
            b"3600000000000\n"
        )

        with requests_mock.Mocker() as m:
            m.register_uri(requests_mock.POST,
                           "http://localhost:8086/write",
                           status_code=204)

            cli = DataFrameClient(database='db')

            cli.write_points(dataframe, 'foo')
            self.assertEqual(m.last_request.body, expected)

            cli.write_points(dataframe, 'foo', tags=None)
            self.assertEqual(m.last_request.body, expected)

    def test_write_points_from_dataframe_in_batches(self):
        """Test write points in batch from df in TestDataFrameClient object."""
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

    def test_write_points_from_dataframe_with_tag_columns(self):
        """Test write points from df w/tag in TestDataFrameClient object."""
        now = pd.Timestamp('1970-01-01 00:00+00:00')
        dataframe = pd.DataFrame(data=[['blue', 1, "1", 1, 1.0],
                                       ['red', 0, "2", 2, 2.0]],
                                 index=[now, now + timedelta(hours=1)],
                                 columns=["tag_one", "tag_two", "column_one",
                                          "column_two", "column_three"])
        expected = (
            b"foo,tag_one=blue,tag_two=1 "
            b"column_one=\"1\",column_two=1i,column_three=1.0 "
            b"0\n"
            b"foo,tag_one=red,tag_two=0 "
            b"column_one=\"2\",column_two=2i,column_three=2.0 "
            b"3600000000000\n"
        )

        with requests_mock.Mocker() as m:
            m.register_uri(requests_mock.POST,
                           "http://localhost:8086/write",
                           status_code=204)

            cli = DataFrameClient(database='db')

            cli.write_points(dataframe, 'foo',
                             tag_columns=['tag_one', 'tag_two'])
            self.assertEqual(m.last_request.body, expected)

            cli.write_points(dataframe, 'foo',
                             tag_columns=['tag_one', 'tag_two'], tags=None)
            self.assertEqual(m.last_request.body, expected)

    def test_write_points_from_dataframe_with_tag_cols_and_global_tags(self):
        """Test write points from df w/tag + cols in TestDataFrameClient."""
        now = pd.Timestamp('1970-01-01 00:00+00:00')
        dataframe = pd.DataFrame(data=[['blue', 1, "1", 1, 1.0],
                                       ['red', 0, "2", 2, 2.0]],
                                 index=[now, now + timedelta(hours=1)],
                                 columns=["tag_one", "tag_two", "column_one",
                                          "column_two", "column_three"])
        expected = (
            b"foo,global_tag=value,tag_one=blue,tag_two=1 "
            b"column_one=\"1\",column_two=1i,column_three=1.0 "
            b"0\n"
            b"foo,global_tag=value,tag_one=red,tag_two=0 "
            b"column_one=\"2\",column_two=2i,column_three=2.0 "
            b"3600000000000\n"
        )

        with requests_mock.Mocker() as m:
            m.register_uri(requests_mock.POST,
                           "http://localhost:8086/write",
                           status_code=204)

            cli = DataFrameClient(database='db')

            cli.write_points(dataframe, 'foo',
                             tag_columns=['tag_one', 'tag_two'],
                             tags={'global_tag': 'value'})
            self.assertEqual(m.last_request.body, expected)

    def test_write_points_from_dataframe_with_tag_cols_and_defaults(self):
        """Test default write points from df w/tag in TestDataFrameClient."""
        now = pd.Timestamp('1970-01-01 00:00+00:00')
        dataframe = pd.DataFrame(data=[['blue', 1, "1", 1, 1.0, 'hot'],
                                       ['red', 0, "2", 2, 2.0, 'cold']],
                                 index=[now, now + timedelta(hours=1)],
                                 columns=["tag_one", "tag_two", "column_one",
                                          "column_two", "column_three",
                                          "tag_three"])
        expected_tags_and_fields = (
            b"foo,tag_one=blue "
            b"column_one=\"1\",column_two=1i "
            b"0\n"
            b"foo,tag_one=red "
            b"column_one=\"2\",column_two=2i "
            b"3600000000000\n"
        )

        expected_tags_no_fields = (
            b"foo,tag_one=blue,tag_two=1 "
            b"column_one=\"1\",column_two=1i,column_three=1.0,"
            b"tag_three=\"hot\" 0\n"
            b"foo,tag_one=red,tag_two=0 "
            b"column_one=\"2\",column_two=2i,column_three=2.0,"
            b"tag_three=\"cold\" 3600000000000\n"
        )

        expected_fields_no_tags = (
            b"foo,tag_one=blue,tag_three=hot,tag_two=1 "
            b"column_one=\"1\",column_two=1i,column_three=1.0 "
            b"0\n"
            b"foo,tag_one=red,tag_three=cold,tag_two=0 "
            b"column_one=\"2\",column_two=2i,column_three=2.0 "
            b"3600000000000\n"
        )

        expected_no_tags_no_fields = (
            b"foo "
            b"tag_one=\"blue\",tag_two=1i,column_one=\"1\","
            b"column_two=1i,column_three=1.0,tag_three=\"hot\" "
            b"0\n"
            b"foo "
            b"tag_one=\"red\",tag_two=0i,column_one=\"2\","
            b"column_two=2i,column_three=2.0,tag_three=\"cold\" "
            b"3600000000000\n"
        )

        with requests_mock.Mocker() as m:
            m.register_uri(requests_mock.POST,
                           "http://localhost:8086/write",
                           status_code=204)

            cli = DataFrameClient(database='db')

            cli.write_points(dataframe, 'foo',
                             field_columns=['column_one', 'column_two'],
                             tag_columns=['tag_one'])
            self.assertEqual(m.last_request.body, expected_tags_and_fields)

            cli.write_points(dataframe, 'foo',
                             tag_columns=['tag_one', 'tag_two'])
            self.assertEqual(m.last_request.body, expected_tags_no_fields)

            cli.write_points(dataframe, 'foo',
                             field_columns=['column_one', 'column_two',
                                            'column_three'])
            self.assertEqual(m.last_request.body, expected_fields_no_tags)

            cli.write_points(dataframe, 'foo')
            self.assertEqual(m.last_request.body, expected_no_tags_no_fields)

    def test_write_points_from_dataframe_with_tag_escaped(self):
        """Test write points from df w/escaped tag in TestDataFrameClient."""
        now = pd.Timestamp('1970-01-01 00:00+00:00')
        dataframe = pd.DataFrame(
            data=[
                ['blue orange', "1", 1, 'hot=cold'],  # space, equal
                ['red,green', "2", 2, r'cold\fire'],  # comma, backslash
                ['some', "2", 2, ''],                 # skip empty
                ['some', "2", 2, None],               # skip None
                ['', "2", 2, None],                   # all tags empty
            ],
            index=pd.period_range(now, freq='H', periods=5),
            columns=["tag_one", "column_one", "column_two", "tag_three"]
        )

        expected_escaped_tags = (
            b"foo,tag_one=blue\\ orange,tag_three=hot\\=cold "
            b"column_one=\"1\",column_two=1i "
            b"0\n"
            b"foo,tag_one=red\\,green,tag_three=cold\\\\fire "
            b"column_one=\"2\",column_two=2i "
            b"3600000000000\n"
            b"foo,tag_one=some "
            b"column_one=\"2\",column_two=2i "
            b"7200000000000\n"
            b"foo,tag_one=some "
            b"column_one=\"2\",column_two=2i "
            b"10800000000000\n"
            b"foo "
            b"column_one=\"2\",column_two=2i "
            b"14400000000000\n"
        )

        with requests_mock.Mocker() as m:
            m.register_uri(requests_mock.POST,
                           "http://localhost:8086/write",
                           status_code=204)
            cli = DataFrameClient(database='db')
            cli.write_points(dataframe, 'foo',
                             field_columns=['column_one', 'column_two'],
                             tag_columns=['tag_one', 'tag_three'])
            self.assertEqual(m.last_request.body, expected_escaped_tags)

    def test_write_points_from_dataframe_with_numeric_column_names(self):
        """Test write points from df with numeric cols."""
        now = pd.Timestamp('1970-01-01 00:00+00:00')
        # df with numeric column names
        dataframe = pd.DataFrame(data=[["1", 1, 1.0], ["2", 2, 2.0]],
                                 index=[now, now + timedelta(hours=1)])

        expected = (
            b'foo,hello=there 0=\"1\",1=1i,2=1.0 0\n'
            b'foo,hello=there 0=\"2\",1=2i,2=2.0 3600000000000\n'
        )

        with requests_mock.Mocker() as m:
            m.register_uri(requests_mock.POST,
                           "http://localhost:8086/write",
                           status_code=204)

            cli = DataFrameClient(database='db')
            cli.write_points(dataframe, "foo", {"hello": "there"})

            self.assertEqual(m.last_request.body, expected)

    def test_write_points_from_dataframe_with_numeric_precision(self):
        """Test write points from df with numeric precision."""
        now = pd.Timestamp('1970-01-01 00:00+00:00')
        # df with numeric column names
        dataframe = pd.DataFrame(data=[["1", 1, 1.1111111111111],
                                       ["2", 2, 2.2222222222222]],
                                 index=[now, now + timedelta(hours=1)])

        expected_default_precision = (
            b'foo,hello=there 0=\"1\",1=1i,2=1.11111111111 0\n'
            b'foo,hello=there 0=\"2\",1=2i,2=2.22222222222 3600000000000\n'
        )

        expected_specified_precision = (
            b'foo,hello=there 0=\"1\",1=1i,2=1.1111 0\n'
            b'foo,hello=there 0=\"2\",1=2i,2=2.2222 3600000000000\n'
        )

        expected_full_precision = (
            b'foo,hello=there 0=\"1\",1=1i,2=1.1111111111111 0\n'
            b'foo,hello=there 0=\"2\",1=2i,2=2.2222222222222 3600000000000\n'
        )

        with requests_mock.Mocker() as m:
            m.register_uri(requests_mock.POST,
                           "http://localhost:8086/write",
                           status_code=204)

            cli = DataFrameClient(database='db')
            cli.write_points(dataframe, "foo", {"hello": "there"})

            self.assertEqual(m.last_request.body, expected_default_precision)

            cli = DataFrameClient(database='db')
            cli.write_points(dataframe, "foo", {"hello": "there"},
                             numeric_precision=4)

            self.assertEqual(m.last_request.body, expected_specified_precision)

            cli = DataFrameClient(database='db')
            cli.write_points(dataframe, "foo", {"hello": "there"},
                             numeric_precision='full')

            self.assertEqual(m.last_request.body, expected_full_precision)

    def test_write_points_from_dataframe_with_period_index(self):
        """Test write points from df with period index."""
        dataframe = pd.DataFrame(data=[["1", 1, 1.0], ["2", 2, 2.0]],
                                 index=[pd.Period('1970-01-01'),
                                        pd.Period('1970-01-02')],
                                 columns=["column_one", "column_two",
                                          "column_three"])

        expected = (
            b"foo column_one=\"1\",column_two=1i,column_three=1.0 0\n"
            b"foo column_one=\"2\",column_two=2i,column_three=2.0 "
            b"86400000000000\n"
        )

        with requests_mock.Mocker() as m:
            m.register_uri(requests_mock.POST,
                           "http://localhost:8086/write",
                           status_code=204)

            cli = DataFrameClient(database='db')
            cli.write_points(dataframe, "foo")

            self.assertEqual(m.last_request.body, expected)

    def test_write_points_from_dataframe_with_time_precision(self):
        """Test write points from df with time precision."""
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
            measurement = "foo"

            cli.write_points(dataframe, measurement, time_precision='h')
            self.assertEqual(m.last_request.qs['precision'], ['h'])
            self.assertEqual(
                b'foo column_one="1",column_two=1i,column_three=1.0 0\nfoo '
                b'column_one="2",column_two=2i,column_three=2.0 1\n',
                m.last_request.body,
            )

            cli.write_points(dataframe, measurement, time_precision='m')
            self.assertEqual(m.last_request.qs['precision'], ['m'])
            self.assertEqual(
                b'foo column_one="1",column_two=1i,column_three=1.0 0\nfoo '
                b'column_one="2",column_two=2i,column_three=2.0 60\n',
                m.last_request.body,
            )

            cli.write_points(dataframe, measurement, time_precision='s')
            self.assertEqual(m.last_request.qs['precision'], ['s'])
            self.assertEqual(
                b'foo column_one="1",column_two=1i,column_three=1.0 0\nfoo '
                b'column_one="2",column_two=2i,column_three=2.0 3600\n',
                m.last_request.body,
            )

            cli.write_points(dataframe, measurement, time_precision='ms')
            self.assertEqual(m.last_request.qs['precision'], ['ms'])
            self.assertEqual(
                b'foo column_one="1",column_two=1i,column_three=1.0 0\nfoo '
                b'column_one="2",column_two=2i,column_three=2.0 3600000\n',
                m.last_request.body,
            )

            cli.write_points(dataframe, measurement, time_precision='u')
            self.assertEqual(m.last_request.qs['precision'], ['u'])
            self.assertEqual(
                b'foo column_one="1",column_two=1i,column_three=1.0 0\nfoo '
                b'column_one="2",column_two=2i,column_three=2.0 3600000000\n',
                m.last_request.body,
            )

            cli.write_points(dataframe, measurement, time_precision='n')
            self.assertEqual(m.last_request.qs['precision'], ['n'])
            self.assertEqual(
                b'foo column_one="1",column_two=1i,column_three=1.0 0\n'
                b'foo column_one="2",column_two=2i,column_three=2.0 '
                b'3600000000000\n',
                m.last_request.body,
            )

    @raises(TypeError)
    def test_write_points_from_dataframe_fails_without_time_index(self):
        """Test failed write points from df without time index."""
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
        """Test failed write points from df with series."""
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
        """Test query into df for TestDataFrameClient object."""
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

    def test_multiquery_into_dataframe(self):
        """Test multiquyer into df for TestDataFrameClient object."""
        data = {
            "results": [
                {
                    "series": [
                        {
                            "name": "cpu_load_short",
                            "columns": ["time", "value"],
                            "values": [
                                ["2015-01-29T21:55:43.702900257Z", 0.55],
                                ["2015-01-29T21:55:43.702900257Z", 23422],
                                ["2015-06-11T20:46:02Z", 0.64]
                            ]
                        }
                    ]
                }, {
                    "series": [
                        {
                            "name": "cpu_load_short",
                            "columns": ["time", "count"],
                            "values": [
                                ["1970-01-01T00:00:00Z", 3]
                            ]
                        }
                    ]
                }
            ]
        }

        pd1 = pd.DataFrame(
            [[0.55], [23422.0], [0.64]], columns=['value'],
            index=pd.to_datetime([
                "2015-01-29 21:55:43.702900257+0000",
                "2015-01-29 21:55:43.702900257+0000",
                "2015-06-11 20:46:02+0000"])).tz_localize('UTC')
        pd2 = pd.DataFrame(
            [[3]], columns=['count'],
            index=pd.to_datetime(["1970-01-01 00:00:00+00:00"]))\
            .tz_localize('UTC')
        expected = [{'cpu_load_short': pd1}, {'cpu_load_short': pd2}]

        cli = DataFrameClient('host', 8086, 'username', 'password', 'db')
        iql = "SELECT value FROM cpu_load_short WHERE region='us-west';"\
            "SELECT count(value) FROM cpu_load_short WHERE region='us-west'"
        with _mocked_session(cli, 'GET', 200, data):
            result = cli.query(iql)
            for r, e in zip(result, expected):
                for k in e:
                    assert_frame_equal(e[k], r[k])

    def test_query_with_empty_result(self):
        """Test query with empty results in TestDataFrameClient object."""
        cli = DataFrameClient('host', 8086, 'username', 'password', 'db')
        with _mocked_session(cli, 'GET', 200, {"results": [{}]}):
            result = cli.query('select column_one from foo;')
            self.assertEqual(result, {})

    def test_get_list_database(self):
        """Test get list of databases in TestDataFrameClient object."""
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
        """Test convert datetime to epoch in TestDataFrameClient object."""
        timestamp = pd.Timestamp('2013-01-01 00:00:00.000+00:00')
        cli = DataFrameClient('host', 8086, 'username', 'password', 'db')

        self.assertEqual(
            cli._datetime_to_epoch(timestamp),
            1356998400.0
        )
        self.assertEqual(
            cli._datetime_to_epoch(timestamp, time_precision='h'),
            1356998400.0 / 3600
        )
        self.assertEqual(
            cli._datetime_to_epoch(timestamp, time_precision='m'),
            1356998400.0 / 60
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

    def test_dsn_constructor(self):
        """Test data source name deconstructor in TestDataFrameClient."""
        client = DataFrameClient.from_dsn('influxdb://localhost:8086')
        self.assertIsInstance(client, DataFrameClient)
        self.assertEqual('http://localhost:8086', client._baseurl)
