# -*- coding: utf-8 -*-
"""
unit tests
"""
import json
import requests
import socket
import unittest
import requests_mock
from nose.tools import raises
from mock import patch
from datetime import datetime, timedelta
import time
import pandas as pd
from pandas.util.testing import assert_frame_equal

from influxdb import InfluxDBClient
from influxdb.client import session


def _build_response_object(status_code=200, content=""):
    resp = requests.Response()
    resp.status_code = status_code
    resp._content = content.encode("utf8")
    return resp


def _mocked_session(method="GET", status_code=200, content=""):

    method = method.upper()

    def request(*args, **kwargs):
        c = content

        # Check method
        assert method == kwargs.get('method', 'GET')

        if method == 'POST':
            data = kwargs.get('data', None)

            if data is not None:
                # Data must be a string
                assert isinstance(data, str)

                # Data must be a JSON string
                assert c == json.loads(data, strict=True)

                c = data

        # Anyway, Content must be a JSON string (or empty string)
        if not isinstance(c, str):
            c = json.dumps(c)

        return _build_response_object(status_code=status_code, content=c)

    mocked = patch.object(
        session,
        'request',
        side_effect=request
    )

    return mocked


class TestInfluxDBClient(unittest.TestCase):

    def setUp(self):
        self.dummy_points = [
            {
                "points": [
                    ["1", 1, 1.0],
                    ["2", 2, 2.0]
                ],
                "name": "foo",
                "columns": ["column_one", "column_two", "column_three"]
            }
        ]

    def test_scheme(self):
        cli = InfluxDBClient('host', 8086, 'username', 'password', 'database')
        assert cli._baseurl == 'http://host:8086'

        cli = InfluxDBClient(
            'host', 8086, 'username', 'password', 'database', ssl=True
        )
        assert cli._baseurl == 'https://host:8086'

    def test_switch_db(self):
        cli = InfluxDBClient('host', 8086, 'username', 'password', 'database')
        cli.switch_db('another_database')
        assert cli._database == 'another_database'

    def test_switch_user(self):
        cli = InfluxDBClient('host', 8086, 'username', 'password', 'database')
        cli.switch_user('another_username', 'another_password')
        assert cli._username == 'another_username'
        assert cli._password == 'another_password'

    def test_write_points(self):
        with requests_mock.Mocker() as m:
            m.register_uri(
                requests_mock.POST,
                "http://localhost:8086/db/db/series"
            )

            cli = InfluxDBClient(database='db')
            cli.write_points(
                self.dummy_points
            )

            self.assertListEqual(
                json.loads(m.last_request.body),
                self.dummy_points
            )

    def test_write_points_string(self):
        with requests_mock.Mocker() as m:
            m.register_uri(
                requests_mock.POST,
                "http://localhost:8086/db/db/series"
            )

            cli = InfluxDBClient(database='db')
            cli.write_points(
                str(json.dumps(self.dummy_points))
            )

            self.assertListEqual(
                json.loads(m.last_request.body),
                self.dummy_points
            )

    def test_write_points_batch(self):
        with _mocked_session('post', 200, self.dummy_points):
            cli = InfluxDBClient('host', 8086, 'username', 'password', 'db')
            assert cli.write_points(
                data=self.dummy_points,
                batch_size=2
            ) is True

    def test_write_points_udp(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.bind(('0.0.0.0', 4444))

        cli = InfluxDBClient(
            'localhost', 8086, 'root', 'root',
            'test', use_udp=True, udp_port=4444
        )
        cli.write_points(self.dummy_points)

        received_data, addr = s.recvfrom(1024)

        assert self.dummy_points == \
            json.loads(received_data.decode(), strict=True)

    def test_write_bad_precision_udp(self):
        cli = InfluxDBClient(
            'localhost', 8086, 'root', 'root',
            'test', use_udp=True, udp_port=4444
        )

        with self.assertRaisesRegexp(
                Exception,
                "InfluxDB only supports seconds precision for udp writes"
        ):
            cli.write_points_with_precision(
                self.dummy_points,
                time_precision='ms'
            )

    @raises(Exception)
    def test_write_points_fails(self):
        with _mocked_session('post', 500):
            cli = InfluxDBClient('host', 8086, 'username', 'password', 'db')
            cli.write_points([])

    def test_write_points_with_precision(self):
        with _mocked_session('post', 200, self.dummy_points):
            cli = InfluxDBClient('host', 8086, 'username', 'password', 'db')
            assert cli.write_points_with_precision(self.dummy_points) is True

    def test_write_points_bad_precision(self):
        cli = InfluxDBClient()
        with self.assertRaisesRegexp(
            Exception,
            "Invalid time precision is given. \(use 's', 'm', 'ms' or 'u'\)"
        ):
            cli.write_points_with_precision(
                self.dummy_points,
                time_precision='g'
            )

    @raises(Exception)
    def test_write_points_with_precision_fails(self):
        with _mocked_session('post', 500):
            cli = InfluxDBClient('host', 8086, 'username', 'password', 'db')
            cli.write_points_with_precision([])

    def test_delete_points(self):
        with _mocked_session('delete', 204) as mocked:
            cli = InfluxDBClient('host', 8086, 'username', 'password', 'db')
            assert cli.delete_points("foo") is True

            assert len(mocked.call_args_list) == 1
            args, kwds = mocked.call_args_list[0]

            assert kwds['params'] == {'u': 'username', 'p': 'password'}
            assert kwds['url'] == 'http://host:8086/db/db/series/foo'

    @raises(Exception)
    def test_delete_points_with_wrong_name(self):
        with _mocked_session('delete', 400):
            cli = InfluxDBClient('host', 8086, 'username', 'password', 'db')
            cli.delete_points("nonexist")

    @raises(NotImplementedError)
    def test_create_scheduled_delete(self):
        cli = InfluxDBClient('host', 8086, 'username', 'password', 'db')
        cli.create_scheduled_delete([])

    @raises(NotImplementedError)
    def test_get_list_scheduled_delete(self):
        cli = InfluxDBClient('host', 8086, 'username', 'password', 'db')
        cli.get_list_scheduled_delete()

    @raises(NotImplementedError)
    def test_remove_scheduled_delete(self):
        cli = InfluxDBClient('host', 8086, 'username', 'password', 'db')
        cli.remove_scheduled_delete(1)

    def test_query(self):
        data = [
            {
                "name": "foo",
                "columns": ["time", "sequence_number", "column_one"],
                "points": [
                    [1383876043, 16, "2"], [1383876043, 15, "1"],
                    [1383876035, 14, "2"], [1383876035, 13, "1"]
                ]
            }
        ]
        with _mocked_session('get', 200, data):
            cli = InfluxDBClient('host', 8086, 'username', 'password', 'db')
            result = cli.query('select column_one from foo;')
            assert len(result[0]['points']) == 4

    def test_query_chunked(self):
        cli = InfluxDBClient(database='db')
        example_object = {
            'points': [
                [1415206250119, 40001, 667],
                [1415206244555, 30001, 7],
                [1415206228241, 20001, 788],
                [1415206212980, 10001, 555],
                [1415197271586, 10001, 23]
            ],
            'name': 'foo',
            'columns': [
                'time',
                'sequence_number',
                'val'
            ]
        }
        example_response = \
            json.dumps(example_object) + json.dumps(example_object)

        with requests_mock.Mocker() as m:
            m.register_uri(
                requests_mock.GET,
                "http://localhost:8086/db/db/series",
                text=example_response
            )

            self.assertListEqual(
                cli.query('select * from foo', chunked=True),
                [example_object, example_object]
            )

    @raises(Exception)
    def test_query_fail(self):
        with _mocked_session('get', 401):
            cli = InfluxDBClient('host', 8086, 'username', 'password', 'db')
            cli.query('select column_one from foo;')

    def test_query_bad_precision(self):
        cli = InfluxDBClient()
        with self.assertRaisesRegexp(
            Exception,
            "Invalid time precision is given. \(use 's', 'm', 'ms' or 'u'\)"
        ):
            cli.query('select column_one from foo', time_precision='g')

    def test_create_database(self):
        with _mocked_session('post', 201, {"name": "new_db"}):
            cli = InfluxDBClient('host', 8086, 'username', 'password', 'db')
            assert cli.create_database('new_db') is True

    @raises(Exception)
    def test_create_database_fails(self):
        with _mocked_session('post', 401):
            cli = InfluxDBClient('host', 8086, 'username', 'password', 'db')
            cli.create_database('new_db')

    def test_delete_database(self):
        with _mocked_session('delete', 204):
            cli = InfluxDBClient('host', 8086, 'username', 'password', 'db')
            assert cli.delete_database('old_db') is True

    @raises(Exception)
    def test_delete_database_fails(self):
        with _mocked_session('delete', 401):
            cli = InfluxDBClient('host', 8086, 'username', 'password', 'db')
            cli.delete_database('old_db')

    def test_get_database_list(self):
        data = [
            {"name": "a_db"}
        ]
        with _mocked_session('get', 200, data):
            cli = InfluxDBClient('host', 8086, 'username', 'password')
            assert len(cli.get_database_list()) == 1
            assert cli.get_database_list()[0]['name'] == 'a_db'

    @raises(Exception)
    def test_get_database_list_fails(self):
        with _mocked_session('get', 401):
            cli = InfluxDBClient('host', 8086, 'username', 'password')
            cli.get_database_list()

    def test_delete_series(self):
        with _mocked_session('delete', 204):
            cli = InfluxDBClient('host', 8086, 'username', 'password', 'db')
            cli.delete_series('old_series')

    @raises(Exception)
    def test_delete_series_fails(self):
        with _mocked_session('delete', 401):
            cli = InfluxDBClient('host', 8086, 'username', 'password', 'db')
            cli.delete_series('old_series')

    def test_get_series_list(self):
        cli = InfluxDBClient(database='db')

        with requests_mock.Mocker() as m:
            example_response = \
                '[{"name":"list_series_result","columns":' \
                '["time","name"],"points":[[0,"foo"],[0,"bar"]]}]'

            m.register_uri(
                requests_mock.GET,
                "http://localhost:8086/db/db/series",
                text=example_response
            )

            self.assertListEqual(
                cli.get_list_series(),
                ['foo', 'bar']
            )

    def test_get_continuous_queries(self):
        cli = InfluxDBClient(database='db')

        with requests_mock.Mocker() as m:

            # Tip: put this in a json linter!
            example_response = '[ { "name": "continuous queries", "columns"' \
                               ': [ "time", "id", "query" ], "points": [ [ ' \
                               '0, 1, "select foo(bar,95) from \\"foo_bar' \
                               's\\" group by time(5m) into response_times.' \
                               'percentiles.5m.95" ], [ 0, 2, "select perce' \
                               'ntile(value,95) from \\"response_times\\" g' \
                               'roup by time(5m) into response_times.percen' \
                               'tiles.5m.95" ] ] } ]'

            m.register_uri(
                requests_mock.GET,
                "http://localhost:8086/db/db/series",
                text=example_response
            )

            self.assertListEqual(
                cli.get_list_continuous_queries(),
                [
                    'select foo(bar,95) from "foo_bars" group '
                    'by time(5m) into response_times.percentiles.5m.95',

                    'select percentile(value,95) from "response_times" group '
                    'by time(5m) into response_times.percentiles.5m.95'
                ]
            )

    def test_get_list_cluster_admins(self):
        pass

    def test_add_cluster_admin(self):
        with requests_mock.Mocker() as m:
            m.register_uri(
                requests_mock.POST,
                "http://localhost:8086/cluster_admins"
            )

            cli = InfluxDBClient(database='db')
            cli.add_cluster_admin(
                new_username='paul',
                new_password='laup'
            )

            self.assertDictEqual(
                json.loads(m.last_request.body),
                {
                    'name': 'paul',
                    'password': 'laup'
                }
            )

    def test_update_cluster_admin_password(self):
        with requests_mock.Mocker() as m:
            m.register_uri(
                requests_mock.POST,
                "http://localhost:8086/cluster_admins/paul"
            )

            cli = InfluxDBClient(database='db')
            cli.update_cluster_admin_password(
                username='paul',
                new_password='laup'
            )

            self.assertDictEqual(
                json.loads(m.last_request.body),
                {'password': 'laup'}
            )

    def test_delete_cluster_admin(self):
        with requests_mock.Mocker() as m:
            m.register_uri(
                requests_mock.DELETE,
                "http://localhost:8086/cluster_admins/paul",
                status_code=200,
            )

            cli = InfluxDBClient(database='db')
            cli.delete_cluster_admin(username='paul')

            self.assertIsNone(m.last_request.body)

    def test_set_database_admin(self):
        pass

    def test_unset_database_admin(self):
        pass

    def test_alter_database_admin(self):
        with requests_mock.Mocker() as m:
            m.register_uri(
                requests_mock.POST,
                "http://localhost:8086/db/db/users/paul"
            )

            cli = InfluxDBClient(database='db')
            cli.alter_database_admin(
                username='paul',
                is_admin=False
            )

            self.assertDictEqual(
                json.loads(m.last_request.body),
                {
                    'admin': False
                }
            )

    @raises(NotImplementedError)
    def test_get_list_database_admins(self):
        cli = InfluxDBClient('host', 8086, 'username', 'password', 'db')
        cli.get_list_database_admins()

    @raises(NotImplementedError)
    def test_add_database_admin(self):
        cli = InfluxDBClient('host', 8086, 'username', 'password', 'db')
        cli.add_database_admin('admin', 'admin_secret_password')

    @raises(NotImplementedError)
    def test_update_database_admin_password(self):
        cli = InfluxDBClient('host', 8086, 'username', 'password', 'db')
        cli.update_database_admin_password('admin', 'admin_secret_password')

    @raises(NotImplementedError)
    def test_delete_database_admin(self):
        cli = InfluxDBClient('host', 8086, 'username', 'password', 'db')
        cli.delete_database_admin('admin')

    def test_get_database_users(self):
        cli = InfluxDBClient('localhost', 8086, 'username', 'password', 'db')

        example_response = \
            '[{"name":"paul","isAdmin":false,"writeTo":".*","readFrom":".*"},' \
            '{"name":"bobby","isAdmin":false,"writeTo":".*","readFrom":".*"}]'

        with requests_mock.Mocker() as m:
            m.register_uri(
                requests_mock.GET,
                "http://localhost:8086/db/db/users",
                text=example_response
            )
            users = cli.get_database_users()

        self.assertEqual(json.loads(example_response), users)

    def test_add_database_user(self):
        with requests_mock.Mocker() as m:
            m.register_uri(
                requests_mock.POST,
                "http://localhost:8086/db/db/users"
            )
            cli = InfluxDBClient(database='db')
            cli.add_database_user(
                new_username='paul',
                new_password='laup',
                permissions=('.*', '.*')
            )

            self.assertDictEqual(
                json.loads(m.last_request.body),
                {
                    'writeTo': '.*',
                    'password': 'laup',
                    'readFrom': '.*',
                    'name': 'paul'
                }
            )

    def test_add_database_user_bad_permissions(self):
        cli = InfluxDBClient()

        with self.assertRaisesRegexp(
                Exception,
                "'permissions' must be \(readFrom, writeTo\) tuple"
        ):
            cli.add_database_user(
                new_password='paul',
                new_username='paul',
                permissions=('hello', 'hello', 'hello')
            )

    def test_update_database_user_password(self):
        with requests_mock.Mocker() as m:
            m.register_uri(
                requests_mock.POST,
                "http://localhost:8086/db/db/users/paul"
            )

            cli = InfluxDBClient(database='db')
            cli.update_database_user_password(
                username='paul',
                new_password='laup'
            )

            self.assertDictEqual(
                json.loads(m.last_request.body),
                {'password': 'laup'}
            )

    def test_update_database_user_password_current_user(self):
        cli = InfluxDBClient(
            username='root',
            password='hello',
            database='database'
        )
        with requests_mock.Mocker() as m:
            m.register_uri(
                requests_mock.POST,
                "http://localhost:8086/db/database/users/root"
            )

            cli.update_database_user_password(
                username='root',
                new_password='bye'
            )

            self.assertEqual(cli._password, 'bye')

    def test_delete_database_user(self):
        with requests_mock.Mocker() as m:
            m.register_uri(
                requests_mock.DELETE,
                "http://localhost:8086/db/db/users/paul"
            )

            cli = InfluxDBClient(database='db')
            cli.delete_database_user(username='paul')

            self.assertIsNone(m.last_request.body)

    @raises(NotImplementedError)
    def test_update_permission(self):
        cli = InfluxDBClient('host', 8086, 'username', 'password', 'db')
        cli.update_permission('admin', [])

    def test_write_points_from_dataframe(self):
        now = datetime(2014, 11, 15, 15, 42, 44, 543)
        self.dummy_points = [
            {
                "points": [
                    ["1", 1, 1.0, time.mktime(now.timetuple())],
                    ["2", 2, 2.0, time.mktime((now + timedelta(hours=1)).timetuple())]
                ],
                "name": "foo",
                "columns": ["column_one", "column_two", "column_three", "time"]
            }
        ]
        self.dummy_dataframe = pd.DataFrame(data=[["1", 1, 1.0], ["2", 2, 2.0]],
                                            index = [now, now + timedelta(hours=1)],
                                            columns=["column_one", "column_two", "column_three"])

        with requests_mock.Mocker() as m:
            m.register_uri(
                requests_mock.POST,
                "http://localhost:8086/db/db/series"
            )

            cli = InfluxDBClient(database='db')
            cli.write_points_from_dataframe(
                self.dummy_dataframe, name="foo"
            )

            self.assertListEqual(
                json.loads(m.last_request.body),
                self.dummy_points
            )

    def test_write_points_from_dataframe_with_period_index(self):
        now = datetime(2014, 11, 16)
        self.dummy_points = [
            {
                "points": [
                    ["1", 1, 1.0, time.mktime(now.timetuple())],
                    ["2", 2, 2.0, time.mktime((now + timedelta(hours=24)).timetuple())]
                ],
                "name": "foo",
                "columns": ["column_one", "column_two", "column_three", "time"]
            }
        ]
        self.dummy_dataframe = pd.DataFrame(data=[["1", 1, 1.0], ["2", 2, 2.0]],
                                            index = [pd.Period('2014-11-16'), pd.Period('2014-11-17')],
                                            columns=["column_one", "column_two", "column_three"])

        with requests_mock.Mocker() as m:
            m.register_uri(
                requests_mock.POST,
                "http://localhost:8086/db/db/series"
            )

            cli = InfluxDBClient(database='db')
            cli.write_points_from_dataframe(
                self.dummy_dataframe, name="foo"
            )

            self.assertListEqual(
                json.loads(m.last_request.body),
                self.dummy_points
            )

    @raises(TypeError)
    def test_write_points_from_dataframe_fails_without_time_index(self):
        self.dummy_dataframe = pd.DataFrame(data=[["1", 1, 1.0], ["2", 2, 2.0]],
                                            columns=["column_one", "column_two", "column_three"])

        with requests_mock.Mocker() as m:
            m.register_uri(requests_mock.POST, "http://localhost:8086/db/db/series")

            cli = InfluxDBClient(database='db')
            cli.write_points_from_dataframe(self.dummy_dataframe, name="foo")

    @raises(TypeError)
    def test_write_points_from_dataframe_fails_with_anything_but_dataframe(self):
        now = datetime(2014, 11, 16)
        self.dummy_dataframe = pd.Series(data=[1.0, 2.0], index=[now, now+timedelta(hours=1)])

        with requests_mock.Mocker() as m:
            m.register_uri(requests_mock.POST, "http://localhost:8086/db/db/series")

            cli = InfluxDBClient(database='db')
            cli.write_points_from_dataframe(self.dummy_dataframe, name="foo")

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
                                 index=pd.to_datetime([1383876043, 1383876043, 1383876035, 1383876035],
                                                       unit='s', utc=True),
                                 columns=['sequence_number', 'column_one'])
        with _mocked_session('get', 200, data):
            cli = InfluxDBClient('host', 8086, 'username', 'password', 'db')
            result = cli.query('select column_one from foo;', output_format='dataframe')
            assert_frame_equal(dataframe, result)
