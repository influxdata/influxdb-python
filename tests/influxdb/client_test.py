# -*- coding: utf-8 -*-
"""
unit tests
"""
import json
import requests
from nose.tools import raises
from mock import patch

from influxdb import InfluxDBClient
from influxdb.client import session


def _build_response_object(status_code=200, content=""):
    resp = requests.Response()
    resp.status_code = status_code
    resp._content = content
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
                assert c == json.loads(data)

                c = data

        # Anyway, Content must be a JSON string (or empty string)
        if not isinstance(c, str):
            c =  json.dumps(c)

        return _build_response_object(status_code=status_code, content=c)

    mocked = patch.object(
        session,
        'request',
        side_effect = request
        )

    return mocked


class TestInfluxDBClient(object):

    def test_scheme(self):
        cli = InfluxDBClient('host', 8086, 'username', 'password', 'database')
        assert cli._baseurl == 'http://host:8086'

        cli = InfluxDBClient('host', 8086, 'username', 'password', 'database', ssl=True)
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
        data = [
            {
                "points": [
                    ["1", 1, 1.0],
                    ["2", 2, 2.0]
                ],
                "name": "foo",
                "columns": ["column_one", "column_two", "column_three"]
            }
        ]

        with _mocked_session('post', 200, data) as mocked:
            cli = InfluxDBClient('host', 8086, 'username', 'password', 'db')
            assert cli.write_points(data) is True

    @raises(Exception)
    def test_write_points_fails(self):
        with _mocked_session('post', 500) as mocked:
            cli = InfluxDBClient('host', 8086, 'username', 'password', 'db')
            cli.write_points([])

    def test_write_points_with_precision(self):
        data = [
            {
                "points": [
                    ["1", 1, 1.0],
                    ["2", 2, 2.0]
                ],
                "name": "foo",
                "columns": ["column_one", "column_two", "column_three"]
            }
        ]

        with _mocked_session('post', 200, data) as mocked:
            cli = InfluxDBClient('host', 8086, 'username', 'password', 'db')
            assert cli.write_points_with_precision(data) is True

    @raises(Exception)
    def test_write_points_with_precision_fails(self):
        with _mocked_session('post', 500) as mocked:
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
        with _mocked_session('delete', 400) as mocked:
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
            {   "name":"foo",
                "columns": ["time", "sequence_number", "column_one"],
                "points": [
                    [1383876043, 16, "2"], [1383876043, 15, "1"],
                    [1383876035, 14, "2"], [1383876035, 13, "1"]
                ]
            }
        ]
        with _mocked_session('get', 200, data) as mocked:
            cli = InfluxDBClient('host', 8086, 'username', 'password', 'db')
            result = cli.query('select column_one from foo;')
            assert len(result[0]['points']) == 4

    @raises(Exception)
    def test_query_fail(self):
        with _mocked_session('get', 401) as mocked:
            cli = InfluxDBClient('host', 8086, 'username', 'password', 'db')
            cli.query('select column_one from foo;')

    def test_create_database(self):
        with _mocked_session('post', 201, {"name": "new_db"}) as mocked:
            cli = InfluxDBClient('host', 8086, 'username', 'password', 'db')
            assert cli.create_database('new_db') is True

    @raises(Exception)
    def test_create_database_fails(self):
        with _mocked_session('post', 401) as mocked:
            cli = InfluxDBClient('host', 8086, 'username', 'password', 'db')
            cli.create_database('new_db')

    def test_delete_database(self):
        with _mocked_session('delete', 204) as mocked:
            cli = InfluxDBClient('host', 8086, 'username', 'password', 'db')
            assert cli.delete_database('old_db') is True

    @raises(Exception)
    def test_delete_database_fails(self):
        with _mocked_session('delete', 401) as mocked:
            cli = InfluxDBClient('host', 8086, 'username', 'password', 'db')
            cli.delete_database('old_db')

    def test_get_database_list(self):
        data = [
            {"name": "a_db"}
        ]
        with _mocked_session('get', 200, data) as mocked:
            cli = InfluxDBClient('host', 8086, 'username', 'password')
            assert len(cli.get_database_list()) == 1
            assert cli.get_database_list()[0]['name'] == 'a_db'

    @raises(Exception)
    def test_get_database_list_fails(self):
        with _mocked_session('get', 401) as mocked:
            cli = InfluxDBClient('host', 8086, 'username', 'password')
            cli.get_database_list()

    def test_delete_series(self):
        with _mocked_session('delete', 204) as mocked:
            cli = InfluxDBClient('host', 8086, 'username', 'password', 'db')
            cli.delete_series('old_series')

    @raises(Exception)
    def test_delete_series_fails(self):
        with _mocked_session('delete', 401) as mocked:
            cli = InfluxDBClient('host', 8086, 'username', 'password', 'db')
            cli.delete_series('old_series')

    def test_get_list_cluster_admins(self):
        pass

    def test_add_cluster_admin(self):
        pass

    def test_update_cluster_admin_password(self):
        pass

    def test_delete_cluster_admin(self):
        pass

    def test_set_database_admin(self):
        pass

    def test_unset_database_admin(self):
        pass

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

    def test_get_database_user(self):
        pass

    def test_add_database_user(self):
        pass

    def test_update_database_user_password(self):
        pass

    def test_delete_database_user(self):
        pass

    @raises(NotImplementedError)
    def test_update_permission(self):
        cli = InfluxDBClient('host', 8086, 'username', 'password', 'db')
        cli.update_permission('admin', [])
