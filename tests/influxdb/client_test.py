# -*- coding: utf-8 -*-
"""
unit tests
"""
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


class TestInfluxDBClient(object):
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

        with patch.object(session, 'post') as mocked_post:
            mocked_post.return_value = _build_response_object(status_code=200)
            cli = InfluxDBClient('host', 8086, 'username', 'password', 'db')
            assert cli.write_points(data) is True

    @raises(Exception)
    def test_write_points_fails(self):
        with patch.object(session, 'post') as mocked_post:
            mocked_post.return_value = _build_response_object(status_code=500)
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

        with patch.object(session, 'post') as mocked_post:
            mocked_post.return_value = _build_response_object(status_code=200)
            cli = InfluxDBClient('host', 8086, 'username', 'password', 'db')
            assert cli.write_points_with_precision(data) is True

    @raises(Exception)
    def test_write_points_with_precision_fails(self):
        with patch.object(session, 'post') as mocked_post:
            mocked_post.return_value = _build_response_object(status_code=500)
            cli = InfluxDBClient('host', 8086, 'username', 'password', 'db')
            cli.write_points_with_precision([])

    @raises(NotImplementedError)
    def test_delete_points(self):
        cli = InfluxDBClient('host', 8086, 'username', 'password', 'db')
        cli.delete_points([])

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
        expected = ('[{"name":"foo",'
                    '"columns":["time","sequence_number","column_one"],'
                    '"points":[[1383876043,16,"2"],[1383876043,15,"1"],'
                    '[1383876035,14,"2"],[1383876035,13,"1"]]}]')
        with patch.object(session, 'get') as mocked_get:
            mocked_get.return_value = _build_response_object(
                status_code=200,
                content=expected)
            cli = InfluxDBClient('host', 8086, 'username', 'password', 'db')
            result = cli.query('select column_one from foo;')
            assert len(result[0]['points']) == 4

    @raises(Exception)
    def test_query_fail(self):
        with patch.object(session, 'get') as mocked_get:
            mocked_get.return_value = _build_response_object(status_code=401)
            cli = InfluxDBClient('host', 8086, 'username', 'password', 'db')
            cli.query('select column_one from foo;')

    def test_create_database(self):
        with patch.object(session, 'post') as mocked_post:
            mocked_post.return_value = _build_response_object(status_code=201)
            cli = InfluxDBClient('host', 8086, 'username', 'password', 'db')
            assert cli.create_database('new_db') is True

    @raises(Exception)
    def test_creata_database_fails(self):
        with patch.object(session, 'post') as mocked_post:
            mocked_post.return_value = _build_response_object(status_code=401)
            cli = InfluxDBClient('host', 8086, 'username', 'password', 'db')
            cli.create_database('new_db')

    def test_delete_database(self):
        with patch.object(session, 'delete') as mocked_post:
            mocked_post.return_value = _build_response_object(status_code=204)
            cli = InfluxDBClient('host', 8086, 'username', 'password', 'db')
            assert cli.delete_database('old_db') is True

    @raises(Exception)
    def test_delete_database_fails(self):
        with patch.object(session, 'delete') as mocked_post:
            mocked_post.return_value = _build_response_object(status_code=401)
            cli = InfluxDBClient('host', 8086, 'username', 'password', 'db')
            cli.delete_database('old_db')

    def test_delete_series(self):
        with patch.object(session, 'delete') as mocked_delete:
            mocked_delete.return_value = _build_response_object(status_code=204)
            cli = InfluxDBClient('host', 8086, 'username', 'password', 'db')
            cli.delete_series('old_series')

    @raises(Exception)
    def test_delete_series_fails(self):
        with patch.object(session, 'delete') as mocked_delete:
            mocked_delete.return_value = _build_response_object(status_code=401)
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
