# -*- coding: utf-8 -*-
"""
unit tests for the InfluxDBClient.

NB/WARNING :
This module implements tests for the InfluxDBClient class
but does so
 + without any server instance running
 + by mocking all the expected responses.

So any change of (response format from) the server will **NOT** be
detected by this module.

See client_test_with_server.py for tests against a running server instance.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import json
import requests
import requests.exceptions
import socket
import time
import requests_mock
import random
from nose.tools import raises
from mock import patch
import warnings
import mock
import unittest

from influxdb import InfluxDBClient, InfluxDBClusterClient
from influxdb.client import InfluxDBServerError


def _build_response_object(status_code=200, content=""):
    resp = requests.Response()
    resp.status_code = status_code
    resp._content = content.encode("utf8")
    return resp


def _mocked_session(cli, method="GET", status_code=200, content=""):

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
        cli._session,
        'request',
        side_effect=request
    )

    return mocked


class TestInfluxDBClient(unittest.TestCase):

    def setUp(self):
        # By default, raise exceptions on warnings
        warnings.simplefilter('error', FutureWarning)

        self.cli = InfluxDBClient('localhost', 8086, 'username', 'password')
        self.dummy_points = [
            {
                "measurement": "cpu_load_short",
                "tags": {
                    "host": "server01",
                    "region": "us-west"
                },
                "time": "2009-11-10T23:00:00.123456Z",
                "fields": {
                    "value": 0.64
                }
            }
        ]

        self.dsn_string = 'influxdb://uSr:pWd@my.host.fr:1886/db'

    def test_scheme(self):
        cli = InfluxDBClient('host', 8086, 'username', 'password', 'database')
        self.assertEqual('http://host:8086', cli._baseurl)

        cli = InfluxDBClient(
            'host', 8086, 'username', 'password', 'database', ssl=True
        )
        self.assertEqual('https://host:8086', cli._baseurl)

    def test_dsn(self):
        cli = InfluxDBClient.from_DSN('influxdb://192.168.0.1:1886')
        self.assertEqual('http://192.168.0.1:1886', cli._baseurl)

        cli = InfluxDBClient.from_DSN(self.dsn_string)
        self.assertEqual('http://my.host.fr:1886', cli._baseurl)
        self.assertEqual('uSr', cli._username)
        self.assertEqual('pWd', cli._password)
        self.assertEqual('db', cli._database)
        self.assertFalse(cli.use_udp)

        cli = InfluxDBClient.from_DSN('udp+' + self.dsn_string)
        self.assertTrue(cli.use_udp)

        cli = InfluxDBClient.from_DSN('https+' + self.dsn_string)
        self.assertEqual('https://my.host.fr:1886', cli._baseurl)

        cli = InfluxDBClient.from_DSN('https+' + self.dsn_string,
                                      **{'ssl': False})
        self.assertEqual('http://my.host.fr:1886', cli._baseurl)

    def test_switch_database(self):
        cli = InfluxDBClient('host', 8086, 'username', 'password', 'database')
        cli.switch_database('another_database')
        self.assertEqual('another_database', cli._database)

    def test_switch_user(self):
        cli = InfluxDBClient('host', 8086, 'username', 'password', 'database')
        cli.switch_user('another_username', 'another_password')
        self.assertEqual('another_username', cli._username)
        self.assertEqual('another_password', cli._password)

    def test_write(self):
        with requests_mock.Mocker() as m:
            m.register_uri(
                requests_mock.POST,
                "http://localhost:8086/write",
                status_code=204
            )
            cli = InfluxDBClient(database='db')
            cli.write(
                {"database": "mydb",
                 "retentionPolicy": "mypolicy",
                 "points": [{"measurement": "cpu_load_short",
                             "tags": {"host": "server01",
                                      "region": "us-west"},
                             "time": "2009-11-10T23:00:00Z",
                             "fields": {"value": 0.64}}]}
            )

            self.assertEqual(
                m.last_request.body,
                b"cpu_load_short,host=server01,region=us-west "
                b"value=0.64 1257894000000000000\n",
            )

    def test_write_points(self):
        with requests_mock.Mocker() as m:
            m.register_uri(
                requests_mock.POST,
                "http://localhost:8086/write",
                status_code=204
            )

            cli = InfluxDBClient(database='db')
            cli.write_points(
                self.dummy_points,
            )
            self.assertEqual(
                'cpu_load_short,host=server01,region=us-west '
                'value=0.64 1257894000123456000\n',
                m.last_request.body.decode('utf-8'),
            )

    def test_write_points_toplevel_attributes(self):
        with requests_mock.Mocker() as m:
            m.register_uri(
                requests_mock.POST,
                "http://localhost:8086/write",
                status_code=204
            )

            cli = InfluxDBClient(database='db')
            cli.write_points(
                self.dummy_points,
                database='testdb',
                tags={"tag": "hello"},
                retention_policy="somepolicy"
            )
            self.assertEqual(
                'cpu_load_short,host=server01,region=us-west,tag=hello '
                'value=0.64 1257894000123456000\n',
                m.last_request.body.decode('utf-8'),
            )

    def test_write_points_batch(self):
        dummy_points = [
            {"measurement": "cpu_usage", "tags": {"unit": "percent"},
             "time": "2009-11-10T23:00:00Z", "fields": {"value": 12.34}},
            {"measurement": "network", "tags": {"direction": "in"},
             "time": "2009-11-10T23:00:00Z", "fields": {"value": 123.00}},
            {"measurement": "network", "tags": {"direction": "out"},
             "time": "2009-11-10T23:00:00Z", "fields": {"value": 12.00}}
        ]
        expected_last_body = (
            "network,direction=out,host=server01,region=us-west "
            "value=12.0 1257894000000000000\n"
        )

        with requests_mock.Mocker() as m:
            m.register_uri(requests_mock.POST,
                           "http://localhost:8086/write",
                           status_code=204)
            cli = InfluxDBClient(database='db')
            cli.write_points(points=dummy_points,
                             database='db',
                             tags={"host": "server01",
                                   "region": "us-west"},
                             batch_size=2)
        self.assertEqual(m.call_count, 2)
        self.assertEqual(expected_last_body,
                         m.last_request.body.decode('utf-8'))

    def test_write_points_udp(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        port = random.randint(4000, 8000)
        s.bind(('0.0.0.0', port))

        cli = InfluxDBClient(
            'localhost', 8086, 'root', 'root',
            'test', use_udp=True, udp_port=port
        )
        cli.write_points(self.dummy_points)

        received_data, addr = s.recvfrom(1024)

        self.assertEqual(
            'cpu_load_short,host=server01,region=us-west '
            'value=0.64 1257894000123456000\n',
            received_data.decode()
        )

    def test_write_bad_precision_udp(self):
        cli = InfluxDBClient(
            'localhost', 8086, 'root', 'root',
            'test', use_udp=True, udp_port=4444
        )

        with self.assertRaisesRegexp(
                Exception,
                "InfluxDB only supports seconds precision for udp writes"
        ):
            cli.write_points(
                self.dummy_points,
                time_precision='ms'
            )

    @raises(Exception)
    def test_write_points_fails(self):
        cli = InfluxDBClient('host', 8086, 'username', 'password', 'db')
        with _mocked_session(cli, 'post', 500):
            cli.write_points([])

    def test_write_points_with_precision(self):
        with requests_mock.Mocker() as m:
            m.register_uri(
                requests_mock.POST,
                "http://localhost:8086/write",
                status_code=204
            )

            cli = InfluxDBClient(database='db')

            cli.write_points(self.dummy_points, time_precision='n')
            self.assertEqual(
                b'cpu_load_short,host=server01,region=us-west '
                b'value=0.64 1257894000123456000\n',
                m.last_request.body,
            )

            cli.write_points(self.dummy_points, time_precision='u')
            self.assertEqual(
                b'cpu_load_short,host=server01,region=us-west '
                b'value=0.64 1257894000123456\n',
                m.last_request.body,
            )

            cli.write_points(self.dummy_points, time_precision='ms')
            self.assertEqual(
                b'cpu_load_short,host=server01,region=us-west '
                b'value=0.64 1257894000123\n',
                m.last_request.body,
            )

            cli.write_points(self.dummy_points, time_precision='s')
            self.assertEqual(
                b"cpu_load_short,host=server01,region=us-west "
                b"value=0.64 1257894000\n",
                m.last_request.body,
            )

            cli.write_points(self.dummy_points, time_precision='m')
            self.assertEqual(
                b'cpu_load_short,host=server01,region=us-west '
                b'value=0.64 20964900\n',
                m.last_request.body,
            )

            cli.write_points(self.dummy_points, time_precision='h')
            self.assertEqual(
                b'cpu_load_short,host=server01,region=us-west '
                b'value=0.64 349415\n',
                m.last_request.body,
            )

    def test_write_points_bad_precision(self):
        cli = InfluxDBClient()
        with self.assertRaisesRegexp(
            Exception,
            "Invalid time precision is given. "
            "\(use 'n', 'u', 'ms', 's', 'm' or 'h'\)"
        ):
            cli.write_points(
                self.dummy_points,
                time_precision='g'
            )

    @raises(Exception)
    def test_write_points_with_precision_fails(self):
        cli = InfluxDBClient('host', 8086, 'username', 'password', 'db')
        with _mocked_session(cli, 'post', 500):
            cli.write_points_with_precision([])

    def test_query(self):
        example_response = (
            '{"results": [{"series": [{"measurement": "sdfsdfsdf", '
            '"columns": ["time", "value"], "values": '
            '[["2009-11-10T23:00:00Z", 0.64]]}]}, {"series": '
            '[{"measurement": "cpu_load_short", "columns": ["time", "value"], '
            '"values": [["2009-11-10T23:00:00Z", 0.64]]}]}]}'
        )

        with requests_mock.Mocker() as m:
            m.register_uri(
                requests_mock.GET,
                "http://localhost:8086/query",
                text=example_response
            )
            rs = self.cli.query('select * from foo')

            self.assertListEqual(
                list(rs[0].get_points()),
                [{'value': 0.64, 'time': '2009-11-10T23:00:00Z'}]
            )

    @unittest.skip('Not implemented for 0.9')
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
            'measurement': 'foo',
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
        with _mocked_session(self.cli, 'get', 401):
            self.cli.query('select column_one from foo;')

    def test_create_database(self):
        with requests_mock.Mocker() as m:
            m.register_uri(
                requests_mock.GET,
                "http://localhost:8086/query",
                text='{"results":[{}]}'
            )
            self.cli.create_database('new_db')
            self.assertEqual(
                m.last_request.qs['q'][0],
                'create database "new_db"'
            )

    def test_create_numeric_named_database(self):
        with requests_mock.Mocker() as m:
            m.register_uri(
                requests_mock.GET,
                "http://localhost:8086/query",
                text='{"results":[{}]}'
            )
            self.cli.create_database('123')
            self.assertEqual(
                m.last_request.qs['q'][0],
                'create database "123"'
            )

    @raises(Exception)
    def test_create_database_fails(self):
        with _mocked_session(self.cli, 'post', 401):
            self.cli.create_database('new_db')

    def test_drop_database(self):
        with requests_mock.Mocker() as m:
            m.register_uri(
                requests_mock.GET,
                "http://localhost:8086/query",
                text='{"results":[{}]}'
            )
            self.cli.drop_database('new_db')
            self.assertEqual(
                m.last_request.qs['q'][0],
                'drop database "new_db"'
            )

    def test_drop_numeric_named_database(self):
        with requests_mock.Mocker() as m:
            m.register_uri(
                requests_mock.GET,
                "http://localhost:8086/query",
                text='{"results":[{}]}'
            )
            self.cli.drop_database('123')
            self.assertEqual(
                m.last_request.qs['q'][0],
                'drop database "123"'
            )

    def test_get_list_database(self):
        data = {'results': [
            {'series': [
                {'name': 'databases',
                 'values': [
                     ['new_db_1'],
                     ['new_db_2']],
                 'columns': ['name']}]}
        ]}

        with _mocked_session(self.cli, 'get', 200, json.dumps(data)):
            self.assertListEqual(
                self.cli.get_list_database(),
                [{'name': 'new_db_1'}, {'name': 'new_db_2'}]
            )

    @raises(Exception)
    def test_get_list_database_fails(self):
        cli = InfluxDBClient('host', 8086, 'username', 'password')
        with _mocked_session(cli, 'get', 401):
            cli.get_list_database()

    def test_get_list_servers(self):
        data = {'results': [
            {'series': [
                {'columns': ['id', 'cluster_addr', 'raft', 'raft-leader'],
                 'values': [
                    [1, 'server01:8088', True, True],
                    [2, 'server02:8088', True, False],
                    [3, 'server03:8088', True, False]]}]}
        ]}

        with _mocked_session(self.cli, 'get', 200, json.dumps(data)):
            self.assertListEqual(
                self.cli.get_list_servers(),
                [{'cluster_addr': 'server01:8088',
                  'id': 1,
                  'raft': True,
                  'raft-leader': True},
                 {'cluster_addr': 'server02:8088',
                  'id': 2,
                  'raft': True,
                  'raft-leader': False},
                 {'cluster_addr': 'server03:8088',
                  'id': 3,
                  'raft': True,
                  'raft-leader': False}]
            )

    @raises(Exception)
    def test_get_list_servers_fails(self):
        cli = InfluxDBClient('host', 8086, 'username', 'password')
        with _mocked_session(cli, 'get', 401):
            cli.get_list_servers()

    def test_get_list_series(self):

        data = {'results': [
            {'series': [
                {
                    'values': [
                        ['cpu_load_short,host=server01,region=us-west'],
                        ['memory_usage,host=server02,region=us-east']],
                    'columns': ['key']
                }
            ]}
        ]}

        with _mocked_session(self.cli, 'get', 200, json.dumps(data)):
            self.assertListEqual(
                self.cli.get_list_series(),
                [{'key': 'cpu_load_short,host=server01,region=us-west'},
                 {'key': 'memory_usage,host=server02,region=us-east'}])

    def test_get_list_series_with_measurement(self):

        data = {'results': [
            {'series': [
                {
                    'values': [
                        ['cpu_load_short,host=server01,region=us-west']],
                    'columns': ['key']
                }
            ]}
        ]}

        with _mocked_session(self.cli, 'get', 200, json.dumps(data)):
            self.assertListEqual(
                self.cli.get_list_series(measurement='cpu_load_short'),
                [{'key': 'cpu_load_short,host=server01,region=us-west'}])

    def test_get_list_series_with_tags(self):
        data = {'results': [
            {'series': [
                {
                    'values': [
                        ['cpu_load_short,host=server01,region=us-west']],
                    'columns': ['key']
                }
            ]}
        ]}

        with _mocked_session(self.cli, 'get', 200, json.dumps(data)):
            self.assertListEqual(
                self.cli.get_list_series(tags={'region': 'us-west'}),
                [{'key': 'cpu_load_short,host=server01,region=us-west'}])

    @raises(Exception)
    def test_get_list_series_fails(self):
        cli = InfluxDBClient('host', 8086, 'username', 'password')
        with _mocked_session(cli, 'get', 401):
            cli.get_list_series()

    def test_create_retention_policy_default(self):
        example_response = '{"results":[{}]}'

        with requests_mock.Mocker() as m:
            m.register_uri(
                requests_mock.GET,
                "http://localhost:8086/query",
                text=example_response
            )
            self.cli.create_retention_policy(
                'somename', '1d', 4, default=True, database='db'
            )

            self.assertEqual(
                m.last_request.qs['q'][0],
                'create retention policy "somename" on '
                '"db" duration 1d replication 4 default'
            )

    def test_create_retention_policy(self):
        example_response = '{"results":[{}]}'

        with requests_mock.Mocker() as m:
            m.register_uri(
                requests_mock.GET,
                "http://localhost:8086/query",
                text=example_response
            )
            self.cli.create_retention_policy(
                'somename', '1d', 4, database='db'
            )

            self.assertEqual(
                m.last_request.qs['q'][0],
                'create retention policy "somename" on '
                '"db" duration 1d replication 4'
            )

    def test_alter_retention_policy(self):
        example_response = '{"results":[{}]}'

        with requests_mock.Mocker() as m:
            m.register_uri(
                requests_mock.GET,
                "http://localhost:8086/query",
                text=example_response
            )
            # Test alter duration
            self.cli.alter_retention_policy('somename', 'db',
                                            duration='4d')
            self.assertEqual(
                m.last_request.qs['q'][0],
                'alter retention policy "somename" on "db" duration 4d'
            )
            # Test alter replication
            self.cli.alter_retention_policy('somename', 'db',
                                            replication=4)
            self.assertEqual(
                m.last_request.qs['q'][0],
                'alter retention policy "somename" on "db" replication 4'
            )

            # Test alter default
            self.cli.alter_retention_policy('somename', 'db',
                                            default=True)
            self.assertEqual(
                m.last_request.qs['q'][0],
                'alter retention policy "somename" on "db" default'
            )

    @raises(Exception)
    def test_alter_retention_policy_invalid(self):
        cli = InfluxDBClient('host', 8086, 'username', 'password')
        with _mocked_session(cli, 'get', 400):
            self.cli.alter_retention_policy('somename', 'db')

    def test_drop_retention_policy(self):
        example_response = '{"results":[{}]}'

        with requests_mock.Mocker() as m:
            m.register_uri(
                requests_mock.GET,
                "http://localhost:8086/query",
                text=example_response
            )
            self.cli.drop_retention_policy('somename', 'db')
            self.assertEqual(
                m.last_request.qs['q'][0],
                'drop retention policy "somename" on "db"'
            )

    @raises(Exception)
    def test_drop_retention_policy_fails(self):
        cli = InfluxDBClient('host', 8086, 'username', 'password')
        with _mocked_session(cli, 'delete', 401):
            cli.drop_retention_policy('default', 'db')

    def test_get_list_retention_policies(self):
        example_response = \
            '{"results": [{"series": [{"values": [["fsfdsdf", "24h0m0s", 2]],'\
            ' "columns": ["name", "duration", "replicaN"]}]}]}'

        with requests_mock.Mocker() as m:
            m.register_uri(
                requests_mock.GET,
                "http://localhost:8086/query",
                text=example_response
            )
            self.assertListEqual(
                self.cli.get_list_retention_policies(),
                [{'duration': '24h0m0s',
                  'name': 'fsfdsdf', 'replicaN': 2}]
            )

    @mock.patch('requests.Session.request')
    def test_request_retry(self, mock_request):
        """Tests that two connection errors will be handled"""

        class CustomMock(object):
            i = 0

            def connection_error(self, *args, **kwargs):
                self.i += 1

                if self.i < 3:
                    raise requests.exceptions.ConnectionError
                else:
                    r = requests.Response()
                    r.status_code = 204
                    return r

        mock_request.side_effect = CustomMock().connection_error

        cli = InfluxDBClient(database='db')
        cli.write_points(
            self.dummy_points
        )

    @mock.patch('requests.Session.request')
    def test_request_retry_raises(self, mock_request):
        """Tests that three connection errors will not be handled"""

        class CustomMock(object):
            i = 0

            def connection_error(self, *args, **kwargs):
                self.i += 1

                if self.i < 4:
                    raise requests.exceptions.ConnectionError
                else:
                    r = requests.Response()
                    r.status_code = 200
                    return r

        mock_request.side_effect = CustomMock().connection_error

        cli = InfluxDBClient(database='db')

        with self.assertRaises(requests.exceptions.ConnectionError):
            cli.write_points(self.dummy_points)

    def test_get_list_users(self):
        example_response = (
            '{"results":[{"series":[{"columns":["user","admin"],'
            '"values":[["test",false]]}]}]}'
        )

        with requests_mock.Mocker() as m:
            m.register_uri(
                requests_mock.GET,
                "http://localhost:8086/query",
                text=example_response
            )

            self.assertListEqual(
                self.cli.get_list_users(),
                [{'user': 'test', 'admin': False}]
            )

    def test_get_list_users_empty(self):
        example_response = (
            '{"results":[{"series":[{"columns":["user","admin"]}]}]}'
        )
        with requests_mock.Mocker() as m:
            m.register_uri(
                requests_mock.GET,
                "http://localhost:8086/query",
                text=example_response
            )

            self.assertListEqual(self.cli.get_list_users(), [])

    def test_grant_admin_privileges(self):
        example_response = '{"results":[{}]}'

        with requests_mock.Mocker() as m:
            m.register_uri(
                requests_mock.GET,
                "http://localhost:8086/query",
                text=example_response
            )
            self.cli.grant_admin_privileges('test')

            self.assertEqual(
                m.last_request.qs['q'][0],
                'grant all privileges to test'
            )

    @raises(Exception)
    def test_grant_admin_privileges_invalid(self):
        cli = InfluxDBClient('host', 8086, 'username', 'password')
        with _mocked_session(cli, 'get', 400):
            self.cli.grant_admin_privileges('')

    def test_revoke_admin_privileges(self):
        example_response = '{"results":[{}]}'

        with requests_mock.Mocker() as m:
            m.register_uri(
                requests_mock.GET,
                "http://localhost:8086/query",
                text=example_response
            )
            self.cli.revoke_admin_privileges('test')

            self.assertEqual(
                m.last_request.qs['q'][0],
                'revoke all privileges from test'
            )

    @raises(Exception)
    def test_revoke_admin_privileges_invalid(self):
        cli = InfluxDBClient('host', 8086, 'username', 'password')
        with _mocked_session(cli, 'get', 400):
            self.cli.revoke_admin_privileges('')

    def test_grant_privilege(self):
        example_response = '{"results":[{}]}'

        with requests_mock.Mocker() as m:
            m.register_uri(
                requests_mock.GET,
                "http://localhost:8086/query",
                text=example_response
            )
            self.cli.grant_privilege('read', 'testdb', 'test')

            self.assertEqual(
                m.last_request.qs['q'][0],
                'grant read on testdb to test'
            )

    @raises(Exception)
    def test_grant_privilege_invalid(self):
        cli = InfluxDBClient('host', 8086, 'username', 'password')
        with _mocked_session(cli, 'get', 400):
            self.cli.grant_privilege('', 'testdb', 'test')

    def test_revoke_privilege(self):
        example_response = '{"results":[{}]}'

        with requests_mock.Mocker() as m:
            m.register_uri(
                requests_mock.GET,
                "http://localhost:8086/query",
                text=example_response
            )
            self.cli.revoke_privilege('read', 'testdb', 'test')

            self.assertEqual(
                m.last_request.qs['q'][0],
                'revoke read on testdb from test'
            )

    @raises(Exception)
    def test_revoke_privilege_invalid(self):
        cli = InfluxDBClient('host', 8086, 'username', 'password')
        with _mocked_session(cli, 'get', 400):
            self.cli.revoke_privilege('', 'testdb', 'test')

    def test_get_list_privileges(self):
        data = {'results': [
            {'series': [
                {'columns': ['database', 'privilege'],
                 'values': [
                     ['db1', 'READ'],
                     ['db2', 'ALL PRIVILEGES'],
                     ['db3', 'NO PRIVILEGES']]}
            ]}
        ]}

        with _mocked_session(self.cli, 'get', 200, json.dumps(data)):
            self.assertListEqual(
                self.cli.get_list_privileges('test'),
                [{'database': 'db1', 'privilege': 'READ'},
                 {'database': 'db2', 'privilege': 'ALL PRIVILEGES'},
                 {'database': 'db3', 'privilege': 'NO PRIVILEGES'}]
            )

    @raises(Exception)
    def test_get_list_privileges_fails(self):
        cli = InfluxDBClient('host', 8086, 'username', 'password')
        with _mocked_session(cli, 'get', 401):
            cli.get_list_privileges('test')

    def test_invalid_port_fails(self):
        with self.assertRaises(ValueError):
            InfluxDBClient('host', '80/redir', 'username', 'password')


class FakeClient(InfluxDBClient):

    def __init__(self, *args, **kwargs):
        super(FakeClient, self).__init__(*args, **kwargs)

    def query(self,
              query,
              params={},
              expected_response_code=200,
              database=None):
        if query == 'Fail':
            raise Exception("Fail")
        elif query == 'Fail once' and self._host == 'host1':
            raise Exception("Fail Once")
        elif query == 'Fail twice' and self._host in 'host1 host2':
            raise Exception("Fail Twice")
        else:
            return "Success"


class TestInfluxDBClusterClient(unittest.TestCase):

    def setUp(self):
        # By default, raise exceptions on warnings
        warnings.simplefilter('error', FutureWarning)

        self.hosts = [('host1', 8086), ('host2', 8086), ('host3', 8086)]
        self.dsn_string = 'influxdb://uSr:pWd@host1:8086,uSr:pWd@host2:8086/db'

    def test_init(self):
        cluster = InfluxDBClusterClient(hosts=self.hosts,
                                        username='username',
                                        password='password',
                                        database='database',
                                        shuffle=False,
                                        client_base_class=FakeClient)
        self.assertEqual(3, len(cluster.hosts))
        self.assertEqual(0, len(cluster.bad_hosts))
        self.assertIn((cluster._client._host,
                       cluster._client._port), cluster.hosts)

    def test_one_server_fails(self):
        cluster = InfluxDBClusterClient(hosts=self.hosts,
                                        database='database',
                                        shuffle=False,
                                        client_base_class=FakeClient)
        self.assertEqual('Success', cluster.query('Fail once'))
        self.assertEqual(2, len(cluster.hosts))
        self.assertEqual(1, len(cluster.bad_hosts))

    def test_two_servers_fail(self):
        cluster = InfluxDBClusterClient(hosts=self.hosts,
                                        database='database',
                                        shuffle=False,
                                        client_base_class=FakeClient)
        self.assertEqual('Success', cluster.query('Fail twice'))
        self.assertEqual(1, len(cluster.hosts))
        self.assertEqual(2, len(cluster.bad_hosts))

    def test_all_fail(self):
        cluster = InfluxDBClusterClient(hosts=self.hosts,
                                        database='database',
                                        shuffle=True,
                                        client_base_class=FakeClient)
        with self.assertRaises(InfluxDBServerError):
            cluster.query('Fail')
        self.assertEqual(0, len(cluster.hosts))
        self.assertEqual(3, len(cluster.bad_hosts))

    def test_all_good(self):
        cluster = InfluxDBClusterClient(hosts=self.hosts,
                                        database='database',
                                        shuffle=True,
                                        client_base_class=FakeClient)
        self.assertEqual('Success', cluster.query(''))
        self.assertEqual(3, len(cluster.hosts))
        self.assertEqual(0, len(cluster.bad_hosts))

    def test_recovery(self):
        cluster = InfluxDBClusterClient(hosts=self.hosts,
                                        database='database',
                                        shuffle=True,
                                        client_base_class=FakeClient)
        with self.assertRaises(InfluxDBServerError):
            cluster.query('Fail')
        self.assertEqual('Success', cluster.query(''))
        self.assertEqual(1, len(cluster.hosts))
        self.assertEqual(2, len(cluster.bad_hosts))

    def test_healing(self):
        cluster = InfluxDBClusterClient(hosts=self.hosts,
                                        database='database',
                                        shuffle=True,
                                        healing_delay=1,
                                        client_base_class=FakeClient)
        with self.assertRaises(InfluxDBServerError):
            cluster.query('Fail')
        self.assertEqual('Success', cluster.query(''))
        time.sleep(1.1)
        self.assertEqual('Success', cluster.query(''))
        self.assertEqual(2, len(cluster.hosts))
        self.assertEqual(1, len(cluster.bad_hosts))
        time.sleep(1.1)
        self.assertEqual('Success', cluster.query(''))
        self.assertEqual(3, len(cluster.hosts))
        self.assertEqual(0, len(cluster.bad_hosts))

    def test_dsn(self):
        cli = InfluxDBClusterClient.from_DSN(self.dsn_string)
        self.assertEqual([('host1', 8086), ('host2', 8086)], cli.hosts)
        self.assertEqual('http://host1:8086', cli._client._baseurl)
        self.assertEqual('uSr', cli._client._username)
        self.assertEqual('pWd', cli._client._password)
        self.assertEqual('db', cli._client._database)
        self.assertFalse(cli._client.use_udp)

        cli = InfluxDBClusterClient.from_DSN('udp+' + self.dsn_string)
        self.assertTrue(cli._client.use_udp)

        cli = InfluxDBClusterClient.from_DSN('https+' + self.dsn_string)
        self.assertEqual('https://host1:8086', cli._client._baseurl)

        cli = InfluxDBClusterClient.from_DSN('https+' + self.dsn_string,
                                             **{'ssl': False})
        self.assertEqual('http://host1:8086', cli._client._baseurl)

    def test_dsn_password_caps(self):
        cli = InfluxDBClusterClient.from_DSN(
            'https+influxdb://usr:pWd@host:8086/db')
        self.assertEqual('pWd', cli._client._password)

    def test_dsn_mixed_scheme_case(self):
        cli = InfluxDBClusterClient.from_DSN(
            'hTTps+inFLUxdb://usr:pWd@host:8086/db')
        self.assertEqual('pWd', cli._client._password)
        self.assertEqual('https://host:8086', cli._client._baseurl)

        cli = InfluxDBClusterClient.from_DSN(
            'uDP+influxdb://usr:pwd@host1:8086,usr:pwd@host2:8086/db')
        self.assertTrue(cli._client.use_udp)
