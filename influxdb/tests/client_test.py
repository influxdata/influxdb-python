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
import json
import requests
import requests.exceptions
import socket
import unittest
import requests_mock
import random
from nose.tools import raises
from mock import patch
import warnings
import mock

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

        self.dsn_string = 'influxdb://uSr:pWd@host:1886/db'

    def test_scheme(self):
        cli = InfluxDBClient('host', 8086, 'username', 'password', 'database')
        self.assertEqual('http://host:8086', cli._baseurl)

        cli = InfluxDBClient(
            'host', 8086, 'username', 'password', 'database', ssl=True
        )
        self.assertEqual('https://host:8086', cli._baseurl)

    def test_dsn(self):
        cli = InfluxDBClient.from_DSN(self.dsn_string)
        self.assertEqual('http://host:1886', cli._baseurl)
        self.assertEqual('uSr', cli._username)
        self.assertEqual('pWd', cli._password)
        self.assertEqual('db', cli._database)
        self.assertFalse(cli.use_udp)

        cli = InfluxDBClient.from_DSN('udp+' + self.dsn_string)
        self.assertTrue(cli.use_udp)

        cli = InfluxDBClient.from_DSN('https+' + self.dsn_string)
        self.assertEqual('https://host:1886', cli._baseurl)

        cli = InfluxDBClient.from_DSN('https+' + self.dsn_string,
                                      **{'ssl': False})
        self.assertEqual('http://host:1886', cli._baseurl)

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
                'create database new_db'
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
                'drop database new_db'
            )

    @raises(Exception)
    def test_drop_database_fails(self):
        cli = InfluxDBClient('host', 8086, 'username', 'password', 'db')
        with _mocked_session(cli, 'delete', 401):
            cli.drop_database('old_db')

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

    def test_get_list_series(self):
        example_response = \
            '{"results": [{"series": [{"name": "cpu_load_short", "columns": ' \
            '["_id", "host", "region"], "values": ' \
            '[[1, "server01", "us-west"]]}]}]}'

        with requests_mock.Mocker() as m:
            m.register_uri(
                requests_mock.GET,
                "http://localhost:8086/query",
                text=example_response
            )

            self.assertListEqual(
                self.cli.get_list_series(),
                [{'name': 'cpu_load_short',
                  'tags': [
                      {'host': 'server01', '_id': 1, 'region': 'us-west'}
                  ]}]
            )

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
                'create retention policy somename on '
                'db duration 1d replication 4 default'
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
                'create retention policy somename on '
                'db duration 1d replication 4'
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
                'alter retention policy somename on db duration 4d'
            )
            # Test alter replication
            self.cli.alter_retention_policy('somename', 'db',
                                            replication=4)
            self.assertEqual(
                m.last_request.qs['q'][0],
                'alter retention policy somename on db replication 4'
            )

            # Test alter default
            self.cli.alter_retention_policy('somename', 'db',
                                            default=True)
            self.assertEqual(
                m.last_request.qs['q'][0],
                'alter retention policy somename on db default'
            )

    @raises(Exception)
    def test_alter_retention_policy_invalid(self):
        cli = InfluxDBClient('host', 8086, 'username', 'password')
        with _mocked_session(cli, 'get', 400):
            self.cli.alter_retention_policy('somename', 'db')

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


class FakeClient(InfluxDBClient):
    fail = False

    def query(self,
              query,
              params={},
              expected_response_code=200,
              database=None):
        if query == 'Fail':
            raise Exception("Fail")

        if self.fail:
            raise Exception("Fail")
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
        self.assertEqual(3, len(cluster.clients))
        self.assertEqual(0, len(cluster.bad_clients))
        for idx, client in enumerate(cluster.clients):
            self.assertEqual(self.hosts[idx][0], client._host)
            self.assertEqual(self.hosts[idx][1], client._port)

    def test_one_server_fails(self):
        cluster = InfluxDBClusterClient(hosts=self.hosts,
                                        database='database',
                                        shuffle=False,
                                        client_base_class=FakeClient)
        cluster.clients[0].fail = True
        self.assertEqual('Success', cluster.query(''))
        self.assertEqual(2, len(cluster.clients))
        self.assertEqual(1, len(cluster.bad_clients))

    def test_two_servers_fail(self):
        cluster = InfluxDBClusterClient(hosts=self.hosts,
                                        database='database',
                                        shuffle=False,
                                        client_base_class=FakeClient)
        cluster.clients[0].fail = True
        cluster.clients[1].fail = True
        self.assertEqual('Success', cluster.query(''))
        self.assertEqual(1, len(cluster.clients))
        self.assertEqual(2, len(cluster.bad_clients))

    def test_all_fail(self):
        cluster = InfluxDBClusterClient(hosts=self.hosts,
                                        database='database',
                                        shuffle=True,
                                        client_base_class=FakeClient)
        with self.assertRaises(InfluxDBServerError):
            cluster.query('Fail')
        self.assertEqual(0, len(cluster.clients))
        self.assertEqual(3, len(cluster.bad_clients))

    def test_all_good(self):
        cluster = InfluxDBClusterClient(hosts=self.hosts,
                                        database='database',
                                        shuffle=True,
                                        client_base_class=FakeClient)
        self.assertEqual('Success', cluster.query(''))
        self.assertEqual(3, len(cluster.clients))
        self.assertEqual(0, len(cluster.bad_clients))

    def test_recovery(self):
        cluster = InfluxDBClusterClient(hosts=self.hosts,
                                        database='database',
                                        shuffle=True,
                                        client_base_class=FakeClient)
        with self.assertRaises(InfluxDBServerError):
            cluster.query('Fail')
        self.assertEqual('Success', cluster.query(''))
        self.assertEqual(1, len(cluster.clients))
        self.assertEqual(2, len(cluster.bad_clients))

    def test_switch_database(self):
        c = InfluxDBClusterClient(hosts=self.hosts,
                                  shuffle=True,
                                  client_base_class=InfluxDBClient)
        self.assertEqual(3, len(c.clients))
        self.assertEqual(0, len(c.bad_clients))
        map(lambda x: self.assertEqual(None, x._database), c.clients)
        c.switch_database('database')
        map(lambda x: self.assertEqual('database', x._database), c.clients)

    def test_switch_user(self):
        c = InfluxDBClusterClient(hosts=self.hosts,
                                  shuffle=True,
                                  client_base_class=InfluxDBClient)
        self.assertEqual(3, len(c.clients))
        self.assertEqual(0, len(c.bad_clients))
        map(lambda x: self.assertEqual('root', x._username), c.clients)
        map(lambda x: self.assertEqual('root', x._password), c.clients)
        c.switch_user('username', 'password')
        map(lambda x: self.assertEqual('username', x._username), c.clients)
        map(lambda x: self.assertEqual('password', x._password), c.clients)

    def test_dsn(self):
        cli = InfluxDBClusterClient.from_DSN(self.dsn_string)
        self.assertEqual(2, len(cli.clients))
        self.assertEqual('http://host1:8086', cli.clients[0]._baseurl)
        self.assertEqual('uSr', cli.clients[0]._username)
        self.assertEqual('pWd', cli.clients[0]._password)
        self.assertEqual('db', cli.clients[0]._database)
        self.assertFalse(cli.clients[0].use_udp)
        self.assertEqual('http://host2:8086', cli.clients[1]._baseurl)
        self.assertEqual('uSr', cli.clients[1]._username)
        self.assertEqual('pWd', cli.clients[1]._password)
        self.assertEqual('db', cli.clients[1]._database)
        self.assertFalse(cli.clients[1].use_udp)

        cli = InfluxDBClusterClient.from_DSN('udp+' + self.dsn_string)
        self.assertTrue(cli.clients[0].use_udp)
        self.assertTrue(cli.clients[1].use_udp)

        cli = InfluxDBClusterClient.from_DSN('https+' + self.dsn_string)
        self.assertEqual('https://host1:8086', cli.clients[0]._baseurl)
        self.assertEqual('https://host2:8086', cli.clients[1]._baseurl)

        cli = InfluxDBClusterClient.from_DSN('https+' + self.dsn_string,
                                             **{'ssl': False})
        self.assertEqual('http://host1:8086', cli.clients[0]._baseurl)
        self.assertEqual('http://host2:8086', cli.clients[1]._baseurl)

    def test_dsn_single_client(self):
        cli = InfluxDBClusterClient.from_DSN('influxdb://usr:pwd@host:8086/db')
        self.assertEqual('http://host:8086', cli.clients[0]._baseurl)
        self.assertEqual('usr', cli.clients[0]._username)
        self.assertEqual('pwd', cli.clients[0]._password)
        self.assertEqual('db', cli.clients[0]._database)
        self.assertFalse(cli.clients[0].use_udp)

        cli = InfluxDBClusterClient.from_DSN(
            'udp+influxdb://usr:pwd@host:8086/db')
        self.assertTrue(cli.clients[0].use_udp)

        cli = InfluxDBClusterClient.from_DSN(
            'https+influxdb://usr:pwd@host:8086/db')
        self.assertEqual('https://host:8086', cli.clients[0]._baseurl)

        cli = InfluxDBClusterClient.from_DSN(
            'https+influxdb://usr:pwd@host:8086/db',
            **{'ssl': False})
        self.assertEqual('http://host:8086', cli.clients[0]._baseurl)

    def test_dsn_password_caps(self):
        cli = InfluxDBClusterClient.from_DSN(
            'https+influxdb://usr:pWd@host:8086/db')
        self.assertEqual('pWd', cli.clients[0]._password)

    def test_dsn_mixed_scheme_case(self):
        cli = InfluxDBClusterClient.from_DSN(
            'hTTps+inFLUxdb://usr:pWd@host:8086/db')
        self.assertEqual('pWd', cli.clients[0]._password)
        self.assertEqual('https://host:8086', cli.clients[0]._baseurl)

        cli = InfluxDBClusterClient.from_DSN(
            'uDP+influxdb://usr:pwd@host1:8086,usr:pwd@host2:8086/db')
        self.assertTrue(cli.clients[0].use_udp)
