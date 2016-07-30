# -*- coding: utf-8 -*-
"""
unit tests for checking the good/expected interaction between :

+ the python client.. (obviously)
+ and a *_real_* server instance running.

This basically duplicates what's in client_test.py
 but without mocking around every call.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from functools import partial
import os
import time
import unittest
import warnings

# By default, raise exceptions on warnings
warnings.simplefilter('error', FutureWarning)

from influxdb import InfluxDBClient
from influxdb.exceptions import InfluxDBClientError

from influxdb.tests import skipIfPYpy, using_pypy, skipServerTests
from influxdb.tests.server_tests.base import ManyTestCasesWithServerMixin
from influxdb.tests.server_tests.base import SingleTestCaseWithServerMixin

if not using_pypy:
    import pandas as pd
    from pandas.util.testing import assert_frame_equal


THIS_DIR = os.path.abspath(os.path.dirname(__file__))


def point(serie_name, timestamp=None, tags=None, **fields):
    res = {'measurement': serie_name}
    if timestamp:
        res['time'] = timestamp
    if tags:
        res['tags'] = tags
    res['fields'] = fields
    return res


dummy_point = [  # some dummy points
    {
        "measurement": "cpu_load_short",
        "tags": {
            "host": "server01",
            "region": "us-west"
        },
        "time": "2009-11-10T23:00:00Z",
        "fields": {
            "value": 0.64
        }
    }
]

dummy_points = [  # some dummy points
    dummy_point[0],
    {
        "measurement": "memory",
        "tags": {
            "host": "server01",
            "region": "us-west"
        },
        "time": "2009-11-10T23:01:35Z",
        "fields": {
            "value": 33.0
        }
    }
]

if not using_pypy:
    dummy_pointDF = {
        "measurement": "cpu_load_short",
        "tags": {"host": "server01",
                 "region": "us-west"},
        "dataframe": pd.DataFrame(
            [[0.64]], columns=['value'],
            index=pd.to_datetime(["2009-11-10T23:00:00Z"]))
    }
    dummy_pointsDF = [{
        "measurement": "cpu_load_short",
        "tags": {"host": "server01", "region": "us-west"},
        "dataframe": pd.DataFrame(
            [[0.64]], columns=['value'],
            index=pd.to_datetime(["2009-11-10T23:00:00Z"])),
    }, {
        "measurement": "memory",
        "tags": {"host": "server01", "region": "us-west"},
        "dataframe": pd.DataFrame(
            [[33]], columns=['value'],
            index=pd.to_datetime(["2009-11-10T23:01:35Z"])
        )
    }]


dummy_point_without_timestamp = [
    {
        "measurement": "cpu_load_short",
        "tags": {
            "host": "server02",
            "region": "us-west"
        },
        "fields": {
            "value": 0.64
        }
    }
]


@skipServerTests
class SimpleTests(SingleTestCaseWithServerMixin,
                  unittest.TestCase):

    influxdb_template_conf = os.path.join(THIS_DIR, 'influxdb.conf.template')

    def test_fresh_server_no_db(self):
        self.assertEqual([], self.cli.get_list_database())

    def test_create_database(self):
        self.assertIsNone(self.cli.create_database('new_db_1'))
        self.assertIsNone(self.cli.create_database('new_db_2'))
        self.assertEqual(
            self.cli.get_list_database(),
            [{'name': 'new_db_1'}, {'name': 'new_db_2'}]
        )

    def test_drop_database(self):
        self.test_create_database()
        self.assertIsNone(self.cli.drop_database('new_db_1'))
        self.assertEqual([{'name': 'new_db_2'}], self.cli.get_list_database())

    def test_query_fail(self):
        with self.assertRaises(InfluxDBClientError) as ctx:
            self.cli.query('select column_one from foo')
        self.assertIn('database not found: db',
                      ctx.exception.content)

    def test_query_fail_ignore_errors(self):
        result = self.cli.query('select column_one from foo',
                                raise_errors=False)
        self.assertEqual(result.error, 'database not found: db')

    def test_create_user(self):
        self.cli.create_user('test_user', 'secret_password')
        rsp = list(self.cli.query("SHOW USERS")['results'])
        self.assertIn({'user': 'test_user', 'admin': False},
                      rsp)

    def test_create_user_admin(self):
        self.cli.create_user('test_user', 'secret_password', True)
        rsp = list(self.cli.query("SHOW USERS")['results'])
        self.assertIn({'user': 'test_user', 'admin': True},
                      rsp)

    def test_create_user_blank_password(self):
        self.cli.create_user('test_user', '')
        rsp = list(self.cli.query("SHOW USERS")['results'])
        self.assertIn({'user': 'test_user', 'admin': False},
                      rsp)

    def test_get_list_users_empty(self):
        rsp = self.cli.get_list_users()
        self.assertEqual([], rsp)

    def test_get_list_users(self):
        self.cli.query("CREATE USER test WITH PASSWORD 'test'")
        rsp = self.cli.get_list_users()

        self.assertEqual(
            [{'user': 'test', 'admin': False}],
            rsp
        )

    def test_create_user_blank_username(self):
        with self.assertRaises(InfluxDBClientError) as ctx:
            self.cli.create_user('', 'secret_password')
        self.assertIn('username required',
                      ctx.exception.content)
        rsp = list(self.cli.query("SHOW USERS")['results'])
        self.assertEqual(rsp, [])

    def test_drop_user(self):
        self.cli.query("CREATE USER test WITH PASSWORD 'test'")
        self.cli.drop_user('test')
        users = list(self.cli.query("SHOW USERS")['results'])
        self.assertEqual(users, [])

    def test_drop_user_nonexisting(self):
        with self.assertRaises(InfluxDBClientError) as ctx:
            self.cli.drop_user('test')
        self.assertIn('user not found',
                      ctx.exception.content)

    def test_drop_user_invalid(self):
        with self.assertRaises(InfluxDBClientError) as ctx:
            self.cli.drop_user('very invalid')
        self.assertEqual(400, ctx.exception.code)
        self.assertIn('{"error":"error parsing query: '
                      'found invalid, expected',
                      ctx.exception.content)

    @unittest.skip("Broken as of 0.9.0")
    def test_revoke_admin_privileges(self):
        self.cli.create_user('test', 'test', admin=True)
        self.assertEqual([{'user': 'test', 'admin': True}],
                         self.cli.get_list_users())
        self.cli.revoke_admin_privileges('test')
        self.assertEqual([{'user': 'test', 'admin': False}],
                         self.cli.get_list_users())

    def test_revoke_admin_privileges_invalid(self):
        with self.assertRaises(InfluxDBClientError) as ctx:
            self.cli.revoke_admin_privileges('')
        self.assertEqual(400, ctx.exception.code)
        self.assertIn('{"error":"error parsing query: ',
                      ctx.exception.content)

    def test_grant_privilege(self):
        self.cli.create_user('test', 'test')
        self.cli.create_database('testdb')
        self.cli.grant_privilege('all', 'testdb', 'test')
        # TODO: when supported by InfluxDB, check if privileges are granted

    def test_grant_privilege_invalid(self):
        self.cli.create_user('test', 'test')
        self.cli.create_database('testdb')
        with self.assertRaises(InfluxDBClientError) as ctx:
            self.cli.grant_privilege('', 'testdb', 'test')
        self.assertEqual(400, ctx.exception.code)
        self.assertIn('{"error":"error parsing query: ',
                      ctx.exception.content)

    def test_revoke_privilege(self):
        self.cli.create_user('test', 'test')
        self.cli.create_database('testdb')
        self.cli.revoke_privilege('all', 'testdb', 'test')
        # TODO: when supported by InfluxDB, check if privileges are revoked

    def test_revoke_privilege_invalid(self):
        self.cli.create_user('test', 'test')
        self.cli.create_database('testdb')
        with self.assertRaises(InfluxDBClientError) as ctx:
            self.cli.revoke_privilege('', 'testdb', 'test')
        self.assertEqual(400, ctx.exception.code)
        self.assertIn('{"error":"error parsing query: ',
                      ctx.exception.content)

    def test_invalid_port_fails(self):
        with self.assertRaises(ValueError):
            InfluxDBClient('host', '80/redir', 'username', 'password')


@skipServerTests
class CommonTests(ManyTestCasesWithServerMixin,
                  unittest.TestCase):

    influxdb_template_conf = os.path.join(THIS_DIR, 'influxdb.conf.template')

    def test_write(self):
        self.assertIs(True, self.cli.write(
            {'points': dummy_point},
            params={'db': 'db'},
        ))

    def test_write_check_read(self):
        self.test_write()
        time.sleep(1)
        rsp = self.cli.query('SELECT * FROM cpu_load_short', database='db')
        self.assertListEqual([{'value': 0.64, 'time': '2009-11-10T23:00:00Z',
                               "host": "server01", "region": "us-west"}],
                             list(rsp.get_points()))

    def test_write_points(self):
        self.assertIs(True, self.cli.write_points(dummy_point))

    @skipIfPYpy
    def test_write_points_DF(self):
        self.assertIs(
            True,
            self.cliDF.write_points(
                dummy_pointDF['dataframe'],
                dummy_pointDF['measurement'],
                dummy_pointDF['tags']
            )
        )

    def test_write_points_check_read(self):
        self.test_write_points()
        time.sleep(1)  # same as test_write_check_read()
        rsp = self.cli.query('SELECT * FROM cpu_load_short')

        self.assertEqual(
            list(rsp),
            [[
                {'value': 0.64,
                 'time': '2009-11-10T23:00:00Z',
                 "host": "server01",
                 "region": "us-west"}
            ]]
        )

        rsp2 = list(rsp.get_points())
        self.assertEqual(len(rsp2), 1)
        pt = rsp2[0]

        self.assertEqual(
            pt,
            {'time': '2009-11-10T23:00:00Z',
             'value': 0.64,
             "host": "server01",
             "region": "us-west"}
        )

    @unittest.skip("Broken as of 0.9.0")
    def test_write_points_check_read_DF(self):
        self.test_write_points_DF()
        time.sleep(1)  # same as test_write_check_read()

        rsp = self.cliDF.query('SELECT * FROM cpu_load_short')
        assert_frame_equal(
            rsp['cpu_load_short'],
            dummy_pointDF['dataframe']
        )

        # Query with Tags
        rsp = self.cliDF.query(
            "SELECT * FROM cpu_load_short GROUP BY *")
        assert_frame_equal(
            rsp[('cpu_load_short',
                 (('host', 'server01'), ('region', 'us-west')))],
            dummy_pointDF['dataframe']
        )

    def test_write_multiple_points_different_series(self):
        self.assertIs(True, self.cli.write_points(dummy_points))
        time.sleep(1)
        rsp = self.cli.query('SELECT * FROM cpu_load_short')
        lrsp = list(rsp)

        self.assertEqual(
            [[
                {'value': 0.64,
                 'time': '2009-11-10T23:00:00Z',
                 "host": "server01",
                 "region": "us-west"}
            ]],
            lrsp
        )

        rsp = list(self.cli.query('SELECT * FROM memory'))

        self.assertEqual(
            rsp,
            [[
                {'value': 33,
                 'time': '2009-11-10T23:01:35Z',
                 "host": "server01",
                 "region": "us-west"}
            ]]
        )

    @unittest.skip("Broken as of 0.9.0")
    def test_write_multiple_points_different_series_DF(self):
        for i in range(2):
            self.assertIs(
                True, self.cliDF.write_points(
                    dummy_pointsDF[i]['dataframe'],
                    dummy_pointsDF[i]['measurement'],
                    dummy_pointsDF[i]['tags']))
        time.sleep(1)
        rsp = self.cliDF.query('SELECT * FROM cpu_load_short')

        assert_frame_equal(
            rsp['cpu_load_short'],
            dummy_pointsDF[0]['dataframe']
        )

        rsp = self.cliDF.query('SELECT * FROM memory')
        assert_frame_equal(
            rsp['memory'],
            dummy_pointsDF[1]['dataframe']
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
        self.cli.write_points(points=dummy_points,
                              tags={"host": "server01",
                                    "region": "us-west"},
                              batch_size=2)
        time.sleep(5)
        net_in = self.cli.query("SELECT value FROM network "
                                "WHERE direction='in'").raw
        net_out = self.cli.query("SELECT value FROM network "
                                 "WHERE direction='out'").raw
        cpu = self.cli.query("SELECT value FROM cpu_usage").raw
        self.assertIn(123, net_in['series'][0]['values'][0])
        self.assertIn(12, net_out['series'][0]['values'][0])
        self.assertIn(12.34, cpu['series'][0]['values'][0])

    def test_query(self):
        self.assertIs(True, self.cli.write_points(dummy_point))

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
            'name': 'foo',
            'columns': [
                'time',
                'sequence_number',
                'val'
            ]
        }
        del cli
        del example_object
        # TODO ?

    def test_delete_series_invalid(self):
        with self.assertRaises(InfluxDBClientError):
            self.cli.delete_series()

    def test_default_retention_policy(self):
        rsp = self.cli.get_list_retention_policies()
        self.assertEqual(
            [
                {'name': 'default',
                 'duration': '0',
                 'replicaN': 1,
                 'shardGroupDuration': u'168h0m0s',
                 'default': True}
            ],
            rsp
        )

    def test_create_retention_policy_default(self):
        self.cli.create_retention_policy('somename', '1d', 1, default=True)
        self.cli.create_retention_policy('another', '2d', 1, default=False)
        rsp = self.cli.get_list_retention_policies()

        self.assertEqual(
            [
                {'duration': '0',
                 'default': False,
                 'replicaN': 1,
                 'shardGroupDuration': u'168h0m0s',
                 'name': 'default'},
                {'duration': '24h0m0s',
                 'default': True,
                 'replicaN': 1,
                 'shardGroupDuration': u'1h0m0s',
                 'name': 'somename'},
                {'duration': '48h0m0s',
                 'default': False,
                 'replicaN': 1,
                 'shardGroupDuration': u'24h0m0s',
                 'name': 'another'}
            ],
            rsp
        )

    def test_create_retention_policy(self):
        self.cli.create_retention_policy('somename', '1d', 1)
        rsp = self.cli.get_list_retention_policies()
        self.assertEqual(
            [
                {'duration': '0',
                 'default': True,
                 'replicaN': 1,
                 'shardGroupDuration': u'168h0m0s',
                 'name': 'default'},
                {'duration': '24h0m0s',
                 'default': False,
                 'replicaN': 1,
                 'shardGroupDuration': u'1h0m0s',
                 'name': 'somename'}
            ],
            rsp
        )

    def test_alter_retention_policy(self):
        self.cli.create_retention_policy('somename', '1d', 1)

        # Test alter duration
        self.cli.alter_retention_policy('somename', 'db',
                                        duration='4d')
        rsp = self.cli.get_list_retention_policies()
        self.assertEqual(
            [
                {'duration': '0',
                 'default': True,
                 'replicaN': 1,
                 'shardGroupDuration': u'168h0m0s',
                 'name': 'default'},
                {'duration': '96h0m0s',
                 'default': False,
                 'replicaN': 1,
                 'shardGroupDuration': u'24h0m0s',
                 'name': 'somename'}
            ],
            rsp
        )

        # Test alter replication
        self.cli.alter_retention_policy('somename', 'db',
                                        replication=4)
        rsp = self.cli.get_list_retention_policies()
        self.assertEqual(
            [
                {'duration': '0',
                 'default': True,
                 'replicaN': 1,
                 'shardGroupDuration': u'168h0m0s',
                 'name': 'default'},
                {'duration': '96h0m0s',
                 'default': False,
                 'replicaN': 4,
                 'shardGroupDuration': u'24h0m0s',
                 'name': 'somename'}
            ],
            rsp
        )

        # Test alter default
        self.cli.alter_retention_policy('somename', 'db',
                                        default=True)
        rsp = self.cli.get_list_retention_policies()
        self.assertEqual(
            [
                {'duration': '0',
                 'default': False,
                 'replicaN': 1,
                 'shardGroupDuration': u'168h0m0s',
                 'name': 'default'},
                {'duration': '96h0m0s',
                 'default': True,
                 'replicaN': 4,
                 'shardGroupDuration': u'24h0m0s',
                 'name': 'somename'}
            ],
            rsp
        )

    def test_alter_retention_policy_invalid(self):
        self.cli.create_retention_policy('somename', '1d', 1)
        with self.assertRaises(InfluxDBClientError) as ctx:
            self.cli.alter_retention_policy('somename', 'db')
        self.assertEqual(400, ctx.exception.code)
        self.assertIn('{"error":"error parsing query: ',
                      ctx.exception.content)
        rsp = self.cli.get_list_retention_policies()
        self.assertEqual(
            [
                {'duration': '0',
                 'default': True,
                 'replicaN': 1,
                 'shardGroupDuration': u'168h0m0s',
                 'name': 'default'},
                {'duration': '24h0m0s',
                 'default': False,
                 'replicaN': 1,
                 'shardGroupDuration': u'1h0m0s',
                 'name': 'somename'}
            ],
            rsp
        )

    def test_drop_retention_policy(self):
        self.cli.create_retention_policy('somename', '1d', 1)

        # Test drop retention
        self.cli.drop_retention_policy('somename', 'db')
        rsp = self.cli.get_list_retention_policies()
        self.assertEqual(
            [
                {'duration': '0',
                 'default': True,
                 'replicaN': 1,
                 'shardGroupDuration': u'168h0m0s',
                 'name': 'default'}
            ],
            rsp
        )

    def test_issue_143(self):
        pt = partial(point, 'a_serie_name', timestamp='2015-03-30T16:16:37Z')
        pts = [
            pt(value=15),
            pt(tags={'tag_1': 'value1'}, value=5),
            pt(tags={'tag_1': 'value2'}, value=10),
        ]
        self.cli.write_points(pts)
        time.sleep(1)
        rsp = list(self.cli.query('SELECT * FROM a_serie_name GROUP BY tag_1'))

        self.assertEqual(
            [
                [{'value': 15, 'time': '2015-03-30T16:16:37Z'}],
                [{'value': 5, 'time': '2015-03-30T16:16:37Z'}],
                [{'value': 10, 'time': '2015-03-30T16:16:37Z'}]
            ],
            rsp
        )

        # a slightly more complex one with 2 tags values:
        pt = partial(point, 'serie2', timestamp='2015-03-30T16:16:37Z')
        pts = [
            pt(tags={'tag1': 'value1', 'tag2': 'v1'}, value=0),
            pt(tags={'tag1': 'value1', 'tag2': 'v2'}, value=5),
            pt(tags={'tag1': 'value2', 'tag2': 'v1'}, value=10),
        ]
        self.cli.write_points(pts)
        time.sleep(1)
        rsp = self.cli.query('SELECT * FROM serie2 GROUP BY tag1,tag2')

        self.assertEqual(
            [
                [{'value': 0, 'time': '2015-03-30T16:16:37Z'}],
                [{'value': 5, 'time': '2015-03-30T16:16:37Z'}],
                [{'value': 10, 'time': '2015-03-30T16:16:37Z'}]
            ],
            list(rsp)
        )

        all_tag2_equal_v1 = list(rsp[None, {'tag2': 'v1'}])

        self.assertEqual(
            [{'value': 0, 'time': '2015-03-30T16:16:37Z'},
             {'value': 10, 'time': '2015-03-30T16:16:37Z'}],
            all_tag2_equal_v1,
        )

    def test_query_multiple_series(self):
        pt = partial(point, 'serie1', timestamp='2015-03-30T16:16:37Z')
        pts = [
            pt(tags={'tag1': 'value1', 'tag2': 'v1'}, value=0),
        ]
        self.cli.write_points(pts)

        pt = partial(point, 'serie2', timestamp='1970-03-30T16:16:37Z')
        pts = [
            pt(tags={'tag1': 'value1', 'tag2': 'v1'},
               value=0, data1=33, data2="bla"),
        ]
        self.cli.write_points(pts)

    def test_get_list_series(self):

        dummy_points = [
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

        dummy_points_2 = [
            {
                "measurement": "memory_usage",
                "tags": {
                    "host": "server02",
                    "region": "us-east"
                },
                "time": "2009-11-10T23:00:00.123456Z",
                "fields": {
                    "value": 80
                }
            }
        ]

        self.cli.write_points(dummy_points)
        self.cli.write_points(dummy_points_2)

        self.assertEquals(
            self.cli.get_list_series(),
            [{'key': 'cpu_load_short,host=server01,region=us-west'},
             {'key': 'memory_usage,host=server02,region=us-east'}]
        )

        self.assertEquals(
            self.cli.get_list_series(measurement='memory_usage'),
            [{'key': 'memory_usage,host=server02,region=us-east'}]
        )

        self.assertEquals(
            self.cli.get_list_series(measurement='memory_usage'),
            [{'key': 'memory_usage,host=server02,region=us-east'}]
        )

        self.assertEquals(
            self.cli.get_list_series(tags={'host': 'server02'}),
            [{'key': 'memory_usage,host=server02,region=us-east'}])

        self.assertEquals(
            self.cli.get_list_series(
                measurement='cpu_load_short', tags={'host': 'server02'}),
            [])


@skipServerTests
class UdpTests(ManyTestCasesWithServerMixin,
               unittest.TestCase):

    influxdb_udp_enabled = True
    influxdb_template_conf = os.path.join(THIS_DIR,
                                          'influxdb.conf.template')

    def test_write_points_udp(self):
        cli = InfluxDBClient(
            'localhost',
            self.influxd_inst.http_port,
            'root',
            '',
            database='db',
            use_udp=True,
            udp_port=self.influxd_inst.udp_port
        )
        cli.write_points(dummy_point)

        # The points are not immediately available after write_points.
        # This is to be expected because we are using udp (no response !).
        # So we have to wait some time,
        time.sleep(3)  # 3 sec seems to be a good choice.
        rsp = self.cli.query('SELECT * FROM cpu_load_short')

        self.assertEqual(
            # this is dummy_points :
            [
                {'value': 0.64,
                 'time': '2009-11-10T23:00:00Z',
                 "host": "server01",
                 "region": "us-west"}
            ],
            list(rsp['cpu_load_short'])
        )
