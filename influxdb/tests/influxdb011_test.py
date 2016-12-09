# -*- coding: utf-8 -*-
"""
unit tests for the influxdb011.InfluxDBClusterClient.

NB/WARNING :
This module implements tests for the InfluxDBClusterClient class
but does so
 + without any server instance running
 + by mocking all the expected responses.

So any change of (response format from) the server will **NOT** be
detected by this module.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import time
import warnings
import unittest

from influxdb.influxdb011 import InfluxDBClusterClient
from influxdb.client import InfluxDBServerError
from .client_test import FakeClient


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
