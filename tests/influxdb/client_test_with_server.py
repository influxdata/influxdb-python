# -*- coding: utf-8 -*-
"""
unit tests for checking the good/expected interaction between :

+ the python client.. (obviously)
+ and a *_real_* server instance running.

This basically duplicates what's in client_test.py
 but without mocking around every call.

"""

from __future__ import print_function
import random
from collections import OrderedDict
import datetime
import distutils.spawn
from functools import partial
import itertools
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import unittest

import warnings

# By default, raise exceptions on warnings
warnings.simplefilter('error', FutureWarning)

from influxdb import InfluxDBClient
from influxdb.client import InfluxDBClientError

from tests.influxdb.misc import get_free_port, is_port_open


THIS_DIR = os.path.abspath(os.path.dirname(__file__))

#############################################################################
# try to find where the 'influxd' binary is located:
# You can define 'InfluxDbPythonClientTest_SERVER_BIN_PATH'
#  env var to force it :
influxdb_bin_path = influxdb_forced_bin = os.environ.get(
    'InfluxDbPythonClientTest_SERVER_BIN_PATH', '')
if not influxdb_bin_path:
    try:
        influxdb_bin_path = distutils.spawn.find_executable('influxd')
        if not influxdb_bin_path:
            raise Exception('not found via distutils')
    except Exception as err:
        try:
            influxdb_bin_path = subprocess.check_output(
                ['which', 'influxdb']).strip()
        except subprocess.CalledProcessError as err:
            # fallback on :
            influxdb_bin_path = '/opt/influxdb/influxd'

is_influxdb_bin_ok = (
    # if the env var is set then consider the influxdb_bin as OK..
    influxdb_forced_bin
    or (os.path.isfile(influxdb_bin_path)
        and os.access(influxdb_bin_path, os.X_OK))
)

if is_influxdb_bin_ok:
    # read version :
    version = subprocess.check_output([influxdb_bin_path, 'version'])
    print(version, file=sys.stderr)


#############################################################################

def point(serie_name, timestamp=None, tags=None, **fields):
    res = {'name': serie_name}
    if timestamp:
        res['timestamp'] = timestamp
    if tags:
        res['tags'] = tags
    res['fields'] = fields
    return res


dummy_point = [  # some dummy points
    {
        "name": "cpu_load_short",
        "tags": {
            "host": "server01",
            "region": "us-west"
        },
        "timestamp": "2009-11-10T23:00:00Z",
        "fields": {
            "value": 0.64
        }
    }
]

dummy_points = [  # some dummy points
    dummy_point[0],
    {
        "name": "memory",
        "tags": {
            "host": "server01",
            "region": "us-west"
        },
        "timestamp": "2009-11-10T23:01:35Z",
        "fields": {
            "value": 33
        }
    }
]

dummy_point_without_timestamp = [
    {
        "name": "cpu_load_short",
        "tags": {
            "host": "server02",
            "region": "us-west"
        },
        "fields": {
            "value": 0.64
        }
    }
]

#############################################################################


class InfluxDbInstance(object):
    ''' A class to launch of fresh influxdb server instance
    in a temporary place, using a config file template.
    '''

    def __init__(self, conf_template):
        # create a temporary dir to store all needed files
        # for the influxdb server instance :
        self.temp_dir_base = tempfile.mkdtemp()
        # "temp_dir_base" will be used for conf file and logs,
        # while "temp_dir_influxdb" is for the databases files/dirs :
        tempdir = self.temp_dir_influxdb = tempfile.mkdtemp(
            dir=self.temp_dir_base)
        # we need some "free" ports :
        self.broker_port = get_free_port()
        self.admin_port = get_free_port()
        self.udp_port = get_free_port()
        self.snapshot_port = get_free_port()

        self.logs_file = os.path.join(self.temp_dir_base, 'logs.txt')

        with open(conf_template) as fh:
            conf = fh.read().format(
                broker_port=self.broker_port,
                admin_port=self.admin_port,
                udp_port=self.udp_port,
                broker_raft_dir=os.path.join(tempdir, 'raft'),
                broker_node_dir=os.path.join(tempdir, 'db'),
                cluster_dir=os.path.join(tempdir, 'state'),
                logfile=self.logs_file,
                snapshot_port=self.snapshot_port,
            )

        conf_file = os.path.join(self.temp_dir_base, 'influxdb.conf')
        with open(conf_file, "w") as fh:
            fh.write(conf)

        # now start the server instance:
        proc = self.proc = subprocess.Popen(
            [influxdb_bin_path, '-config', conf_file],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        print("%s > Started influxdb bin in %r with ports %s and %s.." % (
              datetime.datetime.now(),
              self.temp_dir_base,
              self.admin_port, self.broker_port))

        # wait for it to listen on the broker and admin ports:
        # usually a fresh instance is ready in less than 1 sec ..
        timeout = time.time() + 10  # so 10 secs should be enough,
        # otherwise either your system load is high,
        # or you run a 286 @ 1Mhz ?
        try:
            while time.time() < timeout:
                if (is_port_open(self.broker_port)
                        and is_port_open(self.admin_port)):
                    break
                time.sleep(0.5)
                if proc.poll() is not None:
                    raise RuntimeError('influxdb prematurely exited')
            else:
                proc.terminate()
                proc.wait()
                raise RuntimeError('Timeout waiting for influxdb to listen'
                                   ' on its broker port')
        except RuntimeError as err:
            data = self.get_logs_and_output()
            data['reason'] = str(err)
            data['now'] = datetime.datetime.now()
            raise RuntimeError("%(now)s > %(reason)s. RC=%(rc)s\n"
                               "stdout=%(out)r\nstderr=%(err)r\nlogs=%(logs)r"
                               % data)

    def get_logs_and_output(self):
        proc = self.proc
        try:
            with open(self.logs_file) as fh:
                logs = fh.read()
        except IOError as err:
            logs = "Couldn't read logs: %s" % err
        return {
            'rc': proc.returncode,
            'out': proc.stdout.read(),
            'err': proc.stderr.read(),
            'logs': logs
        }

    def close(self, remove_tree=True):
        self.proc.terminate()
        self.proc.wait()
        if remove_tree:
            shutil.rmtree(self.temp_dir_base)

############################################################################


def _setup_influxdb_server(inst):
    inst.influxd_inst = InfluxDbInstance(inst.influxdb_template_conf)
    inst.cli = InfluxDBClient('localhost',
                              inst.influxd_inst.broker_port,
                              'root', '', database='db')


def _unsetup_influxdb_server(inst):
    remove_tree = sys.exc_info() == (None, None, None)
    inst.influxd_inst.close(remove_tree=remove_tree)

############################################################################


class SingleTestCaseWithServerMixin(object):
    ''' A mixin for unittest.TestCase to start an influxdb server instance
    in a temporary directory **for each test function/case**
    '''

    # 'influxdb_template_conf' attribute must be set
    # on the TestCase class or instance.

    setUp = _setup_influxdb_server
    tearDown = _unsetup_influxdb_server


class ManyTestCasesWithServerMixin(object):
    ''' Same than SingleTestCaseWithServerMixin
    but creates a single instance for the whole class.
    Also pre-creates a fresh database: 'db'.
    '''

    # 'influxdb_template_conf' attribute must be set on the class itself !

    @classmethod
    def setUpClass(cls):
        _setup_influxdb_server(cls)

    def setUp(self):
        self.cli.create_database('db')

    @classmethod
    def tearDownClass(cls):
        _unsetup_influxdb_server(cls)

    def tearDown(self):
        self.cli.drop_database('db')

############################################################################


@unittest.skipIf(not is_influxdb_bin_ok, "could not find influxd binary")
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
            ['new_db_1', 'new_db_2']
        )

    def test_create_database_fails(self):
        self.assertIsNone(self.cli.create_database('new_db'))
        with self.assertRaises(InfluxDBClientError) as ctx:
            self.cli.create_database('new_db')
        self.assertEqual(500, ctx.exception.code)
        self.assertEqual('{"results":[{"error":"database exists"}]}',
                         ctx.exception.content)

    def test_drop_database(self):
        self.test_create_database()
        self.assertIsNone(self.cli.drop_database('new_db_1'))
        self.assertEqual(['new_db_2'], self.cli.get_list_database())

    def test_drop_database_fails(self):
        with self.assertRaises(InfluxDBClientError) as ctx:
            self.cli.drop_database('db')
        self.assertEqual(500, ctx.exception.code)
        self.assertEqual('{"results":[{"error":"database not found"}]}',
                         ctx.exception.content)

    def test_query_fail(self):
        with self.assertRaises(InfluxDBClientError) as ctx:
            self.cli.query('select column_one from foo')
        self.assertEqual(
            ('500: {"results":[{"error":"database not found: db"}]}',),
            ctx.exception.args)

############################################################################


@unittest.skipIf(not is_influxdb_bin_ok, "could not find influxd binary")
class CommonTests(ManyTestCasesWithServerMixin,
                  unittest.TestCase):

    influxdb_template_conf = os.path.join(THIS_DIR, 'influxdb.conf.template')

    def test_write(self):
        new_dummy_point = dummy_point[0].copy()
        new_dummy_point['database'] = 'db'
        self.assertIs(True, self.cli.write(new_dummy_point))

    @unittest.skip("fail against real server instance, "
                   "don't know if it should succeed actually..")
    def test_write_check_read(self):
        self.test_write()
        # hmmmm damn,
        # after write has returned, if we directly query for the data it's not
        #  directly available.. (don't know if this is expected behavior (
        #   but it maybe))
        # So we have to :
        time.sleep(5)
        # so that then the data is available through select :
        rsp = self.cli.query('SELECT * FROM cpu_load_short', database='db')
        self.assertEqual(
            {'cpu_load_short': [
                {'value': 0.64, 'time': '2009-11-10T23:00:00Z'}]},
            rsp
        )

    def test_write_points(self):
        ''' same as test_write() but with write_points \o/ '''
        self.assertIs(True, self.cli.write_points(dummy_point))

    def test_write_points_check_read(self):
        ''' same as test_write_check_read() but with write_points \o/ '''
        self.test_write_points()
        time.sleep(1)  # same as test_write_check_read()
        rsp = self.cli.query('SELECT * FROM cpu_load_short')
        self.assertEqual(
            [{'values': [['2009-11-10T23:00:00Z', 0.64]],
              'name': 'cpu_load_short',
              'columns': ['time', 'value']}],
            list(rsp)
            )

        rsp2 = list(rsp['cpu_load_short'])
        self.assertEqual(len(rsp2), 1)
        pt = rsp2[0]

        self.assertEqual(
            ['cpu_load_short', ['time', 'value'], {}, ['2009-11-10T23:00:00Z', 0.64]],
            [pt.serie, pt.columns, pt.tags, [pt.values.time, pt.values.value]]
        )

    def test_write_multiple_points_different_series(self):
        self.assertIs(True, self.cli.write_points(dummy_points))
        time.sleep(1)
        rsp = self.cli.query('SELECT * FROM cpu_load_short')
        lrsp = list(rsp)
        self.assertEqual(
            [{'values': [['2009-11-10T23:00:00Z', 0.64]],
              'name': 'cpu_load_short',
              'columns': ['time', 'value']}],
            lrsp)
        rsp = list(self.cli.query('SELECT * FROM memory'))
        self.assertEqual(
            [{
                 'values': [['2009-11-10T23:01:35Z', 33]],
                 'name': 'memory', 'columns': ['time', 'value']}],
            rsp
            )

    @unittest.skip('Not implemented for 0.9')
    def test_write_points_batch(self):
        self.cli.write_points(
            points=dummy_point * 3,
            batch_size=2
        )

    def test_write_points_with_precision(self):
        ''' check that points written with an explicit precision have
        actually that precision used.
        '''
        # for that we'll check that - for each precision - the actual 'time'
        #  value returned by a select has the correct regex format..
        # n : u'2015-03-20T15:23:36.615654966Z'
        # u : u'2015-03-20T15:24:10.542554Z'
        # ms : u'2015-03-20T15:24:50.878Z'
        # s : u'2015-03-20T15:20:24Z'
        # m : u'2015-03-20T15:25:00Z'
        # h : u'2015-03-20T15:00:00Z'
        base_regex = '\d{4}-\d{2}-\d{2}T\d{2}:'  # YYYY-MM-DD 'T' hh:
        base_s_regex = base_regex + '\d{2}:\d{2}'  # base_regex + mm:ss

        point = {
            "name": "cpu_load_short",
            "tags": {
                "host": "server01",
                "region": "us-west"
            },
            "timestamp": "2009-11-10T12:34:56.123456789Z",
            "fields": {
                "value": 0.64
            }
        }

        # As far as we can see the values aren't directly available depending
        # on the precision used.
        # The less the precision, the more to wait for the value to be
        # actually written/available.
        for idx, (precision, expected_regex, sleep_time) in enumerate((
            ('n', base_s_regex + '\.\d{9}Z', 1),
            ('u', base_s_regex + '\.\d{6}Z', 1),
            ('ms', base_s_regex + '\.\d{3}Z', 1),
            ('s', base_s_regex + 'Z', 1),
            ('m', base_regex + '\d{2}:00Z', 60),

            # ('h', base_regex + '00:00Z', ),
            # that would require a sleep of possibly up to 3600 secs (/ 2 ?)..
        )):
            db = 'db1'  # to not shoot us in the foot/head,
            # we work on a fresh db each time:
            self.cli.create_database(db)
            before = datetime.datetime.now()
            self.assertIs(
                True,
                self.cli.write_points(
                    [point],
                    time_precision=precision,
                    database=db))

            # sys.stderr.write('checking presision with %r :
            # before=%s\n' % (precision, before))
            after = datetime.datetime.now()

            if sleep_time > 1:
                sleep_time -= (after if before.min != after.min
                               else before).second

                start = time.time()
                timeout = start + sleep_time
                # sys.stderr.write('should sleep %s ..\n' % sleep_time)
                while time.time() < timeout:
                    rsp = self.cli.query('SELECT * FROM cpu_load_short',
                                         database=db)
                    if rsp != {'cpu_load_short': []}:
                        # sys.stderr.write('already ? only slept %s\n' % (
                        # time.time() - start))
                        break
                    time.sleep(1)
                else:
                    pass
                    # sys.stderr.write('ok !\n')
                sleep_time = 0

            if sleep_time:
                # sys.stderr.write('sleeping %s..\n' % sleep_time)
                time.sleep(sleep_time)

            rsp = self.cli.query('SELECT * FROM cpu_load_short', database=db)
            rsp = list(rsp)[0]
            # sys.stderr.write('precision=%s rsp_timestamp = %r\n' % (
            # precision, rsp['cpu_load_short'][0]['time']))
            m = re.match(expected_regex, rsp['cpu_load_short'][0]['time'])
            self.assertIsNotNone(m)
            self.cli.drop_database(db)

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

    def test_get_list_series_empty(self):
        rsp = self.cli.get_list_series()
        self.assertEqual([], rsp)

    def test_get_list_series_non_empty(self):
        self.cli.write_points(dummy_point)
        rsp = self.cli.get_list_series()
        self.assertEqual(
            {'cpu_load_short': [
                {'region': 'us-west', 'host': 'server01', '_id': 1}]},
            rsp
        )

    def test_default_retention_policy(self):
        rsp = self.cli.get_list_retention_policies()
        self.assertEqual(
            [
                {'duration': '0', 'default': True,
                 'replicaN': 1, 'name': 'default'}],
            rsp
        )

    def test_create_retention_policy_default(self):
        rsp = self.cli.create_retention_policy('somename', '1d', 4,
                                               default=True)
        self.assertIsNone(rsp)
        rsp = self.cli.get_list_retention_policies()
        self.assertEqual([
                {'columns': ['name', 'duration', 'replicaN', 'default'],
                 'values': [['default', '0', 1, False],
                            ['somename', '24h0m0s', 4, True]]}],
            rsp
        )

    def test_create_retention_policy(self):
        self.cli.create_retention_policy('somename', '1d', 4)
        rsp = self.cli.get_list_retention_policies()
        self.assertEqual(
            [
                {'duration': '0', 'default': True, 'replicaN': 1,
                 'name': 'default'},
                {'duration': '24h0m0s', 'default': False, 'replicaN': 4,
                 'name': 'somename'}
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
        # print(rsp, file=sys.stderr)

        self.assertEqual([
                {'name': 'a_serie_name', 'columns': ['time', 'value'],
                 'values': [['2015-03-30T16:16:37Z', 15]],
                 'tags': {'tag_1': ''}},
                {'name': 'a_serie_name', 'columns': ['time', 'value'],
                 'values': [['2015-03-30T16:16:37Z', 5]],
                 'tags': {'tag_1': 'value1'}},
                {'name': 'a_serie_name', 'columns': ['time', 'value'],
                 'values': [['2015-03-30T16:16:37Z', 10]],
                 'tags': {'tag_1': 'value2'}}],
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
        # print(rsp, file=sys.stderr)
        self.assertEqual([
            {'name': 'serie2', 'columns': ['time', 'value'],
             'values': [['2015-03-30T16:16:37Z', 0]],
             'tags': {'tag2': 'v1', 'tag1': 'value1'}},
            {'name': 'serie2', 'columns': ['time', 'value'],
             'values': [['2015-03-30T16:16:37Z', 5]],
             'tags': {'tag2': 'v2', 'tag1': 'value1'}},
            {'name': 'serie2', 'columns': ['time', 'value'],
             'values': [['2015-03-30T16:16:37Z', 10]],
             'tags': {'tag2': 'v1', 'tag1': 'value2'}}],
            list(rsp)
        )

        d = all_tag2_equal_v1 = list(rsp[None, {'tag2': 'v1'}])
        self.assertEqual(
            [
                2,
                ['time', 'value'],
                {'tag2': 'v1', 'tag1': 'value1'},
                ['2015-03-30T16:16:37Z', 0],
                {'tag2': 'v1', 'tag1': 'value2'},
                ['2015-03-30T16:16:37Z', 10]
            ],
            [
                len(d),
                d[0].columns,
                d[0].tags, [d[0].values.time, d[0].values.value],
                d[1].tags, [d[1].values.time, d[1].values.value]
            ]
        )


    def test_tags_json_order(self):
        n_pts = 100
        n_tags = 5  # that will make 120 possible orders (fact(5) == 120)
        all_tags = ['tag%s' % i for i in range(n_tags)]
        n_tags_values = 1 + n_tags // 3
        all_tags_values = ['value%s' % random.randint(0, i)
                           for i in range(n_tags_values)]
        pt = partial(point, 'serie', timestamp='2015-03-30T16:16:37Z')
        pts = [
            pt(value=random.randint(0, 100))
            for _ in range(n_pts)
        ]
        for pt in pts:
            tags = pt['tags'] = {}
            for tag in all_tags:
                tags[tag] = random.choice(all_tags_values)

        self.cli.write_points(pts)
        time.sleep(1)

        # Influxd, when queried with a "group by tag1(, tag2, ..)" and as far
        # as we currently see, always returns the tags (alphabetically-)
        # ordered by their name in the json response..
        # That might not always be the case so here we will also be
        # asserting that behavior.
        expected_ordered_tags = tuple(sorted(all_tags))

        # try all the possible orders of tags for the group by in the query:
        for tags in itertools.permutations(all_tags):
            query = ('SELECT * FROM serie '
                     'GROUP BY %s' % ','.join(tags))
            rsp = self.cli.query(query)
            # and verify that, for each "serie_key" in the response,
            # the tags names are ordered as we expect it:
            for serie_key in rsp:
                # first also asserts that the serie key is a 2-tuple:
                self.assertTrue(isinstance(serie_key, tuple))
                self.assertEqual(2, len(serie_key))
                # also assert that the first component is an str instance:
                self.assertIsInstance(serie_key[0], type(b''.decode()))
                self.assertIsInstance(serie_key[1], tuple)
                # also assert that the number of items in the second component
                # is the number of tags requested in the group by actually,
                # and that each one has correct format/type/..
                self.assertEqual(n_tags, len(serie_key[1]))
                for tag_data in serie_key[1]:
                    self.assertIsInstance(tag_data, tuple)
                    self.assertEqual(2, len(tag_data))
                    tag_name = tag_data[0]
                    self.assertIsInstance(tag_name, type(b''.decode()))
                # then check the tags order:
                rsp_tags = tuple(t[0] for t in serie_key[1])
                self.assertEqual(expected_ordered_tags, rsp_tags)


    def test_query_multiple_series(self):
        pt = partial(point, 'serie1', timestamp='2015-03-30T16:16:37Z')
        pts = [
            pt(tags={'tag1': 'value1', 'tag2': 'v1'}, value=0),
            #pt(tags={'tag1': 'value1', 'tag2': 'v2'}, value=5),
            #pt(tags={'tag1': 'value2', 'tag2': 'v1'}, value=10),
        ]
        self.cli.write_points(pts)

        pt = partial(point, 'serie2', timestamp='1970-03-30T16:16:37Z')
        pts = [
            pt(tags={'tag1': 'value1', 'tag2': 'v1'}, value=0, data1=33, data2="bla"),
            #pt(tags={'tag1': 'value1', 'tag2': 'v2'}, value=5),
            #pt(tags={'tag1': 'value2', 'tag2': 'v3'}, value=10),  # data2="what"),
        ]
        self.cli.write_points(pts)

        rsp = self.cli.query('SELECT * FROM serie1, serie2')
        print(rsp)

        # same but with the tags given :
        #rsp = self.cli.query('SELECT * FROM serie1, serie2 GROUP BY *')
        print(rsp)



############################################################################


@unittest.skipIf(not is_influxdb_bin_ok, "could not find influxd binary")
class UdpTests(ManyTestCasesWithServerMixin,
               unittest.TestCase):

    influxdb_template_conf = os.path.join(THIS_DIR,
                                          'influxdb.udp_conf.template')

    def test_write_points_udp(self):
        cli = InfluxDBClient(
            'localhost', self.influxd_inst.broker_port,
            'dont', 'care',
            database='db',
            use_udp=True, udp_port=self.influxd_inst.udp_port
        )
        cli.write_points(dummy_point)

        # The points are not immediately available after write_points.
        # This is to be expected because we are using udp (no response !).
        # So we have to wait some time,
        time.sleep(1)  # 1 sec seems to be a good choice.
        rsp = cli.query('SELECT * FROM cpu_load_short')

        self.assertEqual(
            # this is dummy_points :
            {'cpu_load_short': [
                {'value': 0.64, 'time': '2009-11-10T23:00:00Z'}]},
            rsp
        )
