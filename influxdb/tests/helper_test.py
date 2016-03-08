# -*- coding: utf-8 -*-

import datetime
import pytz
import sys
if sys.version_info < (2, 7):
    import unittest2 as unittest
else:
    import unittest
import warnings

import mock
from influxdb import SeriesHelper, InfluxDBClient
from requests.exceptions import ConnectionError


class TestSeriesHelper(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super(TestSeriesHelper, cls).setUpClass()

        TestSeriesHelper.client = InfluxDBClient(
            'host',
            8086,
            'username',
            'password',
            'database'
        )

        class MySeriesHelper(SeriesHelper):

            class Meta:
                client = TestSeriesHelper.client
                series_name = 'events.stats.{server_name}'
                fields = ['some_stat']
                tags = ['server_name', 'other_tag']
                bulk_size = 5
                autocommit = True

        TestSeriesHelper.MySeriesHelper = MySeriesHelper

        class MySeriesTimeHelper(SeriesHelper):

            class Meta:
                client = TestSeriesHelper.client
                series_name = 'events.stats.{server_name}'
                fields = ['time', 'some_stat']
                tags = ['server_name', 'other_tag']
                bulk_size = 5
                autocommit = True

        TestSeriesHelper.MySeriesTimeHelper = MySeriesTimeHelper

    def test_auto_commit(self):
        """
        Tests that write_points is called after the right number of events
        """
        class AutoCommitTest(SeriesHelper):

            class Meta:
                series_name = 'events.stats.{server_name}'
                fields = ['some_stat']
                tags = ['server_name', 'other_tag']
                bulk_size = 5
                client = InfluxDBClient()
                autocommit = True

        fake_write_points = mock.MagicMock()
        AutoCommitTest(server_name='us.east-1', some_stat=159, other_tag='gg')
        AutoCommitTest._client.write_points = fake_write_points
        AutoCommitTest(server_name='us.east-1', some_stat=158, other_tag='gg')
        AutoCommitTest(server_name='us.east-1', some_stat=157, other_tag='gg')
        AutoCommitTest(server_name='us.east-1', some_stat=156, other_tag='gg')
        self.assertFalse(fake_write_points.called)
        AutoCommitTest(server_name='us.east-1', some_stat=3443, other_tag='gg')
        self.assertTrue(fake_write_points.called)

    def testSingleSeriesName(self):
        """
        Tests JSON conversion when there is only one series name.
        """
        dt = datetime.datetime(2016, 1, 2, 3, 4, 5, 678912)
        ts1 = dt
        ts2 = "2016-10-11T01:02:03.123456789-04:00"
        ts3 = 1234567890123456789
        ts4 = pytz.timezone("Europe/Berlin").localize(dt)

        TestSeriesHelper.MySeriesTimeHelper(
            time=ts1, server_name='us.east-1', other_tag='ello', some_stat=159)
        TestSeriesHelper.MySeriesTimeHelper(
            time=ts2, server_name='us.east-1', other_tag='ello', some_stat=158)
        TestSeriesHelper.MySeriesTimeHelper(
            time=ts3, server_name='us.east-1', other_tag='ello', some_stat=157)
        TestSeriesHelper.MySeriesTimeHelper(
            time=ts4, server_name='us.east-1', other_tag='ello', some_stat=156)
        expectation = [
            {
                "measurement": "events.stats.us.east-1",
                "tags": {
                    "other_tag": "ello",
                    "server_name": "us.east-1"
                },
                "fields": {
                    "some_stat": 159
                },
                "time": "2016-01-02T03:04:05.678912+00:00",
            },
            {
                "measurement": "events.stats.us.east-1",
                "tags": {
                    "other_tag": "ello",
                    "server_name": "us.east-1"
                },
                "fields": {
                    "some_stat": 158
                },
                "time": "2016-10-11T01:02:03.123456789-04:00",
            },
            {
                "measurement": "events.stats.us.east-1",
                "tags": {
                    "other_tag": "ello",
                    "server_name": "us.east-1"
                },
                "fields": {
                    "some_stat": 157
                },
                "time": 1234567890123456789,
            },
            {
                "measurement": "events.stats.us.east-1",
                "tags": {
                    "other_tag": "ello",
                    "server_name": "us.east-1"
                },
                "fields": {
                    "some_stat": 156
                },
                "time": "2016-01-02T03:04:05.678912+01:00",
            }
        ]

        rcvd = TestSeriesHelper.MySeriesTimeHelper._json_body_()
        self.assertTrue(all([el in expectation for el in rcvd]) and
                        all([el in rcvd for el in expectation]),
                        'Invalid JSON body of time series returned from '
                        '_json_body_ for one series name: {0}.'.format(rcvd))
        TestSeriesHelper.MySeriesTimeHelper._reset_()
        self.assertEqual(
            TestSeriesHelper.MySeriesTimeHelper._json_body_(),
            [],
            'Resetting helper did not empty datapoints.')

    def testSeveralSeriesNames(self):
        '''
        Tests JSON conversion when there are multiple series names.
        '''
        TestSeriesHelper.MySeriesHelper(
            server_name='us.east-1', some_stat=159, other_tag='ello')
        TestSeriesHelper.MySeriesHelper(
            server_name='fr.paris-10', some_stat=158, other_tag='ello')
        TestSeriesHelper.MySeriesHelper(
            server_name='lu.lux', some_stat=157, other_tag='ello')
        TestSeriesHelper.MySeriesHelper(
            server_name='uk.london', some_stat=156, other_tag='ello')
        expectation = [
            {
                'fields': {
                    'some_stat': 157
                },
                'measurement': 'events.stats.lu.lux',
                'tags': {
                    'other_tag': 'ello',
                    'server_name': 'lu.lux'
                }
            },
            {
                'fields': {
                    'some_stat': 156
                },
                'measurement': 'events.stats.uk.london',
                'tags': {
                    'other_tag': 'ello',
                    'server_name': 'uk.london'
                }
            },
            {
                'fields': {
                    'some_stat': 158
                },
                'measurement': 'events.stats.fr.paris-10',
                'tags': {
                    'other_tag': 'ello',
                    'server_name': 'fr.paris-10'
                }
            },
            {
                'fields': {
                    'some_stat': 159
                },
                'measurement': 'events.stats.us.east-1',
                'tags': {
                    'other_tag': 'ello',
                    'server_name': 'us.east-1'
                }
            }
        ]

        rcvd = TestSeriesHelper.MySeriesHelper._json_body_()
        for r in rcvd:
            self.assertTrue(r.get('time'),
                            "No time field in received JSON body.")
            del(r["time"])
        self.assertTrue(all([el in expectation for el in rcvd]) and
                        all([el in rcvd for el in expectation]),
                        'Invalid JSON body of time series returned from '
                        '_json_body_ for several series names: {0}.'
                        .format(rcvd))
        TestSeriesHelper.MySeriesHelper._reset_()
        self.assertEqual(
            TestSeriesHelper.MySeriesHelper._json_body_(),
            [],
            'Resetting helper did not empty datapoints.')

    def testInvalidHelpers(self):
        '''
        Tests errors in invalid helpers.
        '''
        class MissingMeta(SeriesHelper):
            pass

        class MissingClient(SeriesHelper):

            class Meta:
                series_name = 'events.stats.{server_name}'
                fields = ['time', 'server_name']
                autocommit = True

        class MissingSeriesName(SeriesHelper):

            class Meta:
                fields = ['time', 'server_name']

        class MissingFields(SeriesHelper):

            class Meta:
                series_name = 'events.stats.{server_name}'

        for cls in [MissingMeta, MissingClient, MissingFields,
                    MissingSeriesName]:
            self.assertRaises(
                AttributeError, cls, **{'time': 159,
                                        'server_name': 'us.east-1'})

    @unittest.skip("Fails on py32")
    def testWarnBulkSizeZero(self):
        """
        Tests warning for an invalid bulk size.
        """
        class WarnBulkSizeZero(SeriesHelper):

            class Meta:
                client = TestSeriesHelper.client
                series_name = 'events.stats.{server_name}'
                fields = ['time', 'server_name']
                tags = []
                bulk_size = 0
                autocommit = True

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            try:
                WarnBulkSizeZero(time=159, server_name='us.east-1')
            except ConnectionError:
                # Server defined in the client is invalid, we're testing
                # the warning only.
                pass
            self.assertEqual(len(w), 1,
                             '{0} call should have generated one warning.'
                             .format(WarnBulkSizeZero))
            self.assertIn('forced to 1', str(w[-1].message),
                          'Warning message did not contain "forced to 1".')

    def testWarnBulkSizeNoEffect(self):
        """
        Tests warning for a set bulk size but autocommit False.
        """
        class WarnBulkSizeNoEffect(SeriesHelper):

            class Meta:
                series_name = 'events.stats.{server_name}'
                fields = ['time', 'server_name']
                bulk_size = 5
                tags = []
                autocommit = False

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            WarnBulkSizeNoEffect(time=159, server_name='us.east-1')
            self.assertEqual(len(w), 1,
                             '{0} call should have generated one warning.'
                             .format(WarnBulkSizeNoEffect))
            self.assertIn('has no affect', str(w[-1].message),
                          'Warning message did not contain "has not affect".')
