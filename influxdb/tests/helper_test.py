# -*- coding: utf-8 -*-

import sys
if sys.version_info < (2, 7):
    import unittest2 as unittest
else:
    import unittest
import warnings

import mock
from datetime import datetime, timedelta
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

    def tearDown(self):
        super(TestSeriesHelper, self).tearDown()
        TestSeriesHelper.MySeriesHelper._reset_()
        self.assertEqual(
            TestSeriesHelper.MySeriesHelper._json_body_(),
            [],
            'Resetting helper did not empty datapoints.')

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

    @mock.patch('influxdb.helper.SeriesHelper._current_timestamp')
    def testSingleSeriesName(self, current_timestamp):
        """
        Tests JSON conversion when there is only one series name.
        """
        current_timestamp.return_value = current_date = datetime.today()
        TestSeriesHelper.MySeriesHelper(
            server_name='us.east-1', other_tag='ello', some_stat=159)
        TestSeriesHelper.MySeriesHelper(
            server_name='us.east-1', other_tag='ello', some_stat=158)
        TestSeriesHelper.MySeriesHelper(
            server_name='us.east-1', other_tag='ello', some_stat=157)
        TestSeriesHelper.MySeriesHelper(
            server_name='us.east-1', other_tag='ello', some_stat=156)
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
                "time": current_date,
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
                "time": current_date,
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
                "time": current_date,
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
                "time": current_date,
            }
        ]

        rcvd = TestSeriesHelper.MySeriesHelper._json_body_()
        self.assertTrue(all([el in expectation for el in rcvd]) and
                        all([el in rcvd for el in expectation]),
                        'Invalid JSON body of time series returned from '
                        '_json_body_ for one series name: {0}.'.format(rcvd))

    @mock.patch('influxdb.helper.SeriesHelper._current_timestamp')
    def testSeveralSeriesNames(self, current_timestamp):
        """
        Tests JSON conversion when there are multiple series names.
        """
        current_timestamp.return_value = current_date = datetime.today()
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
                },
                "time": current_date,
            },
            {
                'fields': {
                    'some_stat': 156
                },
                'measurement': 'events.stats.uk.london',
                'tags': {
                    'other_tag': 'ello',
                    'server_name': 'uk.london'
                },
                "time": current_date,
            },
            {
                'fields': {
                    'some_stat': 158
                },
                'measurement': 'events.stats.fr.paris-10',
                'tags': {
                    'other_tag': 'ello',
                    'server_name': 'fr.paris-10'
                },
                "time": current_date,
            },
            {
                'fields': {
                    'some_stat': 159
                },
                'measurement': 'events.stats.us.east-1',
                'tags': {
                    'other_tag': 'ello',
                    'server_name': 'us.east-1'
                },
                "time": current_date,
            }
        ]

        rcvd = TestSeriesHelper.MySeriesHelper._json_body_()
        self.assertTrue(all([el in expectation for el in rcvd]) and
                        all([el in rcvd for el in expectation]),
                        'Invalid JSON body of time series returned from '
                        '_json_body_ for several series names: {0}.'
                        .format(rcvd))

    @mock.patch('influxdb.helper.SeriesHelper._current_timestamp')
    def testSeriesWithoutTimeField(self, current_timestamp):
        """
        Tests that time is optional on a series without a time field.
        """
        current_date = datetime.today()
        yesterday = current_date - timedelta(days=1)
        current_timestamp.return_value = yesterday
        TestSeriesHelper.MySeriesHelper(
            server_name='us.east-1', other_tag='ello',
            some_stat=159, time=current_date
        )
        TestSeriesHelper.MySeriesHelper(
            server_name='us.east-1', other_tag='ello',
            some_stat=158,
        )
        point1, point2 = TestSeriesHelper.MySeriesHelper._json_body_()
        self.assertTrue('time' in point1 and 'time' in point2)
        self.assertEqual(point1['time'], current_date)
        self.assertEqual(point2['time'], yesterday)

    @mock.patch('influxdb.helper.SeriesHelper._current_timestamp')
    def testSeriesWithTimeField(self, current_timestamp):
        """
        Test that time is optional on a series with a time field.
        """
        current_date = datetime.today()
        yesterday = current_date - timedelta(days=1)
        current_timestamp.return_value = yesterday

        class MyTimeFieldSeriesHelper(SeriesHelper):

            class Meta:
                client = TestSeriesHelper.client
                series_name = 'events.stats.{server_name}'
                fields = ['some_stat', 'time']
                tags = ['server_name', 'other_tag']
                bulk_size = 5
                autocommit = True

        MyTimeFieldSeriesHelper(
            server_name='us.east-1', other_tag='ello',
            some_stat=159, time=current_date
        )
        MyTimeFieldSeriesHelper(
            server_name='us.east-1', other_tag='ello',
            some_stat=158,
        )
        point1, point2 = MyTimeFieldSeriesHelper._json_body_()
        self.assertTrue('time' in point1 and 'time' in point2)
        self.assertEqual(point1['time'], current_date)
        self.assertEqual(point2['time'], yesterday)

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
