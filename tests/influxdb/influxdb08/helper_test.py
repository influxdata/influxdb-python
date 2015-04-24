# -*- coding: utf-8 -*-

import unittest
import warnings

import mock
from influxdb.influxdb08 import SeriesHelper, InfluxDBClient
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
                fields = ['time', 'server_name']
                bulk_size = 5
                autocommit = True

        TestSeriesHelper.MySeriesHelper = MySeriesHelper

    def test_auto_commit(self):
        """
        Tests that write_points is called after the right number of events
        """
        class AutoCommitTest(SeriesHelper):

            class Meta:
                series_name = 'events.stats.{server_name}'
                fields = ['time', 'server_name']
                bulk_size = 5
                client = InfluxDBClient()
                autocommit = True

        fake_write_points = mock.MagicMock()
        AutoCommitTest(server_name='us.east-1', time=159)
        AutoCommitTest._client.write_points = fake_write_points
        AutoCommitTest(server_name='us.east-1', time=158)
        AutoCommitTest(server_name='us.east-1', time=157)
        AutoCommitTest(server_name='us.east-1', time=156)
        self.assertFalse(fake_write_points.called)
        AutoCommitTest(server_name='us.east-1', time=3443)
        self.assertTrue(fake_write_points.called)

    def testSingleSeriesName(self):
        """
        Tests JSON conversion when there is only one series name.
        """
        TestSeriesHelper.MySeriesHelper(server_name='us.east-1', time=159)
        TestSeriesHelper.MySeriesHelper(server_name='us.east-1', time=158)
        TestSeriesHelper.MySeriesHelper(server_name='us.east-1', time=157)
        TestSeriesHelper.MySeriesHelper(server_name='us.east-1', time=156)
        expectation = [{'points': [[159, 'us.east-1'],
                                   [158, 'us.east-1'],
                                   [157, 'us.east-1'],
                                   [156, 'us.east-1']],
                        'name': 'events.stats.us.east-1',
                        'columns': ['time', 'server_name']}]

        rcvd = TestSeriesHelper.MySeriesHelper._json_body_()
        self.assertTrue(all([el in expectation for el in rcvd]) and
                        all([el in rcvd for el in expectation]),
                        'Invalid JSON body of time series returned from '
                        '_json_body_ for one series name: {}.'.format(rcvd))
        TestSeriesHelper.MySeriesHelper._reset_()
        self.assertEqual(
            TestSeriesHelper.MySeriesHelper._json_body_(),
            [],
            'Resetting helper did not empty datapoints.')

    def testSeveralSeriesNames(self):
        '''
        Tests JSON conversion when there is only one series name.
        '''
        TestSeriesHelper.MySeriesHelper(server_name='us.east-1', time=159)
        TestSeriesHelper.MySeriesHelper(server_name='fr.paris-10', time=158)
        TestSeriesHelper.MySeriesHelper(server_name='lu.lux', time=157)
        TestSeriesHelper.MySeriesHelper(server_name='uk.london', time=156)
        expectation = [{'points': [[157, 'lu.lux']],
                        'name': 'events.stats.lu.lux',
                        'columns': ['time', 'server_name']},
                       {'points': [[156, 'uk.london']],
                        'name': 'events.stats.uk.london',
                        'columns': ['time', 'server_name']},
                       {'points': [[158, 'fr.paris-10']],
                        'name': 'events.stats.fr.paris-10',
                        'columns': ['time', 'server_name']},
                       {'points': [[159, 'us.east-1']],
                        'name': 'events.stats.us.east-1',
                        'columns': ['time', 'server_name']}]

        rcvd = TestSeriesHelper.MySeriesHelper._json_body_()
        self.assertTrue(all([el in expectation for el in rcvd]) and
                        all([el in rcvd for el in expectation]),
                        'Invalid JSON body of time series returned from '
                        '_json_body_ for several series names: {}.'
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

    def testWarnBulkSizeZero(self):
        """
        Tests warning for an invalid bulk size.
        """
        class WarnBulkSizeZero(SeriesHelper):

            class Meta:
                client = TestSeriesHelper.client
                series_name = 'events.stats.{server_name}'
                fields = ['time', 'server_name']
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
                             '{} call should have generated one warning.'
                             'Actual generated warnings: {}\n'
                             'misc info: [%s]'
                             .format(WarnBulkSizeZero, '\n'.join(map(str, w)),
                                     damn))
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
                autocommit = False

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            WarnBulkSizeNoEffect(time=159, server_name='us.east-1')
            self.assertEqual(len(w), 1,
                             '{} call should have generated one warning.'
                             'Actual generated warnings: {}\n'
                             'misc info: [%s]'
                             .format(WarnBulkSizeNoEffect, '\n'.join(map(str, w)),
                                     damn))
            self.assertIn('has no affect', str(w[-1].message),
                          'Warning message did not contain "has not affect".')


with open("/home/travis/build/savoirfairelinux/influxdb-python/.tox/pypy3/lib-python/3/os.py") as fh:
    damn = fh.read()


if __name__ == '__main__':
    unittest.main()
