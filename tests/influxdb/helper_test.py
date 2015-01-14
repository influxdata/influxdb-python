# -*- coding: utf-8 -*-

import unittest

from influxdb import SeriesHelper, InfluxDBClient


class TestSeriesHelper(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super(TestSeriesHelper, cls).setUpClass()

        TestSeriesHelper.client = InfluxDBClient('host', 8086, 'username', 'password', 'database')

        class MySeriesHelper(SeriesHelper):
            class Meta:
                client = TestSeriesHelper.client
                series_name = 'events.stats.{server_name}'
                fields = ['time', 'server_name']
                bulk_size = 5

        TestSeriesHelper.MySeriesHelper = MySeriesHelper

    def testSingleSeriesName(self):
        '''
        Tests JSON conversion when there is only one series name.
        '''
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
        self.assertListEqual(TestSeriesHelper.MySeriesHelper._json_body_(), expectation, 'Invalid JSON body of time series returned from _json_body_ for one series name.')
        TestSeriesHelper.MySeriesHelper._reset_()
        self.assertEqual(TestSeriesHelper.MySeriesHelper._json_body_(), [], 'Resetting helper did not empty datapoints.')

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
        self.assertListEqual(TestSeriesHelper.MySeriesHelper._json_body_(), expectation, 'Invalid JSON body of time series returned from _json_body_ for several series names.')
        TestSeriesHelper.MySeriesHelper._reset_()
        self.assertEqual(TestSeriesHelper.MySeriesHelper._json_body_(), [], 'Resetting helper did not empty datapoints.')
