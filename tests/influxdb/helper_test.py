# -*- coding: utf-8 -*-

import unittest

from influxdb.helper import InfluxDBSeriesHelper


class TestSeriesHelper(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super(TestSeriesHelper, cls).setUpClass()
        
        class MySeriesHelper(InfluxDBSeriesHelper):
            class Meta:
                series_name = 'events.stats.{server_name}'
                fields = ['time', 'server_name']
                bulk_size = 5
        
        TestSeriesHelper.MySeriesHelper = MySeriesHelper

    def testFeatures(self):
        '''
        + Create event
        + JSON
        + Commit ? May be tough to test.
        '''
        TestSeriesHelper.MySeriesHelper(server_name='us.east-1', time=159)