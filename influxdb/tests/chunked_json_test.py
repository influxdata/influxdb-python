# -*- coding: utf-8 -*-

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from influxdb import chunked_json

import unittest


class TestChunkJson(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super(TestChunkJson, cls).setUpClass()

    def test_load(self):
        """
        Tests reading a sequence of JSON values from a string
        """
        example_response = \
            '{"results": [{"series": [{"measurement": "sdfsdfsdf", ' \
            '"columns": ["time", "value"], "values": ' \
            '[["2009-11-10T23:00:00Z", 0.64]]}]}, {"series": ' \
            '[{"measurement": "cpu_load_short", "columns": ["time", "value"],'\
            '"values": [["2009-11-10T23:00:00Z", 0.64]]}]}]}'

        res = list(chunked_json.loads(example_response))
        # import ipdb; ipdb.set_trace()

        # self.assertTrue(res)
        self.assertListEqual(
            [
                {
                    'results': [
                        {'series': [{
                            'values': [['2009-11-10T23:00:00Z', 0.64]],
                            'measurement': 'sdfsdfsdf',
                            'columns':
                                ['time', 'value']}]},
                        {'series': [{
                            'values': [['2009-11-10T23:00:00Z', 0.64]],
                            'measurement': 'cpu_load_short',
                            'columns': ['time', 'value']}]}
                    ]
                }
            ],
            res
        )
