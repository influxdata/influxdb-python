# -*- coding: utf-8 -*-

import unittest
from influxdb import line_protocol


class TestLineProtocol(unittest.TestCase):

    def test_empty_tag(self):
        data = {
            "tags": {
                "my_tag": ""
            },
            "points": [
                {
                    "measurement": "test",
                    "fields": {
                        "value": "hello!"
                    }
                }
            ]
        }

        self.assertEqual(
            line_protocol.make_lines(data),
            'test value="hello!"\n'
        )
