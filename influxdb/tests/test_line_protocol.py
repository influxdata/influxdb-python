# -*- coding: utf-8 -*-

import unittest
from influxdb import line_protocol


class TestLineProtocol(unittest.TestCase):

    def test_make_lines(self):
        data = {
            "tags": {
                "empty_tag": "",
                "integer_tag": 2,
                "string_tag": "hello"
            },
            "points": [
                {
                    "measurement": "test",
                    "fields": {
                        "string_val": "hello!",
                        "int_val": 1,
                    }
                }
            ]
        }

        self.assertEqual(
            line_protocol.make_lines(data),
            'test,integer_tag=2,string_tag=hello '
            'int_val=1,string_val="hello!"\n'
        )
