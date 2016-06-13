# -*- coding: utf-8 -*-

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unittest

from influxdb import line_protocol


class TestLineProtocol(unittest.TestCase):

    def test_make_lines(self):
        data = {
            "tags": {
                "empty_tag": "",
                "none_tag": None,
                "integer_tag": 2,
                "string_tag": "hello"
            },
            "points": [
                {
                    "measurement": "test",
                    "fields": {
                        "string_val": "hello!",
                        "int_val": 1,
                        "float_val": 1.1,
                        "none_field": None,
                        "bool_val": True,
                    }
                }
            ]
        }

        self.assertEqual(
            line_protocol.make_lines(data),
            'test,integer_tag=2,string_tag=hello '
            'bool_val=True,float_val=1.1,int_val=1i,string_val="hello!"\n'
        )

    def test_string_val_newline(self):
        data = {
            "points": [
                {
                    "measurement": "m1",
                    "fields": {
                        "multi_line": "line1\nline1\nline3"
                    }
                }
            ]
        }

        self.assertEqual(
            line_protocol.make_lines(data),
            'm1 multi_line="line1\\nline1\\nline3"\n'
        )

    def test_make_lines_unicode(self):
        data = {
            "tags": {
                "unicode_tag": "\'Привет!\'"  # Hello! in Russian
            },
            "points": [
                {
                    "measurement": "test",
                    "fields": {
                        "unicode_val": "Привет!",  # Hello! in Russian
                    }
                }
            ]
        }

        self.assertEqual(
            line_protocol.make_lines(data),
            'test,unicode_tag=\'Привет!\' unicode_val="Привет!"\n'
        )
