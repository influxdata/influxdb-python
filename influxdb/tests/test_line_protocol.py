# -*- coding: utf-8 -*-
"""Define the line protocol test module."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from datetime import datetime
import unittest
from pytz import UTC, timezone

from influxdb import line_protocol
import pandas as pd


class TestLineProtocol(unittest.TestCase):
    """Define the LineProtocol test object."""

    def test_make_lines(self):
        """Test make new lines in TestLineProtocol object."""
        data = {
            "tags": {
                "empty_tag": "",
                "none_tag": None,
                "backslash_tag": "C:\\",
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
            'test,backslash_tag=C:\\\\ ,integer_tag=2,string_tag=hello '
            'bool_val=True,float_val=1.1,int_val=1i,string_val="hello!"\n'
        )

    def test_convert_timestamp(self):
        """Test line_protocol _convert_timestamp
        Precisions factors must be int for correct calculation to ints. if float the result of a floor calc is an approximation
        Choosing the test value is important that nanosecond resolution values are greater than 895ns
        Example : the issue is only observable ns > 895ns
        # ts = pd.Timestamp('2013-01-01 23:10:55.123456987+00:00')
        # ts_ns = np.int64(ts.value)
        # # For conversion to microsecond
        # precision_factor=1e3
        # expected_ts_us = 1357081855123456
        # np.int64(ts_ns // precision_factor) # results in INCORRECT 1357081855123457
        # np.int64(ts_ns // np.int64(precision_factor) # results in CORRECT 1357081855123456

        """

        #TODO: add test for timestamp isinstance(timestamp, Integral)
        #TODO: add test for timestamp isinstance(_get_unicode(timestamp), text_type)
        #TODO: add test for timestamp isinstance(timestamp, datetime) also include test tzinfo present or not



        timestamp = pd.Timestamp('2013-01-01 23:10:55.123456987+00:00')
        self.assertEqual(
            line_protocol._convert_timestamp(timestamp),
            1357081855123456987
        )
        self.assertEqual(
            line_protocol._convert_timestamp(timestamp, time_precision='h'),
            1357081855 // 3600
        )
        self.assertEqual(
            line_protocol._convert_timestamp(timestamp, time_precision='m'),
            1357081855 // 60
        )
        self.assertEqual(
            line_protocol._convert_timestamp(timestamp, time_precision='s'),
            1357081855
        )
        self.assertEqual(
            line_protocol._convert_timestamp(timestamp, time_precision='ms'),
            1357081855123
        )
        self.assertEqual(
            line_protocol._convert_timestamp(timestamp, time_precision='u'),
            1357081855123456
        )
        self.assertEqual(
            line_protocol._convert_timestamp(timestamp, time_precision='n'),
            1357081855123456987
        )

    def test_timezone(self):
        """Test timezone in TestLineProtocol object."""
        # datetime tests
        dt = datetime(2009, 11, 10, 23, 0, 0, 123456)
        utc = UTC.localize(dt)
        berlin = timezone('Europe/Berlin').localize(dt)
        eastern = berlin.astimezone(timezone('US/Eastern'))
        # pandas ns timestamp tests
        pddt = pd.Timestamp('2009-11-10 23:00:00.123456987')
        pdutc = pd.Timestamp(pddt, tz='UTC')
        pdberlin = pdutc.astimezone('Europe/Berlin')
        pdeastern = pdberlin.astimezone('US/Eastern')

        data = {"points": [
            {"measurement": "A", "fields": {"val": 1}, "time": 0},
            # string representations
            # String version for datetime
            {"measurement": "A", "fields": {"val": 1},
             "time": "2009-11-10T23:00:00.123456Z"},
            # String version for pandas ns timestamp
            {"measurement": "A", "fields": {"val": 1},
             "time": "2009-11-10 23:00:00.123456987"},
            # datetime
            {"measurement": "A", "fields": {"val": 1}, "time": dt},
            {"measurement": "A", "fields": {"val": 1}, "time": utc},
            {"measurement": "A", "fields": {"val": 1}, "time": berlin},
            {"measurement": "A", "fields": {"val": 1}, "time": eastern},
            # pandas timestamp
            {"measurement": "A", "fields": {"val": 1}, "time": pddt},
            {"measurement": "A", "fields": {"val": 1}, "time": pdutc},
            {"measurement": "A", "fields": {"val": 1}, "time": pdberlin},
            {"measurement": "A", "fields": {"val": 1}, "time": pdeastern},
        ]
        }

        self.assertEqual(
            line_protocol.make_lines(data),
            '\n'.join([
                'A val=1i 0',
                'A val=1i 1257894000123456000',
                'A val=1i 1257894000123456987',
                # datetime results
                'A val=1i 1257894000123456000',
                'A val=1i 1257894000123456000',
                'A val=1i 1257890400123456000',
                'A val=1i 1257890400123456000',
                # pandas ns timestamp results
                'A val=1i 1257894000123456987',
                'A val=1i 1257894000123456987',
                'A val=1i 1257894000123456987',
                'A val=1i 1257894000123456987',
            ]) + '\n'
        )

    def test_string_val_newline(self):
        """Test string value with newline in TestLineProtocol object."""
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
        """Test make unicode lines in TestLineProtocol object."""
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

    def test_quote_ident(self):
        """Test quote indentation in TestLineProtocol object."""
        self.assertEqual(
            line_protocol.quote_ident(r"""\foo ' bar " Örf"""),
            r'''"\\foo ' bar \" Örf"'''
        )

    def test_quote_literal(self):
        """Test quote literal in TestLineProtocol object."""
        self.assertEqual(
            line_protocol.quote_literal(r"""\foo ' bar " Örf"""),
            r"""'\\foo \' bar " Örf'"""
        )

    def test_float_with_long_decimal_fraction(self):
        """Ensure precision is preserved when casting floats into strings."""
        data = {
            "points": [
                {
                    "measurement": "test",
                    "fields": {
                        "float_val": 1.0000000000000009,
                    }
                }
            ]
        }
        self.assertEqual(
            line_protocol.make_lines(data),
            'test float_val=1.0000000000000009\n'
        )
