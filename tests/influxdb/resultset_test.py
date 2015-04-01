# -*- coding: utf-8 -*-

import unittest

from influxdb.resultset import ResultSet


class TestResultSet(unittest.TestCase):

    def setUp(self):
        self.query_response = {
            ('serie', (('tag_1', ''),)): [
                {'time': '2015-03-30T16:16:37Z', 'value': 15}],
            ('serie', (('tag_1', 'value1'),)): [
                {'time': '2015-03-30T16:16:37Z', 'value': 5}],
            ('serie', (('tag_1', 'value2'),)): [
                {'time': '2015-03-30T16:16:37Z', 'value': 10}]
        }
        self.rs = ResultSet(self.query_response)

    def test_filter_by_name(self):
        self.assertItemsEqual(
            self.rs['serie'],
            [
                {'points': [{'value': 10, 'time': '2015-03-30T16:16:37Z'}],
                 'tags': {'tag_1': 'value2'}},
                {'points': [{'value': 15, 'time': '2015-03-30T16:16:37Z'}],
                 'tags': {'tag_1': ''}},
                {'points': [{'value': 5, 'time': '2015-03-30T16:16:37Z'}],
                 'tags': {'tag_1': 'value1'}}
            ]
        )

    def test_filter_by_tags(self):
        self.assertItemsEqual(
            self.rs[('serie', {'tag_1': 'value2'})],
            [{'points': [{'value': 10, 'time': '2015-03-30T16:16:37Z'}],
              'tags': {'tag_1': 'value2'}}]
        )

    def test_repr(self):
        expected = \
            "ResultSet(('serie', {'tag_1': 'value2'}): [{'value': 10, " \
            "'time': '2015-03-30T16:16:37Z'}]('serie', {'tag_1': ''}):" \
            " [{'value': 15, 'time': '2015-03-30T16:16:37Z'}]('serie'," \
            " {'tag_1': 'value1'}): [{'value': 5," \
            " 'time': '2015-03-30T16:16:37Z'}])"

        self.assertEqual(
            str(self.rs),
            expected
        )
