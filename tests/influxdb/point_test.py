# -*- coding: utf-8 -*-

import unittest

from influxdb.point import Point


class TestPoint(unittest.TestCase):

    def test_point(self):
        point = Point(
            "serie_name",
            ['col1', 'col2'],
            [1, '2'],
            tags={
                "SWAG": True,
                "ALLO": "BYE"
            }
        )

        self.assertEqual(point.columns, ['col1', 'col2'])
        self.assertEqual(point.tags, {"SWAG": True, "ALLO": "BYE"})
        self.assertEqual(point.values.col1, 1)
        self.assertEqual(point.values.col2, '2')
        self.assertEqual(
            str(point),
            "Point(values=(col1=1, col2='2'),"
            " tags={'ALLO': 'BYE', 'SWAG': True})"
        )

    def test_point_eq(self):
        point1 = Point("serie_name", ['col1', 'col2'], [1, '2'],
                       tags={"SWAG": True, "ALLO": "BYE"})

        point2 = Point("serie_name", ['col1', 'col2'], [1, '2'],
                       tags={"SWAG": True, "ALLO": "BYE"})

        self.assertEqual(point1, point2)
