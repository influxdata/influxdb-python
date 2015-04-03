# -*- coding: utf-8 -*-

import unittest

from influxdb.point import Point


class TestPoint(unittest.TestCase):
    def setUp(self):
        self.point = Point(
            "serie_name",
            ['col1', 'col2'],
            [1, '2'],
            tags={
                "SWAG": True,
                "ALLO": "BYE"
            }
        )

    def test_point(self):
        self.assertEqual(self.point.columns, ['col1', 'col2'])
        self.assertEqual(self.point.tags, {"SWAG": True, "ALLO": "BYE"})
        self.assertEqual(self.point.values.col1, 1)
        self.assertEqual(self.point.values.col2, '2')
        self.assertEqual(
            str(self.point),
            "Point(values=(col1=1, col2='2'),"
            " tags={'ALLO': 'BYE', 'SWAG': True})"
        )

    def test_point_eq(self):
        point1 = Point("serie_name", ['col1', 'col2'], [1, '2'],
                       tags={"SWAG": True, "ALLO": "BYE"})

        point2 = Point("serie_name", ['col1', 'col2'], [1, '2'],
                       tags={"SWAG": True, "ALLO": "BYE"})

        self.assertEqual(point1, point2)

    def test_as_dict(self):
        self.assertEqual(
            self.point.as_dict(),
            {'point': [{'col1': 1}, {'col2': '2'}], 'serie': 'serie_name'}
        )
