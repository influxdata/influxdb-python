# -*- coding: utf-8 -*-
"""Initialize the influxdb package."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import os

from .client import InfluxDBClient
from .helper import SeriesHelper


__all__ = [
    'InfluxDBClient',
    'SeriesHelper',
]

if "INFLUXDB_NO_DATAFRAME_CLIENT" not in os.environ:
   from .dataframe_client import DataFrameClient
   __all__.append( "DataFrameClient" )


__version__ = '5.3.0'
