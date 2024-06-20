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

NO_DATAFRAME_CLIENT = os.environ.get("INFLUXDB_NO_DATAFRAME_CLIENT", "0")
if NO_DATAFRAME_CLIENT.lower() not in ("1", "true"):
    from .dataframe_client import DataFrameClient  # noqa: F401 unused import
    __all__.append("DataFrameClient")


__version__ = '5.3.2'
