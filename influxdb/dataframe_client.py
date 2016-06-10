# -*- coding: utf-8 -*-
"""
DataFrame client for InfluxDB
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

__all__ = ['DataFrameClient']

try:
    import pandas
    del pandas
except ImportError as err:
    from .client import InfluxDBClient

    class DataFrameClient(InfluxDBClient):
        def __init__(self, *a, **kw):
            raise ImportError("DataFrameClient requires Pandas "
                              "which couldn't be imported: %s" % err)
else:
    from ._dataframe_client import DataFrameClient
