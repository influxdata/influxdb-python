# -*- coding: utf-8 -*-
"""
DataFrame client for InfluxDB
"""

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
