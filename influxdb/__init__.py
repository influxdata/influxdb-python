# -*- coding: utf-8 -*-
from .client import InfluxDBClient


__all__ = ['InfluxDBClient']

try:
    import pandas
    from .dataframe_client import DataFrameClient
    __all__ += 'DataFrameClient'
except ImportError:
    pass

__version__ = '0.1.13'
