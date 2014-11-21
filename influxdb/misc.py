# -*- coding: utf-8 -*-
"""
Miscellaneous
"""
from time import mktime

from .client import InfluxDBClient


class DataFrameClient(InfluxDBClient):
    """
    The ``DataFrameClient`` object holds information necessary to connect
    to InfluxDB. Requests can be made to InfluxDB directly through the client.
    The client reads and writes from pandas DataFrames.
    """

    def write_points(self, data, *args, **kwargs):
        """
        write_points()

        Write to multiple time series names.

        :param data: A dictionary mapping series names to pandas DataFrames
        :param batch_size: [Optional] Value to write the points in batches
            instead of all at one time. Useful for when doing data dumps from
            one database to another or when doing a massive write operation
        :type batch_size: int
        """

        data = [self._convert_dataframe_to_json(name=key, dataframe=value)
                for key, value in data.items()]
        return InfluxDBClient.write_points_with_precision(self, data,
                                                          *args, **kwargs)

    def write_points_with_precision(self, data, time_precision='s'):
        """
        Write to multiple time series names
        """
        return self.write_points(data, time_precision='s')

    def query(self, query, time_precision='s', chunked=False):
        """
        Quering data into a DataFrame.

        :param time_precision: [Optional, default 's'] Either 's', 'm', 'ms'
            or 'u'.
        :param chunked: [Optional, default=False] True if the data shall be
            retrieved in chunks, False otherwise.

        """
        result = InfluxDBClient.query(self, query=query,
                                      time_precision=time_precision,
                                      chunked=chunked)
        return self._to_dataframe(result[0], time_precision)

    def _to_dataframe(self, json_result, time_precision):
        try:
            import pandas as pd
        except ImportError:
            raise ImportError('pandas required for retrieving as dataframe.')
        dataframe = pd.DataFrame(data=json_result['points'],
                                 columns=json_result['columns'])
        pandas_time_unit = time_precision
        if time_precision == 'm':
            pandas_time_unit = 'ms'
        elif time_precision == 'u':
            pandas_time_unit = 'us'
        dataframe.index = pd.to_datetime(list(dataframe['time']),
                                         unit=pandas_time_unit,
                                         utc=True)
        del dataframe['time']
        return dataframe

    def _convert_dataframe_to_json(self, dataframe, name):
        try:
            import pandas as pd
        except ImportError:
            raise ImportError('pandas required for writing as dataframe.')
        if not isinstance(dataframe, pd.DataFrame):
            raise TypeError('Must be DataFrame, but type was: {}.'
                            .format(type(dataframe)))
        if not (isinstance(dataframe.index, pd.tseries.period.PeriodIndex) or
                isinstance(dataframe.index, pd.tseries.index.DatetimeIndex)):
            raise TypeError('Must be DataFrame with DatetimeIndex or \
                            PeriodIndex.')
        dataframe.index = dataframe.index.to_datetime()
        dataframe['time'] = [mktime(dt.timetuple()) for dt in dataframe.index]
        data = {'name': name,
                'columns': [str(column) for column in dataframe.columns],
                'points': list([list(x) for x in dataframe.values])}
        return data
