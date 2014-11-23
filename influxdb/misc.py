# -*- coding: utf-8 -*-
"""
Miscellaneous
"""
import math

from .client import InfluxDBClient


class DataFrameClient(InfluxDBClient):
    """
    The ``DataFrameClient`` object holds information necessary to connect
    to InfluxDB. Requests can be made to InfluxDB directly through the client.
    The client reads and writes from pandas DataFrames.
    """

    import pandas as pd
    EPOCH = pd.Timestamp('1970-01-01 00:00:00.000+00:00')

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

        batch_size = kwargs.get('batch_size')
        if batch_size:
            kwargs.pop('batch_size')  # don't hand over to InfluxDBClient
            for key, data_frame in data.items():
                number_batches = int(math.ceil(len(data_frame)
                                     / float(batch_size)))
                for batch in range(number_batches):
                    start_index = batch * batch_size
                    end_index = (batch + 1) * batch_size
                    data = [self._convert_dataframe_to_json(
                        name=key,
                        dataframe=data_frame.ix[start_index:end_index].copy())]
                    InfluxDBClient.write_points_with_precision(self, data,
                                                               *args, **kwargs)
            return True
        else:
            data = [self._convert_dataframe_to_json(name=key,
                                                    dataframe=dataframe)
                    for key, dataframe in data.items()]
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
        if 'sequence_number' in dataframe.keys():
            dataframe.sort(['time', 'sequence_number'], inplace=True)
        else:
            dataframe.sort(['time'], inplace=True)
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
        if dataframe.index.tzinfo is None:
            dataframe.index = dataframe.index.tz_localize('UTC')
        dataframe['time'] = [self._datetime_to_epoch(dt)
                             for dt in dataframe.index]
        data = {'name': name,
                'columns': [str(column) for column in dataframe.columns],
                'points': list([list(x) for x in dataframe.values])}
        return data

    def _datetime_to_epoch(self, datetime):
        return (datetime - DataFrameClient.EPOCH).total_seconds()
