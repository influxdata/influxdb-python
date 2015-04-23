# -*- coding: utf-8 -*-
"""
DataFrame client for InfluxDB
"""
import math

import pandas as pd

from .client import InfluxDBClient


def _pandas_time_unit(time_precision):
    unit = time_precision
    if time_precision == 'm':
        unit = 'ms'
    elif time_precision == 'u':
        unit = 'us'
    elif time_precision == 'n':
        unit = 'ns'
    assert unit in ('s', 'ms', 'us', 'ns')
    return unit


class DataFrameClient(InfluxDBClient):
    """
    The ``DataFrameClient`` object holds information necessary to connect
    to InfluxDB. Requests can be made to InfluxDB directly through the client.
    The client reads and writes from pandas DataFrames.
    """

    EPOCH = pd.Timestamp('1970-01-01 00:00:00.000+00:00')

    def write_points(self,
                     data,
                     time_precision=None,
                     database=None,
                     retention_policy=None,
                     tags=None, **kwargs):
        """
        Write to multiple time series names.

        :param data: A dictionary mapping series to pandas DataFrames
        :param time_precision: [Optional, default 's'] Either 's', 'ms', 'u'
            or 'n'.
        :param batch_size: [Optional] Value to write the points in batches
            instead of all at one time. Useful for when doing data dumps from
            one database to another or when doing a massive write operation
        :type batch_size: int
        """

        batch_size = kwargs.get('batch_size')
        time_precision = kwargs.get('time_precision', 's')
        if batch_size:
            kwargs.pop('batch_size')  # don't hand over to InfluxDBClient
            for key, data_frame in data.items():
                number_batches = int(math.ceil(
                    len(data_frame) / float(batch_size)))
                for batch in range(number_batches):
                    start_index = batch * batch_size
                    end_index = (batch + 1) * batch_size
                    data = self._convert_dataframe_to_json(
                        key=key,
                        dataframe=data_frame.ix[start_index:end_index].copy(),
                        time_precision=time_precision)
                    super(DataFrameClient, self).write_points(data, **kwargs)
            return True
        else:
            for key, data_frame in data.items():
                data = self._convert_dataframe_to_json(
                    key=key, dataframe=data_frame,
                    time_precision=time_precision)
                super(DataFrameClient, self).write_points(data, **kwargs)
            return True

    def query(self, query, time_precision='s', chunked=False, database=None):
        """
        Quering data into a DataFrame.

        :param time_precision: [Optional, default 's'] Either 's', 'm', 'ms'
            or 'u'.
        :param chunked: [Optional, default=False] True if the data shall be
            retrieved in chunks, False otherwise.

        """
        results = super(DataFrameClient, self).query(query, database=database)
        if len(results) > 0:
            return self._to_dataframe(results.raw, time_precision)
        else:
            return {}

    def get_list_series(self, database=None):
        """
        Get the list of series, in DataFrame

        """
        results = super(DataFrameClient, self)\
            .query("SHOW SERIES", database=database)
        return dict(
            (s['name'], pd.DataFrame(s['values'], columns=s['columns'])) for
            s in results.raw['results'][0]['series']
        )

    def _to_dataframe(self, json_result, time_precision):

        result = {}
        series = json_result['results'][0]['series']
        for s in series:
            tags = s.get('tags')
            key = (s['name'], tuple(tags.items()) if tags else None)
            df = pd.DataFrame(s['values'], columns=s['columns'])
            df.time = pd.to_datetime(
                df.time, unit=_pandas_time_unit(time_precision), utc=True)
            df.set_index(['time'], inplace=True)
            result[key] = df
        return result

    def _convert_dataframe_to_json(self, key, dataframe, time_precision='s'):

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

        # Convert column to strings
        dataframe.columns = dataframe.columns.astype('str')

        name, tags = key
        points = [
            {'name': name,
             'tags': dict(tags) if tags else {},
             'fields': rec,
             'timestamp': ts.isoformat()
             }
            for ts, rec in zip(dataframe.index, dataframe.to_dict('record'))]
        return points

    def _datetime_to_epoch(self, datetime, time_precision='s'):
        seconds = (datetime - self.EPOCH).total_seconds()
        if time_precision == 's':
            return seconds
        elif time_precision == 'ms':
            return seconds * 10 ** 3
        elif time_precision == 'u':
            return seconds * 10 ** 6
        elif time_precision == 'n':
            return seconds * 10 ** 9
