# -*- coding: utf-8 -*-
"""
DataFrame client for InfluxDB
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import math

import pandas as pd

from .client import InfluxDBClient
from .line_protocol import _escape_tag


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


def _escape_pandas_series(s):
    return s.apply(lambda v: _escape_tag(v))


class DataFrameClient(InfluxDBClient):
    """
    The ``DataFrameClient`` object holds information necessary to connect
    to InfluxDB. Requests can be made to InfluxDB directly through the client.
    The client reads and writes from pandas DataFrames.
    """

    EPOCH = pd.Timestamp('1970-01-01 00:00:00.000+00:00')

    def write_points(self,
                     dataframe,
                     measurement,
                     tags=None,
                     tag_columns=None,
                     field_columns=None,
                     time_precision=None,
                     database=None,
                     retention_policy=None,
                     batch_size=None,
                     protocol='line',
                     numeric_precision=None):
        """
        Write to multiple time series names.

        :param dataframe: data points in a DataFrame
        :param measurement: name of measurement
        :param tags: dictionary of tags, with string key-values
        :param time_precision: [Optional, default None] Either 's', 'ms', 'u'
            or 'n'.
        :param batch_size: [Optional] Value to write the points in batches
            instead of all at one time. Useful for when doing data dumps from
            one database to another or when doing a massive write operation
        :type batch_size: int
        :param protocol: Protocol for writing data. Either 'line' or 'json'.
        :param numeric_precision: Precision for floating point values.
            Either None, 'full' or some int, where int is the desired decimal
            precision. 'full' preserves full precision for int and float
            datatypes. Defaults to None, which preserves 14-15 significant
            figures for float and all significant figures for int datatypes.

        """
        if tag_columns is None:
            tag_columns = []
        if field_columns is None:
            field_columns = []
        if batch_size:
            number_batches = int(math.ceil(len(dataframe) / float(batch_size)))
            for batch in range(number_batches):
                start_index = batch * batch_size
                end_index = (batch + 1) * batch_size
                if protocol == 'line':
                    points = self._convert_dataframe_to_lines(
                        dataframe.ix[start_index:end_index].copy(),
                        measurement=measurement,
                        global_tags=tags,
                        time_precision=time_precision,
                        tag_columns=tag_columns,
                        field_columns=field_columns,
                        numeric_precision=numeric_precision)
                else:
                    points = self._convert_dataframe_to_json(
                        dataframe.ix[start_index:end_index].copy(),
                        measurement=measurement,
                        tags=tags,
                        time_precision=time_precision,
                        tag_columns=tag_columns,
                        field_columns=field_columns)
                super(DataFrameClient, self).write_points(
                    points,
                    time_precision,
                    database,
                    retention_policy,
                    protocol=protocol)
            return True
        else:
            if protocol == 'line':
                points = self._convert_dataframe_to_lines(
                    dataframe,
                    measurement=measurement,
                    global_tags=tags,
                    tag_columns=tag_columns,
                    field_columns=field_columns,
                    time_precision=time_precision,
                    numeric_precision=numeric_precision)
            else:
                points = self._convert_dataframe_to_json(
                    dataframe,
                    measurement=measurement,
                    tags=tags,
                    time_precision=time_precision,
                    tag_columns=tag_columns,
                    field_columns=field_columns)
            super(DataFrameClient, self).write_points(
                points,
                time_precision,
                database,
                retention_policy,
                protocol=protocol)
            return True

    def query(self, query, chunked=False, database=None):
        """
        Quering data into a DataFrame.

        :param chunked: [Optional, default=False] True if the data shall be
            retrieved in chunks, False otherwise.

        """
        results = super(DataFrameClient, self).query(query, database=database)
        if query.upper().startswith("SELECT"):
            if len(results) > 0:
                return self._to_dataframe(results)
            else:
                return {}
        else:
            return results

    def _to_dataframe(self, rs):
        result = {}
        if isinstance(rs, list):
            return map(self._to_dataframe, rs)
        for key, data in rs.items():
            name, tags = key
            if tags is None:
                key = name
            else:
                key = (name, tuple(sorted(tags.items())))
            df = pd.DataFrame(data)
            df.time = pd.to_datetime(df.time)
            df.set_index('time', inplace=True)
            df.index = df.index.tz_localize('UTC')
            df.index.name = None
            result[key] = df
        return result

    def _convert_dataframe_to_json(self,
                                   dataframe,
                                   measurement,
                                   tags=None,
                                   tag_columns=None,
                                   field_columns=None,
                                   time_precision=None):

        if not isinstance(dataframe, pd.DataFrame):
            raise TypeError('Must be DataFrame, but type was: {0}.'
                            .format(type(dataframe)))
        if not (isinstance(dataframe.index, pd.tseries.period.PeriodIndex) or
                isinstance(dataframe.index, pd.tseries.index.DatetimeIndex)):
            raise TypeError('Must be DataFrame with DatetimeIndex or \
                            PeriodIndex.')

        # Make sure tags and tag columns are correctly typed
        tag_columns = tag_columns if tag_columns else []
        field_columns = field_columns if field_columns else []
        tags = tags if tags else {}
        # Assume field columns are all columns not included in tag columns
        if not field_columns:
            field_columns = list(
                set(dataframe.columns).difference(set(tag_columns)))

        dataframe.index = dataframe.index.to_datetime()
        if dataframe.index.tzinfo is None:
            dataframe.index = dataframe.index.tz_localize('UTC')

        # Convert column to strings
        dataframe.columns = dataframe.columns.astype('str')

        # Convert dtype for json serialization
        dataframe = dataframe.astype('object')

        precision_factor = {
            "n": 1,
            "u": 1e3,
            "ms": 1e6,
            "s": 1e9,
            "m": 1e9 * 60,
            "h": 1e9 * 3600,
        }.get(time_precision, 1)

        points = [
            {'measurement': measurement,
             'tags': dict(list(tag.items()) + list(tags.items())),
             'fields': rec,
             'time': int(ts.value / precision_factor)}
            for ts, tag, rec in zip(dataframe.index,
                                    dataframe[tag_columns].to_dict('record'),
                                    dataframe[field_columns].to_dict('record'))
        ]

        return points

    def _convert_dataframe_to_lines(self,
                                    dataframe,
                                    measurement,
                                    field_columns=None,
                                    tag_columns=None,
                                    global_tags=None,
                                    time_precision=None,
                                    numeric_precision=None):

        if not isinstance(dataframe, pd.DataFrame):
            raise TypeError('Must be DataFrame, but type was: {0}.'
                            .format(type(dataframe)))
        if not (isinstance(dataframe.index, pd.tseries.period.PeriodIndex) or
                isinstance(dataframe.index, pd.tseries.index.DatetimeIndex)):
            raise TypeError('Must be DataFrame with DatetimeIndex or \
                            PeriodIndex.')

        # Create a Series of columns for easier indexing
        column_series = pd.Series(dataframe.columns)

        if field_columns is None:
            field_columns = []
        if tag_columns is None:
            tag_columns = []
        if global_tags is None:
            global_tags = {}

        # Make sure field_columns and tag_columns are lists
        field_columns = list(field_columns) if list(field_columns) else []
        tag_columns = list(tag_columns) if list(tag_columns) else []

        # Make global_tags as tag_columns
        if global_tags:
            for tag in global_tags:
                dataframe[tag] = global_tags[tag]
                tag_columns.append(tag)

        # If field columns but no tag columns, assume rest of columns are tags
        if field_columns and (not tag_columns):
            tag_columns = list(column_series[~column_series.isin(
                field_columns)])

        # If no field columns, assume non-tag columns are fields
        if not field_columns:
            field_columns = list(column_series[~column_series.isin(
                tag_columns)])

        precision_factor = {
            "n": 1,
            "u": 1e3,
            "ms": 1e6,
            "s": 1e9,
            "m": 1e9 * 60,
            "h": 1e9 * 3600,
        }.get(time_precision, 1)

        # Make array of timestamp ints
        if isinstance(dataframe.index, pd.tseries.period.PeriodIndex):
            time = ((dataframe.index.to_timestamp().values.astype(int) /
                     precision_factor).astype(int).astype(str))
        else:
            time = ((pd.to_datetime(dataframe.index).values.astype(int) /
                     precision_factor).astype(int).astype(str))

        # If tag columns exist, make an array of formatted tag keys and values
        if tag_columns:
            tag_df = dataframe[tag_columns]
            tag_df = tag_df.sort_index(axis=1)
            tag_df = self._stringify_dataframe(
                tag_df, numeric_precision, datatype='tag')
            tags = (',' + (
                (tag_df.columns.values + '=').tolist() + tag_df)).sum(axis=1)
            del tag_df

        else:
            tags = ''

        # Make an array of formatted field keys and values
        field_df = dataframe[field_columns]
        field_df = self._stringify_dataframe(
            field_df, numeric_precision, datatype='field')
        field_df = (field_df.columns.values + '=').tolist() + field_df
        field_df[field_df.columns[1:]] = ',' + field_df[field_df.columns[1:]]
        fields = field_df.sum(axis=1)
        del field_df

        # Generate line protocol string
        points = (measurement + tags + ' ' + fields + ' ' + time).tolist()
        return points

    def _stringify_dataframe(self,
                             dataframe,
                             numeric_precision,
                             datatype='field'):

        # Find int and string columns for field-type data
        int_columns = dataframe.select_dtypes(include=['integer']).columns
        string_columns = dataframe.select_dtypes(include=['object']).columns

        # Convert dataframe to string
        if numeric_precision is None:
            # If no precision specified, convert directly to string (fast)
            dataframe = dataframe.astype(str)
        elif numeric_precision == 'full':
            # If full precision, use repr to get full float precision
            float_columns = (dataframe.select_dtypes(include=['floating'])
                             .columns)
            nonfloat_columns = dataframe.columns[~dataframe.columns.isin(
                float_columns)]
            dataframe[float_columns] = dataframe[float_columns].applymap(repr)
            dataframe[nonfloat_columns] = (dataframe[nonfloat_columns]
                                           .astype(str))
        elif isinstance(numeric_precision, int):
            # If precision is specified, round to appropriate precision
            float_columns = (dataframe.select_dtypes(include=['floating'])
                             .columns)
            nonfloat_columns = dataframe.columns[~dataframe.columns.isin(
                float_columns)]
            dataframe[float_columns] = (dataframe[float_columns]
                                        .round(numeric_precision))
            # If desired precision is > 10 decimal places, need to use repr
            if numeric_precision > 10:
                dataframe[float_columns] = (dataframe[float_columns]
                                            .applymap(repr))
                dataframe[nonfloat_columns] = (dataframe[nonfloat_columns]
                                               .astype(str))
            else:
                dataframe = dataframe.astype(str)
        else:
            raise ValueError('Invalid numeric precision.')

        if datatype == 'field':
            # If dealing with fields, format ints and strings correctly
            dataframe[int_columns] = dataframe[int_columns] + 'i'
            dataframe[string_columns] = '"' + dataframe[string_columns] + '"'
        elif datatype == 'tag':
            dataframe = dataframe.apply(_escape_pandas_series)

        dataframe.columns = dataframe.columns.astype(str)
        return dataframe

    def _datetime_to_epoch(self, datetime, time_precision='s'):
        seconds = (datetime - self.EPOCH).total_seconds()
        if time_precision == 'h':
            return seconds / 3600
        elif time_precision == 'm':
            return seconds / 60
        elif time_precision == 's':
            return seconds
        elif time_precision == 'ms':
            return seconds * 1e3
        elif time_precision == 'u':
            return seconds * 1e6
        elif time_precision == 'n':
            return seconds * 1e9
