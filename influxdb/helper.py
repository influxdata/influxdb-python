# -*- coding: utf-8 -*-
"""
Helper class for InfluxDB
"""
from collections import namedtuple, defaultdict
from warnings import warn

import six


class SeriesHelper(object):

    '''
    Subclassing this helper eases writing data points in bulk.
    All data points are immutable, insuring they do not get overwritten.
    Each subclass can write to its own database.
    The time series names can also be based on one or more defined fields.

    Annotated example:
    ```
    class MySeriesHelper(SeriesHelper):
        class Meta:
            # Meta class stores time series helper configuration.
            series_name = 'events.stats.{server_name}'
            # Series name must be a string, curly brackets for dynamic use.
            fields = ['time', 'server_name']
            # Defines all the fields in this time series.
            ### Following attributes are optional. ###
            client = TestSeriesHelper.client
            # Client should be an instance of InfluxDBClient.
            :warning: Only used if autocommit is True.
            bulk_size = 5
            # Defines the number of data points to write simultaneously.
            :warning: Only applicable if autocommit is True.
            autocommit = True
            # If True and no bulk_size, then will set bulk_size to 1.

    # The following will create *five* (immutable) data points.
    # Since bulk_size is set to 5, upon the fifth construction call, all data
    # points will be written on the wire via MySeriesHelper.Meta.client.
    MySeriesHelper(server_name='us.east-1', time=159)
    MySeriesHelper(server_name='us.east-1', time=158)
    MySeriesHelper(server_name='us.east-1', time=157)
    MySeriesHelper(server_name='us.east-1', time=156)
    MySeriesHelper(server_name='us.east-1', time=155)

    # If autocommit None or False, one must call commit to write datapoints.
    # To manually submit data points which are not yet written, call commit:
    MySeriesHelper.commit()

    # To inspect the JSON which will be written, call _json_body_():
    MySeriesHelper._json_body_()
    ```
    '''
    __initialized__ = False

    def __new__(cls, *args, **kwargs):
        '''
        Initializes class attributes for subsequent constructor calls.
        :note: *args and **kwargs are not explicitly used in this function,
        but needed for Python 2 compatibility.
        '''
        if not cls.__initialized__:
            cls.__initialized__ = True
            try:
                _meta = getattr(cls, 'Meta')
            except AttributeError:
                raise AttributeError(
                    'Missing Meta class in {}.'.format(
                        cls.__name__))

            for attr in ['series_name', 'fields']:
                try:
                    setattr(cls, '_' + attr, getattr(_meta, attr))
                except AttributeError:
                    raise AttributeError(
                        'Missing {} in {} Meta class.'.format(
                            attr,
                            cls.__name__))

            cls._autocommit = getattr(_meta, 'autocommit', False)

            cls._client = getattr(_meta, 'client', None)
            if cls._autocommit and not cls._client:
                raise AttributeError(
                    'In {}, autocommit is set to True, but no client is set.'
                    .format(cls.__name__))

            try:
                cls._bulk_size = getattr(_meta, 'bulk_size')
                if cls._bulk_size < 1 and cls._autocommit:
                    warn(
                        'Definition of bulk_size in {} forced to 1, '
                        'was less than 1.'.format(cls.__name__))
                    cls._bulk_size = 1
            except AttributeError:
                cls._bulk_size = -1
            else:
                if not cls._autocommit:
                    warn(
                        'Definition of bulk_size in {} has no affect because'
                        ' autocommit is false.'.format(cls.__name__))

            cls._datapoints = defaultdict(list)
            cls._type = namedtuple(cls.__name__, cls._fields)

        return super(SeriesHelper, cls).__new__(cls)

    def __init__(self, **kw):
        '''
        Constructor call creates a new data point. All fields must be present.
        :note: Data points written when `bulk_size` is reached per Helper.
        :warning: Data points are *immutable* (`namedtuples`).
        '''
        cls = self.__class__

        if sorted(cls._fields) != sorted(kw.keys()):
            raise NameError(
                'Expected {0}, got {1}.'.format(
                    cls._fields,
                    kw.keys()))

        cls._datapoints[cls._series_name.format(**kw)].append(cls._type(**kw))

        if cls._autocommit and len(cls._datapoints) >= cls._bulk_size:
            cls.commit()

    @classmethod
    def commit(cls, client=None):
        '''
        Commit everything from datapoints via the client.
        :param client: InfluxDBClient instance for writing points to InfluxDB.
        :attention: any provided client will supersede the class client.
        :return result of client.write_points.
        '''
        if not client:
            client = cls._client
        rtn = client.write_points(cls._json_body_())
        cls._reset_()
        return rtn

    @classmethod
    def _json_body_(cls):
        '''
        :return: JSON body of these datapoints.
        '''
        json = []
        for series_name, data in six.iteritems(cls._datapoints):
            json.append({'name': series_name,
                         'columns': cls._fields,
                         'points': [[point.__dict__[k] for k in cls._fields] \
                                    for point in data]
                         })
        return json

    @classmethod
    def _reset_(cls):
        '''
        Reset data storage.
        '''
        cls._datapoints = defaultdict(list)
