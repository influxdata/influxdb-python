# -*- coding: utf-8 -*-
"""
Helper class for InfluxDB
"""
from collections import namedtuple, defaultdict
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
            client = TestSeriesHelper.client
            # The client should be an instance of InfluxDBClient.
            series_name = 'events.stats.{server_name}'
            # The series name must be a string. Add dependent field names in curly brackets.
            fields = ['time', 'server_name']
            # Defines all the fields in this time series.
            bulk_size = 5
            # Defines the number of data points to store prior to writing on the wire.
            autocommit = True
            # Sets autocommit: must be set to True for bulk_size to have any affect.
    
    # The following will create *five* (immutable) data points.
    # Since bulk_size is set to 5, upon the fifth construction call, *all* data
    # points will be written on the wire via MySeriesHelper.Meta.client.
    MySeriesHelper(server_name='us.east-1', time=159)
    MySeriesHelper(server_name='us.east-1', time=158)
    MySeriesHelper(server_name='us.east-1', time=157)
    MySeriesHelper(server_name='us.east-1', time=156)
    MySeriesHelper(server_name='us.east-1', time=155)
    
    # If autocommit unset (or set to False), one must call commit to write datapoints.
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
        '''
        if not SeriesHelper.__initialized__:
            SeriesHelper.__initialized__ = True
            try:
                _meta = getattr(cls, 'Meta')
            except AttributeError:
                raise AttributeError('Missing Meta class in {}.'.format(cls.__name__))
    
            for attr in ['series_name', 'fields', 'client']:
                try:
                    setattr(cls, '_' + attr, getattr(_meta, attr))
                except AttributeError:
                    raise AttributeError('Missing {} in {} Meta class.'.format(attr, cls.__name__))
    
            cls._autocommit = getattr(_meta, 'autocommit', False)
            cls._bulk_size = getattr(_meta, 'bulk_size', 1)

            cls._datapoints = defaultdict(list)
            cls._type = namedtuple(cls.__name__, cls._fields)

        return super(SeriesHelper, cls).__new__(cls, *args, **kwargs)

    def __init__(self, **kw):
        '''
        Constructor call creates a new data point. All fields must be present.
        :note: Data points written when `bulk_size` is reached per Helper.
        :warning: Data points are *immutable* (`namedtuples`). 
        '''
        cls = self.__class__

        if sorted(cls._fields) != sorted(kw.keys()):
            raise NameError('Expected {0}, got {1}.'.format(cls._fields, kw.keys()))

        cls._datapoints[cls._series_name.format(**kw)].append(cls._type(**kw))

        if cls._autocommit and len(cls._datapoints) >= cls._bulk_size:
            cls.commit()

    @classmethod
    def commit(cls):
        '''
        Commit everything from datapoints via the client.
        :return result of client.write_points.
        '''
        rtn = cls._client.write_points(cls._json_body_())
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
                         'points': [[point.__dict__[k] for k in cls._fields] for point in data]
                         })
        return json

    @classmethod
    def _reset_(cls):
        '''
        Reset data storage.
        '''
        cls._datapoints = defaultdict(list)
