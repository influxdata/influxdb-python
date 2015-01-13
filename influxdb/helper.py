# -*- coding: utf-8 -*-
"""
Helper class for InfluxDB
"""
from collections import namedtuple, defaultdict


class InfluxDBSeriesHelper(object):

    def __new__(cls, *args, **kwargs):
        # Introspect series representation.
        try:
            _meta = getattr(cls, 'Meta')
        except AttributeError:
            raise AttributeError('SeriesHelper {} does not contain a Meta class.'.format(cls.__name__))

        for attribute in ['series_name', 'fields', 'client']:
            try:
                setattr(cls, '_' + attribute, getattr(_meta, attribute))
            except AttributeError:
                raise AttributeError('SeriesHelper\' {0} Meta class does not define {1}.'.format(cls.__name__, attribute))

        cls._bulk_size = getattr(_meta, 'bulk_size', 1)

        # Class attribute definitions
        cls._datapoints = defaultdict(list) # keys are the series name for ease of commit.
        cls._type = namedtuple(cls.__name__, cls._fields)

        return super(InfluxDBSeriesHelper, cls).__new__(cls, *args, **kwargs)

    def __init__(self, **kwargs): # Does not support positional arguments.
        cls = self.__class__

        if sorted(cls._fields) != sorted(kwargs.keys()):
            raise KeyError('[Fields enforced] Expected fields {0} and got {1}.'.format(sorted(cls._fields), sorted(kwargs.keys())))

        cls._datapoints[cls._series_name.format(**kwargs)] = cls._type(**kwargs)

        if len(cls._datapoints) > cls._bulk_size:
            cls.commit()

    @staticmethod
    def _json_body():
        '''
        :return: JSON body of these datapoints.
        '''
        pass

    @staticmethod
    def commit():
        '''
        Commit everything from datapoints via the client.
        '''
        pass
