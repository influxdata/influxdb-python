# -*- coding: utf-8 -*-
"""
Helper class for InfluxDB
"""
from collections import namedtuple, defaultdict
import six

class SeriesHelper(object):
    __initialized__ = False

    def __new__(cls, *args, **kwargs):
        if not SeriesHelper.__initialized__:
            SeriesHelper.__initialized__ = True
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

        return super(SeriesHelper, cls).__new__(cls, *args, **kwargs)

    def __init__(self, **kwargs): # Does not support positional arguments.
        cls = self.__class__

        if sorted(cls._fields) != sorted(kwargs.keys()):
            raise NameError('Expected fields {0} and got {1}.'.format(', '.join(sorted(cls._fields)), ', '.join(sorted(kwargs.keys()))))

        cls._datapoints[cls._series_name.format(**kwargs)].append(cls._type(**kwargs))

        if len(cls._datapoints) > cls._bulk_size:
            cls.commit()

    @classmethod
    def commit(cls):
        '''
        Commit everything from datapoints via the client.
        '''
        rtn = cls._client.write_points(cls._json_body_())
        cls._reset_()
        return rtn

    @classmethod
    def _json_body_(cls):
        '''
        :return: JSON body of these datapoints.
        '''
        json_datapoints = []
        for series_name, data in six.iteritems(cls._datapoints):
            json_datapoints.append({'name': series_name,
                                    'columns': cls._fields,
                                    'points': [[point.__dict__[k] for k in cls._fields] for point in data]
                                    })
        return json_datapoints

    @classmethod
    def _reset_(cls):
        cls._datapoints = defaultdict(list)
