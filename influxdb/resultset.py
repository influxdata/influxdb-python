# -*- coding: utf-8 -*-

import collections
from collections import namedtuple


_sentinel = object()


class NamedValues(object):
    def __init__(self, point):
        self._point = point

    def __getattr__(self, item):
        try:
            index = self._point.columns.index(item)
        except ValueError:
            raise AttributeError('Point have no such attribute (%r)' % item)
        return self._point._point_values[index]

    def __repr__(self):
        return 'Values(%s)' % ', '.join(
            '%s=%r' % (k, self._point._point_values[k])
            for k in self._point.columns)


class Point(object):

    def __init__(self, serie, columns, values, tags=None):
        assert len(columns) == len(values)
        self.columns = columns
        self._point_values = values
        if tags is None:
            tags = {}
        self.serie = serie
        self.tags = tags
        self.values = NamedValues(self)

    def __getitem__(self, tag_name):
        """Indexing a Point return the tag value associated with
        the given tag name"""
        return self._tags[tag_name]

    def __iter__(self):
        """Iterating over a Point will return its tags names one per one"""
        return iter(self._tags)

    def __len__(self):
        """The len of a Point is its number of columns/values"""
        return len(self.columns)

    def __repr__(self):
        return 'Point(values=%s, tags=%s)' % (
            ', '.join('%s=%r' % (
                k, getattr(self.values, k)) for k in self.columns),
            self.tags)



class ResultSet(object):
    """A wrapper around series results """

    def __init__(self, series):
        self.raw = series  # ['results']
        results = series['results']
        if False:
            self.have_tags = (
            results
            and 'series' in results[0]
            and results[0]['series']
            and 'tags' in results[0]['series'][0]
        )
        # self.raw.update(series)  # use the free update to set keys

    def __getitem__(self, key):
        '''
        :param key: Either a serie name or a 2-tuple(serie_name, tags_dict)
                    If the given serie name is None then any serie (matching the eventual
                    given tags) will be given its points one after the other.
        :return: A generator yielding `Point`s matching the given key
        '''
        if isinstance(key, tuple):
            if 2 != len(key):
                raise TypeError('only 2-tuples allowed')
            name = key[0]
            tags = key[1]
            if not isinstance(tags, dict):
                raise TypeError('should be a dict')
        else:
            name = key
            tags = None

        for result in self.raw['results']:
            for serie in result['series']:
                serie_name = serie.get('name', None)
                if serie_name is None:
                    # this is a "system" query or a query which doesn't returned a named "serie"
                    # 'list retention' is in this case..
                    if key is None:
                        for point in serie['values']:
                            yield Point(None, serie['columns'], point)

                elif name in (None, serie_name):
                    # by default if no tags was provided then
                    # we will matches every returned serie
                    serie_matches = True
                    serie_tags = serie.get('tags', {})
                    if tags:
                        serie_matches = False
                        for tag_name, tag_value in tags.items():
                            serie_tag_value = serie_tags.get(tag_name, _sentinel)
                            if serie_tag_value != tag_value:
                                break
                        else:
                            serie_matches = True

                    if serie_matches:
                        for point in serie['values']:
                            yield Point(serie_name, serie['columns'], point, serie_tags)

    def __repr__(self):
        return str(self.raw)

    def __iter__(self):
        ''' Iterating a ResultSet will yield one dict instance per serie result.
        '''
        for results in self.raw['results']:
            for many_series in results['series']:
                yield many_series

    #def __len__(self):
    #    return len(self.raw)
