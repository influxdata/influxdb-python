# -*- coding: utf-8 -*-

import collections
from collections import namedtuple


_sentinel = object()


# could it be a namedtuple .. ??
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
        '''

        :param serie:    The name of the serie in which this point resides.
                         If None then it's a "system" point/result.
        :param columns:  The ordered list of the columns.
        :param values:   The actualy list of values of this point. Same order than columns.
        :param tags:     The eventual tags (dict) associated with the point.
        :return:
        '''
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
        the given tag name, if it exists"""
        return self._tags[tag_name]

    def __iter__(self):
        """Iterating over a Point will return its eventual tag names one per one"""
        return iter(self._tags)

    def __len__(self):
        """The len of a Point is its number of columns/values"""
        return len(self.columns)

    def __repr__(self):
        return 'Point(values=(%s), tags=%s)' % (
            ', '.join('%s=%r' % (
                k, getattr(self.values, k)) for k in self.columns),
            self.tags)



class ResultSet(object):
    """A wrapper around series results """

    def __init__(self, series):
        self.raw = series

    def __getitem__(self, key):
        '''
        :param key: Either a serie name or a 2-tuple(serie_name, tags_dict)
                    If the given serie name is None then any serie (matching
                    the eventual given tags) will be given its points one
                    after the other.
        :return: A generator yielding `Point`s matching the given key.
        NB:
        The order in which the points are yielded is actually undefined but
        it might change..
        '''
        if isinstance(key, tuple):
            if 2 != len(key):
                raise TypeError('only 2-tuples allowed')
            name = key[0]
            tags = key[1]
            if not isinstance(tags, dict):
                raise TypeError('tags should be a dict')
        else:
            name = key
            tags = None
        if not isinstance(name, (str, type(None))):
            raise TypeError('serie_name must be an str or None')

        for result in self.raw['results']:
            for serie in result['series']:
                serie_name = serie.get('name', None)
                if serie_name is None:
                    # this is a "system" query or a query which
                    # doesn't return a name attribute.
                    # like 'show retention policies' ..
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
                        # if there are some tags requested,
                        # let's check them:
                        for tag_name, tag_value in tags.items():
                            # using _sentinel as I'm not sure that "None"
                            # could be used, because it could be a valid
                            # serie_tags value : when a serie has no such tag
                            # then I think it's set to /null/None/.. TBC..
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
            for serie in results['series']:
                yield serie

    #def __len__(self):
    #    return len(self.raw)
