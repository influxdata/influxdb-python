# -*- coding: utf-8 -*-


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
        """

        :param serie:    The name of the serie in which this point resides.
                         If None then it's a "system" point/result.
        :param columns:  The ordered list of the columns.
        :param values:   The actualy list of values of this point. Same order than columns.
        :param tags:     The eventual tags (dict) associated with the point.
        :return:
        """
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

    def __eq__(self, other):
        return (isinstance(other, self.__class__)
                and self.tags == other.tags
                and self._point_values == other._point_values
                and self.serie == other.serie
                and self.columns == other.columns)

    def __ne__(self, other):
        return not self.__eq__(other)
