# -*- coding: utf-8 -*-

_sentinel = object()


class ResultSet(object):
    """A wrapper around series results """

    def __init__(self, series):
        self.raw = series

    def __getitem__(self, key):
        """
        :param key: Either a serie name, or a tags_dict, or
                    a 2-tuple(serie_name, tags_dict).
                    If the serie name is None (or not given) then any serie
                    matching the eventual given tags will be given its points
                    one after the other.
                    To get the points of every serie in this resultset then
                    you have to provide None as key.
        :return: A generator yielding `Point`s matching the given key.
        NB:
        The order in which the points are yielded is actually undefined but
        it might change..
        """
        if isinstance(key, tuple):
            if 2 != len(key):
                raise TypeError('only 2-tuples allowed')
            name = key[0]
            tags = key[1]
            if not isinstance(tags, dict) and tags is not None:
                raise TypeError('tags should be a dict')
        elif isinstance(key, dict):
            name = None
            tags = key
        else:
            name = key
            tags = None

        if not isinstance(name, (bytes, type(b''.decode()), type(None))):
            raise TypeError('serie_name must be an str or None')

        for serie in self._get_series():
            serie_name = serie.get('name', 'results')
            if serie_name is None:
                # this is a "system" query or a query which
                # doesn't return a name attribute.
                # like 'show retention policies' ..
                if key is None:
                    for point in serie['values']:
                        yield self.point_from_cols_vals(
                            serie['columns'],
                            point
                        )

            elif name in (None, serie_name):
                # by default if no tags was provided then
                # we will matches every returned serie
                serie_tags = serie.get('tags', {})
                if tags is None or self._tag_matches(serie_tags, tags):
                    for point in serie.get('values', []):
                        yield self.point_from_cols_vals(
                            serie['columns'],
                            point
                        )

    def __repr__(self):
        items = []

        for item in self.items():
            items.append("'%s': %s" % (item[0], list(item[1])))

        return "ResultSet({%s})" % ", ".join(items)

    def __iter__(self):
        """ Iterating a ResultSet will yield one dict instance per serie result.
        """
        for key in self.keys():
            yield list(self.__getitem__(key))

    def _tag_matches(self, tags, filter):
        """Checks if all key/values in filter match in tags"""
        for tag_name, tag_value in filter.items():
            # using _sentinel as I'm not sure that "None"
            # could be used, because it could be a valid
            # serie_tags value : when a serie has no such tag
            # then I think it's set to /null/None/.. TBC..
            serie_tag_value = tags.get(tag_name, _sentinel)
            if serie_tag_value != tag_value:
                return False
        return True

    def _get_series(self):
        """Returns all series"""
        series = []
        try:
            for result in self.raw['results']:
                series.extend(result['series'])
        except KeyError:
            pass
        return series

    def __len__(self):
        return len(self.keys())

    def keys(self):
        """
        :return: List of keys. Keys are tuples (serie_name, tags)
        """
        keys = []
        for serie in self._get_series():
            keys.append(
                (serie.get('name', 'results'), serie.get('tags', None))
            )
        return keys

    def items(self):
        """
        :return: List of tuples, (key, generator)
        """
        items = []
        for serie in self._get_series():
            serie_key = (serie.get('name', 'results'), serie.get('tags', None))
            items.append(
                (serie_key, self[serie_key])
            )
        return items

    @staticmethod
    def point_from_cols_vals(cols, vals):
        """ Creates a dict from columns and values lists

        :param cols: List of columns
        :param vals: List of values
        :return: Dict where keys are columns.
        """
        point = {}
        for col_index, col_name in enumerate(cols):
            point[col_name] = vals[col_index]
        return point
