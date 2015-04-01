# -*- coding: utf-8 -*-
import collections


class ResultSet(collections.MutableMapping):
    """A dictionary that applies an arbitrary key-altering
       function before accessing the keys"""

    def __init__(self, series):
        self.raw = dict()
        self.update(series)  # use the free update to set keys

    def __getitem__(self, key):
        if isinstance(key, tuple):
            name = key[0]
            tags = key[1]
        else:
            name = key
            tags = None

        results = []

        for result in self.raw['results']:
            for serie in result['series']:
                if serie['name'] == name:
                    serie_matches = True

                    if tags is not None:
                        for tag in tags.items():
                            if serie['tags'][tag[0]] != tag[1]:
                                serie_matches = False
                                break

                    if serie_matches:
                        items = []
                        for value in serie['values']:
                            item = {}
                            for cur_col, field in enumerate(value):
                                item[serie['columns'][cur_col]] = field
                            items.append(item)

                        results.append({"points": items, "tags": serie['tags']})
                        continue
        return results

    def __setitem__(self, key, value):
        self.raw[key] = value

    def __repr__(self):
        return str(self.raw)

    def __delitem__(self, key):
        del self.raw[key]

    def __iter__(self):
        for serie in self.raw:
            yield {"points": self.raw[serie], "tags": dict((tag, value) for tag, value in serie[1])}

    def __len__(self):
        return len(self.raw)
