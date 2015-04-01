# -*- coding: utf-8 -*-
import collections


class ResultSet(collections.MutableMapping):
    """A dictionary that applies an arbitrary key-altering
       function before accessing the keys"""

    def __init__(self, series):
        self.store = dict()
        self.update(series)  # use the free update to set keys

    def __getitem__(self, key):
        if isinstance(key, tuple):
            name = key[0]
            tags = key[1]
        else:
            name = key
            tags = None

        for serie in self.store.keys():
            if serie[0] == name:
                serie_matches = True
                serie_tags = dict((tag, value) for tag, value in serie[1])
                if tags is not None:
                    for tag in tags.items():
                        try:
                            if serie_tags[tag[0]] != tag[1]:
                                serie_matches = False
                                break
                        except KeyError:
                            serie_matches = False
                            break
                if serie_matches:
                    yield {"points": self.store[serie], "tags": serie_tags}

    def __setitem__(self, key, value):
        self.store[key] = value

    def __repr__(self):
        rep = ""
        for serie in self.store.keys():
            rep += "('%s', %s): %s" % (serie[0], dict((tag, value) for tag, value in serie[1]), self.store[serie])
        return '%s(%s)' % (type(self).__name__, rep)

    def __delitem__(self, key):
        del self.store[key]

    def __iter__(self):
        return iter(self.store)

    def __len__(self):
        return len(self.store)
