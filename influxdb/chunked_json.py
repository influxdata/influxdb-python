# -*- coding: utf-8 -*-

#
# Author: Adrian Sampson <adrian@radbox.org>
# Source: https://gist.github.com/sampsyo/920215
#

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import json

_decoder = json.JSONDecoder()


def loads(s):
    """A generator reading a sequence of JSON values from a string."""
    while s:
        s = s.strip()
        obj, pos = _decoder.raw_decode(s)
        if not pos:
            raise ValueError('no JSON object found at %i' % pos)
        yield obj
        s = s[pos:]
