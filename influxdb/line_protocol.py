# -*- coding: utf-8 -*-
"""Define the line_protocol handler."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from datetime import datetime
from numbers import Integral

from six import iteritems, binary_type, text_type, integer_types, PY2

import pandas as pd  # Provide for ns timestamps

EPOCH = pd.Timestamp(0, tz='UTC')


def _convert_timestamp(timestamp, precision=None):
    if isinstance(timestamp, Integral):
        return timestamp  # assume precision is correct if timestamp is int

    if isinstance(_get_unicode(timestamp), text_type):
        timestamp = pd.Timestamp(timestamp)

    if isinstance(timestamp, datetime):  # change to pandas.Timestamp
        if not timestamp.tzinfo:
            timestamp = pd.Timestamp(timestamp, tz='UTC')
        else:
            timestamp = pd.Timestamp(timestamp)

    if isinstance(timestamp, pd._libs.tslib.Timestamp):
        if not timestamp.tzinfo:  # set to UTC for time since EPOCH
            timestamp = pd.Timestamp(timestamp, tz='UTC')
        else:
            timestamp = timestamp.astimezone('UTC')

        ns = (timestamp - EPOCH).value
        if precision is None or precision == 'n':
            return ns
        elif precision == 'u':
            return ns // 1e3
        elif precision == 'ms':
            return ns // 1e6
        elif precision == 's':
            return ns // 1e9
        elif precision == 'm':
            return ns // 1e9 // 60
        elif precision == 'h':
            return ns // 1e9 // 3600

    raise ValueError(timestamp)


def _escape_tag(tag):
    tag = _get_unicode(tag, force=True)
    return tag.replace(
        "\\", "\\\\"
    ).replace(
        " ", "\\ "
    ).replace(
        ",", "\\,"
    ).replace(
        "=", "\\="
    )


def _escape_tag_value(value):
    ret = _escape_tag(value)
    if ret.endswith('\\'):
        ret += ' '
    return ret


def quote_ident(value):
    """Indent the quotes."""
    return "\"{}\"".format(value
                           .replace("\\", "\\\\")
                           .replace("\"", "\\\"")
                           .replace("\n", "\\n"))


def quote_literal(value):
    """Quote provided literal."""
    return "'{}'".format(value
                         .replace("\\", "\\\\")
                         .replace("'", "\\'"))


def _is_float(value):
    try:
        float(value)
    except (TypeError, ValueError):
        return False

    return True


def _escape_value(value):
    value = _get_unicode(value)

    if isinstance(value, text_type) and value != '':
        return quote_ident(value)
    elif isinstance(value, integer_types) and not isinstance(value, bool):
        return str(value) + 'i'
    elif _is_float(value):
        return repr(value)

    return str(value)


def _get_unicode(data, force=False):
    """Try to return a text aka unicode object from the given data."""
    if isinstance(data, binary_type):
        return data.decode('utf-8')
    elif data is None:
        return ''
    elif force:
        if PY2:
            return unicode(data)
        else:
            return str(data)
    else:
        return data


def make_lines(data, precision=None):
    """Extract points from given dict.

    Extracts the points from the given dict and returns a Unicode string
    matching the line protocol introduced in InfluxDB 0.9.0.
    """
    lines = []
    static_tags = data.get('tags')
    for point in data['points']:
        elements = []

        # add measurement name
        measurement = _escape_tag(_get_unicode(
            point.get('measurement', data.get('measurement'))))
        key_values = [measurement]

        # add tags
        if static_tags:
            tags = dict(static_tags)  # make a copy, since we'll modify
            tags.update(point.get('tags') or {})
        else:
            tags = point.get('tags') or {}

        # tags should be sorted client-side to take load off server
        for tag_key, tag_value in sorted(iteritems(tags)):
            key = _escape_tag(tag_key)
            value = _escape_tag_value(tag_value)

            if key != '' and value != '':
                key_values.append(key + "=" + value)

        elements.append(','.join(key_values))

        # add fields
        field_values = []
        for field_key, field_value in sorted(iteritems(point['fields'])):
            key = _escape_tag(field_key)
            value = _escape_value(field_value)

            if key != '' and value != '':
                field_values.append(key + "=" + value)

        elements.append(','.join(field_values))

        # add timestamp
        if 'time' in point:
            timestamp = _get_unicode(str(int(
                _convert_timestamp(point['time'], precision))))
            elements.append(timestamp)

        line = ' '.join(elements)
        lines.append(line)

    return '\n'.join(lines) + '\n'
