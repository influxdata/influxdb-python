# -*- coding: utf-8 -*-
"""Define the line_protocol handler."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from datetime import datetime
from numbers import Integral
from six import binary_type, text_type, integer_types, PY2

import pandas as pd  # Provide for ns timestamps
import numpy as np  # Provided for accurate precision_factor conversion

EPOCH = pd.Timestamp(0, tz='UTC')

# Precisions factors must be int for correct calculation to ints.
# if precision is float the result of a floor calc is an approximation
# Example : the issue is only observable with nanosecond resolution
#           values are greater than 895ns
# ts = pd.Timestamp('2013-01-01 23:10:55.123456987+00:00')
# ts_ns = np.int64(ts.value)
# # For conversion to microsecond
# precision_factor=1e3
# expected_ts_us = 1357081855123456
# np.int64(ts_ns // precision_factor) #  is INCORRECT 1357081855123457
# np.int64(ts_ns // np.int64(precision_factor) #  is CORRECT 1357081855123456

_time_precision_factors = {"n": 1,
                           "u": np.int64(1e3),
                           "ms": np.int64(1e6),
                           "s": np.int64(1e9),
                           "m": np.int64(1e9 * 60),
                           "h": np.int64(1e9 * 3600), }


def _convert_timestamp(timestamp, time_precision=None):
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

        nanoseconds = (timestamp - EPOCH).value
        precision_factor = _time_precision_factors.get(time_precision, 1)
        return np.int64(nanoseconds // np.int64(precision_factor))
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
    ).replace(
        "\n", "\\n"
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

    if isinstance(value, integer_types) and not isinstance(value, bool):
        return str(value) + 'i'

    if _is_float(value):
        return repr(value)

    return str(value)


def _get_unicode(data, force=False):
    """Try to return a text aka unicode object from the given data."""
    if isinstance(data, binary_type):
        return data.decode('utf-8')

    if data is None:
        return ''

    if force:
        if PY2:
            return unicode(data)
        return str(data)

    return data


def make_line(measurement, tags=None, fields=None, time=None, precision=None):
    """Extract the actual point from a given measurement line."""
    tags = tags or {}
    fields = fields or {}

    line = _escape_tag(_get_unicode(measurement))

    # tags should be sorted client-side to take load off server
    tag_list = []
    for tag_key in sorted(tags.keys()):
        key = _escape_tag(tag_key)
        value = _escape_tag(tags[tag_key])

        if key != '' and value != '':
            tag_list.append(
                "{key}={value}".format(key=key, value=value)
            )

    if tag_list:
        line += ',' + ','.join(tag_list)

    field_list = []
    for field_key in sorted(fields.keys()):
        key = _escape_tag(field_key)
        value = _escape_value(fields[field_key])

        if key != '' and value != '':
            field_list.append("{key}={value}".format(
                key=key,
                value=value
            ))

    if field_list:
        line += ' ' + ','.join(field_list)

    if time is not None:
        timestamp = _get_unicode(str(int(
            _convert_timestamp(time, precision)
        )))
        line += ' ' + timestamp

    return line


def make_lines(data, precision=None):
    """Extract points from given dict.

    Extracts the points from the given dict and returns a Unicode string
    matching the line protocol introduced in InfluxDB 0.9.0.
    """
    lines = []
    static_tags = data.get('tags')
    for point in data['points']:
        if static_tags:
            tags = dict(static_tags)  # make a copy, since we'll modify
            tags.update(point.get('tags') or {})
        else:
            tags = point.get('tags') or {}

        line = make_line(
            point.get('measurement', data.get('measurement')),
            tags=tags,
            fields=point.get('fields'),
            precision=precision,
            time=point.get('time')
        )
        lines.append(line)

    return '\n'.join(lines) + '\n'
