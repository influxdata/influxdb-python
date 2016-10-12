# -*- coding: utf-8 -*-

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from copy import copy
from datetime import datetime
from numbers import Integral

from pytz import UTC
from dateutil.parser import parse
from six import binary_type, text_type, integer_types, PY2

EPOCH = UTC.localize(datetime.utcfromtimestamp(0))


def _convert_timestamp(timestamp, precision=None):
    if isinstance(timestamp, Integral):
        return timestamp  # assume precision is correct if timestamp is int
    if isinstance(_get_unicode(timestamp), text_type):
        timestamp = parse(timestamp)
    if isinstance(timestamp, datetime):
        if not timestamp.tzinfo:
            timestamp = UTC.localize(timestamp)
        ns = (timestamp - EPOCH).total_seconds() * 1e9
        if precision is None or precision == 'n':
            return ns
        elif precision == 'u':
            return ns / 1e3
        elif precision == 'ms':
            return ns / 1e6
        elif precision == 's':
            return ns / 1e9
        elif precision == 'm':
            return ns / 1e9 / 60
        elif precision == 'h':
            return ns / 1e9 / 3600
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


def quote_ident(value):
    return "\"{0}\"".format(
        value.replace(
            "\\", "\\\\"
        ).replace(
            "\"", "\\\""
        ).replace(
            "\n", "\\n"
        )
    )


def quote_literal(value):
    return "'{0}'".format(
        value.replace(
            "\\", "\\\\"
        ).replace(
            "'", "\\'"
        )
    )


def _escape_value(value):
    value = _get_unicode(value)
    if isinstance(value, text_type) and value != '':
        return quote_ident(value)
    elif isinstance(value, integer_types) and not isinstance(value, bool):
        return str(value) + 'i'
    else:
        return str(value)


def _get_unicode(data, force=False):
    """
    Try to return a text aka unicode object from the given data.
    """
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


def make_line(measurement, tags=None, fields=None, time=None, precision=None):
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

    if time:
        timestamp = _get_unicode(str(int(
            _convert_timestamp(time, precision)
        )))
        line += ' ' + timestamp

    return line


def make_lines(data, precision=None):
    """
    Extracts the points from the given dict and returns a Unicode string
    matching the line protocol introduced in InfluxDB 0.9.0.
    """
    lines = []
    static_tags = data.get('tags', None)
    for point in data['points']:
        if static_tags is None:
            tags = point.get('tags', {})
        else:
            tags = copy(static_tags)
            tags.update(point.get('tags', {}))

        line = make_line(
            point.get('measurement', data.get('measurement')),
            tags=tags,
            fields=point['fields'],
            precision=precision,
            time=point.get('time')
        )
        lines.append(line)

    lines = '\n'.join(lines)
    return lines + '\n'
