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


def _to_nanos(timestamp):
    delta = timestamp - EPOCH
    nanos_in_days = delta.days * 86400 * 10 ** 9
    nanos_in_seconds = delta.seconds * 10 ** 9
    nanos_in_micros = delta.microseconds * 10 ** 3
    return nanos_in_days + nanos_in_seconds + nanos_in_micros


def _convert_timestamp(timestamp, precision=None):
    if isinstance(timestamp, Integral):
        return timestamp  # assume precision is correct if timestamp is int
    if isinstance(_get_unicode(timestamp), text_type):
        timestamp = parse(timestamp)
    if isinstance(timestamp, datetime):
        if not timestamp.tzinfo:
            timestamp = UTC.localize(timestamp)
        ns = _to_nanos(timestamp)
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


def make_lines(data, precision=None):
    """
    Extracts the points from the given dict and returns a Unicode string
    matching the line protocol introduced in InfluxDB 0.9.0.
    """
    lines = []
    static_tags = data.get('tags', None)
    for point in data['points']:
        elements = []

        # add measurement name
        measurement = _escape_tag(_get_unicode(
            point.get('measurement', data.get('measurement'))
        ))
        key_values = [measurement]

        # add tags
        if static_tags is None:
            tags = point.get('tags', {})
        else:
            tags = copy(static_tags)
            tags.update(point.get('tags', {}))

        # tags should be sorted client-side to take load off server
        for tag_key in sorted(tags.keys()):
            key = _escape_tag(tag_key)
            value = _escape_tag(tags[tag_key])

            if key != '' and value != '':
                key_values.append("{key}={value}".format(key=key, value=value))
        key_values = ','.join(key_values)
        elements.append(key_values)

        # add fields
        field_values = []
        for field_key in sorted(point['fields'].keys()):
            key = _escape_tag(field_key)
            value = _escape_value(point['fields'][field_key])
            if key != '' and value != '':
                field_values.append("{key}={value}".format(
                    key=key,
                    value=value
                ))
        field_values = ','.join(field_values)
        elements.append(field_values)

        # add timestamp
        if 'time' in point:
            timestamp = _get_unicode(str(int(
                _convert_timestamp(point['time'], precision)
            )))
            elements.append(timestamp)

        line = ' '.join(elements)
        lines.append(line)
    lines = '\n'.join(lines)
    return lines + '\n'
