# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from calendar import timegm
from copy import copy
from datetime import datetime

from dateutil.parser import parse
from pytz import utc
from six import binary_type, text_type


def _convert_timestamp(timestamp):
    if isinstance(timestamp, int):
        return timestamp
    if isinstance(_force_text(timestamp), text_type):
        timestamp = parse(timestamp)
    if isinstance(timestamp, datetime):
        if timestamp.tzinfo:
            timestamp = timestamp.astimezone(utc)
            timestamp.replace(tzinfo=None)
        return (
            timegm(timestamp.timetuple()) * 1e9 +
            timestamp.microsecond * 1e3
        )
    raise ValueError(timestamp)


def _escape_tag(tag):
    return tag.replace(
        "\\", "\\\\"
    ).replace(
        " ", "\\ "
    ).replace(
        ",", "\\,"
    ).replace(
        "=", "\\="
    )


def _escape_value(value):
    value = _force_text(value)
    if isinstance(value, text_type):
        return "\"{}\"".format(value.replace(
            "\"", "\\\""
        ))
    else:
        return str(value)


def _force_text(data):
    """
    Try to return a text aka unicode object from the given data.
    """
    if isinstance(data, binary_type):
        return data.decode('utf-8', 'replace')
    else:
        return data


def make_lines(data):
    """
    Extracts the points from the given dict and returns a Unicode string
    matching the line protocol introduced in InfluxDB 0.9.0.
    """
    lines = ""
    static_tags = data.get('tags', None)
    for point in data['points']:
        # add measurement name
        lines += _escape_tag(_force_text(
            point.get('measurement', data.get('measurement'))
        )) + ","

        # add tags
        if static_tags is None:
            tags = point.get('tags', {})
        else:
            tags = copy(static_tags)
            tags.update(point.get('tags', {}))
        # tags should be sorted client-side to take load off server
        for tag_key in sorted(tags.keys()):
            lines += "{key}={value},".format(
                key=_escape_tag(tag_key),
                value=_escape_tag(tags[tag_key]),
            )
        lines = lines[:-1] + " "  # strip the trailing comma

        # add fields
        for field_key in sorted(point['fields'].keys()):
            lines += "{key}={value},".format(
                key=_escape_tag(field_key),
                value=_escape_value(point['fields'][field_key]),
            )
        lines = lines[:-1]  # strip the trailing comma

        # add timestamp
        if 'time' in point:
            lines += " " + _force_text(str(int(
                _convert_timestamp(point['time'])
            )))

        lines += "\n"
    return lines
