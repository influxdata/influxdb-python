from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals


class InfluxDBClientError(Exception):
    """Raised when an error occurs in the request."""
    def __init__(self, content, code=None):
        if isinstance(content, type(b'')):
            content = content.decode('UTF-8', 'replace')

        if code is not None:
            message = "%s: %s" % (code, content)
        else:
            message = content

        super(InfluxDBClientError, self).__init__(
            message
        )
        self.content = content
        self.code = code


class InfluxDBServerError(Exception):
    """Raised when a server error occurs."""
    def __init__(self, content):
        super(InfluxDBServerError, self).__init__(content)
