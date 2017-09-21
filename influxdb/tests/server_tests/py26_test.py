# -*- coding: utf-8 -*-
"""Define the resultset test package."""

from __future__ import unicode_literals

import datetime
import os
import socket
import subprocess
import time
import unittest

from influxdb.tests.server_tests.base import SingleTestCaseWithServerMixin

THIS_DIR = os.path.abspath(os.path.dirname(__file__))

# hack in check_output if it's not defined, like for python 2.6
if "check_output" not in dir(subprocess):
    def f(*popenargs, **kwargs):
        """Check for output."""
        if 'stdout' in kwargs:
            raise ValueError(
                'stdout argument not allowed, it will be overridden.'
            )
        process = subprocess.Popen(stdout=subprocess.PIPE,
                                   *popenargs,
                                   **kwargs)
        output, unused_err = process.communicate()
        retcode = process.poll()
        if retcode:
            cmd = kwargs.get("args")
            if cmd is None:
                cmd = popenargs[0]
            raise subprocess.CalledProcessError(retcode, cmd)
        return output
    subprocess.check_output = f


class TestPython26Set(SingleTestCaseWithServerMixin, unittest.TestCase):
    """Define the Python26Set test object."""

    influxdb_template_conf = os.path.join(THIS_DIR, 'influxdb.conf.template')

    def test_write_points(self):
        """Test write points for Python26Set object."""
        self.host_name = socket.gethostname()
        self.assertTrue(self.cli.create_database('db') is None)

        # System Load
        self.lf = ["cat", "/proc/loadavg"]

        c = 0

        while c < 5:
            d = subprocess.check_output(self.lf).strip().split()

            load_1 = [
                {
                    "measurement": "Load_1_minute",
                    "tags": {"hosts": self.host_name},
                    "time": datetime.datetime.now(),
                    "fields": {"load_avg_1": float(d[0])}
                }
            ]
            self.cli.write_points(load_1)

            load_5 = [
                {
                    "measurement": "Load_5_minutes",
                    "tags": {"hosts": self.host_name},
                    "time": datetime.datetime.now(),
                    "fields": {"load_avg_5": float(d[1])}
                }
            ]
            self.cli.write_points(load_5)

            load_15 = [
                {
                    "measurement": "Load_15_minute",
                    "tags": {"hosts": self.host_name},
                    "time": datetime.datetime.now(),
                    "fields": {"load_avg_15": float(d[2])}
                }
            ]
            self.cli.write_points(load_15)

            c += 1
            time.sleep(1)

        result = self.cli.query("select load_avg_1 from Load_1_minute;")
        self.assertTrue(result is not None)
