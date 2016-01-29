# -*- coding: utf-8 -*-

import sys
import os
if sys.version_info < (2, 7):
    import unittest2 as unittest
else:
    import unittest

using_pypy = hasattr(sys, "pypy_version_info")
skipIfPYpy = unittest.skipIf(using_pypy, "Skipping this test on pypy.")

_skip_server_tests = os.environ.get(
    'INFLUXDB_PYTHON_SKIP_SERVER_TESTS',
    None) == 'True'
skipServerTests = unittest.skipIf(_skip_server_tests,
                                  "Skipping server tests...")
