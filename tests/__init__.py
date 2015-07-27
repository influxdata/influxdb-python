# -*- coding: utf-8 -*-
import unittest
import sys
import os

using_pypy = hasattr(sys, "pypy_version_info")
skipIfPYpy = unittest.skipIf(using_pypy, "Skipping this test on pypy.")

_skip_server_tests = os.environ.get('INFLUXDB_PYTHON_SKIP_SERVER_TESTS', None) == 'True'
skipServerTests = unittest.skipIf(_skip_server_tests, "Skipping server tests...")
