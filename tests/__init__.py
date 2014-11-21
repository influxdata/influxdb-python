# -*- coding: utf-8 -*-
import unittest
import sys

using_pypy = hasattr(sys, "pypy_version_info")
skipIfPYpy = unittest.skipIf(using_pypy, "Skipping this test on pypy.")
