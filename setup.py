#!/usr/bin/env python
# -*- coding: utf-8 -*-

try:
    import distribute_setup
    distribute_setup.use_setuptools()
except:
    pass

try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup

import os
import re


with open(os.path.join(os.path.dirname(__file__),
    'influxdb', '__init__.py')) as f:
    version = re.search("__version__ = '([^']+)'", f.read()).group(1)

with open('requirements.txt', 'r') as f:
    requires = [x.strip() for x in f if x.strip()]

with open('test-requirements.txt', 'r') as f:
    test_requires = [x.strip() for x in f if x.strip()]

setup(
    name='influxdb',
    version=version,
    description="influxdb client",
    packages=find_packages(exclude=['tests']),
    test_suite='tests',
    tests_require=test_requires,
    install_requires=requires,
    extras_require={'test': test_requires},
)
