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


with open(os.path.join(os.path.dirname(__file__), 'influxdb', '__init__.py')) as f:
    version = re.search("__version__ = '([^']+)'", f.read()).group(1)

with open('requirements.txt', 'r') as f:
    requires = [x.strip() for x in f if x.strip()]

with open('test-requirements.txt', 'r') as f:
    test_requires = [x.strip() for x in f if x.strip()]

with open('README.rst', 'r') as f:
    readme = f.read()


setup(
    name='influxdb',
    version=version,
    description="InfluxDB client",
    long_description=readme,
    url='https://github.com/influxdb/influxdb-python',
    license='MIT License',
    packages=find_packages(exclude=['tests']),
    test_suite='tests',
    tests_require=test_requires,
    install_requires=requires,
    extras_require={'test': test_requires},
    classifiers=(
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ),
)
