influxdb-python
===============

[![Build Status](https://travis-ci.org/influxdb/influxdb-python.png?branch=master)](https://travis-ci.org/influxdb/influxdb-python)
[![Downloads](https://pypip.in/download/influxdb/badge.svg)](https://pypi.python.org/pypi/influxdb/)
[![Latest Version](https://pypip.in/version/influxdb/badge.svg)](https://pypi.python.org/pypi/influxdb/)
[![Supported Python versions](https://pypip.in/py_versions/influxdb/badge.svg)](https://pypi.python.org/pypi/influxdb/)
[![License](https://pypip.in/license/influxdb/badge.svg)](https://pypi.python.org/pypi/influxdb/)

Python client for InfluxDB

For developers
--------------

### Uploading

```
$ python setup.py sdist upload
```

### Testing

Make sure you have tox by running the following:

```
$ pip install tox
```

To test influxdb-python with multiple version of Python, you can use tox:

````
$ tox
````

If you don't have all Python version listed in tox.ini, then

````
$ tox -e py27
````
