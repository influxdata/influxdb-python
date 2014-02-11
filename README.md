influxdb-python
===============

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
