================================
Query response object: ResultSet
================================

Using the ``InfluxDBClient.query()`` function will return a ``ResultSet`` Object.

A ResultSet behaves like a dict. Its keys are series and values are points. However, it is a little bit smarter than a regular dict. Its ``__getitem__`` method can be used to query the ResultSet in several ways.

Filtering by serie name
-----------------------

Using ``rs['cpu']`` will return a generator for all the points that are in a serie named ``cpu``, no matter the tags.
::

    rs = cli.query("SELECT * from cpu")
    cpu_points = list(rs['cpu'])

Filtering by tags
-----------------

Using ``rs[{'host_name': 'influxdb.com'}]`` will return a generator for all the points that are tagged with the specified tags, no matter the serie name.
::

    rs = cli.query("SELECT * from cpu")
    cpu_influxdb_com_points = list(rs[{"host_name": "influxdb.com"}])

Filtering by serie name and tags
--------------------------------

Using a tuple with a serie name and a dict will return a generator for all the points that are in a serie with the given name AND whose tags match the given tags.
::

    rs = cli.query("SELECT * from cpu")
    points = list(rs[('cpu', {'host_name': 'influxdb.com'})])

See the :ref:`api-documentation` page for more information.
