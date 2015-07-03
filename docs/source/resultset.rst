
.. _resultset:

================================
Query response object: ResultSet
================================

Using the ``InfluxDBClient.query()`` function will return a ``ResultSet`` Object.

A ResultSet can be browsed in several ways. Its ``get_points`` method can be used to retrieve points generators that filter either by measurement, tags, or both.

Getting all points
------------------

Using ``rs.get_points()`` will return a generator for all the points in the ResultSet.


Filtering by measurement
------------------------

Using ``rs.get_points('cpu')`` will return a generator for all the points that are in a serie with measurement name ``cpu``, no matter the tags.
::

    rs = cli.query("SELECT * from cpu")
    cpu_points = list(rs.get_points(measurement='cpu')])

Filtering by tags
-----------------

Using ``rs.get_points(tags={'host_name': 'influxdb.com'})`` will return a generator for all the points that are tagged with the specified tags, no matter the measurement name.
::

    rs = cli.query("SELECT * from cpu")
    cpu_influxdb_com_points = list(rs.get_points(tags={"host_name": "influxdb.com"}))

Filtering by measurement and tags
---------------------------------

Using measurement name and tags will return a generator for all the points that are in a serie with the specified measurement name AND whose tags match the given tags.
::

    rs = cli.query("SELECT * from cpu")
    points = list(rs.get_points(measurement='cpu', tags={'host_name': 'influxdb.com'}))

See the :ref:`api-documentation` page for more information.
