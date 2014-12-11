
.. _api-documentation:

=================
API Documentation
=================

To connect to a InfluxDB, you must create a
:py:class:`~influxdb.InfluxDBClient` object. The default configuration
connects to InfluxDB on ``localhost`` with the default
ports. The below instantiation statements are all equivalent::

    from influxdb import InfluxDBClient

    # using Http
    client = InfluxDBClient(database='dbname')
    client = InfluxDBClient(host='127.0.0.1', port=8086, database='dbname')
    client = InfluxDBClient(host='127.0.0.1', port=8086, username='root', password='root', database='dbname')

    # using UDP
    client = InfluxDBClient(host='127.0.0.1', database='dbname', use_udp=True, udp_port=4444)

To write pandas DataFrames or to read data into a
pandas DataFrame, use a :py:class:`~influxdb.DataFrameClient` object.
These clients are initiated in the same way as the
:py:class:`~influxdb.InfluxDBClient`::

    from influxdb import DataFrameClient

    client = DataFrameClient(host='127.0.0.1', port=8086, username='root', password='root', dbname='dbname')


.. note:: Only when using UDP (use_udp=True) the connections is established.


.. _InfluxDBClient-api:

-----------------------
:class:`InfluxDBClient`
-----------------------


.. currentmodule:: influxdb.InfluxDBClient
.. autoclass:: influxdb.InfluxDBClient
    :members:
    :undoc-members:

-----------------------
:class:`DataFrameClient`
-----------------------


.. currentmodule:: influxdb.DataFrameClient
.. autoclass:: influxdb.DataFrameClient
    :members:
    :undoc-members:
