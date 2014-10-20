
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
    client = InfluxDBClient(dbname='dbname')
    client = InfluxDBClient(host='127.0.0.1', port=8086, dbname='dbname')
    client = InfluxDBClient(host='127.0.0.1', port=8086, username='root', password='root', dbname='dbname')

    # using UDP
    client = InfluxDBClient(host='127.0.0.1', dbname='dbname', use_udp=True, udp_port=4444)


.. note:: Only when using UDP (use_udp=True) the connections is established.



.. _InfluxDBClient-api:

-----------------------
:class:`InfluxDBClient`
-----------------------


.. currentmodule:: influxdb.InfluxDBClient
.. autoclass:: influxdb.InfluxDBClient
    :members:
    :undoc-members:
