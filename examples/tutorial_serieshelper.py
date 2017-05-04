"""
Tutorial/Example how to use the class helper `SeriesHelper`
"""

from influxdb import InfluxDBClient
from influxdb import SeriesHelper

# InfluxDB connections settings
host = 'localhost'
port = 8086
user = 'root'
password = 'root'
dbname = 'mydb'

myclient = InfluxDBClient(host, port, user, password, dbname)

# Uncomment the following code if the database is not yet created
# myclient.create_database(dbname)
# myclient.create_retention_policy('awesome_policy', '3d', 3, default=True)


class MySeriesHelper(SeriesHelper):
    # Meta class stores time series helper configuration.
    class Meta:
        # The client should be an instance of InfluxDBClient.
        client = myclient
        # The series name must be a string. Add dependent fields/tags in curly brackets.
        series_name = 'events.stats.{server_name}'
        # Defines all the fields in this time series.
        fields = ['some_stat', 'other_stat']
        # Defines all the tags for the series.
        tags = ['server_name']
        # Defines the number of data points to store prior to writing on the wire.
        bulk_size = 5
        # autocommit must be set to True when using bulk_size
        autocommit = True


# The following will create *five* (immutable) data points.
# Since bulk_size is set to 5, upon the fifth construction call, *all* data
# points will be written on the wire via MySeriesHelper.Meta.client.
MySeriesHelper(server_name='us.east-1', some_stat=88, other_stat=10)
MySeriesHelper(server_name='us.east-1', some_stat=158, other_stat=20)
MySeriesHelper(server_name='us.east-1', some_stat=157, other_stat=30)
MySeriesHelper(server_name='us.east-1', some_stat=156, other_stat=40)
MySeriesHelper(server_name='us.east-1', some_stat=155, other_stat=50)

# To manually submit data points which are not yet written, call commit:
MySeriesHelper.commit()

# To inspect the JSON which will be written, call _json_body_():
MySeriesHelper._json_body_()


#
# Different example allowing to set the timestamp
#
class MySeriesTimestampHelper(SeriesHelper):
    # Meta class stores time series helper configuration.
    class Meta:
        # The client should be an instance of InfluxDBClient.
        client = myclient
        # The series name must be a string. Add dependent fields/tags in curly brackets.
        series_name = 'events.stats.{server_name}'
        # Defines all the fields in this time series.
        fields = ['some_stat', 'other_stat', 'timestamp']
        # Defines all the tags for the series.
        tags = ['server_name']
        # Defines the number of data points to store prior to writing on the wire.
        bulk_size = 5
        # autocommit must be set to True when using bulk_size
        autocommit = True


# The following will create *five* (immutable) data points.
# Since bulk_size is set to 5, upon the fifth construction call, *all* data
# points will be written on the wire via MySeriesHelper.Meta.client.
MySeriesTimestampHelper(server_name='us.east-1', some_stat=88, other_stat=10,
                        timestamp="2015-03-11T18:00:24.017486904Z")
MySeriesTimestampHelper(server_name='us.east-1', some_stat=158, other_stat=20,
                        timestamp="2015-03-11T18:00:24.017486994Z")
MySeriesTimestampHelper(server_name='us.east-1', some_stat=157, other_stat=30,
                        timestamp="2015-03-11T18:00:24.017487080Z")
MySeriesTimestampHelper(server_name='us.east-1', some_stat=156, other_stat=40,
                        timestamp="2015-03-11T18:00:24.017487305Z")
MySeriesTimestampHelper(server_name='us.east-1', some_stat=155, other_stat=50,
                        timestamp="2015-03-11T18:00:24.017487512Z")

# To manually submit data points which are not yet written, call commit:
MySeriesTimestampHelper.commit()

# To inspect the JSON which will be written, call _json_body_():
MySeriesTimestampHelper._json_body_()
