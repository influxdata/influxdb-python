import argparse

from influxdb import InfluxDBClient
import math
import datetime


USER = 'root'
PASSWORD = 'root'
DBNAME = 'tutorial'


def main(host='localhost', port=8086):
    """
    main function to generate the sin wave
    """
    now = datetime.datetime.today()
    data = [{
        'name':    "foobar",
        'columns': ["time", "value"],
        'points':  []
    }]

    for angle in range(0, 360):
        y = 10 + math.sin(math.radians(angle)) * 10
        point = [int(now.strftime('%s')) + angle, y]
        data[0]['points'].append(point)

    client = InfluxDBClient(host, port, USER, PASSWORD, DBNAME)

    print("Create database: " + DBNAME)
    client.create_database(DBNAME)

    #Write points
    client.write_points(data)

    query = 'SELECT time, value FROM foobar GROUP BY value, time(1s)'
    print("Queying data: " + query)
    result = client.query(query)
    print("Result: {0}".format(result))

    """
    You might want to comment the delete and plot the result on InfluxDB Interface
    Connect on InfluxDB Interface at http://127.0.0.1:8083/
    Select  the database tutorial -> Explore Data

    Then run the following query:

        SELECT time, value FROM foobar  GROUP BY value, time(1s)
    """

    print("Delete database: " + DBNAME)
    client.delete_database(DBNAME)


def parse_args():
    parser = argparse.ArgumentParser(
        description='example code to play with InfluxDB')
    parser.add_argument('--host', type=str, required=False, default='localhost',
                        help='hostname influxdb http API')
    parser.add_argument('--port', type=int, required=False, default=8086,
                        help='port influxdb http API')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    main(host=args.host, port=args.port)
