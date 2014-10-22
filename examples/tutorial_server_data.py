import argparse

from influxdb import InfluxDBClient
import datetime
import random


USER = 'root'
PASSWORD = 'root'
DBNAME = 'tutorial'


def main(host='localhost', port=8086, nb_day=15):

    nb_day = 15  # number of day to generate time series
    timeinterval_min = 5  # create an event every x minutes
    total_minutes = 1440 * nb_day
    total_records = int(total_minutes / timeinterval_min)
    now = datetime.datetime.today()
    cpu_series = [{
        'name':    "server_data.cpu_idle",
        'columns': ["time", "value", "hostName"],
        'points':  []
    }]

    for i in range(0, total_records):
        past_date = now - datetime.timedelta(minutes=i * timeinterval_min)
        value = random.randint(0, 200)
        hostName = "server-%d" % random.randint(1, 5)
        pointValues = [int(past_date.strftime('%s')), value, hostName]
        cpu_series[0]['points'].append(pointValues)

    client = InfluxDBClient(host, port, USER, PASSWORD, DBNAME)

    print("Create database: " + DBNAME)
    client.create_database(DBNAME)

    print("Write points #: {0}".format(total_records))
    client.write_points(cpu_series)

    query = 'SELECT MEAN(value) FROM server_data.cpu_idle GROUP BY time(30m) WHERE time > now() - 1d;'
    print("Queying data: " + query)
    result = client.query(query)
    print("Result: {0}".format(result))

    print("Delete database: " + DBNAME)
    client.delete_database(DBNAME)


def parse_args():
    parser = argparse.ArgumentParser(
        description='example code to play with InfluxDB')
    parser.add_argument('--host', type=str, required=False, default='localhost',
                        help='hostname influxdb http API')
    parser.add_argument('--port', type=int, required=False, default=8086,
                        help='port influxdb http API')
    parser.add_argument('--nb_day', type=int, required=False, default=15,
                        help='number of days to generate time series data')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    main(host=args.host, port=args.port, nb_day=args.nb_day)
