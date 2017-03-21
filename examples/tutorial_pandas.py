import argparse
import pandas as pd

from influxdb import DataFrameClient


def main(host='localhost', port=8086):
    user = 'root'
    password = 'root'
    dbname = 'example'
    protocol = 'json'

    client = DataFrameClient(host, port, user, password, dbname)

    print("Create pandas DataFrame")
    df = pd.DataFrame(data=list(range(30)),
                      index=pd.date_range(start='2014-11-16',
                                          periods=30, freq='H'))

    print("Create database: " + dbname)
    client.create_database(dbname)

    print("Write DataFrame")
    client.write_points(df, 'demo')

    print("Write DataFrame with Tags")
    client.write_points(df, 'demo', {'k1': 'v1', 'k2': 'v2'})

    print("Read DataFrame")
    client.query("select * from demo")

    print("Delete database: " + dbname)
    client.delete_database(dbname)


def parse_args():
    parser = argparse.ArgumentParser(
        description='example code to play with InfluxDB')
    parser.add_argument('--host', type=str, required=False,
                        default='localhost',
                        help='hostname of InfluxDB http API')
    parser.add_argument('--port', type=int, required=False, default=8086,
                        help='port of InfluxDB http API')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    main(host=args.host, port=args.port)
