# -*- coding: utf-8 -*-
"""Tutorial how to authorize InfluxDB client by custom Authorization token."""

import argparse
from influxdb import InfluxDBClient


def main(token='my-token'):
    """Instantiate a connection to the InfluxDB."""
    client = InfluxDBClient(username=None, password=None,
                            headers={"Authorization": token})

    print("Use authorization token: " + token)

    version = client.ping()
    print("Successfully connected to InfluxDB: " + version)
    pass


def auth_per_query(token="my-token"):
    """Set headers per query, most prevelantly for authorization headers."""
    client = InfluxDBClient(username=None, password=None, headers=None)

    print(f"Use authorization token {token}")

    print(f"""Response for query with token:
        {client.query(
            f'SHOW DATABASES',
            headers={"Authorization": token})}""")
    pass


def parse_args():
    """Parse the args from main."""
    parser = argparse.ArgumentParser(
        description='example code to play with InfluxDB')
    parser.add_argument('--token', type=str, required=False,
                        default='my-token',
                        help='Authorization token for the proxy that is ahead the InfluxDB.')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    main(token=args.token)
    auth_per_query(token=args.token)
