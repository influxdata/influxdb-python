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
