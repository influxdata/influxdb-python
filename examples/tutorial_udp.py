# -*- coding: utf-8 -*-
"""Example for sending batch information to InfluxDB via UDP."""

"""
INFO: In order to use UDP, one should enable the UDP service from the
`influxdb.conf` under section
    [[udp]]
        enabled = true
        bind-address = ":8089" # port number for sending data via UDP
        database = "udp1" # name of database to be stored
    [[udp]]
        enabled = true
        bind-address = ":8090"
        database = "udp2"
"""


import argparse

from influxdb import InfluxDBClient


def main(uport):
    """Instantiate connection to the InfluxDB."""
    # NOTE: structure of the UDP packet is different than that of information
    #       sent via HTTP
    json_body = {
        "tags": {
            "host": "server01",
            "region": "us-west"
        },
        "time": "2009-11-10T23:00:00Z",
        "points": [{
            "measurement": "cpu_load_short",
            "fields": {
                "value": 0.64
            }
        },
            {
                "measurement": "cpu_load_short",
                "fields": {
                    "value": 0.67
                }
        }]
    }

    # make `use_udp` True and  add `udp_port` number from `influxdb.conf` file
    # no need to mention the database name since it is already configured
    client = InfluxDBClient(use_udp=True, udp_port=uport)

    # Instead of `write_points` use `send_packet`
    client.send_packet(json_body)


def parse_args():
    """Parse the args."""
    parser = argparse.ArgumentParser(
        description='example code to play with InfluxDB along with UDP Port')
    parser.add_argument('--uport', type=int, required=True,
                        help=' UDP port of InfluxDB')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    main(uport=args.uport)
