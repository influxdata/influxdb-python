# -*- coding: utf-8 -*-

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import os
import sys
import subprocess
import socket
import distutils


def get_free_ports(num_ports, ip='127.0.0.1'):
    """Get `num_ports` free/available ports on the interface linked to the `ip´
    :param int num_ports: The number of free ports to get
    :param str ip: The ip on which the ports have to be taken
    :return: a set of ports number
    """
    sock_ports = []
    ports = set()
    try:
        for _ in range(num_ports):
            sock = socket.socket()
            cur = [sock, -1]
            # append the socket directly,
            # so that it'll be also closed (no leaked resource)
            # in the finally here after.
            sock_ports.append(cur)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((ip, 0))
            cur[1] = sock.getsockname()[1]
    finally:
        for sock, port in sock_ports:
            sock.close()
            ports.add(port)
    assert num_ports == len(ports)
    return ports


def is_port_open(port, ip='127.0.0.1'):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        result = sock.connect_ex((ip, port))
        if not result:
            sock.shutdown(socket.SHUT_RDWR)
        return result == 0
    finally:
        sock.close()


def find_influxd_path():
    influxdb_bin_path = os.environ.get(
        'INFLUXDB_PYTHON_INFLUXD_PATH',
        None
    )

    if influxdb_bin_path is None:
        influxdb_bin_path = distutils.spawn.find_executable('influxd')
        if not influxdb_bin_path:
            try:
                influxdb_bin_path = subprocess.check_output(
                    ['which', 'influxd']
                ).strip()
            except subprocess.CalledProcessError:
                # fallback on :
                influxdb_bin_path = '/opt/influxdb/influxd'

    if not os.path.isfile(influxdb_bin_path):
        raise unittest.SkipTest("Could not find influxd binary")

    version = subprocess.check_output([influxdb_bin_path, 'version'])
    print("InfluxDB version: %s" % version, file=sys.stderr)

    return influxdb_bin_path
