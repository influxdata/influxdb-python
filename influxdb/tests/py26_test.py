#!/usr/bin/env python

import sys
import os
import datetime
import requests
import socket
import time

from influxdb import InfluxDBClient

# InfluxDB Server parameters
influxdb_server = "localhost"
port = 8086
database_name = "test_db"
host_name = socket.gethostname()

# InfluxDB connection and database creation if it doesn't exists
client = InfluxDBClient(influxdb_server, port, "influx-user", "influx-pass", database_name)

try:
    client.create_database(database_name)
except requests.ConnectionError as e:
    print("Failed to start, unable to establish connection with InfluxDB server %s - %s" % (influxdb_server, e))
    sys.exit(2)

# System Load
load_avg_1 = os.popen("cat /proc/loadavg|awk '{print $1,$2,$3}'").read().split()[0]
load_avg_5 = os.popen("cat /proc/loadavg|awk '{print $1,$2,$3}'").read().split()[1]
load_avg_15 = os.popen("cat /proc/loadavg|awk '{print $1,$2,$3}'").read().split()[2]

counter = 0

while counter <= 5:
    load_1 = [
        {"measurement": "Load_1_minute",
         "tags": {"hosts": host_name},
         "time": datetime.datetime.now(), "fields": {"load_avg_1": float(load_avg_1)}}]
    client.write_points(load_1)

    load_5 = [
        {"measurement": "Load_5_minutes",
         "tags": {"hosts": host_name},
         "time": datetime.datetime.now(), "fields": {"load_avg_5": float(load_avg_5)}}]
    client.write_points(load_5)

    load_15 = [
        {"measurement": "Load_15_minute",
         "tags": {"hosts": host_name},
         "time": datetime.datetime.now(), "fields": {"load_avg_15": float(load_avg_15)}}]
    client.write_points(load_15)

    counter += 1
    time.sleep(1)

result = client.query("select load_avg_1 from Load_1_minute;")

print("Result: %s" % result)