# -*- coding: utf-8 -*-
"""
Python client for InfluxDB
"""
import json
import socket
import requests
import requests.exceptions


try:
    xrange
except NameError:
    xrange = range

session = requests.Session()


class InfluxDBClientError(Exception):
    """Raised when an error occurs in the request"""
    def __init__(self, content, code):
        if isinstance(content, type(b'')):
            content = content.decode('UTF-8', errors='replace')
        super(InfluxDBClientError, self).__init__(
            "{0}: {1}".format(code, content))
        self.content = content
        self.code = code


class InfluxDBClient(object):
    """
    The ``InfluxDBClient`` object holds information necessary to connect
    to InfluxDB. Requests can be made to InfluxDB directly through the client.

    :param host: hostname to connect to InfluxDB, defaults to 'localhost'
    :type host: string
    :param port: port to connect to InfluxDB, defaults to 'localhost'
    :type port: int
    :param username: user to connect, defaults to 'root'
    :type username: string
    :param password: password of the user, defaults to 'root'
    :type password: string
    :param database: database name to connect to, defaults is None
    :type database: string
    :param ssl: use https instead of http to connect to InfluxDB, defaults is
        False
    :type ssl: boolean
    :param verify_ssl: verify SSL certificates for HTTPS requests, defaults is
        False
    :type verify_ssl: boolean
    :param timeout: number of seconds Requests will wait for your client to
        establish a connection, defaults to None
    :type timeout: int
    :param use_udp: use UDP to connect to InfluxDB, defaults is False
    :type use_udp: int
    :param udp_port: UDP port to connect to InfluxDB, defaults is 4444
    :type udp_port: int
    """

    def __init__(self,
                 host='localhost',
                 port=8086,
                 username='root',
                 password='root',
                 database=None,
                 ssl=False,
                 verify_ssl=False,
                 timeout=None,
                 use_udp=False,
                 udp_port=4444):
        """
        Construct a new InfluxDBClient object.
        """
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._database = database
        self._timeout = timeout

        self._verify_ssl = verify_ssl

        self.use_udp = use_udp
        self.udp_port = udp_port
        if use_udp:
            self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        self._scheme = "http"

        if ssl is True:
            self._scheme = "https"

        self._baseurl = "{0}://{1}:{2}".format(
            self._scheme,
            self._host,
            self._port)

        self._headers = {
            'Content-type': 'application/json',
            'Accept': 'text/plain'}

    @staticmethod
    def format_query_response(response):
        """Returns a list of items from a query response"""
        series = {}
        if 'results' in response.keys():
            for result in response['results']:
                if 'series' in result.keys():
                    for row in result['series']:
                        items = []
                        if 'name' in row.keys():
                            series[row['name']] = items
                        else:
                            series = items  # Special case for system queries.
                        if 'columns' in row.keys() and 'values' in row.keys():
                            for value in row['values']:
                                item = {}
                                for cur_col, field in enumerate(value):
                                    item[row['columns'][cur_col]] = field
                                items.append(item)
        return series

    def switch_database(self, database):
        """
        switch_database()

        Change client database.

        :param database: the new database name to switch to
        :type database: string
        """
        self._database = database

    def switch_user(self, username, password):
        """
        switch_user()

        Change client username.

        :param username: the new username to switch to
        :type username: string
        :param password: the new password to switch to
        :type password: string
        """
        self._username = username
        self._password = password

    def request(self, url, method='GET', params=None, data=None,
                expected_response_code=200):
        """
        Make a http request to API
        """
        url = "{0}/{1}".format(self._baseurl, url)

        if params is None:
            params = {}

        auth = {
            'u': self._username,
            'p': self._password
        }

        params.update(auth)

        if data is not None and not isinstance(data, str):
            data = json.dumps(data)

        # Try to send the request a maximum of three times. (see #103)
        # TODO (aviau): Make this configurable.
        for i in range(0, 3):
            try:
                response = session.request(
                    method=method,
                    url=url,
                    params=params,
                    data=data,
                    headers=self._headers,
                    verify=self._verify_ssl,
                    timeout=self._timeout
                )
                break
            except requests.exceptions.ConnectionError as e:
                if i < 2:
                    continue
                else:
                    raise e

        if response.status_code == expected_response_code:
            return response
        else:
            raise InfluxDBClientError(response.content, response.status_code)

    def write(self, data, params=None, expected_response_code=200):
        """ Write to influxdb """
        self.request(
            url="write",
            method='POST',
            params=params,
            data=data,
            expected_response_code=expected_response_code
        )
        return True

    def query(self,
              query,
              params={},
              expected_response_code=200,
              database=None,
              raw=False):
        """
        Query data

        :param params: Additional parameters to be passed to requests.
        :param database: Database to query, default to None.
        :param expected_response_code: Expected response code. Defaults to 200.
        :param raw: Wether or not to return the raw influxdb response.
        """

        params['q'] = query
        params['db'] = database or self._database

        response = self.request(
            url="query",
            method='GET',
            params=params,
            data=None,
            expected_response_code=expected_response_code
        )

        if raw:
            return response.json()
        else:
            return self.format_query_response(response.json())

    def write_points(self,
                     points,
                     time_precision=None,
                     database=None,
                     retention_policy=None,
                     ):
        """
        Write to multiple time series names.

        :param points: A list of dicts.
        :param time_precision: [Optional, default None] Either 's', 'm', 'ms'
            or 'u'.
        :param database The database to write the points to. Defaults to
            the client's current db.
        :param retention_policy The retention policy for the points.
        """
        # TODO: re-implement chunks.
        return self._write_points(points=points,
                                  time_precision=time_precision,
                                  database=database,
                                  retention_policy=retention_policy)

    def _write_points(self,
                      points,
                      time_precision,
                      database,
                      retention_policy):
        if time_precision not in ['n', 'u', 'ms', 's', 'm', 'h', None]:
            raise ValueError(
                "Invalid time precision is given. "
                "(use 'n', 'u', 'ms', 's', 'm' or 'h')")

        if self.use_udp and time_precision and time_precision != 's':
            raise ValueError(
                "InfluxDB only supports seconds precision for udp writes"
            )

        data = {
            'points': points
        }

        if time_precision:
            data['precision'] = time_precision

        if retention_policy:
            data['retentionPolicy'] = retention_policy

        data['database'] = database or self._database

        if self.use_udp:
            self.send_packet(data)
        else:
            self.write(
                data=data,
                expected_response_code=200
            )

        return True

    def get_list_database(self):
        """
        Get the list of databases
        """
        rsp = self.query("SHOW DATABASES")
        return [db['name'] for db in rsp['databases']]

    def create_database(self, dbname):
        """
        Create a new database
        """
        self.query("CREATE DATABASE %s" % dbname)

    def drop_database(self, dbname):
        """
        Create a new database
        """
        self.query("DROP DATABASE %s" % dbname)

    def create_retention_policy(
            self, name, duration,
            replication, database=None, default=False):
        """
        Create a retention policy

        :param duration: The duration. Ex: '1d'
        :param replication: The replication.
        :param database: The database. Defaults to current database
        :param default: (bool) Wether or not to set the policy as default
        """

        query_string = \
            "CREATE RETENTION POLICY %s ON %s " \
            "DURATION %s REPLICATION %s" % \
            (name, database or self._database, duration, replication)

        if default is True:
            query_string += " DEFAULT"

        self.query(query_string)

    def get_list_retention_policies(self, database=None):
        """
        Get the list of retention policies
        """
        return self.query(
            "SHOW RETENTION POLICIES %s" % (database or self._database)
        )

    def get_list_series(self, database=None):
        """
        Get the list of series
        """
        return self.query("SHOW SERIES", database=database)

    def get_list_users(self):
        """
        Get the list of users
        """
        return self.query("SHOW USERS")

    def delete_series(self, name, database=None):
        database = database or self._database
        self.query('DROP SERIES \"%s\"' % name, database=database)

    def send_packet(self, packet):
        data = json.dumps(packet)
        byte = data.encode('utf-8')
        self.udp_socket.sendto(byte, (self._host, self.udp_port))
