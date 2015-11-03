# -*- coding: utf-8 -*-
"""
Python client for InfluxDB
"""

from functools import wraps
import json
import socket
import random
import requests
import requests.exceptions
from sys import version_info

from influxdb.line_protocol import make_lines
from influxdb.resultset import ResultSet
from .exceptions import InfluxDBClientError
from .exceptions import InfluxDBServerError

try:
    xrange
except NameError:
    xrange = range

if version_info[0] == 3:
    from urllib.parse import urlparse
else:
    from urlparse import urlparse


class InfluxDBClient(object):
    """The :class:`~.InfluxDBClient` object holds information necessary to
    connect to InfluxDB. Requests can be made to InfluxDB directly through
    the client.

    :param host: hostname to connect to InfluxDB, defaults to 'localhost'
    :type host: str
    :param port: port to connect to InfluxDB, defaults to 8086
    :type port: int
    :param username: user to connect, defaults to 'root'
    :type username: str
    :param password: password of the user, defaults to 'root'
    :type password: str
    :param database: database name to connect to, defaults to None
    :type database: str
    :param ssl: use https instead of http to connect to InfluxDB, defaults to
        False
    :type ssl: bool
    :param verify_ssl: verify SSL certificates for HTTPS requests, defaults to
        False
    :type verify_ssl: bool
    :param timeout: number of seconds Requests will wait for your client to
        establish a connection, defaults to None
    :type timeout: int
    :param use_udp: use UDP to connect to InfluxDB, defaults to False
    :type use_udp: int
    :param udp_port: UDP port to connect to InfluxDB, defaults to 4444
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
                 udp_port=4444,
                 ):
        """Construct a new InfluxDBClient object."""
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._database = database
        self._timeout = timeout

        self._verify_ssl = verify_ssl

        self.use_udp = use_udp
        self.udp_port = udp_port
        self._session = requests.Session()
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
            'Accept': 'text/plain'
        }

    @staticmethod
    def from_DSN(dsn, **kwargs):
        """Return an instance of :class:`~.InfluxDBClient` from the provided
        data source name. Supported schemes are "influxdb", "https+influxdb"
        and "udp+influxdb". Parameters for the :class:`~.InfluxDBClient`
        constructor may also be passed to this method.

        :param dsn: data source name
        :type dsn: string
        :param kwargs: additional parameters for `InfluxDBClient`
        :type kwargs: dict
        :raises ValueError: if the provided DSN has any unexpected values

        :Example:

        ::

            >> cli = InfluxDBClient.from_DSN('influxdb://username:password@\
localhost:8086/databasename', timeout=5)
            >> type(cli)
            <class 'influxdb.client.InfluxDBClient'>
            >> cli = InfluxDBClient.from_DSN('udp+influxdb://username:pass@\
localhost:8086/databasename', timeout=5, udp_port=159)
            >> print('{0._baseurl} - {0.use_udp} {0.udp_port}'.format(cli))
            http://localhost:8086 - True 159

        .. note:: parameters provided in `**kwargs` may override dsn parameters
        .. note:: when using "udp+influxdb" the specified port (if any) will
            be used for the TCP connection; specify the UDP port with the
            additional `udp_port` parameter (cf. examples).
        """

        init_args = {}
        conn_params = urlparse(dsn)
        scheme_info = conn_params.scheme.split('+')
        if len(scheme_info) == 1:
            scheme = scheme_info[0]
            modifier = None
        else:
            modifier, scheme = scheme_info

        if scheme != 'influxdb':
            raise ValueError('Unknown scheme "{}".'.format(scheme))
        if modifier:
            if modifier == 'udp':
                init_args['use_udp'] = True
            elif modifier == 'https':
                init_args['ssl'] = True
            else:
                raise ValueError('Unknown modifier "{}".'.format(modifier))

        if conn_params.hostname:
            init_args['host'] = conn_params.hostname
        if conn_params.port:
            init_args['port'] = conn_params.port
        if conn_params.username:
            init_args['username'] = conn_params.username
        if conn_params.password:
            init_args['password'] = conn_params.password
        if conn_params.path and len(conn_params.path) > 1:
            init_args['database'] = conn_params.path[1:]

        init_args.update(kwargs)

        return InfluxDBClient(**init_args)

    def switch_database(self, database):
        """Change the client's database.

        :param database: the name of the database to switch to
        :type database: str
        """
        self._database = database

    def switch_user(self, username, password):
        """Change the client's username.

        :param username: the username to switch to
        :type username: str
        :param password: the password for the username
        :type password: str
        """
        self._username = username
        self._password = password

    def request(self, url, method='GET', params=None, data=None,
                expected_response_code=200, headers=None):
        """Make a HTTP request to the InfluxDB API.

        :param url: the path of the HTTP request, e.g. write, query, etc.
        :type url: str
        :param method: the HTTP method for the request, defaults to GET
        :type method: str
        :param params: additional parameters for the request, defaults to None
        :type params: dict
        :param data: the data of the request, defaults to None
        :type data: str
        :param expected_response_code: the expected response code of
            the request, defaults to 200
        :type expected_response_code: int
        :returns: the response from the request
        :rtype: :class:`requests.Response`
        :raises InfluxDBServerError: if the response code is any server error
            code (5xx)
        :raises InfluxDBClientError: if the response code is not the
            same as `expected_response_code` and is not a server error code
        """
        url = "{0}/{1}".format(self._baseurl, url)

        if headers is None:
            headers = self._headers

        if params is None:
            params = {}

        if isinstance(data, (dict, list)):
            data = json.dumps(data)

        # Try to send the request a maximum of three times. (see #103)
        # TODO (aviau): Make this configurable.
        for i in range(0, 3):
            try:
                response = self._session.request(
                    method=method,
                    url=url,
                    auth=(self._username, self._password),
                    params=params,
                    data=data,
                    headers=headers,
                    verify=self._verify_ssl,
                    timeout=self._timeout
                )
                break
            except requests.exceptions.ConnectionError as e:
                if i < 2:
                    continue
                else:
                    raise e

        if response.status_code >= 500 and response.status_code < 600:
            raise InfluxDBServerError(response.content)
        elif response.status_code == expected_response_code:
            return response
        else:
            raise InfluxDBClientError(response.content, response.status_code)

    def write(self, data, params=None, expected_response_code=204):
        """Write data to InfluxDB.

        :param data: the data to be written
        :type data: dict
        :param params: additional parameters for the request, defaults to None
        :type params: dict
        :param expected_response_code: the expected response code of the write
            operation, defaults to 204
        :type expected_response_code: int
        :returns: True, if the write operation is successful
        :rtype: bool
        """

        headers = self._headers
        headers['Content-type'] = 'application/octet-stream'

        if params:
            precision = params.get('precision')
        else:
            precision = None

        self.request(
            url="write",
            method='POST',
            params=params,
            data=make_lines(data, precision).encode('utf-8'),
            expected_response_code=expected_response_code,
            headers=headers
        )
        return True

    def query(self,
              query,
              params={},
              epoch=None,
              expected_response_code=200,
              database=None,
              raise_errors=True):
        """Send a query to InfluxDB.

        :param query: the actual query string
        :type query: str

        :param params: additional parameters for the request, defaults to {}
        :type params: dict

        :param expected_response_code: the expected status code of response,
            defaults to 200
        :type expected_response_code: int

        :param database: database to query, defaults to None
        :type database: str

        :param raise_errors: Whether or not to raise exceptions when InfluxDB
            returns errors, defaults to True
        :type raise_errors: bool

        :returns: the queried data
        :rtype: :class:`~.ResultSet`
        """
        params['q'] = query
        params['db'] = database or self._database

        if epoch is not None:
            params['epoch'] = epoch

        response = self.request(
            url="query",
            method='GET',
            params=params,
            data=None,
            expected_response_code=expected_response_code
        )

        data = response.json()

        results = [
            ResultSet(result, raise_errors=raise_errors)
            for result
            in data.get('results', [])
        ]

        # TODO(aviau): Always return a list. (This would be a breaking change)
        if len(results) == 1:
            return results[0]
        else:
            return results

    def write_points(self,
                     points,
                     time_precision=None,
                     database=None,
                     retention_policy=None,
                     tags=None,
                     batch_size=None,
                     ):
        """Write to multiple time series names.

        :param points: the list of points to be written in the database
        :type points: list of dictionaries, each dictionary represents a point
        :param time_precision: Either 's', 'm', 'ms' or 'u', defaults to None
        :type time_precision: str
        :param database: the database to write the points to. Defaults to
            the client's current database
        :type database: str
        :param tags: a set of key-value pairs associated with each point. Both
            keys and values must be strings. These are shared tags and will be
            merged with point-specific tags, defaults to None
        :type tags: dict
        :param retention_policy: the retention policy for the points. Defaults
            to None
        :type retention_policy: str
        :param batch_size: value to write the points in batches
            instead of all at one time. Useful for when doing data dumps from
            one database to another or when doing a massive write operation,
            defaults to None
        :type batch_size: int
        :returns: True, if the operation is successful
        :rtype: bool

        .. note:: if no retention policy is specified, the default retention
            policy for the database is used
        """

        if batch_size and batch_size > 0:
            for batch in self._batches(points, batch_size):
                self._write_points(points=batch,
                                   time_precision=time_precision,
                                   database=database,
                                   retention_policy=retention_policy,
                                   tags=tags)
            return True
        else:
            return self._write_points(points=points,
                                      time_precision=time_precision,
                                      database=database,
                                      retention_policy=retention_policy,
                                      tags=tags)

    def _batches(self, iterable, size):
        for i in xrange(0, len(iterable), size):
            yield iterable[i:i + size]

    def _write_points(self,
                      points,
                      time_precision,
                      database,
                      retention_policy,
                      tags):
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

        if tags is not None:
            data['tags'] = tags

        params = {
            'db': database or self._database
        }

        if time_precision is not None:
            params['precision'] = time_precision

        if retention_policy is not None:
            params['rp'] = retention_policy

        if self.use_udp:
            self.send_packet(data)
        else:
            self.write(
                data=data,
                params=params,
                expected_response_code=204
            )

        return True

    def get_list_database(self):
        """Get the list of databases in InfluxDB.

        :returns: all databases in InfluxDB
        :rtype: list of dictionaries

        :Example:

        ::

            >> dbs = client.get_list_database()
            >> dbs
            [{u'name': u'db1'}, {u'name': u'db2'}, {u'name': u'db3'}]
        """
        return list(self.query("SHOW DATABASES").get_points())

    def create_database(self, dbname):
        """Create a new database in InfluxDB.

        :param dbname: the name of the database to create
        :type dbname: str
        """
        self.query("CREATE DATABASE %s" % dbname)

    def drop_database(self, dbname):
        """Drop a database from InfluxDB.

        :param dbname: the name of the database to drop
        :type dbname: str
        """
        self.query("DROP DATABASE %s" % dbname)

    def create_retention_policy(self, name, duration, replication,
                                database=None, default=False):
        """Create a retention policy for a database.

        :param name: the name of the new retention policy
        :type name: str
        :param duration: the duration of the new retention policy.
            Durations such as 1h, 90m, 12h, 7d, and 4w, are all supported
            and mean 1 hour, 90 minutes, 12 hours, 7 day, and 4 weeks,
            respectively. For infinite retention – meaning the data will
            never be deleted – use 'INF' for duration.
            The minimum retention period is 1 hour.
        :type duration: str
        :param replication: the replication of the retention policy
        :type replication: str
        :param database: the database for which the retention policy is
            created. Defaults to current client's database
        :type database: str
        :param default: whether or not to set the policy as default
        :type default: bool
        """
        query_string = \
            "CREATE RETENTION POLICY %s ON %s " \
            "DURATION %s REPLICATION %s" % \
            (name, database or self._database, duration, replication)

        if default is True:
            query_string += " DEFAULT"

        self.query(query_string)

    def alter_retention_policy(self, name, database=None,
                               duration=None, replication=None, default=None):
        """Mofidy an existing retention policy for a database.

        :param name: the name of the retention policy to modify
        :type name: str
        :param database: the database for which the retention policy is
            modified. Defaults to current client's database
        :type database: str
        :param duration: the new duration of the existing retention policy.
            Durations such as 1h, 90m, 12h, 7d, and 4w, are all supported
            and mean 1 hour, 90 minutes, 12 hours, 7 day, and 4 weeks,
            respectively. For infinite retention – meaning the data will
            never be deleted – use 'INF' for duration.
            The minimum retention period is 1 hour.
        :type duration: str
        :param replication: the new replication of the existing
            retention policy
        :type replication: str
        :param default: whether or not to set the modified policy as default
        :type default: bool

        .. note:: at least one of duration, replication, or default flag
            should be set. Otherwise the operation will fail.
        """
        query_string = (
            "ALTER RETENTION POLICY {} ON {}"
        ).format(name, database or self._database)
        if duration:
            query_string += " DURATION {}".format(duration)
        if replication:
            query_string += " REPLICATION {}".format(replication)
        if default is True:
            query_string += " DEFAULT"

        self.query(query_string)

    def get_list_retention_policies(self, database=None):
        """Get the list of retention policies for a database.

        :param database: the name of the database, defaults to the client's
            current database
        :type database: str
        :returns: all retention policies for the database
        :rtype: list of dictionaries

        :Example:

        ::

            >> ret_policies = client.get_list_retention_policies('my_db')
            >> ret_policies
            [{u'default': True,
              u'duration': u'0',
              u'name': u'default',
              u'replicaN': 1}]
            """
        rsp = self.query(
            "SHOW RETENTION POLICIES ON %s" % (database or self._database)
        )
        return list(rsp.get_points())

    def get_list_series(self, database=None):
        """Get the list of series for a database.

        :param database: the name of the database, defaults to the client's
            current database
        :type database: str
        :returns: all series in the specified database
        :rtype: list of dictionaries

        :Example:

        >> series = client.get_list_series('my_database')
        >> series
        [{'name': u'cpu_usage',
          'tags': [{u'_id': 1,
                    u'host': u'server01',
                    u'region': u'us-west'}]}]
        """
        rsp = self.query("SHOW SERIES", database=database)
        series = []
        for serie in rsp.items():
            series.append(
                {
                    "name": serie[0][0],
                    "tags": list(serie[1])
                }
            )
        return series

    def get_list_users(self):
        """Get the list of all users in InfluxDB.

        :returns: all users in InfluxDB
        :rtype: list of dictionaries

        :Example:

        ::

            >> users = client.get_list_users()
            >> users
            [{u'admin': True, u'user': u'user1'},
             {u'admin': False, u'user': u'user2'},
             {u'admin': False, u'user': u'user3'}]
        """
        return list(self.query("SHOW USERS").get_points())

    def create_user(self, username, password, admin=False):
        """Create a new user in InfluxDB

        :param username: the new username to create
        :type username: str
        :param password: the password for the new user
        :type password: str
        :param admin: whether the user should have cluster administration
            privileges or not
        :type admin: boolean
        """
        text = "CREATE USER {} WITH PASSWORD '{}'".format(username, password)
        if admin:
            text += ' WITH ALL PRIVILEGES'
        self.query(text)

    def drop_user(self, username):
        """Drop an user from InfluxDB.

        :param username: the username to drop
        :type username: str
        """
        text = "DROP USER {}".format(username)
        self.query(text)

    def set_user_password(self, username, password):
        """Change the password of an existing user.

        :param username: the username who's password is being changed
        :type username: str
        :param password: the new password for the user
        :type password: str
        """
        text = "SET PASSWORD FOR {} = '{}'".format(username, password)
        self.query(text)

    def delete_series(self, database=None, measurement=None, tags=None):
        """Delete series from a database. Series can be filtered by
        measurement and tags.

        :param measurement: Delete all series from a measurement
        :type id: string
        :param tags: Delete all series that match given tags
        :type id: dict
        :param database: the database from which the series should be
            deleted, defaults to client's current database
        :type database: str
        """
        database = database or self._database
        query_str = 'DROP SERIES'
        if measurement:
            query_str += ' FROM "{}"'.format(measurement)

        if tags:
            query_str += ' WHERE ' + ' and '.join(["{}='{}'".format(k, v)
                                                   for k, v in tags.items()])
        self.query(query_str, database=database)

    def revoke_admin_privileges(self, username):
        """Revoke cluster administration privileges from an user.

        :param username: the username to revoke privileges from
        :type username: str

        .. note:: Only a cluster administrator can create/ drop databases
            and manage users.
        """
        text = "REVOKE ALL PRIVILEGES FROM {}".format(username)
        self.query(text)

    def grant_privilege(self, privilege, database, username):
        """Grant a privilege on a database to an user.

        :param privilege: the privilege to grant, one of 'read', 'write'
            or 'all'. The string is case-insensitive
        :type privilege: str
        :param database: the database to grant the privilege on
        :type database: str
        :param username: the username to grant the privilege to
        :type username: str
        """
        text = "GRANT {} ON {} TO {}".format(privilege,
                                             database,
                                             username)
        self.query(text)

    def revoke_privilege(self, privilege, database, username):
        """Revoke a privilege on a database from an user.

        :param privilege: the privilege to revoke, one of 'read', 'write'
            or 'all'. The string is case-insensitive
        :type privilege: str
        :param database: the database to revoke the privilege on
        :type database: str
        :param username: the username to revoke the privilege from
        :type username: str
        """
        text = "REVOKE {} ON {} FROM {}".format(privilege,
                                                database,
                                                username)
        self.query(text)

    def send_packet(self, packet):
        """Send an UDP packet.

        :param packet: the packet to be sent
        :type packet: dict
        """
        data = make_lines(packet).encode('utf-8')
        self.udp_socket.sendto(data, (self._host, self.udp_port))


class InfluxDBClusterClient(object):
    """The :class:`~.InfluxDBClusterClient` is the client for connecting
    to a cluster of InfluxDB servers. It's basically a proxy to multiple
    InfluxDBClients.

    :param hosts: all hosts to be included in the cluster, each of which
        should be in the format (address, port),
        e.g. [('127.0.0.1', 8086), ('127.0.0.1', 9096)]. Defaults to
        [('localhost', 8086)]
    :type hosts: list of tuples
    :param shuffle: whether the queries should hit servers evenly(randomly),
        defaults to True
    :type shuffle: bool
    :param client_base_class: the base class for all clients in the cluster.
        This parameter is used to enable the support of different client
        types. Defaults to :class:`~.InfluxDBClient`
    """

    def __init__(self,
                 hosts=[('localhost', 8086)],
                 username='root',
                 password='root',
                 database=None,
                 ssl=False,
                 verify_ssl=False,
                 timeout=None,
                 use_udp=False,
                 udp_port=4444,
                 shuffle=True,
                 client_base_class=InfluxDBClient,
                 ):
        self.clients = []
        self.bad_clients = []   # Corresponding server has failures in history
        self.shuffle = shuffle
        for h in hosts:
            self.clients.append(client_base_class(host=h[0], port=h[1],
                                                  username=username,
                                                  password=password,
                                                  database=database,
                                                  ssl=ssl,
                                                  verify_ssl=verify_ssl,
                                                  timeout=timeout,
                                                  use_udp=use_udp,
                                                  udp_port=udp_port))
        for method in dir(client_base_class):
            if method.startswith('_') or method in ('switch_database',
                                                    'switch_user'):
                continue

            orig_func = getattr(client_base_class, method)
            if not callable(orig_func):
                continue

            setattr(self, method, self._make_func(orig_func))

    def switch_database(self, database):
        """Change the database of all clients in the cluster.

        :param database: the name of the database to switch to
        :type database: str
        """
        for client in self.clients + self.bad_clients:
            client.switch_database(database)

    def switch_user(self, username, password):
        """Change the username of all clients in the cluster.

        :param username: the username to switch to
        :type username: str
        :param password: the password for the username
        :type password: str
        """
        for client in self.clients + self.bad_clients:
            client.switch_user(username, password)

    @staticmethod
    def from_DSN(dsn, client_base_class=InfluxDBClient,
                 shuffle=True, **kwargs):
        """Same as :meth:`~.InfluxDBClient.from_DSN`, but supports
        multiple servers.

        :param shuffle: whether the queries should hit servers
            evenly(randomly), defaults to True
        :type shuffle: bool
        :param client_base_class: the base class for all clients in the
            cluster. This parameter is used to enable the support of
            different client types. Defaults to :class:`~.InfluxDBClient`

        :Example:

        ::

            >> cluster = InfluxDBClusterClient.from_DSN('influxdb://usr:pwd\
@host1:8086,usr:pwd@host2:8086/db_name', timeout=5)
            >> type(cluster)
            <class 'influxdb.client.InfluxDBClusterClient'>
            >> cluster.clients
            [<influxdb.client.InfluxDBClient at 0x7feb480295d0>,
             <influxdb.client.InfluxDBClient at 0x7feb438ec950>]
        """
        conn_params = urlparse(dsn)
        netlocs = conn_params.netloc.split(',')
        cluster_client = InfluxDBClusterClient(
            hosts=[],
            client_base_class=client_base_class,
            shuffle=shuffle,
            **kwargs)
        for netloc in netlocs:
            single_dsn = '%(scheme)s://%(netloc)s%(path)s' % (
                {'scheme': conn_params.scheme,
                 'netloc': netloc,
                 'path': conn_params.path}
            )
            cluster_client.clients.append(client_base_class.from_DSN(
                single_dsn,
                **kwargs))
        return cluster_client

    def _make_func(self, orig_func):

        @wraps(orig_func)
        def func(*args, **kwargs):
            if self.shuffle:
                random.shuffle(self.clients)
            clients = self.clients + self.bad_clients
            for c in clients:
                bad_client = False
                try:
                    return orig_func(c, *args, **kwargs)
                except InfluxDBClientError as e:
                    # Errors caused by user's requests, re-raise
                    raise e
                except Exception as e:
                    # Errors that might caused by server failure, try another
                    bad_client = True
                    if c in self.clients:
                        self.clients.remove(c)
                        self.bad_clients.append(c)
                finally:
                    if not bad_client and c in self.bad_clients:
                        self.bad_clients.remove(c)
                        self.clients.append(c)

            raise InfluxDBServerError("InfluxDB: no viable server!")

        return func
