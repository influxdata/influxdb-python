# -*- coding: utf-8 -*-
"""
Python cluster client for InfluxDB 0.11

WARNING: only use this code with a old InfluxDB 0.11 where all node are
equivalent.
Newer cluster using influxdb-relay, node are NOT requivalent and this code
should not be used or you will end up with inconsitancy.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from functools import wraps
import time
import threading
import random

from .client import InfluxDBClient, parse_dsn
from .exceptions import InfluxDBClientError
from .exceptions import InfluxDBServerError


class InfluxDBClusterClient(object):
    """The :class:`~.InfluxDBClusterClient` is the client for connecting
    to a cluster of InfluxDB servers. Each query hits different host from the
    list of hosts.

    This works with old cluster using InfluxDB 0.11. NOT the newer cluster with
    influxdb-relay.

    :param hosts: all hosts to be included in the cluster, each of which
        should be in the format (address, port),
        e.g. [('127.0.0.1', 8086), ('127.0.0.1', 9096)]. Defaults to
        [('localhost', 8086)]
    :type hosts: list of tuples
    :param shuffle: whether the queries should hit servers evenly(randomly),
        defaults to True
    :type shuffle: bool
    :param client_base_class: the base class for the cluster client.
        This parameter is used to enable the support of different client
        types. Defaults to :class:`~.InfluxDBClient`
    :param healing_delay: the delay in seconds, counting from last failure of
        a server, before re-adding server to the list of working servers.
        Defaults to 15 minutes (900 seconds)
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
                 healing_delay=900,
                 ):
        self.clients = [self]  # Keep it backwards compatible
        self.hosts = hosts
        self.bad_hosts = []   # Corresponding server has failures in history
        self.shuffle = shuffle
        self.healing_delay = healing_delay
        self._last_healing = time.time()
        host, port = self.hosts[0]
        self._hosts_lock = threading.Lock()
        self._thread_local = threading.local()
        self._client = client_base_class(host=host,
                                         port=port,
                                         username=username,
                                         password=password,
                                         database=database,
                                         ssl=ssl,
                                         verify_ssl=verify_ssl,
                                         timeout=timeout,
                                         use_udp=use_udp,
                                         udp_port=udp_port)
        for method in dir(client_base_class):
            orig_attr = getattr(client_base_class, method, '')
            if method.startswith('_') or not callable(orig_attr):
                continue

            setattr(self, method, self._make_func(orig_attr))

        self._client._get_host = self._get_host
        self._client._get_port = self._get_port
        self._client._get_baseurl = self._get_baseurl
        self._update_client_host(self.hosts[0])

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
            >> cluster.hosts
            [('host1', 8086), ('host2', 8086)]
            >> cluster._client
             <influxdb.client.InfluxDBClient at 0x7feb438ec950>]
        """
        init_args = parse_dsn(dsn)
        init_args.update(**kwargs)
        init_args['shuffle'] = shuffle
        init_args['client_base_class'] = client_base_class
        cluster_client = InfluxDBClusterClient(**init_args)
        return cluster_client

    def _update_client_host(self, host):
        self._thread_local.host, self._thread_local.port = host
        self._thread_local.baseurl = "{0}://{1}:{2}".format(
            self._client._scheme,
            self._client._host,
            self._client._port
        )

    def _get_baseurl(self):
        return self._thread_local.baseurl

    def _get_host(self):
        return self._thread_local.host

    def _get_port(self):
        return self._thread_local.port

    def _make_func(self, orig_func):

        @wraps(orig_func)
        def func(*args, **kwargs):
            now = time.time()
            with self._hosts_lock:
                if (self.bad_hosts and
                        self._last_healing + self.healing_delay < now):
                    h = self.bad_hosts.pop(0)
                    self.hosts.append(h)
                    self._last_healing = now

                if self.shuffle:
                    random.shuffle(self.hosts)

                hosts = self.hosts + self.bad_hosts

            for h in hosts:
                bad_host = False
                try:
                    self._update_client_host(h)
                    return orig_func(self._client, *args, **kwargs)
                except InfluxDBClientError as e:
                    # Errors caused by user's requests, re-raise
                    raise e
                except ValueError as e:
                    raise e
                except Exception as e:
                    # Errors that might caused by server failure, try another
                    bad_host = True
                    with self._hosts_lock:
                        if h in self.hosts:
                            self.hosts.remove(h)
                            self.bad_hosts.append(h)
                        self._last_healing = now
                finally:
                    with self._hosts_lock:
                        if not bad_host and h in self.bad_hosts:
                            self.bad_hosts.remove(h)
                            self.hosts.append(h)

            raise InfluxDBServerError("InfluxDB: no viable server!")

        return func
