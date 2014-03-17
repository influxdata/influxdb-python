# -*- coding: utf-8 -*-
"""
python client for influxdb
"""
import json

import requests
session = requests.Session()


class InfluxDBClient(object):
    """
    InfluxDB Client
    """

    def __init__(self, host='localhost', port=8086, username='root',
                 password='root', database=None):
        """
        Initialize client
        """
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._database = database
        self._baseurl = "http://{0}:{1}".format(self._host, self._port)

        self._headers = {
            'Content-type': 'application/json',
            'Accept': 'text/plain'}

    # Change member variables

    def switch_db(self, database):
        """
        Change client database

        Parameters
        ----------
        database : string
        """
        self._database = database

    def switch_user(self, username, password):
        """
        Change client username

        Parameters
        ----------
        username : string
        password : string
        """
        self._username = username
        self._password = password

    # Writing Data
    #
    # Assuming you have a database named foo_production you can write data
    # by doing a POST to /db/foo_production/series?u=some_user&p=some_password
    # with a JSON body of points.

    def write_points(self, *args, **kwargs):
        """
        Write to multiple time series names
        """
        return self.write_points_with_precision(*args, **kwargs)

    def write_points_with_precision(self, data, time_precision='s'):
        """
        Write to multiple time series names
        """
        if time_precision not in ['s', 'm', 'u']:
            raise Exception(
                "Invalid time precision is given. (use 's','m' or 'u')")

        url_format = "{0}/db/{1}/series?u={2}&p={3}&time_precision={4}"

        response = session.post(url_format.format(
            self._baseurl,
            self._database,
            self._username,
            self._password,
            time_precision),
            data=json.dumps(data),
            headers=self._headers)

        if response.status_code == 200:
            return True
        else:
            raise Exception(
                "{0}: {1}".format(response.status_code, response.content))

    # One Time Deletes

    def delete_points(self, name,
                      regex=None, start_epoch=None, end_epoch=None):
        """
        TODO: Delete a range of data

        2013-11-08: This endpoint has not been implemented yet in ver0.0.8,
        but it is documented in http://influxdb.org/docs/api/http.html.
        See also: src/api/http/api.go:l57
        """
        raise NotImplementedError()

    # Regularly Scheduled Deletes

    def create_scheduled_delete(self, json_body):
        """
        TODO: Create scheduled delete

        2013-11-08: This endpoint has not been implemented yet in ver0.0.8,
        but it is documented in http://influxdb.org/docs/api/http.html.
        See also: src/api/http/api.go:l57
        """
        raise NotImplementedError()

    # get list of deletes
    # curl http://localhost:8086/db/site_dev/scheduled_deletes
    #
    # remove a regularly scheduled delete
    # curl -X DELETE http://localhost:8086/db/site_dev/scheduled_deletes/:id

    def get_list_scheduled_delete(self):
        """
        TODO: Get list of scheduled deletes

        2013-11-08: This endpoint has not been implemented yet in ver0.0.8,
        but it is documented in http://influxdb.org/docs/api/http.html.
        See also: src/api/http/api.go:l57
        """
        raise NotImplementedError()

    def remove_scheduled_delete(self, delete_id):
        """
        TODO: Remove scheduled delete

        2013-11-08: This endpoint has not been implemented yet in ver0.0.8,
        but it is documented in http://influxdb.org/docs/api/http.html.
        See also: src/api/http/api.go:l57
        """
        raise NotImplementedError()

    # Querying Data
    #
    # GET db/:name/series. It takes five parameters
    def query(self, query, time_precision='s', chunked=False):
        """
        Quering data
        """
        if time_precision not in ['s', 'm', 'u']:
            raise Exception(
                "Invalid time precision is given. (use 's','m' or 'u')")

        if chunked is True:
            chunked_param = 'true'
        else:
            chunked_param = 'false'

        url = "{0}/db/{1}/series"

        url.format(
            self._baseurl,
            self._database
        )

        params = {
            'u': self._username,
            'p': self._password,
            'q': query,
            'time_precision': time_precision,
            'chunked': chunked_param
        }

        response = session.get(url, params=params)

        if response.status_code == 200:
            return json.loads(response.content)
        else:
            raise Exception(
                "{0}: {1}".format(response.status_code, response.content))

    # Creating and Dropping Databases
    #
    # ### create a database
    # curl -X POST http://localhost:8086/db -d '{"name": "site_development"}'
    #
    # ### drop a database
    # curl -X DELETE http://localhost:8086/db/site_development

    def create_database(self, database):
        """
        Create a database

        Parameters
        ----------
        database: string
            database name
        """
        response = session.post("{0}/db?u={1}&p={2}".format(
            self._baseurl,
            self._username,
            self._password),
            data=json.dumps({'name': database}),
            headers=self._headers)

        if response.status_code == 201:
            return True
        else:
            raise Exception(
                "{0}: {1}".format(response.status_code, response.content))

    def delete_database(self, database):
        """
        Drop a database

        Parameters
        ----------
        database: string
            database name
        """
        response = session.delete("{0}/db/{1}?u={2}&p={3}".format(
            self._baseurl,
            database,
            self._username,
            self._password))

        if response.status_code == 204:
            return True
        else:
            raise Exception(
                "{0}: {1}".format(response.status_code, response.content))

    # ### get list of databases
    # curl -X GET http://localhost:8086/db

    def get_database_list(self):
        """
        Get the list of databases
        """
        response = session.get("{0}/db?u={1}&p={2}".format(
            self._baseurl,
            self._username,
            self._password))

        if response.status_code == 200:
            return json.loads(response.content)
        else:
            raise Exception(
                "{0}: {1}".format(response.status_code, response.content))

    def delete_series(self, series):
        """
        Drop a series

        Parameters
        ----------
        series: string
            series name
        """
        response = session.delete("{0}/db/{1}/series/{2}?u={3}&p={4}".format(
            self._baseurl,
            self._database,
            series,
            self._username,
            self._password))

        if response.status_code == 204:
            return True
        else:
            raise Exception(
                "{0}: {1}".format(response.status_code, response.content))

    # Security
    # get list of cluster admins
    # curl http://localhost:8086/cluster_admins?u=root&p=root

    # add cluster admin
    # curl -X POST http://localhost:8086/cluster_admins?u=root&p=root \
    #      -d '{"name": "paul", "password": "i write teh docz"}'

    # update cluster admin password
    # curl -X POST http://localhost:8086/cluster_admins/paul?u=root&p=root \
    #      -d '{"password": "new pass"}'

    # delete cluster admin
    # curl -X DELETE http://localhost:8086/cluster_admins/paul?u=root&p=root

    # Database admins, with a database name of site_dev
    # get list of database admins
    # curl http://localhost:8086/db/site_dev/admins?u=root&p=root

    # add database admin
    # curl -X POST http://localhost:8086/db/site_dev/admins?u=root&p=root \
    #      -d '{"name": "paul", "password": "i write teh docz"}'

    # update database admin password
    # curl -X POST http://localhost:8086/db/site_dev/admins/paul?u=root&p=root\
    #      -d '{"password": "new pass"}'

    # delete database admin
    # curl -X DELETE \
    #        http://localhost:8086/db/site_dev/admins/paul?u=root&p=root

    def get_list_cluster_admins(self):
        """
        Get list of cluster admins
        """
        response = session.get(
            "{0}/cluster_admins?u={1}&p={2}".format(
                self._baseurl,
                self._username,
                self._password))

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(
                "{0}: {1}".format(response.status_code, response.content))

    def add_cluster_admin(self, new_username, new_password):
        """
        Add cluster admin
        """
        response = session.post(
            "{0}/cluster_admins?u={1}&p={2}".format(
                self._baseurl,
                self._username,
                self._password),
            data=json.dumps({
                'name': new_username,
                'password': new_password}),
            headers=self._headers)

        if response.status_code == 200:
            return True
        else:
            raise Exception(
                "{0}: {1}".format(response.status_code, response.content))

    def update_cluster_admin_password(self, username, new_password):
        """
        Update cluster admin password
        """
        response = session.post(
            "{0}/cluster_admins/{1}?u={2}&p={3}".format(
                self._baseurl,
                username,
                self._username,
                self._password),
            data=json.dumps({
                'password': new_password}),
            headers=self._headers)

        if response.status_code == 200:
            return True
        else:
            raise Exception(
                "{0}: {1}".format(response.status_code, response.content))

    def delete_cluster_admin(self, username):
        """
        Delete cluster admin
        """
        response = session.delete("{0}/cluster_admins/{1}?u={2}&p={3}".format(
            self._baseurl,
            username,
            self._username,
            self._password))

        if response.status_code == 204:
            return True
        else:
            raise Exception(
                "{0}: {1}".format(response.status_code, response.content))

    def set_database_admin(self, username):
        """
        Set user as database admin
        """
        return self.alter_database_admin(username, True)

    def unset_database_admin(self, username):
        """
        Unset user as database admin
        """
        return self.alter_database_admin(username, False)

    def alter_database_admin(self, username, is_admin):
        response = session.post(
            "{0}/db/{1}/users/{2}?u={3}&p={4}".format(
                self._baseurl,
                self._database,
                username,
                self._username,
                self._password),
            data=json.dumps({'admin': is_admin}),
            headers=self._headers)
        if response.status_code == 200:
            return True
        else:
            raise Exception(
                "{0}: {1}".format(response.status_code, response.content))

    def get_list_database_admins(self):
        """
        TODO: Get list of database admins

        2013-11-08: This endpoint has not been implemented yet in ver0.0.8,
        but it is documented in http://influxdb.org/docs/api/http.html.
        See also: src/api/http/api.go:l57
        """
        raise NotImplementedError()

    def add_database_admin(self, new_username, new_password):
        """
        TODO: Add cluster admin

        2013-11-08: This endpoint has not been implemented yet in ver0.0.8,
        but it is documented in http://influxdb.org/docs/api/http.html.
        See also: src/api/http/api.go:l57
        """
        raise NotImplementedError()

    def update_database_admin_password(self, username, new_password):
        """
        TODO: Update database admin password

        2013-11-08: This endpoint has not been implemented yet in ver0.0.8,
        but it is documented in http://influxdb.org/docs/api/http.html.
        See also: src/api/http/api.go:l57
        """
        raise NotImplementedError()

    def delete_database_admin(self, username):
        """
        TODO: Delete database admin

        2013-11-08: This endpoint has not been implemented yet in ver0.0.8,
        but it is documented in http://influxdb.org/docs/api/http.html.
        See also: src/api/http/api.go:l57
        """
        raise NotImplementedError()

    ###
    # Limiting User Access

    # Database users
    # get list of database users
    # curl http://localhost:8086/db/site_dev/users?u=root&p=root

    # add database user
    # curl -X POST http://localhost:8086/db/site_dev/users?u=root&p=root \
    #       -d '{"name": "paul", "password": "i write teh docz"}'

    # update database user password
    # curl -X POST http://localhost:8086/db/site_dev/users/paul?u=root&p=root \
    #       -d '{"password": "new pass"}'

    # delete database user
    # curl -X DELETE http://localhost:8086/db/site_dev/users/paul?u=root&p=root

    def get_database_users(self):
        """
        Get list of database users
        """
        response = session.get(
            "{0}/db/{1}/users?u={2}&p={3}".format(
                self._baseurl,
                self._database,
                self._username,
                self._password))

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(
                "{0}: {1}".format(response.status_code, response.content))

    def add_database_user(self, new_username, new_password):
        """
        Add database user
        """
        response = session.post(
            "{0}/db/{1}/users?u={2}&p={3}".format(
                self._baseurl,
                self._database,
                self._username,
                self._password),
            data=json.dumps({
                'name': new_username,
                'password': new_password}),
            headers=self._headers)

        if response.status_code == 200:
            return True
        else:
            raise Exception(
                "{0}: {1}".format(response.status_code, response.content))

    def update_database_user_password(self, username, new_password):
        """
        Update password
        """
        response = session.post(
            "{0}/db/{1}/users/{2}?u={3}&p={4}".format(
                self._baseurl,
                self._database,
                username,
                self._username,
                self._password),
            data=json.dumps({
                'password': new_password}),
            headers=self._headers)

        if response.status_code == 200:
            if username == self._username:
                self._password = new_password
            return True
        else:
            raise Exception(
                "{0}: {1}".format(response.status_code, response.content))

    def delete_database_user(self, username):
        """
        Delete database user
        """
        response = session.delete(
            "{0}/db/{1}/users/{2}?u={3}&p={4}".format(
                self._baseurl,
                self._database,
                username,
                self._username,
                self._password))

        if response.status_code == 200:
            return True
        else:
            raise Exception(
                "{0}: {1}".format(response.status_code, response.content))

    # update the user by POSTing to db/site_dev/users/paul

    def update_permission(self, username, json_body):
        """
        TODO: Update read/write permission

        2013-11-08: This endpoint has not been implemented yet in ver0.0.8,
        but it is documented in http://influxdb.org/docs/api/http.html.
        See also: src/api/http/api.go:l57
        """
        raise NotImplementedError()
