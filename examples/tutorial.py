import argparse

from influxdb import InfluxDBClient


def main(host='localhost', port=8086):
    user = 'root'
    password = 'root'
    dbname = 'example'
    dbuser = 'smly'
    dbuser_password = 'my_secret_password'
    query = 'select column_one from foo;'
    json_body = [{
        "points": [
            ["1", 1, 1.0],
            ["2", 2, 2.0]
        ],
        "name": "foo",
        "columns": ["column_one", "column_two", "column_three"]
    }]

    client = InfluxDBClient(host, port, user, password, dbname)

    print("Create database: " + dbname)
    client.create_database(dbname)

    dbusers = client.get_database_users()
    print("Get list of database users: {0}".format(dbusers))

    print("Add database user: " + dbuser)
    client.add_database_user(dbuser, dbuser_password)

    print("Make user a database admin")
    client.set_database_admin(dbuser)

    print("Remove admin privilege from user")
    client.unset_database_admin(dbuser)

    dbusers = client.get_database_users()
    print("Get list of database users again: {0}".format(dbusers))

    print("Switch user: " + dbuser)
    client.switch_user(dbuser, dbuser_password)

    print("Write points: {0}".format(json_body))
    client.write_points(json_body)

    print("Querying data: " + query)
    result = client.query(query)

    print("Result: {0}".format(result))

#
    print("Remove admin privilege from user")
    client.unset_database_admin(dbuser)

    dbusers = client.get_database_users()
    print("Get list of database users again: {0}".format(dbusers))

    print("Switch user: " + dbuser)
    client.switch_user(dbuser, dbuser_password)

    print("Write points: {0}".format(json_body))
    client.write_points(json_body)

    print("Switch user: " + user)
    client.switch_user(user, password)

    print("Delete database: " + dbname)
    client.delete_database(dbname)


def parse_args():
    parser = argparse.ArgumentParser(
        description='example code to play with InfluxDB')
    parser.add_argument('--host', type=str, required=True)
    parser.add_argument('--port', type=int, required=True)
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    main(host=args.host, port=args.port)
