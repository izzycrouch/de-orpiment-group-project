import pg8000.native
from extract_layer.utils.db_credentials import get_db_credentials


def connect_to_db():
    secrets = get_db_credentials()
    return pg8000.native.Connection(
        user=secrets['user'],
        password=secrets['password'],
        database=secrets['database'],
        host=secrets['host'],
        port=int(secrets['port'])
    )


def close_db_connection(conn):
    conn.close()

