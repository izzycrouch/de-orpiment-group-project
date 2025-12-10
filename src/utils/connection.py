from dotenv import load_dotenv
import pg8000.native
from src.utils.get_database_credentials import get_database_credentials
import json

def connect_to_db():
    secrets = get_database_credentials()
    return pg8000.native.Connection(
        user=secrets['user'],
        password=secrets['password'],
        database=secrets['database'],
        host=secrets['host'],
        port=int(secrets['port'])
    )


def close_db_connection(conn):
    conn.close()

