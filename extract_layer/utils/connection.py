import pg8000.native
from extract_layer.utils.db_credentials import get_db_credentials
import os
from dotenv import load_dotenv
load_dotenv(override=True)

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

def connect_to_local_db():
    return pg8000.native.Connection(
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        database=os.getenv("PG_DATABASE"),
        host=os.getenv("PG_HOST"),
        port=int(os.getenv("PG_PORT"))
    )

