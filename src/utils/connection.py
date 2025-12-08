import os
from dotenv import load_dotenv
import pg8000.native
from src.utils import get_secret


load_dotenv(override=True)

secrets = get_secret()


def connect_to_db():
    return pg8000.native.Connection(
        user=os.getenv(secrets['user']), 
        password=os.getenv(secrets['password']),
        database=os.getenv(secrets['database']),
        host=os.getenv(secrets['host']),
        port=int(os.getenv(secrets['port']))
    )


def close_db_connection(conn):
    conn.close()