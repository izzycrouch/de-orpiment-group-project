from dotenv import load_dotenv
import pg8000.native
from src.utils.get_secret import get_secret
import json

def connect_to_db():
    secrets = json.loads(get_secret())
    return pg8000.native.Connection(
        user=secrets['user'],
        password=secrets['password'],
        database=secrets['database'],
        host=secrets['host'],
        port=int(secrets['port'])
    )


def close_db_connection(conn):
    conn.close()

