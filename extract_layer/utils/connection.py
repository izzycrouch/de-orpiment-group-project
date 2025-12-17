import pg8000.native
from extract_layer.utils.db_credentials import get_db_credentials
import os
from dotenv import load_dotenv
load_dotenv(override=True)


def ensure_database_exists():
    conn = pg8000.connect(
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        host=os.getenv("PG_HOST"),
        port=int(os.getenv("PG_PORT")),
        database="postgres"
    )
    db_name = os.getenv("PG_DATABASE")
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(
        "SELECT 1 FROM pg_database WHERE datname = %s;",
        (db_name,)
    )

    if not cur.fetchone():
        cur.execute(f'CREATE DATABASE "{db_name}";')
        print(f"Created database: {db_name}")
    else:
        print(f"Database already exists: {db_name}")

    cur.close()
    conn.close()





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

    ensure_database_exists()

    return pg8000.native.Connection(
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        database=os.getenv("PG_DATABASE"),
        host=os.getenv("PG_HOST"),
        port=int(os.getenv("PG_PORT"))
    )


