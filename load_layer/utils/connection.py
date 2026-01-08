import pg8000.native
from load_layer.utils.db_credentials import get_db_credentials
import os
from dotenv import load_dotenv
load_dotenv(override=True)

def connect_to_db():
    secrets = get_db_credentials(secret_name = "project-orpiment-star-database", region_name = "eu-west-2")
    return pg8000.native.Connection(
        user=secrets['warehouse_user'],
        password=secrets['warehouse_password'],
        database=secrets['warehouse_database'],
        host=secrets['warehouse_host'],
        port=int(secrets['warehouse_port'])
    )

def close_db_connection(conn):
    conn.close()

