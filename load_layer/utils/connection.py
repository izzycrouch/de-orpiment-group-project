import pg8000.native
from load_layer.utils.db_credentials import get_db_credentials
from sqlalchemy import create_engine
from dotenv import load_dotenv
load_dotenv(override=True)

def connect_to_pg_db():
    secrets = get_db_credentials(secret_name = "orpiment_db_secret", region_name = "eu-west-2")
    return pg8000.native.Connection(
        user=secrets['warehouse_user'],
        password=secrets['warehouse_password'],
        database=secrets['warehouse_database'],
        host=secrets['warehouse_host'],
        port=int(secrets['warehouse_port'])
    )

def close_pg_db_connection(db):
    db.dispose()



def connect_to_db():
    secrets = get_db_credentials(secret_name = "orpiment_db_secret", region_name = "eu-west-2")
    connection_string = (
    f"postgresql+pg8000://{secrets['warehouse_user']}:"
    f"{secrets['warehouse_password']}@"
    f"{secrets['warehouse_host']}:"
    f"{secrets['warehouse_port']}/"
    f"{secrets['warehouse_database']}"
    )
    engine = create_engine(connection_string)
    return engine



def close_db_connection(engine):
    engine.dispose()