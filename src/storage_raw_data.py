from botocore.exceptions import ClientError
import logging
import pandas as pd
from datetime import datetime, timezone
from src.utils.connection import connect_to_db,close_db_connection
from src.utils.storage_data import storage_data
from src.utils.get_latest import get_latest,save_latest
import os

def lambda_handler(event,content):
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    BUCKET_NAME = os.environ["S3_BUCKET_NAME"]

    dt = datetime.now()
    year = dt.year
    month = dt.month
    day = dt.day
    prefix = '/year=' + str(year) + '/month=' + str(month) + '/day=' + str(day) + '/'
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    db = None


    try:
        db = connect_to_db()
        tables = db.run("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
        old_json = get_latest(BUCKET_NAME)
        if not old_json:
            old_json = build_inital_json(tables)
        new_json = {}
        for table in tables:
            table = table[1:]
            file_name = table +  prefix + 'batch_' + timestamp +'.parquet'
            latest_timestamp = old_json[table]
            rows =  db.run(f"""
                        SELECT * FROM {table}
                        WHERE last_update > :last_update;
                        """,
                        {'last_update': latest_timestamp }
                        )
            if rows:
                column_names = [col["name"] for col in db.columns]
                df = pd.DataFrame(rows,columns=column_names)
                data = df.to_parquet
                storage_data(data, BUCKET_NAME, file_name)
                latest_timestamp = df['last_updated'].max()

            new_json[table] = latest_timestamp
        save_latest(new_json,BUCKET_NAME)

    except Exception as e:
        logger.error("storage_raw_data_error: ", e)
    finally:
        if db:
            close_db_connection(db)




def build_inital_json(tables):
    very_old_time = datetime.min
    dict = {}
    for table in tables:
        table = table[1:]
    dict[table] = very_old_time
    return dict




