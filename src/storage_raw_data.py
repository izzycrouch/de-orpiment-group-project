
import logging
import pandas as pd
from datetime import datetime, timezone
from src.utils.connection import connect_to_db,close_db_connection
from src.utils.storage_data import storage_data
from src.utils.get_latest import get_latest,save_latest
import os
import io

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
        tables = ['counterparty', 'address', 'department', 'purchase_order', 'staff', 'payment_type', 'payment', 'transaction', 'design', 'sales_order', 'currency']
        old_json = get_latest(BUCKET_NAME)
        if not old_json:
            old_json = build_inital_json(tables)
        new_json = {}
        for table in tables:
            file_name = table +  prefix + 'batch_' + timestamp +'.parquet'
            latest_timestamp = old_json[table]
            rows =  db.run(f"""
                        SELECT * FROM {table}
                        WHERE last_updated > :last_updated;
                        """,
                        last_updated = latest_timestamp
                        )
            if rows:
                column_names = [col["name"] for col in db.columns]
                df = pd.DataFrame(rows,columns=column_names)

                buffer = io.StringIO()
                df.to_csv(buffer, index=False)
                buffer.seek(0)

                storage_data(buffer.getvalue().encode("utf-8"), BUCKET_NAME, file_name.replace(".parquet", ".csv"))
                latest_timestamp = df['last_updated'].max()

            new_json[table] = latest_timestamp
        save_latest(new_json,BUCKET_NAME)

    except Exception as e:
        return str(e)
    finally:
        if db:
            close_db_connection(db)





def build_inital_json(tables):
    very_old_time = datetime.min
    res = {}
    for table in tables:
        res[table] = very_old_time
    return res
