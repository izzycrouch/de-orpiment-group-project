#link to the database
#read all the files in 'totesys-transformed-data-aci-2' for each tabl
#using sql to add into the database
#if dim - update whole table
#if fact - read lastest date in databse and add new part into database
import os
import logging
from load_layer.utils.connection import connect_to_db, close_db_connection
from load_layer.utils.get_df import get_df


FILE_LIST =[
        "dim_date.parquet",
        "dim_staff.parquet",
        "dim_location.parquet",
        "dim_currency.parquet",
        "dim_design.parquet",
        "dim_counterparty.parquet",
        "dim_payment_type.parquet",
        "dim_transaction.parquet",
        "fact_payment.parquet",
        "fact_purchase_order.parquet",
        "fact_sales_order.parquet",
    ]


def lambda_handler(event, content):
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    BUCKET_NAME = os.environ["S3_BUCKET_NAME"]

    try:
        logger.info("Start load table")
        db = connect_to_db()
        for file in FILE_LIST:
            table_name = file.removesuffix(".parquet")
            df = get_df(BUCKET_NAME,file)

            df.to_sql(name = table_name,con = db,if_exists = 'replace',index=False,method='multi',chunksize=1000)
            logger.info("finish create table")

    except Exception as e:
        logger.error(f"MAJOR_ERROR: Ingestion failed: %s", str(e))
        raise
    finally:
        if db:
            close_db_connection(db)

