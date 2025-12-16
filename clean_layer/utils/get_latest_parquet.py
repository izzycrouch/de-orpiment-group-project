## receive a table name,

##iterate table file, using latest.json find new parquet, using function to clean the table,save into cleaned bucket,



import logging
import pandas as pd
from datetime import datetime, timezone
from extract_layer.utils.connection import connect_to_db, close_db_connection,connect_to_local_db
from extract_layer.utils.save_data import save_data
from extract_layer.utils.extraction_info import get_latest_extraction_info, save_new_extraction_info
import os
import io


def get_latest_dict():
    pass
def init_prefix_dict():
    pass


def lambda_handler(event, content):
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    BUCKET_NAME = os.environ["S3_BUCKET_NAME"]


    tables = ['counterparty', 'address', 'department', 'purchase_order', 'staff', 'payment_type', 'payment', 'transaction', 'design', 'sales_order', 'currency']
    prefix_dict = get_latest_dict()
    if not prefix_dict:
        prefix_dict = init_prefix_dict
    for table in tables:
        latest = prefix_dict[table]

import boto3

s3 = boto3.client("s3")

bucket_name = "your-bucket-name"

response = s3.list_objects_v2(
    Bucket=bucket_name
)



# prefix = '/year=' + str(year) + '/month=' + str(month) + '/day=' + str(day) + '/'
# timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
# file_name = table +  prefix + 'batch_' + timestamp +'.parquet'





lastest_json = {
    'counterparty' : {
        'year':2025,
        'month':12,
        'day':24,
        'time':'20251215T135936Z'
    }
}



# A Python application to transform data landing in the "ingestion" S3 bucket and place the results in the "processed" S3 bucket. The data should be transformed to conform to the warehouse schema (see above).

# processed_bucket


# /facts/
#     fact_sales_order/
#         year/month/day/timestamp
#     fact_purchase_orders
#     fact_payment

# /dims/
#     dim_transaction
#         year/month/day/timestamp
#     dim_staff
#     dim_payment_type
#     dim_location
#     dim_design
#     dim_date
#     dim_currency
#     dim_counterparty



# processed_bucket



#     fact_sales_order/
#         year/month/day/timestamp
#          dims/
#            year/month/day/timestamp

#     fact_purchase_orders
#         year/month/day/timestamp
#          dims/
#            year/month/day/timestamp


#     fact_payment
#         year/month/day/timestamp
#          dims/
#            year/month/day/timestamp


# how to keep linage
# using extra json file
# add extra column
# keep the name same with injest bucket?