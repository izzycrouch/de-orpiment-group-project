#get the parquet file
#convert into dataframe
#clean the data

# --transaction——type should be PURCHASE OR SALE
# -- IF SALE THEN HAVE UNIQUE SALSE ID SAME FOR PURCHASE

#convert and saved as parquet


# import pandas as pd

# def get_df(bucket_name,file_name):
#     df = pd.read_parquet(
#         f"s3://{bucket_name}/{file_name}",
#         engine="pyarrow"
#     )
#     return df


from extract_layer.utils.connection import connect_to_db, close_db_connection
from tabulate import tabulate
import pandas as pd

db = None

db = connect_to_db()

rows =  db.run('''
            SELECT *
            FROM
            transaction
            ''')
columns = [col["name"] for col in db.columns]

df = pd.DataFrame(rows, columns=columns)
print(df["transaction_type"].nunique(False))



import pandas as pd

def clean_transcation_data(df):
    df = df.copy()
    df["transaction_type"] = df["transaction_type"].astype("string").str.strip().str.upper()

    df = df[df["transaction_type"].isin(["PURCHASE", "SALE"])]

    mask_both_null = df["sales_order_id"].isna() & df["purchase_order_id"].isna()
    df = df[~mask_both_null]


    mask_both_notnull = df["sales_order_id"].notna() & df["purchase_order_id"].notna()
    df = df[~mask_both_notnull]


    mask_sale_missing = (df["transaction_type"] == "SALE") & (df["sales_order_id"].isna())
    df = df[~mask_sale_missing]


    mask_purchase_missing = (df["transaction_type"] == "PURCHASE") & (df["purchase_order_id"].isna())
    df = df[~mask_purchase_missing]

    now = pd.Timestamp.now()
    df = df[(df["created_at"] <= now) & (df["last_updated"] <= now)]

    return df




import io
import boto3
from botocore.exceptions import ClientError
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def save_data(df, bucket_name: str, file_name: str):
    try:
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        s3_client = boto3.client("s3")
        s3_client.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.getvalue())

    except ClientError as c:
        logger.info("Boto3 ClientError: %s", str(c))
        raise c
