import pandas as pd
from datetime import datetime
from io import BytesIO
import boto3

def get_df(bucket_name: str, file_path: str) -> pd.DataFrame:
    s3 = boto3.client("s3", region_name="eu-west-2")

    obj = s3.get_object(Bucket=bucket_name, Key=file_path)
    body = obj["Body"].read()

    return pd.read_parquet(BytesIO(body))

def clean_payment_table(bucket_name, file_path):

    df = get_df(bucket_name, file_path)

    df['payment_amount'] = (
        df['payment_amount'].astype(str)
        .str.replace(',', '')
        .str.replace('£', '')
        .str.strip()
    )
    df['payment_amount'] = pd.to_numeric(df['payment_amount'], errors="coerce")


    df['payment_date'] = pd.to_datetime(df['payment_date'], format='%Y-%m-%d', errors='coerce')
    
    today = datetime.today()

    df = df[(df["created_at"] <= today) & (df["last_updated"] <= today) & (df["payment_date"] <= today)]

    df = df.dropna(how='any',axis=0) 

    return df