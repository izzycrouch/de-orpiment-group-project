import pandas as pd
from io import BytesIO
import boto3


def get_df(bucket_name: str, file_path: str) -> pd.DataFrame:
    s3 = boto3.client("s3", region_name="eu-west-2")

    obj = s3.get_object(Bucket=bucket_name, Key=file_path)
    body = obj["Body"].read()

    return pd.read_parquet(BytesIO(body))

def clean_staff_table(bucket_name, file_path):

    df = get_df(bucket_name, file_path)

    df['first_name'] = (
        df['first_name']
        .str.strip()
        .str.capitalize()
        )   
    df['last_name'] = (
        df['last_name']
        .str.strip()
        .str.capitalize()
        )
    
    df = df.drop_duplicates(subset=['staff_id'], keep='first')

    now = pd.Timestamp.now()
    df = df[(df["created_at"] <= now) & (df["last_updated"] <= now)]

    df = df.dropna(how='any',axis=0) 

    pattern = r"^[a-zA-Z0-9._%+'-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    df = df[df["email_address"].str.match(pattern)]

    return df