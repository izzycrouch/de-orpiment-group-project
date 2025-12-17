import pandas as pd
from io import BytesIO
import boto3

def get_df(bucket_name: str, file_path: str) -> pd.DataFrame:
    s3 = boto3.client("s3", region_name="eu-west-2")

    obj = s3.get_object(Bucket=bucket_name, Key=file_path)
    body = obj["Body"].read()

    return pd.read_parquet(BytesIO(body))

# - make sure capital letters or countries, district, city
# - make sure and or & - choose one
# - split address line 1
# - check valid date and time

def clean_address_table(bucket_name, file_path):

    df = get_df(bucket_name, file_path)

    df["address_id"] = pd.to_numeric(df["address_id"], errors="coerce")
    df["address_line_1"] = df["address_line_1"].astype("string")
    df["address_line_2"] = df["address_line_2"].astype("string")
    df["district"] = df["district"].astype("string")
    df["city"] = df["city"].astype("string")
    df["postal_code"] = df["postal_code"].astype("string")
    df["country"] = df["country"].astype("string")
    df["phone"] = df["phone"].astype("string")
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df["last_updated"] = pd.to_datetime(df["last_updated"], errors="coerce")

    df = df[df['phone'].str.len() == 11]

    df = df.drop_duplicates(subset=['address_id'], keep='first')

    filler_words = ['and', 'of']
    cols = ['address_line_1', 'address_line_2', 'district', 'city', 'country']
    for col in cols:
        df[col] = df[col].apply(
            lambda v: v if pd.isnull(v) else " ".join(
                k.title() if k.lower() not in filler_words else k.lower()
                for k in v.split()
            )
        )
    today = pd.Timestamp.now()
    df = df[(df["created_at"] <= today) & (df["last_updated"] <= today)]

    non_null = ['address_id', 'address_line_1', 'city', 'postal_code', 'country', 'phone', 'created_at', 'last_updated']
    df = df.dropna(subset=non_null)

    return df

#clean_address_table('totesys-raw-data-aci', 'address/year=2025/month=12/day=15/batch_20251215T135936Z.parquet')