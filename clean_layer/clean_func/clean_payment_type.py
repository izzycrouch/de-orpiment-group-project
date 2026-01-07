import pandas as pd
from clean_layer.utils.get_df import get_df

def clean_payment_type(file_path: str, bucket_name: str = 'totesys-raw-data-aci'):
    df = get_df(bucket_name=bucket_name, file_name=file_path)

    df['payment_type_id'] = pd.to_numeric(df['payment_type_id'], errors='coerce')
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
    df['last_updated'] = pd.to_datetime(df['last_updated'], errors='coerce')
    df["payment_type_name"] = df["payment_type_name"].astype("string")
    df = df.dropna()
    df = df.drop_duplicates(subset=['payment_type_id'], keep='first')
    # now = pd.Timestamp.now()
    # df = df[(df['created_at'] <= now) & (df['last_updated'] <= now) & (df['created_at'] <= df['last_updated'])]
    return df
