import pandas as pd
from clean_layer.utils.get_df import get_df

def clean_transcation(file_path: str, bucket_name: str = 'totesys-raw-data-aci'):
    df = get_df(bucket_name, file_path)

    df['transaction_id'] = pd.to_numeric(df['transaction_id'], errors='coerce')
    df['transaction_type'] = df['transaction_type'].astype('string').str.strip().str.upper()
    df['sales_order_id'] = pd.to_numeric(df['sales_order_id'], errors='coerce')
    df['purchase_order_id'] = pd.to_numeric(df['purchase_order_id'], errors='coerce')
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
    df['last_updated'] = pd.to_datetime(df['last_updated'], errors='coerce')
    df = df.dropna()

    df = df.drop_duplicates(subset=['transaction_id'], keep='first')

    df = df[df["transaction_type"].isin(["PURCHASE", "SALE"])]

    now = pd.Timestamp.now()
    df = df[(df['created_at'] <= now) & (df['last_updated'] <= now) & (df['created_at'] <= df['last_updated'])]

    return df