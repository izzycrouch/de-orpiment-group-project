from clean_layer.utils.get_df import get_df
import pandas as pd

def clean_currency(file_path: str, bucket_name: str = 'totesys-raw-data-aci'):
    df = get_df(bucket_name=bucket_name, file_name=file_path)

    df['currency_id'] = pd.to_numeric(df['currency_id'], errors='coerce')
    df['currency_code'] = df['currency_code'].astype('string').str.strip().str.upper()
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
    df['last_updated'] = pd.to_datetime(df['last_updated'], errors='coerce')
    non_null = ['currency_id', 'currency_code', 'created_at', 'last_updated']
    df = df.dropna(subset=non_null)

    df = df.drop_duplicates(subset=['currency_id'], keep='first')

    # now = pd.Timestamp.now()
    # df = df[(df['created_at'] <= now) & (df['last_updated'] <= now) & (df['created_at'] <= df['last_updated'])]

    valid_currency_codes = ['USD', 'GBP', 'EUR']

    df = df[df['currency_code'].isin(valid_currency_codes)]
    return df