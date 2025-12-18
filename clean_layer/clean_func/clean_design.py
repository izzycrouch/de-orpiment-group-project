import pandas as pd
from clean_layer.utils.get_df import get_df



def clean_design(file_path: str, bucket_name: str = 'totesys-raw-data-aci'):
    df = get_df(bucket_name=bucket_name, file_name=file_path)

    # drops row where values cant be cast into correct datatype
    df['design_id'] = pd.to_numeric(df['design_id'], errors='coerce')
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
    df['last_updated'] = pd.to_datetime(df['last_updated'], errors='coerce')
    df['design_name'] = df['design_name'].astype('string').str.strip()
    df['file_location'] = df['file_location'].astype('string').str.strip()
    df['file_name'] = df['file_name'].astype('string').str.strip()
    df = df.dropna()

    # drops duplicates of design_id
    df = df.drop_duplicates(subset=['design_id'], keep='first')

    # drops row where created_at and last_updated after current time
    now = pd.Timestamp.now()
    df = df[(df['created_at'] <= now) & (df['last_updated'] <= now) & (df['created_at'] <= df['last_updated'])]

    # drop rows where file_location doesnt start with '/'
    df = df[(df['file_location'].str.startswith('/'))]

    # drop rows where file_name doesnt end with correct suffix
    valid_suffix = ('.json', '.csv', '.parquet')
    df = df[(df['file_name'].str.endswith(valid_suffix))]

    return df
