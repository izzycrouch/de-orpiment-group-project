import pandas as pd

def get_df(bucket_name,file_name):
    df = pd.read_parquet(
        f"s3://{bucket_name}/{file_name}",
        engine="pyarrow"
    )
    return df