import pandas as pd
from datetime import datetime
from clean_layer.utils.get_df import get_df

def clean_payment(bucket_name, file_path):

    df = get_df(bucket_name, file_path)

    df['payment_amount'] = (
        df['payment_amount'].astype(str)
        .str.replace(',', '')
        .str.replace('£', '')
        .str.strip()
    )
    df['payment_amount'] = pd.to_numeric(df['payment_amount'], errors="coerce")


    df['payment_date'] = pd.to_datetime(df['payment_date'], format='%Y-%m-%d', errors='coerce')

    # today = datetime.today()

    # df = df[(df["created_at"] <= today) & (df["last_updated"] <= today) & (df["payment_date"] <= today)]

    df = df.dropna(how='any',axis=0)

    return df