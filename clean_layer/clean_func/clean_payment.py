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

    non_null = ['payment_id', 'created_at', 'last_updated', 'transaction_id',
                'counterparty_id', 'payment_amount', 'currency_id', 'payment_type_id',
                'paid', 'payment_date', 'company_ac_number', 'counterparty_ac_number']
    df = df.dropna(subset=non_null)
    return df