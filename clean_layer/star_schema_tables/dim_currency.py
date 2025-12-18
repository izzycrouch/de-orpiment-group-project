import pandas as pd
from clean_layer.clean_func.clean_currency import clean_currency

def create_dim_currency(file_path: str, bucket_name: str = 'totesys-raw-data-aci'):
    df = clean_currency(file_path=file_path, bucket_name=bucket_name)
    df = df[['currency_id', 'currency_code']]
    
    currency_map = {'USD': 'United States Dollar',
                    'GBP': 'British Pound Sterling',
                    'EUR': 'Euro'}
    
    df['currency_name'] = df['currency_code'].map(currency_map)

    return df