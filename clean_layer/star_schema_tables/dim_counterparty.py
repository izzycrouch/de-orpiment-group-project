import pandas as pd
from clean_layer.clean_func.clean_counterparty import clean_counterparty
from clean_layer.clean_func.clean_address import clean_address

def create_dim_counterparty(counterparty_file_path: str, address_file_path: str, bucket_name: str = 'totesys-raw-data-aci'):
    counterparty_df = clean_counterparty(file_path=counterparty_file_path, bucket_name=bucket_name)
    columns = ['counterparty_id', 'counterparty_legal_name', 'legal_address_id']
    c_df = counterparty_df[columns]
    
    address_df = clean_address(file_path=address_file_path, bucket_name=bucket_name)
    address_df.rename(columns={'address_id': 'legal_address_id'}, inplace=True)
    columns = ['legal_address_id', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone']
    a_df = address_df[columns]


    df = pd.merge(c_df, a_df, on='legal_address_id')
    rename_dict = {'address_line_1': 'counterparty_legal_address_line_1',
                   'address_line_2': 'counterparty_legal_address_line_2',
                   'district': 'counterparty_legal_district',
                   'city': 'counterparty_legal_city',
                   'postal_code': 'counterparty_legal_postal_code',
                   'country': 'counterparty_legal_country',
                   'phone': 'counterparty_legal_phone_number'}
    df.rename(columns=rename_dict, inplace=True)

    columns = ['counterparty_id', 'counterparty_legal_name', 'counterparty_legal_address_line_1', 'counterparty_legal_address_line_2', 'counterparty_legal_district', 'counterparty_legal_city', 'counterparty_legal_postal_code', 'counterparty_legal_country', 'counterparty_legal_phone_number']
    df = df[columns]
    return df