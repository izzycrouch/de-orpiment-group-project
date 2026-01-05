import pandas as pd
from clean_layer.utils.get_df import get_df
from clean_layer.utils.save_df_into_parquet import save_data

def create_dim_counterparty( counterparty_df , address_df,id_col_name ='address_id' ):

    columns = ['counterparty_id', 'counterparty_legal_name', 'legal_address_id']
    c_df = counterparty_df[columns]

    address_df.rename(columns={id_col_name: 'legal_address_id'}, inplace=True)
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


def update_counterparty(df,processed_bucket_name):
    dim_counterparty_df =  get_df(processed_bucket_name,'dim_counterparty.parquet')
    dim_location_df = get_df(processed_bucket_name,'dim_location.parquet')
    df = create_dim_counterparty(df,dim_location_df,'location_id')
    new_df = pd.concat([dim_counterparty_df,df],axis=0, ignore_index=True)
    key = 'dim_counterparty.parquet'
    save_data(new_df,key)
