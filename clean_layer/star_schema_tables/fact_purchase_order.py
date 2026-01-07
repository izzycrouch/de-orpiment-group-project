import pandas as pd
from clean_layer.clean_func.clean_purchase_order import clean_purchase_order

def create_fact_purchase_order(dim_date_df: pd.DataFrame,
                               dim_currency_df: pd.DataFrame,
                               dim_staff_df: pd.DataFrame,
                               dim_counterparty_df: pd.DataFrame,
                               dim_location_df: pd.DataFrame,
                               purchase_order_df: pd.DataFrame):

    columns = ['purchase_order_id', 'created_at', 'last_updated', 'staff_id', 'counterparty_id', 'item_code', 'item_quantity', 'item_unit_price', 'currency_id', 'agreed_delivery_date', 'agreed_payment_date', 'agreed_delivery_location_id']

    df = purchase_order_df[columns]
    df.insert(loc=0, column='purchase_record_id', value=df.index + 1)

    df = pd.merge(df, dim_currency_df, on='currency_id')
    df = pd.merge(df, dim_staff_df, on='staff_id')
    df = pd.merge(df, dim_counterparty_df, on='counterparty_id')
    df = pd.merge(df, dim_location_df['location_id'], left_on='agreed_delivery_location_id', right_on='location_id', how='left').drop('location_id', axis=1)

    df.insert(loc=2, column='created_date', value=pd.to_datetime(df['created_at'].dt.date))
    df.insert(loc=3, column='created_time', value=df['created_at'].dt.time)
    # df['created_time'] = pd.to_datetime(df['created_time'], format='%H:%M:%S.%f')
    df = pd.merge(df, dim_date_df['date_id'], left_on='created_date', right_on='date_id', how='left').drop(['date_id', 'created_at'], axis=1)

    df.insert(loc=4, column='last_updated_date', value=pd.to_datetime(df['last_updated'].dt.date))
    df.insert(loc=5, column='last_updated_time', value=df['last_updated'].dt.time)
    # df['last_updated_time'] = pd.to_datetime(df['last_updated_time'], format='%H:%M:%S.%f')
    df = pd.merge(df, dim_date_df['date_id'], left_on='last_updated_date', right_on='date_id', how='left').drop(['date_id', 'last_updated'], axis=1)

    return df