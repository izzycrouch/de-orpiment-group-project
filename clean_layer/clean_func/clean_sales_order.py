import pandas as pd
from clean_layer.utils.get_df import get_df

def clean_sales_order(file_path: str, bucket_name: str = 'totesys-raw-data-aci'):
    df = get_df(bucket_name=bucket_name, file_name=file_path)



    # drops row where values cant be cast into correct datatype
    df['sales_order_id'] = pd.to_numeric(df['sales_order_id'], errors='coerce')
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
    df['last_updated'] = pd.to_datetime(df['last_updated'], errors='coerce')
    df['design_id'] = pd.to_numeric(df['design_id'], errors='coerce')
    df['staff_id'] = pd.to_numeric(df['staff_id'], errors='coerce')
    df['counterparty_id'] = pd.to_numeric(df['counterparty_id'], errors='coerce')
    df['units_sold'] = pd.to_numeric(df['units_sold'], errors='coerce')
    df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce', downcast='float')
    df['currency_id'] = pd.to_numeric(df['currency_id'], errors='coerce')
    df['agreed_delivery_date'] = pd.to_datetime(df['agreed_delivery_date'], errors='coerce')
    df['agreed_payment_date'] = pd.to_datetime(df['agreed_payment_date'], errors='coerce')
    df['agreed_delivery_location_id'] = pd.to_numeric(df['agreed_delivery_location_id'], errors='coerce')
    non_null = ['sales_order_id', 'created_at', 'last_updated', 'design_id', 'staff_id',
            'counterparty_id', 'units_sold', 'unit_price', 'currency_id',
            'agreed_delivery_date', 'agreed_payment_date', 'agreed_delivery_location_id']
    df = df.dropna(subset=non_null)
    df = df.drop_duplicates(subset=['sales_order_id'], keep='first')

    # now = pd.Timestamp.now()
    # df = df[(df['created_at'] <= now) & (df['last_updated'] <= now) & (df['created_at'] <= df['last_updated'])]
    # df = df[(df['created_at'] <= df['agreed_delivery_date']) & (df['created_at'] <= df['agreed_payment_date'])]

    return df