import pandas as pd
from clean_layer.utils.get_df import get_df


def clean_purchase_order(
    file_path: str,
    bucket_name: str = "totesys-raw-data-aci"
):
    df = get_df(bucket_name=bucket_name, file_name=file_path)
    int_cols = [
        "purchase_order_id",
        "staff_id",
        "counterparty_id",
        "item_quantity",
        "currency_id",
        "agreed_delivery_location_id",
    ]
    for col in int_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["item_unit_price"] = pd.to_numeric(df["item_unit_price"], errors="coerce")
    df["item_code"] = df["item_code"].astype("string")

    df = df.drop_duplicates(subset=["purchase_order_id"], keep="first")
    non_null = ['purchase_order_id', 'created_at', 'last_updated', 'staff_id',
                'counterparty_id', 'item_code', 'item_quantity', 'item_unit_price',
                'currency_id', 'agreed_delivery_date', 'agreed_payment_date',
                'agreed_delivery_location_id']
    df = df.dropna(subset=non_null)
    return df
