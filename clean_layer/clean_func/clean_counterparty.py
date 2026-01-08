import pandas as pd
from clean_layer.utils.get_df import get_df


def clean_counterparty(
    file_path: str,
    bucket_name: str = "totesys-raw-data-aci"
):
    df = get_df(bucket_name=bucket_name, file_name=file_path)


    df["counterparty_id"] = pd.to_numeric(df["counterparty_id"], errors="coerce")
    df["legal_address_id"] = pd.to_numeric(df["legal_address_id"], errors="coerce")


    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df["last_updated"] = pd.to_datetime(df["last_updated"], errors="coerce")


    df["counterparty_legal_name"] = df["counterparty_legal_name"].astype("string")
    df["commercial_contact"] = df["commercial_contact"].astype("string")
    df["delivery_contact"] = df["delivery_contact"].astype("string")

    non_null = ['counterparty_id', 'counterparty_legal_name', 'legal_address_id',
                'created_at', 'last_updated']
    df = df.dropna(subset=non_null)

    df = df.drop_duplicates(subset=["counterparty_id"], keep="first")

    # now = pd.Timestamp.now()
    # df = df[
    #     (df["created_at"] <= now)
    #     & (df["last_updated"] <= now)
    #     & (df["created_at"] <= df["last_updated"])
    # ]

    return df
