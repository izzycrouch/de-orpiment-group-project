import pandas as pd
from clean_layer.utils.get_df import get_df

def clean_department(file_path: str, bucket_name: str = 'totesys-raw-data-aci'):
    df = get_df(bucket_name=bucket_name, file_name=file_path)

    df["department_id"] = pd.to_numeric(df["department_id"], errors="coerce")

    df["department_name"] = df["department_name"].astype("string")
    df["location"] = df["location"].astype("string")
    df["manager"] = df["manager"].astype("string")

    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df["last_updated"] = pd.to_datetime(df["last_updated"], errors="coerce")

    non_null = ['department_id', 'department_name', 'created_at', 'last_updated']
    df = df.dropna(subset=non_null)

    for col in ["department_name", "location", "manager"]:
        df[col] = df[col].str.strip()
        df = df[df[col] != ""]


    df = df.sort_values("last_updated").drop_duplicates(
        subset=["department_id"], keep="last"
    )


    # now = pd.Timestamp.now()
    # df = df[(df["created_at"] <= now) & (df["last_updated"] <= now)]
    # df = df[df["created_at"] <= df["last_updated"]]
    return df
