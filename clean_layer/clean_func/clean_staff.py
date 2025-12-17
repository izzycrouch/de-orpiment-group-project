import pandas as pd
from clean_layer.utils.get_df import get_df

def clean_staff_table(bucket_name, file_path):

    df = get_df(bucket_name, file_path)

    df['first_name'] = (
        df['first_name']
        .str.strip()
        .str.capitalize()
        )
    df['last_name'] = (
        df['last_name']
        .str.strip()
        .str.capitalize()
        )

    df = df.drop_duplicates(subset=['staff_id'], keep='first')

    now = pd.Timestamp.now()
    df = df[(df["created_at"] <= now) & (df["last_updated"] <= now)]

    df = df.dropna(how='any',axis=0)

    pattern = r"^[a-zA-Z0-9._%+'-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    df = df[df["email_address"].str.match(pattern)]

    return df