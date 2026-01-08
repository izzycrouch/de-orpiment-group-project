import pandas as pd
from clean_layer.utils.get_df import get_df

def clean_staff(bucket_name, file_path):

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
    non_null = ['staff_id', 'first_name', 'last_name', 'department_id',
            'email_address', 'created_at', 'last_updated']
    df = df.dropna(subset=non_null)

    # now = pd.Timestamp.now()
    # df = df[(df["created_at"] <= now) & (df["last_updated"] <= now)]



    # pattern = r"^[a-zA-Z0-9._%+'-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    # df = df[df["email_address"].str.match(pattern)]

    return df