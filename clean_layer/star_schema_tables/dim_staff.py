import pandas as pd
from clean_layer.utils.get_df import get_df
from clean_layer.utils.save_df_into_parquet import save_data


def create_dim_staff(cleaned_staff, cleaned_department):
    dim_staff = (
        cleaned_staff.merge(cleaned_department, on='department_id', how='inner')
        [
            [
                'staff_id',
                'first_name',
                'last_name',
                'department_name',
                'location',
                'email_address'
            ]
        ]
    )
    return dim_staff


def update_dim_staff(df,processed_bucket_name):

    cleaned_department = get_df(processed_bucket_name,'cleaned_department_df.parquet')
    new_dim_staff = create_dim_staff(df,cleaned_department)
    dim_staff_df = get_df(processed_bucket_name,'dim_staff.parquet')
    new_df = pd.concat([dim_staff_df,new_dim_staff],axis=0, ignore_index=True)
    key = 'dim_staff.parquet'
    save_data(new_df,key)
