import pandas as pd
from clean_layer.utils.get_df import get_df



def clean_design(file_path: str, bucket_name: str = 'totesys-raw-data-aci'):
    design_df = get_df(bucket_name=bucket_name, file_name=file_path)

    # drops row where values cant be cast into correct datatype
    design_df['design_id'] = pd.to_numeric(design_df['design_id'], errors='coerce')
    design_df['created_at'] = pd.to_datetime(design_df['created_at'], errors='coerce')
    design_df['last_updated'] = pd.to_datetime(design_df['last_updated'], errors='coerce')
    design_df['design_name'] = design_df['design_name'].astype('string').str.strip()
    design_df['file_location'] = design_df['file_location'].astype('string').str.strip()
    design_df['file_name'] = design_df['file_name'].astype('string').str.strip()
    design_df = design_df.dropna()

    # drops duplicates of design_id
    design_df = design_df.drop_duplicates(subset=['design_id'], keep='first')

    # drops row where created_at and last_updated after current time
    now = pd.Timestamp.now()
    design_df = design_df[(design_df['created_at'] <= now) & (design_df['last_updated'] <= now) & (design_df['created_at'] <= design_df['last_updated'])]

    # drop rows where file_location doesnt start with '/'
    design_df = design_df[(design_df['file_location'].str.startswith('/'))]

    # drop rows where file_name doesnt end with correct suffix
    valid_suffix = ('.json', '.csv', '.parquet')
    design_df = design_df[(design_df['file_name'].str.endswith(valid_suffix))]

    return design_df
