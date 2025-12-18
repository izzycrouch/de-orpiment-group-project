import pandas as pd

def create_dim_date(start: str = '2022-01-01', end: str = '2030-12-31'):
    df = pd.DataFrame({'date_id': pd.date_range(start, end)})
    df['date_id'] = pd.to_datetime(df['date_id'])
    df['year'] = df['date_id'].dt.year
    df['month'] = df['date_id'].dt.month
    df['day'] = df['date_id'].dt.day
    df['day_of_week'] = df['date_id'].dt.day_of_week
    df['day_name'] = df['date_id'].dt.day_name()
    df['month_name'] = df['date_id'].dt.month_name()
    df['quarter'] = df['date_id'].dt.quarter
    
    return df
