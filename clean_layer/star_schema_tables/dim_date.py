import pandas as pd

def create_dim_date(start: str = '2022-01-01', end: str = '2030-12-31'):
    df = pd.DataFrame({'date': pd.date_range(start, end)})
    df['date_id'] = df['date'].astype('string').str.replace('-', '', regex=True).astype(int)
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month
    df['day'] = df['date'].dt.day
    df['day_of_week'] = df['date'].dt.day_of_week
    df['day_name'] = df['date'].dt.day_name()
    df['month_name'] = df['date'].dt.month_name()
    df['quarter'] = df['date'].dt.quarter
    columns = ['date_id', 'year', 'month', 'day', 'day_of_week', 'day_name', 'month_name', 'quarter']
    df = df[columns]
    return df
