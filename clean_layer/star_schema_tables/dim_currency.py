
def create_dim_currency(df):
    df = df[['currency_id', 'currency_code']]

    currency_map = {'USD': 'United States Dollar',
                    'GBP': 'British Pound Sterling',
                    'EUR': 'Euro'}

    df['currency_name'] = df['currency_code'].map(currency_map)

    return df

