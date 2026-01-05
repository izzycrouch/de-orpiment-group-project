import pandas as pd
from datetime import datetime
from clean_layer.star_schema_tables.dim_currency import create_dim_currency

class TestDimCurrency:

    def test_dim_currency_returns_dataframe(self):
        df = pd.DataFrame([{'currency_id' : 1,
                      'currency_code' : 'USD',
                      'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099')}])
        
        result = create_dim_currency(df)
        
        assert isinstance(result, pd.DataFrame)

    
    def test_dim_currency_returns_correct_column_names(self):
        df = pd.DataFrame([{'currency_id' : 1,
                      'currency_code' : 'USD',
                      'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099')}])
        
        expected_columns = ['currency_id', 'currency_code', 'currency_name']
        result = create_dim_currency(df)
        output_columns = result.columns.values.tolist()

        assert expected_columns == output_columns


    def test_dim_currency_returns_correct_currency_name_for_code(self):
        df = pd.DataFrame([{'currency_id' : 1,
                      'currency_code' : 'USD',
                      'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099')}])
        
        result = create_dim_currency(df)

        assert result['currency_name'].iloc[0] == 'United States Dollar'


    def test_dim_currency_returns_correct_currency_name_for_code_2(self):
        df = pd.DataFrame([{'currency_id' : 1,
                      'currency_code' : 'GBP',
                      'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099')},
                      {'currency_id' : 2,
                      'currency_code' : 'EUR',
                      'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099')}])
        
        result = create_dim_currency(df)

        assert result['currency_name'].iloc[0] == 'British Pound Sterling'
        assert result['currency_name'].iloc[1] == 'Euro'
