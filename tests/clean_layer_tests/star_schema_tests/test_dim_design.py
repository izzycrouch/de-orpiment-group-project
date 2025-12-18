from clean_layer.star_schema_tables.dim_design import create_dim_design
import pandas as pd
from datetime import datetime

class TestDimDesign:
    def test_dim_design_returns_dataframe(self):

        test_data = [{'design_id' : 1,
                      'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'design_name' : 'test',
                      'file_location' : '/test',
                      'file_name' : 'test.json',
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099')}]
        cleaned = pd.DataFrame(test_data)

        df = create_dim_design(cleaned)

        assert isinstance(df, pd.DataFrame)

    def test_correct_columns(self):

        test_data = [{'design_id' : 1,
                      'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'design_name' : 'test',
                      'file_location' : '/test',
                      'file_name' : 'test.json',
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099')}]
        cleaned = pd.DataFrame(test_data)
        
        df = create_dim_design(cleaned)

        expected_cols = [
                'design_id',
                'design_name',
                'file_location',
                'file_name',
            ]
        assert (expected_cols == df.columns).all()

    def test_correct_data_types(self):
        test_data = [{'design_id' : 1,
                      'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'design_name' : 'test',
                      'file_location' : '/test',
                      'file_name' : 'test.json',
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099')}]
        cleaned = pd.DataFrame(test_data)
        
        df = create_dim_design(cleaned)

        assert isinstance(df, pd.DataFrame)

        assert df['design_id'].dtype == int
        assert df['design_name'].dtype == object
        assert df['file_location'].dtype == object
        assert df['file_name'].dtype == object
