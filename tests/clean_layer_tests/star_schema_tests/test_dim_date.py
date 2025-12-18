from clean_layer.star_schema_tables.dim_date import create_dim_date
import pandas as pd

class TestDimDate:
    def test_dim_date_returns_isinstance_dataframe(self):
        result = create_dim_date(start='2025-11-01', end='2025-11-30')
        assert isinstance(result, pd.DataFrame)
    

    def test_dim_date_returns_correct_column_names(self):
        expected_columns = ['date_id', 'year', 'month', 'day', 'day_of_week', 'day_name', 'month_name', 'quarter']

        result = create_dim_date(start='2025-11-01', end='2025-11-30')
        output_columns = result.columns.values.tolist()

        assert expected_columns == output_columns
    

    def test_dim_date_returns_correct_column_datatypes(self):
        result = create_dim_date(start='2025-11-01', end='2025-11-30')

        assert str(result['date_id'].dtype) == 'datetime64[ns]'
        assert result['year'].dtype == 'int32'
        assert result['month'].dtype == 'int32'
        assert result['day'].dtype == 'int32'
        assert result['day_of_week'].dtype == 'int32'
        assert result['day_name'].dtype == 'object'
        assert result['month_name'].dtype == 'object'
        assert result['quarter'].dtype == 'int32'