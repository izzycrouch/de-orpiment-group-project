from clean_layer.star_schema_tables.dim_transaction import create_dim_transaction
from datetime import datetime
import pandas as pd

class TestDimtTransaction:
    def test_returns_dataframe(self):

        test_data = [{
            'transaction_id' : 1,
            'transaction_type' : 'SALE',
            'sales_order_id' : 1,
            'purchase_order_id' : 1,
            'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
            'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099')
            }]
        cleaned = pd.DataFrame(test_data)
        df = create_dim_transaction(cleaned)

        assert isinstance(df, pd.DataFrame)

    def test_correct_columns(self):

        test_data = [{
            'transaction_id' : 1,
            'transaction_type' : 'SALE',
            'sales_order_id' : 1,
            'purchase_order_id' : 1,
            'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
            'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099')
            }]
        cleaned = pd.DataFrame(test_data)
        df = create_dim_transaction(cleaned)

        expected_cols = [
                'transaction_id', 
                'transaction_type',
                'sales_order_id',
                'purchase_order_id'
            ]
        
        assert (expected_cols == df.columns).any()

    def test_correct_data_types(self):

        test_data = [{
            'transaction_id' : 1,
            'transaction_type' : 'SALE',
            'sales_order_id' : 1,
            'purchase_order_id' : 1,
            'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
            'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099')
            }]
        cleaned = pd.DataFrame(test_data)
        df = create_dim_transaction(cleaned)

        assert df['transaction_id'].dtype == 'int64'
        assert df['transaction_type'].dtype == object
        assert df['sales_order_id'].dtype == 'int64'
        assert df['purchase_order_id'].dtype == 'int64'