from clean_layer.star_schema_tables.dim_payment_type import create_dim_payment_type
import pandas as pd
from datetime import datetime

class TestDimPaymentType:
    def test_returns_dataframe(self):
        
        test_data = [{
            "payment_type_id": 1,
            "payment_type_name": "SALES_RECEIPT",
            "created_at": datetime.fromisoformat("2022-11-03 14:20:49.962000"),
            "last_updated": datetime.fromisoformat("2022-11-03 14:20:49.962000"),
            }]
        cleaned = pd.DataFrame(test_data)
        df = create_dim_payment_type(cleaned)

        assert isinstance(df, pd.DataFrame)

    def test_correct_columns(self):
        
        test_data = [{
            "payment_type_id": 1,
            "payment_type_name": "SALES_RECEIPT",
            "created_at": datetime.fromisoformat("2022-11-03 14:20:49.962000"),
            "last_updated": datetime.fromisoformat("2022-11-03 14:20:49.962000"),
            }]
        cleaned = pd.DataFrame(test_data)
        df = create_dim_payment_type(cleaned)

        expected_cols = [
                'payment_type_id',
                'payment_type_name'
            ]

        assert (expected_cols == df.columns).any()

    def test_correct_data_types(self):

        test_data = [{
            "payment_type_id": 1,
            "payment_type_name": "SALES_RECEIPT",
            "created_at": datetime.fromisoformat("2022-11-03 14:20:49.962000"),
            "last_updated": datetime.fromisoformat("2022-11-03 14:20:49.962000"),
            }]
        cleaned = pd.DataFrame(test_data)
        df = create_dim_payment_type(cleaned)

        assert df["payment_type_id"].dtype == "int64"
        assert df["payment_type_name"].dtype == object
