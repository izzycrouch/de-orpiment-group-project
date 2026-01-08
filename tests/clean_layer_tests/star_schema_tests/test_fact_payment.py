from clean_layer.star_schema_tables.fact_payment import create_fact_payment
import pandas as pd
from datetime import datetime

class TestFactPayment:
    def test_returns_dataframe(self):

        payment = pd.DataFrame([{
            'payment_id' : 1,
            'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
            'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
            'transaction_id' : 2,
            'counterparty_id' : 3,
            'payment_amount': 2.50,
            'currency_id': 4,
            'payment_type_id': 5,
            'paid': True,
            'payment_date': datetime.fromisoformat('2025-12-15'),
            'company_ac_number' : 12345678,
            'counterparty_ac_number' : 87654321
            }])
        dim_payment_type = pd.DataFrame([{"payment_type_id": 1}])
        dim_transaction = pd.DataFrame([{'transaction_id' : 1}])
        dim_counterparty = pd.DataFrame([{'counterparty_id': 1}])
        dim_currency = pd.DataFrame([{'currency_id': 1}])
        dim_date = pd.DataFrame([{'date_id': 1}])

        df = create_fact_payment(payment, dim_payment_type, dim_transaction, dim_counterparty, dim_currency, dim_date)

        assert isinstance(df, pd.DataFrame)

    def test_correct_columns(self):

        payment = pd.DataFrame([{
            'payment_id' : 1,
            'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
            'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
            'transaction_id' : 2,
            'counterparty_id' : 3,
            'payment_amount': 2.50,
            'currency_id': 4,
            'payment_type_id': 5,
            'paid': True,
            'payment_date': datetime.fromisoformat('2025-12-15'),
            'company_ac_number' : 12345678,
            'counterparty_ac_number' : 87654321
            }])
        dim_payment_type = pd.DataFrame([{"payment_type_id": 1}])
        dim_transaction = pd.DataFrame([{'transaction_id' : 1}])
        dim_counterparty = pd.DataFrame([{'counterparty_id': 1}])
        dim_currency = pd.DataFrame([{'currency_id': 1}])
        dim_date = pd.DataFrame([{'date_id': 1}])

        df = create_fact_payment(payment, dim_payment_type, dim_transaction, dim_counterparty, dim_currency, dim_date)

        expected_cols = [
                "payment_record_id",
                "payment_id",
                "created_date",
                "created_time",
                "last_updated_date",
                "last_updated_time",
                "transaction_id",
                "counterparty_id",
                "payment_amount",
                "currency_id",
                "payment_type_id",
                "paid",
                "payment_date"
            ]

        assert (expected_cols == df.columns).any()

    def test_correct_data_types(self):

        payment = pd.DataFrame([{
            'payment_id' : 1,
            'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
            'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
            'transaction_id' : 2,
            'counterparty_id' : 3,
            'payment_amount': 2.50,
            'currency_id': 4,
            'payment_type_id': 5,
            'paid': True,
            'payment_date': datetime.fromisoformat('2025-12-15'),
            'company_ac_number' : 12345678,
            'counterparty_ac_number' : 87654321
            }])
        dim_payment_type = pd.DataFrame([{"payment_type_id": 1}])
        dim_transaction = pd.DataFrame([{'transaction_id' : 1}])
        dim_counterparty = pd.DataFrame([{'counterparty_id': 1}])
        dim_currency = pd.DataFrame([{'currency_id': 1}])
        dim_date = pd.DataFrame([{'date_id': 1}])

        df = create_fact_payment(payment, dim_payment_type, dim_transaction, dim_counterparty, dim_currency, dim_date)
        print(df.dtypes)
        assert df["payment_record_id"].dtype == int
        assert df["payment_id"].dtype == int
        assert df["created_date"].dtype == 'datetime64[ns]'
        assert df["created_time"].dtype == object
        assert df["last_updated_date"].dtype == 'datetime64[ns]'
        assert df["last_updated_time"].dtype == object
        assert df["transaction_id"].dtype == int
        assert df["counterparty_id"].dtype == int
        assert df["payment_amount"].dtype == float
        assert df["currency_id"].dtype == int
        assert df["payment_type_id"].dtype == int
        assert df["paid"].dtype == bool