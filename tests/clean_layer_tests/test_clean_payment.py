from clean_layer.clean_payment import clean_payment_table
import pandas as pd
from datetime import datetime

class TestCleanDf:

    def test_correct_data_types(self):
        df = clean_payment_table()

        assert isinstance(df, pd.DataFrame)
        assert df["payment_id"].dtypes == int
        assert df["created_at"].dtypes == 'datetime64[ns]'
        assert df["transaction_id"].dtypes == int
        assert df["payment_amount"].dtypes == float
        assert df["paid"].dtypes == bool
        assert df["payment_date"].dtypes == 'datetime64[ns]'
        assert df["company_ac_number"].dtypes == int

    def test_no_null_values(self):
        df = clean_payment_table()
        null_mask = df.isnull().any(axis=1)

        assert null_mask.any() == False

    def test_valid_datetime(self):
        df = clean_payment_table()
        today = datetime.today()
        
        assert (df['payment_date'] <= today).all()
        assert (df['created_at'] <= today).all()
        assert (df['last_updated'] <= today).all()
        