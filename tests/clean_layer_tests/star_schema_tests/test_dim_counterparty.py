import pandas as pd
import pytest
from moto import mock_aws
from io import BytesIO
import boto3
from datetime import datetime
from clean_layer.star_schema_tables.dim_counterparty import create_dim_counterparty

@pytest.fixture(autouse=True)
def aws_mock():
    with mock_aws():
        yield

@pytest.fixture()
def mock_s3_bucket():
    s3 = boto3.client("s3", region_name="eu-west-2")
    s3.create_bucket(
        Bucket='test_bucket',
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )   
    return s3

class TestDimCounterparty:
    def _upload_parquet(self, mock_s3_bucket, df, key, bucket_name='test_bucket'):
        buffer = BytesIO()
        df.to_parquet(buffer, index=False)

        mock_s3_bucket.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=buffer.getvalue(),
        )

    def test_dim_counterparty_returns_isinstance_dataframe(self, mock_s3_bucket):
        counterparty_key='counterparty/example.parquet'
        cp_df = pd.DataFrame([{"counterparty_id": 1,
                        "counterparty_legal_name": "Fahey and Sons",
                        "legal_address_id": 1,
                        "commercial_contact": "Micheal Toy",
                        "delivery_contact": "Mrs. Lucy Runolfsdottir",
                        "created_at": datetime.fromisoformat("2022-11-03 14:20:51.563000"),
                        "last_updated": datetime.fromisoformat("2022-11-03 14:20:51.563000")}])
        self._upload_parquet(mock_s3_bucket=mock_s3_bucket, df=cp_df, key=counterparty_key)

        address_key='address/example.parquet'
        ad_df = pd.DataFrame([{'address_id' : 1,
                                'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                                'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                                'address_line_1' : '123 Some Street',
                                'address_line_2' : None,
                                'district': 'Test',
                                'city': 'Test City',
                                'postal_code': '12345',
                                'country': 'United Kingdom',
                                'phone': '07625532556'}])
        self._upload_parquet(mock_s3_bucket=mock_s3_bucket, df=ad_df, key=address_key)
        
        result = create_dim_counterparty(counterparty_key, address_key, bucket_name='test_bucket')
        assert isinstance(result, pd.DataFrame)
    

    def test_dim_counterparty_returns_correct_column_names(self, mock_s3_bucket):
        counterparty_key='counterparty/example.parquet'
        cp_df = pd.DataFrame([{"counterparty_id": 1,
                        "counterparty_legal_name": "Ms Jane Doe",
                        "legal_address_id": 1,
                        "commercial_contact": "test",
                        "delivery_contact": "test",
                        "created_at": datetime.fromisoformat("2022-11-03 14:20:51.563000"),
                        "last_updated": datetime.fromisoformat("2022-11-03 14:20:51.563000")}])
        self._upload_parquet(mock_s3_bucket=mock_s3_bucket, df=cp_df, key=counterparty_key)

        address_key='address/example.parquet'
        ad_df = pd.DataFrame([{'address_id' : 1,
                                'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                                'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                                'address_line_1' : '123 Some Street',
                                'address_line_2' : None,
                                'district': 'Test',
                                'city': 'Test City',
                                'postal_code': '12345',
                                'country': 'United Kingdom',
                                'phone': '07625532556'}])
        self._upload_parquet(mock_s3_bucket=mock_s3_bucket, df=ad_df, key=address_key)
        
        expected_columns = ['counterparty_id', 'counterparty_legal_name', 'counterparty_legal_address_line_1', 'counterparty_legal_address_line_2', 'counterparty_legal_district', 'counterparty_legal_city', 'counterparty_legal_postal_code', 'counterparty_legal_country', 'counterparty_legal_phone_number']

        result = create_dim_counterparty(counterparty_key, address_key, bucket_name='test_bucket')
        output_columns = result.columns.values.tolist()

        assert expected_columns == output_columns
    

    def test_dim_counterparty_returns_correct_column_datatypes(self, mock_s3_bucket):
        counterparty_key='counterparty/example.parquet'
        cp_df = pd.DataFrame([{"counterparty_id": 1,
                        "counterparty_legal_name": "Ms Jane Doe",
                        "legal_address_id": 1,
                        "commercial_contact": "test",
                        "delivery_contact": "test",
                        "created_at": datetime.fromisoformat("2022-11-03 14:20:51.563000"),
                        "last_updated": datetime.fromisoformat("2022-11-03 14:20:51.563000")}])
        self._upload_parquet(mock_s3_bucket=mock_s3_bucket, df=cp_df, key=counterparty_key)

        address_key='address/example.parquet'
        ad_df = pd.DataFrame([{'address_id' : 1,
                                'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                                'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                                'address_line_1' : '123 Some Street',
                                'address_line_2' : None,
                                'district': 'Test',
                                'city': 'Test City',
                                'postal_code': '12345',
                                'country': 'United Kingdom',
                                'phone': '07625532556'}])
        self._upload_parquet(mock_s3_bucket=mock_s3_bucket, df=ad_df, key=address_key)
        
        expected_columns = ['counterparty_id', 'counterparty_legal_name', 'counterparty_legal_address_line_1', 'counterparty_legal_address_line_2', 'counterparty_legal_district', 'counterparty_legal_city', 'counterparty_legal_postal_code', 'counterparty_legal_country', 'counterparty_legal_phone_number']

        result = create_dim_counterparty(counterparty_key, address_key, bucket_name='test_bucket')

        assert result["counterparty_id"].dtypes == int
        assert result["counterparty_legal_name"].dtypes == 'string'
        assert result["counterparty_legal_address_line_1"].dtypes == object
        assert result["counterparty_legal_address_line_2"].dtypes == object
        assert result["counterparty_legal_district"].dtypes == object
        assert result["counterparty_legal_city"].dtypes == object
        assert result["counterparty_legal_postal_code"].dtypes =='string'
        assert result["counterparty_legal_country"].dtypes == object
        assert result["counterparty_legal_phone_number"].dtypes == 'string'


    def test_dim_counterparty_returns_merged_data_if_address_id_the_same(self, mock_s3_bucket):
        counterparty_key='counterparty/example.parquet'
        cp_df = pd.DataFrame([{"counterparty_id": 1,
                        "counterparty_legal_name": "Ms Jane Doe",
                        "legal_address_id": 1,
                        "commercial_contact": "test",
                        "delivery_contact": "test",
                        "created_at": datetime.fromisoformat("2022-11-03 14:20:51.563000"),
                        "last_updated": datetime.fromisoformat("2022-11-03 14:20:51.563000")}])
        self._upload_parquet(mock_s3_bucket=mock_s3_bucket, df=cp_df, key=counterparty_key)

        address_key='address/example.parquet'
        ad_df = pd.DataFrame([{'address_id' : 1,
                                'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                                'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                                'address_line_1' : '123 Some Street',
                                'address_line_2' : None,
                                'district': 'Test',
                                'city': 'Test City',
                                'postal_code': '12345',
                                'country': 'United Kingdom',
                                'phone': '07625532556'}])
        self._upload_parquet(mock_s3_bucket=mock_s3_bucket, df=ad_df, key=address_key)

        result = create_dim_counterparty(counterparty_key, address_key, bucket_name='test_bucket')

        assert result.shape == (1,9)
    

    def test_dim_counterparty_returns_merged_data_if_address_id_the_same_with_seperate_counterparty_ids(self, mock_s3_bucket):
        counterparty_key='counterparty/example.parquet'
        cp_df = pd.DataFrame([{"counterparty_id": 1,
                        "counterparty_legal_name": "Ms Jane Doe",
                        "legal_address_id": 1,
                        "commercial_contact": "test",
                        "delivery_contact": "test",
                        "created_at": datetime.fromisoformat("2022-11-03 14:20:51.563000"),
                        "last_updated": datetime.fromisoformat("2022-11-03 14:20:51.563000")}, 
                        {"counterparty_id": 2,
                        "counterparty_legal_name": "Mr John Doe",
                        "legal_address_id": 1,
                        "commercial_contact": "test",
                        "delivery_contact": "test",
                        "created_at": datetime.fromisoformat("2022-11-03 14:20:51.563000"),
                        "last_updated": datetime.fromisoformat("2022-11-03 14:20:51.563000")}]
                        )
        self._upload_parquet(mock_s3_bucket=mock_s3_bucket, df=cp_df, key=counterparty_key)

        address_key='address/example.parquet'
        ad_df = pd.DataFrame([{'address_id' : 1,
                                'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                                'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                                'address_line_1' : '123 Some Street',
                                'address_line_2' : None,
                                'district': 'Test',
                                'city': 'Test City',
                                'postal_code': '12345',
                                'country': 'United Kingdom',
                                'phone': '07625532556'}])
        self._upload_parquet(mock_s3_bucket=mock_s3_bucket, df=ad_df, key=address_key)

        result = create_dim_counterparty(counterparty_key, address_key, bucket_name='test_bucket')

        assert result.shape == (2,9)