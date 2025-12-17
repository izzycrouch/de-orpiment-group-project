import pandas as pd
import pytest
import boto3
from moto import mock_aws
from datetime import datetime
from io import BytesIO

from clean_layer.clean_counterparty import clean_counterparty


@pytest.fixture(autouse=True)
def aws_mock():
    with mock_aws():
        yield


class TestCleanCounterparty:

    def _upload_parquet(self, df, bucket_name, file_name):
        buffer = BytesIO()
        df.to_parquet(buffer, index=False)

        s3 = boto3.client("s3", region_name="eu-west-2")
        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3.put_object(
            Bucket=bucket_name,
            Key=file_name,
            Body=buffer.getvalue(),
        )

    def test_clean_counterparty_returns_dataframe(self):
        bucket = "test-bucket"
        key = "counterparty/example.parquet"

        df = pd.DataFrame(
            [
                {
                    "counterparty_id": 1,
                    "counterparty_legal_name": "Fahey and Sons",
                    "legal_address_id": 15,
                    "commercial_contact": "Micheal Toy",
                    "delivery_contact": "Mrs. Lucy Runolfsdottir",
                    "created_at": datetime.fromisoformat("2022-11-03 14:20:51.563000"),
                    "last_updated": datetime.fromisoformat("2022-11-03 14:20:51.563000"),
                }
            ]
        )

        self._upload_parquet(df, bucket, key)
        result = clean_counterparty(file_path=key, bucket_name=bucket)

        assert isinstance(result, pd.DataFrame)

    def test_clean_counterparty_casts_correct_dtypes(self):
        bucket = "test-bucket"
        key = "counterparty/example.parquet"

        df = pd.DataFrame(
            [
                {
                    "counterparty_id": "1",
                    "counterparty_legal_name": "Fahey and Sons",
                    "legal_address_id": "15",
                    "commercial_contact": "Micheal Toy",
                    "delivery_contact": "Mrs. Lucy Runolfsdottir",
                    "created_at": "2022-11-03 14:20:51.563000",
                    "last_updated": "2022-11-03 14:20:51.563000",
                }
            ]
        )

        self._upload_parquet(df, bucket, key)
        result = clean_counterparty(file_path=key, bucket_name=bucket)

        assert result["counterparty_id"].dtype == "int64"
        assert result["legal_address_id"].dtype == "int64"
        assert result["counterparty_legal_name"].dtype.name == "string"
        assert result["commercial_contact"].dtype.name == "string"
        assert result["delivery_contact"].dtype.name == "string"
        assert result["created_at"].dtype == "datetime64[ns]"
        assert result["last_updated"].dtype == "datetime64[ns]"

    def test_clean_counterparty_drops_rows_with_invalid_numeric_types(self):
        bucket = "test-bucket"
        key = "counterparty/example.parquet"

        df = pd.DataFrame(
            [
                {
                    "counterparty_id": "abc",
                    "counterparty_legal_name": "Fahey and Sons",
                    "legal_address_id": 15,
                    "commercial_contact": "Micheal Toy",
                    "delivery_contact": "Mrs. Lucy Runolfsdottir",
                    "created_at": "2022-11-03 14:20:51.563000",
                    "last_updated": "2022-11-03 14:20:51.563000",
                }
            ]
        )

        self._upload_parquet(df, bucket, key)
        result = clean_counterparty(file_path=key, bucket_name=bucket)

        assert result.empty

    def test_clean_counterparty_drops_null_values(self):
        bucket = "test-bucket"
        key = "counterparty/example.parquet"

        df = pd.DataFrame(
            [
                {
                    "counterparty_id": None,
                    "counterparty_legal_name": "Fahey and Sons",
                    "legal_address_id": 15,
                    "commercial_contact": "Micheal Toy",
                    "delivery_contact": "Mrs. Lucy Runolfsdottir",
                    "created_at": datetime.fromisoformat("2022-11-03 14:20:51.563000"),
                    "last_updated": datetime.fromisoformat("2022-11-03 14:20:51.563000"),
                }
            ]
        )

        self._upload_parquet(df, bucket, key)
        result = clean_counterparty(file_path=key, bucket_name=bucket)

        assert result.empty

    def test_clean_counterparty_removes_duplicate_ids_keeps_first(self):
        bucket = "test-bucket"
        key = "counterparty/example.parquet"

        df = pd.DataFrame(
            [
                {
                    "counterparty_id": 1,
                    "counterparty_legal_name": "Fahey and Sons",
                    "legal_address_id": 15,
                    "commercial_contact": "Micheal Toy",
                    "delivery_contact": "Mrs. Lucy Runolfsdottir",
                    "created_at": datetime.fromisoformat("2022-11-01 10:00:00"),
                    "last_updated": datetime.fromisoformat("2022-11-01 10:00:00"),
                },
                {
                    "counterparty_id": 1,
                    "counterparty_legal_name": "Fahey and Sons Ltd",
                    "legal_address_id": 16,
                    "commercial_contact": "Someone Else",
                    "delivery_contact": "Another Person",
                    "created_at": datetime.fromisoformat("2022-11-02 10:00:00"),
                    "last_updated": datetime.fromisoformat("2022-11-02 10:00:00"),
                },
            ]
        )

        self._upload_parquet(df, bucket, key)
        result = clean_counterparty(file_path=key, bucket_name=bucket)

        assert len(result) == 1
        assert result["created_at"].iloc[0] == datetime.fromisoformat("2022-11-01 10:00:00")
        assert result["legal_address_id"].iloc[0] == 15

    def test_clean_counterparty_drops_future_dates(self):
        bucket = "test-bucket"
        key = "counterparty/example.parquet"

        df = pd.DataFrame(
            [
                {
                    "counterparty_id": 1,
                    "counterparty_legal_name": "Fahey and Sons",
                    "legal_address_id": 15,
                    "commercial_contact": "Micheal Toy",
                    "delivery_contact": "Mrs. Lucy Runolfsdottir",
                    "created_at": datetime.fromisoformat("2050-01-01 00:00:00"),
                    "last_updated": datetime.fromisoformat("2050-01-01 00:00:00"),
                }
            ]
        )

        self._upload_parquet(df, bucket, key)
        result = clean_counterparty(file_path=key, bucket_name=bucket)

        assert result.empty

    def test_clean_counterparty_drops_last_updated_before_created_at(self):
        bucket = "test-bucket"
        key = "counterparty/example.parquet"

        df = pd.DataFrame(
            [
                {
                    "counterparty_id": 1,
                    "counterparty_legal_name": "Fahey and Sons",
                    "legal_address_id": 15,
                    "commercial_contact": "Micheal Toy",
                    "delivery_contact": "Mrs. Lucy Runolfsdottir",
                    "created_at": datetime.fromisoformat("2022-11-03 14:20:51.563000"),
                    "last_updated": datetime.fromisoformat("2022-11-01 14:20:51.563000"),
                }
            ]
        )

        self._upload_parquet(df, bucket, key)
        result = clean_counterparty(file_path=key, bucket_name=bucket)

        assert result.empty
