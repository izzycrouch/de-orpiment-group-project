import pandas as pd
import boto3
import pytest
from moto import mock_aws
from datetime import datetime
from io import BytesIO

from clean_layer.clean_func.clean_department import clean_department


@pytest.fixture(autouse=True)
def aws_mock():
    with mock_aws():
        yield


class TestCleanDepartment:

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

    def test_clean_department_returns_dataframe(self):
        bucket = "test-bucket"
        key = "department/example.parquet"

        df = pd.DataFrame(
            [
                {
                    "department_id": 1,
                    "department_name": "Sales",
                    "location": "Manchester",
                    "manager": "Richard Roma",
                    "created_at": datetime.fromisoformat("2022-11-03 14:20:49.962000"),
                    "last_updated": datetime.fromisoformat("2022-11-03 14:20:49.962000"),
                }
            ]
        )

        self._upload_parquet(df, bucket, key)

        result = clean_department(file_path=key, bucket_name=bucket)

        assert isinstance(result, pd.DataFrame)

    def test_clean_department_casts_correct_dtypes(self):
        bucket = "test-bucket"
        key = "department/example.parquet"

        df = pd.DataFrame(
            [
                {
                    "department_id": "1",
                    "department_name": "Sales",
                    "location": "Manchester",
                    "manager": "Richard Roma",
                    "created_at": "2022-11-03 14:20:49.962000",
                    "last_updated": "2022-11-03 14:20:49.962000",
                }
            ]
        )

        self._upload_parquet(df, bucket, key)

        result = clean_department(file_path=key, bucket_name=bucket)

        assert result["department_id"].dtype == "int64"
        assert result["department_name"].dtype.name == "string"
        assert result["location"].dtype.name == "string"
        assert result["manager"].dtype.name == "string"
        assert result["created_at"].dtype == "datetime64[ns]"
        assert result["last_updated"].dtype == "datetime64[ns]"

    def test_clean_department_drops_rows_with_nulls(self):
        bucket = "test-bucket"
        key = "department/example.parquet"

        df = pd.DataFrame(
            [
                {
                    "department_id": None,
                    "department_name": "Sales",
                    "location": "Manchester",
                    "manager": "Richard Roma",
                    "created_at": datetime.fromisoformat("2022-11-03 14:20:49.962000"),
                    "last_updated": datetime.fromisoformat("2022-11-03 14:20:49.962000"),
                }
            ]
        )

        self._upload_parquet(df, bucket, key)

        result = clean_department(file_path=key, bucket_name=bucket)

        assert result.empty

    def test_clean_department_drops_empty_strings(self):
        bucket = "test-bucket"
        key = "department/example.parquet"

        df = pd.DataFrame(
            [
                {
                    "department_id": 1,
                    "department_name": "   ",
                    "location": "Manchester",
                    "manager": "Richard Roma",
                    "created_at": datetime.fromisoformat("2022-11-03 14:20:49.962000"),
                    "last_updated": datetime.fromisoformat("2022-11-03 14:20:49.962000"),
                }
            ]
        )

        self._upload_parquet(df, bucket, key)

        result = clean_department(file_path=key, bucket_name=bucket)

        assert result.empty

    def test_clean_department_removes_duplicate_department_id_keeps_latest(self):
        bucket = "test-bucket"
        key = "department/example.parquet"

        df = pd.DataFrame(
            [
                {
                    "department_id": 1,
                    "department_name": "Sales",
                    "location": "Manchester",
                    "manager": "Old Manager",
                    "created_at": datetime.fromisoformat("2022-11-01 10:00:00"),
                    "last_updated": datetime.fromisoformat("2022-11-01 10:00:00"),
                },
                {
                    "department_id": 1,
                    "department_name": "Sales",
                    "location": "Manchester",
                    "manager": "New Manager",
                    "created_at": datetime.fromisoformat("2022-11-02 10:00:00"),
                    "last_updated": datetime.fromisoformat("2022-11-02 10:00:00"),
                },
            ]
        )

        self._upload_parquet(df, bucket, key)

        result = clean_department(file_path=key, bucket_name=bucket)

        assert len(result) == 1
        assert result["manager"].iloc[0] == "New Manager"

    # def test_clean_department_drops_future_dates(self):
    #     bucket = "test-bucket"
    #     key = "department/example.parquet"

    #     df = pd.DataFrame(
    #         [
    #             {
    #                 "department_id": 1,
    #                 "department_name": "Sales",
    #                 "location": "Manchester",
    #                 "manager": "Richard Roma",
    #                 "created_at": datetime.fromisoformat("2050-01-01 00:00:00"),
    #                 "last_updated": datetime.fromisoformat("2050-01-01 00:00:00"),
    #             }
    #         ]
    #     )

    #     self._upload_parquet(df, bucket, key)

    #     result = clean_department(file_path=key, bucket_name=bucket)

    #     assert result.empty

    # def test_clean_department_drops_last_updated_before_created_at(self):
    #     bucket = "test-bucket"
    #     key = "department/example.parquet"

    #     df = pd.DataFrame(
    #         [
    #             {
    #                 "department_id": 1,
    #                 "department_name": "Sales",
    #                 "location": "Manchester",
    #                 "manager": "Richard Roma",
    #                 "created_at": datetime.fromisoformat("2022-11-03 14:20:49.962000"),
    #                 "last_updated": datetime.fromisoformat("2022-11-01 14:20:49.962000"),
    #             }
    #         ]
    #     )

    #     self._upload_parquet(df, bucket, key)

    #     result = clean_department(file_path=key, bucket_name=bucket)

    #     assert result.empty
