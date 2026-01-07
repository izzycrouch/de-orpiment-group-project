import pandas as pd
import pytest
import boto3
from moto import mock_aws
from datetime import datetime
from io import BytesIO

from clean_layer.clean_func.clean_purchase_order import clean_purchase_order


@pytest.fixture(autouse=True)
def aws_mock():
    with mock_aws():
        yield


class TestCleanPurchaseOrder:

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

    def test_clean_purchase_order_returns_dataframe(self):
        bucket = "test-bucket"
        key = "purchase_order/example.parquet"

        df = pd.DataFrame(
            [
                {
                    "purchase_order_id": 1,
                    "created_at": datetime.fromisoformat("2022-11-03 14:20:52.187000"),
                    "last_updated": datetime.fromisoformat("2022-11-03 14:20:52.187000"),
                    "staff_id": 12,
                    "counterparty_id": 11,
                    "item_code": "ZDOI5EA",
                    "item_quantity": 371,
                    "item_unit_price": 361.39,
                    "currency_id": 2,
                    "agreed_delivery_date": "2022-11-09",
                    "agreed_payment_date": "2022-11-07",
                    "agreed_delivery_location_id": 6,
                }
            ]
        )

        self._upload_parquet(df, bucket, key)
        result = clean_purchase_order(file_path=key, bucket_name=bucket)

        assert isinstance(result, pd.DataFrame)

    def test_clean_purchase_order_casts_correct_dtypes(self):
        bucket = "test-bucket"
        key = "purchase_order/example.parquet"

        df = pd.DataFrame(
            [
                {
                    "purchase_order_id": "1",
                    "created_at": datetime.fromisoformat("2022-11-03 14:20:52.187000"),
                    "last_updated": datetime.fromisoformat("2022-11-03 14:20:52.187000"),
                    "staff_id": "12",
                    "counterparty_id": "11",
                    "item_code": "ZDOI5EA",
                    "item_quantity": "371",
                    "item_unit_price": "361.39",
                    "currency_id": "2",
                    "agreed_delivery_date": "2022-11-09",
                    "agreed_payment_date": "2022-11-07",
                    "agreed_delivery_location_id": "6",
                }
            ]
        )

        self._upload_parquet(df, bucket, key)
        result = clean_purchase_order(file_path=key, bucket_name=bucket)

        assert result["purchase_order_id"].dtype == "int64"
        assert result["staff_id"].dtype == "int64"
        assert result["counterparty_id"].dtype == "int64"
        assert result["item_quantity"].dtype == "int64"
        assert result["currency_id"].dtype == "int64"
        assert result["agreed_delivery_location_id"].dtype == "int64"
        assert result["item_unit_price"].dtype == "float64"
        assert result["item_code"].dtype.name == "string"

    def test_clean_purchase_order_drops_rows_with_invalid_numeric_types(self):
        bucket = "test-bucket"
        key = "purchase_order/example.parquet"

        df = pd.DataFrame(
            [
                {
                    "purchase_order_id": "abc",
                    "created_at": datetime.fromisoformat("2022-11-03 14:20:52.187000"),
                    "last_updated": datetime.fromisoformat("2022-11-03 14:20:52.187000"),
                    "staff_id": 12,
                    "counterparty_id": 11,
                    "item_code": "ZDOI5EA",
                    "item_quantity": 371,
                    "item_unit_price": 361.39,
                    "currency_id": 2,
                    "agreed_delivery_date": "2022-11-09",
                    "agreed_payment_date": "2022-11-07",
                    "agreed_delivery_location_id": 6,
                }
            ]
        )

        self._upload_parquet(df, bucket, key)
        result = clean_purchase_order(file_path=key, bucket_name=bucket)

        assert result.empty

    def test_clean_purchase_order_drops_null_values(self):
        bucket = "test-bucket"
        key = "purchase_order/example.parquet"

        df = pd.DataFrame(
            [
                {
                    "purchase_order_id": None,
                    "created_at": datetime.fromisoformat("2022-11-03 14:20:52.187000"),
                    "last_updated": datetime.fromisoformat("2022-11-03 14:20:52.187000"),
                    "staff_id": 12,
                    "counterparty_id": 11,
                    "item_code": "ZDOI5EA",
                    "item_quantity": 371,
                    "item_unit_price": 361.39,
                    "currency_id": 2,
                    "agreed_delivery_date": "2022-11-09",
                    "agreed_payment_date": "2022-11-07",
                    "agreed_delivery_location_id": 6,
                }
            ]
        )

        self._upload_parquet(df, bucket, key)
        result = clean_purchase_order(file_path=key, bucket_name=bucket)

        assert result.empty

    def test_clean_purchase_order_removes_duplicate_ids_keeps_first(self):
        bucket = "test-bucket"
        key = "purchase_order/example.parquet"

        df = pd.DataFrame(
            [
                {
                    "purchase_order_id": 1,
                    "created_at": datetime.fromisoformat("2022-11-01 10:00:00"),
                    "last_updated": datetime.fromisoformat("2022-11-01 10:00:00"),
                    "staff_id": 12,
                    "counterparty_id": 11,
                    "item_code": "AAA111",
                    "item_quantity": 10,
                    "item_unit_price": 1.0,
                    "currency_id": 2,
                    "agreed_delivery_date": "2022-11-09",
                    "agreed_payment_date": "2022-11-07",
                    "agreed_delivery_location_id": 6,
                },
                {
                    "purchase_order_id": 1,
                    "created_at": datetime.fromisoformat("2022-11-02 10:00:00"),
                    "last_updated": datetime.fromisoformat("2022-11-02 10:00:00"),
                    "staff_id": 12,
                    "counterparty_id": 11,
                    "item_code": "BBB222",
                    "item_quantity": 20,
                    "item_unit_price": 2.0,
                    "currency_id": 2,
                    "agreed_delivery_date": "2022-11-10",
                    "agreed_payment_date": "2022-11-08",
                    "agreed_delivery_location_id": 6,
                },
            ]
        )

        self._upload_parquet(df, bucket, key)
        result = clean_purchase_order(file_path=key, bucket_name=bucket)

        assert len(result) == 1
        assert result["created_at"].iloc[0] == datetime.fromisoformat("2022-11-01 10:00:00")
        assert result["item_code"].iloc[0] == "AAA111"

    # def test_clean_purchase_order_drops_future_dates(self):
    #     bucket = "test-bucket"
    #     key = "purchase_order/example.parquet"

    #     df = pd.DataFrame(
    #         [
    #             {
    #                 "purchase_order_id": 1,
    #                 "created_at": datetime.fromisoformat("2050-01-01 00:00:00"),
    #                 "last_updated": datetime.fromisoformat("2050-01-01 00:00:00"),
    #                 "staff_id": 12,
    #                 "counterparty_id": 11,
    #                 "item_code": "ZDOI5EA",
    #                 "item_quantity": 371,
    #                 "item_unit_price": 361.39,
    #                 "currency_id": 2,
    #                 "agreed_delivery_date": "2022-11-09",
    #                 "agreed_payment_date": "2022-11-07",
    #                 "agreed_delivery_location_id": 6,
    #             }
    #         ]
    #     )

    #     self._upload_parquet(df, bucket, key)
    #     result = clean_purchase_order(file_path=key, bucket_name=bucket)

    #     assert result.empty

    # def test_clean_purchase_order_drops_last_updated_before_created_at(self):
        # bucket = "test-bucket"
        # key = "purchase_order/example.parquet"

        # df = pd.DataFrame(
        #     [
        #         {
        #             "purchase_order_id": 1,
        #             "created_at": datetime.fromisoformat("2022-11-03 14:20:52.187000"),
        #             "last_updated": datetime.fromisoformat("2022-11-01 14:20:52.187000"),
        #             "staff_id": 12,
        #             "counterparty_id": 11,
        #             "item_code": "ZDOI5EA",
        #             "item_quantity": 371,
        #             "item_unit_price": 361.39,
        #             "currency_id": 2,
        #             "agreed_delivery_date": "2022-11-09",
        #             "agreed_payment_date": "2022-11-07",
        #             "agreed_delivery_location_id": 6,
        #         }
        #     ]
        # )

        # self._upload_parquet(df, bucket, key)
        # result = clean_purchase_order(file_path=key, bucket_name=bucket)

        # assert result.empty

    # def test_clean_purchase_order_drops_non_positive_quantity_or_price(self):
    #     bucket = "test-bucket"
    #     key = "purchase_order/example.parquet"

    #     df = pd.DataFrame(
    #         [
    #             {
    #                 "purchase_order_id": 1,
    #                 "created_at": datetime.fromisoformat("2022-11-03 14:20:52.187000"),
    #                 "last_updated": datetime.fromisoformat("2022-11-03 14:20:52.187000"),
    #                 "staff_id": 12,
    #                 "counterparty_id": 11,
    #                 "item_code": "ZDOI5EA",
    #                 "item_quantity": 0,
    #                 "item_unit_price": 361.39,
    #                 "currency_id": 2,
    #                 "agreed_delivery_date": "2022-11-09",
    #                 "agreed_payment_date": "2022-11-07",
    #                 "agreed_delivery_location_id": 6,
    #             },
    #             {
    #                 "purchase_order_id": 2,
    #                 "created_at": datetime.fromisoformat("2022-11-03 14:20:52.187000"),
    #                 "last_updated": datetime.fromisoformat("2022-11-03 14:20:52.187000"),
    #                 "staff_id": 12,
    #                 "counterparty_id": 11,
    #                 "item_code": "QLZLEXR",
    #                 "item_quantity": 10,
    #                 "item_unit_price": -1.0,
    #                 "currency_id": 2,
    #                 "agreed_delivery_date": "2022-11-09",
    #                 "agreed_payment_date": "2022-11-07",
    #                 "agreed_delivery_location_id": 6,
    #             },
    #         ]
    #     )

    #     self._upload_parquet(df, bucket, key)
    #     result = clean_purchase_order(file_path=key, bucket_name=bucket)

    #     assert result.empty
