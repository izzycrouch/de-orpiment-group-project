from clean_layer.clean_func.clean_sales_order import clean_sales_order
import pandas as pd
import pytest
import boto3
from moto import mock_aws
from datetime import datetime, date
from io import BytesIO

@pytest.fixture(autouse=True)
def aws_mock():
    with mock_aws():
        yield

class TestCleanSalesOrder:
    def test_clean_sales_order_returns_df(self):
        mock_s3 = boto3.client('s3', region_name='eu-west-2')

        bucket_name = "test-bucket"
        file_name = "sales_order/example.parquet"
        test_data = [{'sales_order_id' : 1,
                      'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'design_id' : 1,
                      'staff_id' : 1,
                      'counterparty_id' : 1,
                      'units_sold' : 1,
                      'unit_price' : 2.00,
                      'currency_id' : 1,
                      'agreed_delivery_date' : date.fromisoformat('2025-12-16'),
                      'agreed_payment_date' : date.fromisoformat('2025-12-16'),
                      'agreed_delivery_location_id' : 1}]
        test_df = pd.DataFrame(test_data)
        buffer = BytesIO()
        test_df.to_parquet(buffer, index=False)

        mock_s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        mock_s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.getvalue())

        result = clean_sales_order(file_path=file_name, bucket_name=bucket_name)

        assert isinstance(result, pd.DataFrame)


    def test_clean_sales_order_returns_correct_datatypes_for_columns(self):
        mock_s3 = boto3.client('s3', region_name='eu-west-2')

        bucket_name = "test-bucket"
        file_name = "sales_order/example.parquet"
        test_data = [{'sales_order_id' : 1,
                      'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'design_id' : 1,
                      'staff_id' : 1,
                      'counterparty_id' : 1,
                      'units_sold' : 1,
                      'unit_price' : 2.00,
                      'currency_id' : 1,
                      'agreed_delivery_date' : date.fromisoformat('2025-12-16'),
                      'agreed_payment_date' : date.fromisoformat('2025-12-16'),
                      'agreed_delivery_location_id' : 1}]
        test_df = pd.DataFrame(test_data)
        buffer = BytesIO()
        test_df.to_parquet(buffer, index=False)

        mock_s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        mock_s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.getvalue())

        result = clean_sales_order(file_path=file_name, bucket_name=bucket_name)

        assert result['sales_order_id'].dtype == 'int64'
        assert result['created_at'].dtype == 'datetime64[ns]'
        assert result['last_updated'].dtype == 'datetime64[ns]'
        assert result['design_id'].dtype == 'int64'
        assert result['staff_id'].dtype == 'int64'
        assert result['counterparty_id'].dtype == 'int64'
        assert result['units_sold'].dtype == 'int64'
        assert result['unit_price'].dtype == 'float32'
        assert result['currency_id'].dtype == 'int64'
        assert result['agreed_delivery_date'].dtype == 'datetime64[ns]'
        assert result['agreed_payment_date'].dtype == 'datetime64[ns]'
        assert result['agreed_delivery_location_id'].dtype == 'int64'


    def test_clean_sales_order_returns_correct_datatypes_for_sales_order_id_when_input_is_string(self):
        mock_s3 = boto3.client('s3', region_name='eu-west-2')

        bucket_name = "test-bucket"
        file_name = "sales_order/example.parquet"
        test_data = [{'sales_order_id' : '1',
                      'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'design_id' : 1,
                      'staff_id' : 1,
                      'counterparty_id' : 1,
                      'units_sold' : 1,
                      'unit_price' : 2.00,
                      'currency_id' : 1,
                      'agreed_delivery_date' : date.fromisoformat('2025-12-16'),
                      'agreed_payment_date' : date.fromisoformat('2025-12-16'),
                      'agreed_delivery_location_id' : 1}]
        test_df = pd.DataFrame(test_data)
        buffer = BytesIO()
        test_df.to_parquet(buffer, index=False)

        mock_s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        mock_s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.getvalue())

        result = clean_sales_order(file_path=file_name, bucket_name=bucket_name)

        assert result['sales_order_id'].dtype == 'int64'


    def test_clean_sales_order_returns_correct_datatypes_for_date_columns_when_input_is_string(self):
        mock_s3 = boto3.client('s3', region_name='eu-west-2')

        bucket_name = "test-bucket"
        file_name = "sales_order/example.parquet"
        test_data = [{'sales_order_id' : '1',
                      'created_at' : '2025-12-15 15:51:20.825099',
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'design_id' : 1,
                      'staff_id' : 1,
                      'counterparty_id' : 1,
                      'units_sold' : 1,
                      'unit_price' : 2.00,
                      'currency_id' : 1,
                      'agreed_delivery_date' : date.fromisoformat('2025-12-16'),
                      'agreed_payment_date' : date.fromisoformat('2025-12-16'),
                      'agreed_delivery_location_id' : 1}]
        test_df = pd.DataFrame(test_data)
        buffer = BytesIO()
        test_df.to_parquet(buffer, index=False)

        mock_s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        mock_s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.getvalue())

        result = clean_sales_order(file_path=file_name, bucket_name=bucket_name)

        assert result['created_at'].dtype == 'datetime64[ns]'


    def test_clean_sales_order_returns_correct_datatype_for_unit_price_when_input_is_whole_number(self):
        mock_s3 = boto3.client('s3', region_name='eu-west-2')

        bucket_name = "test-bucket"
        file_name = "sales_order/example.parquet"
        test_data = [{'sales_order_id' : '1',
                      'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'design_id' : 1,
                      'staff_id' : 1,
                      'counterparty_id' : 1,
                      'units_sold' : 1,
                      'unit_price' : 2,
                      'currency_id' : 1,
                      'agreed_delivery_date' : date.fromisoformat('2025-12-16'),
                      'agreed_payment_date' : date.fromisoformat('2025-12-16'),
                      'agreed_delivery_location_id' : 1}]
        test_df = pd.DataFrame(test_data)
        buffer = BytesIO()
        test_df.to_parquet(buffer, index=False)

        mock_s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        mock_s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.getvalue())

        result = clean_sales_order(file_path=file_name, bucket_name=bucket_name)

        assert result['unit_price'].dtype == 'float32'


    def test_clean_sales_order_removes_data_if_datatype_cant_be_cast(self):
        mock_s3 = boto3.client('s3', region_name='eu-west-2')

        bucket_name = "test-bucket"
        file_name = "sales_order/example.parquet"
        test_data = [{'sales_order_id' : 'test',
                      'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'design_id' : 1,
                      'staff_id' : 1,
                      'counterparty_id' : 1,
                      'units_sold' : 1,
                      'unit_price' : 2.00,
                      'currency_id' : 1,
                      'agreed_delivery_date' : date.fromisoformat('2025-12-16'),
                      'agreed_payment_date' : date.fromisoformat('2025-12-16'),
                      'agreed_delivery_location_id' : 1}]
        test_df = pd.DataFrame(test_data)
        buffer = BytesIO()
        test_df.to_parquet(buffer, index=False)

        mock_s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        mock_s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.getvalue())

        result = clean_sales_order(file_path=file_name, bucket_name=bucket_name)

        assert result.empty


    def test_clean_sales_order_removes_data_with_nulls(self):
        mock_s3 = boto3.client('s3', region_name='eu-west-2')

        bucket_name = "test-bucket"
        file_name = "sales_order/example.parquet"
        test_data = [{'sales_order_id' : None,
                      'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'design_id' : 1,
                      'staff_id' : 1,
                      'counterparty_id' : 1,
                      'units_sold' : 1,
                      'unit_price' : 2.00,
                      'currency_id' : 1,
                      'agreed_delivery_date' : date.fromisoformat('2025-12-16'),
                      'agreed_payment_date' : date.fromisoformat('2025-12-16'),
                      'agreed_delivery_location_id' : 1}]
        test_df = pd.DataFrame(test_data)
        buffer = BytesIO()
        test_df.to_parquet(buffer, index=False)

        mock_s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        mock_s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.getvalue())

        result = clean_sales_order(file_path=file_name, bucket_name=bucket_name)

        assert result.empty

    def test_clean_sales_order_removes_data_if_duplicate_sales_order_id(self):
        mock_s3 = boto3.client('s3', region_name='eu-west-2')

        bucket_name = "test-bucket"
        file_name = "sales_order/example.parquet"
        test_data = [{'sales_order_id' : 1,
                      'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'design_id' : 1,
                      'staff_id' : 1,
                      'counterparty_id' : 1,
                      'units_sold' : 1,
                      'unit_price' : 2.00,
                      'currency_id' : 1,
                      'agreed_delivery_date' : date.fromisoformat('2025-12-16'),
                      'agreed_payment_date' : date.fromisoformat('2025-12-16'),
                      'agreed_delivery_location_id' : 1},
                      {'sales_order_id' : 1,
                      'created_at' : datetime.fromisoformat('2025-12-12 15:51:20.825099'),
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'design_id' : 1,
                      'staff_id' : 2,
                      'counterparty_id' : 1,
                      'units_sold' : 1,
                      'unit_price' : 2.00,
                      'currency_id' : 1,
                      'agreed_delivery_date' : date.fromisoformat('2025-12-16'),
                      'agreed_payment_date' : date.fromisoformat('2025-12-16'),
                      'agreed_delivery_location_id' : 1}]
        test_df = pd.DataFrame(test_data)
        buffer = BytesIO()
        test_df.to_parquet(buffer, index=False)

        mock_s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        mock_s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.getvalue())

        result = clean_sales_order(file_path=file_name, bucket_name=bucket_name)

        assert result['staff_id'].iloc[0] == 1
        assert result.shape == (1, 12)


    def test_clean_sales_order_removes_data_if_created_at_date_after_current_time(self):
        mock_s3 = boto3.client('s3', region_name='eu-west-2')

        bucket_name = "test-bucket"
        file_name = "sales_order/example.parquet"
        test_data = [{'sales_order_id' : 1,
                      'created_at' : datetime.fromisoformat('2050-12-15 15:51:20.825099'),
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'design_id' : 1,
                      'staff_id' : 1,
                      'counterparty_id' : 1,
                      'units_sold' : 1,
                      'unit_price' : 2.00,
                      'currency_id' : 1,
                      'agreed_delivery_date' : date.fromisoformat('2025-12-16'),
                      'agreed_payment_date' : date.fromisoformat('2025-12-16'),
                      'agreed_delivery_location_id' : 1}]
        test_df = pd.DataFrame(test_data)
        buffer = BytesIO()
        test_df.to_parquet(buffer, index=False)

        mock_s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        mock_s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.getvalue())

        result = clean_sales_order(file_path=file_name, bucket_name=bucket_name)

        assert result.empty


    def test_clean_sales_order_removes_data_if_last_updated_date_before_created_at(self):
        mock_s3 = boto3.client('s3', region_name='eu-west-2')

        bucket_name = "test-bucket"
        file_name = "sales_order/example.parquet"
        test_data = [{'sales_order_id' : 1,
                      'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'last_updated' : datetime.fromisoformat('2025-12-14 15:51:20.825099'),
                      'design_id' : 1,
                      'staff_id' : 1,
                      'counterparty_id' : 1,
                      'units_sold' : 1,
                      'unit_price' : 2.00,
                      'currency_id' : 1,
                      'agreed_delivery_date' : date.fromisoformat('2025-12-16'),
                      'agreed_payment_date' : date.fromisoformat('2025-12-16'),
                      'agreed_delivery_location_id' : 1}]
        test_df = pd.DataFrame(test_data)
        buffer = BytesIO()
        test_df.to_parquet(buffer, index=False)

        mock_s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        mock_s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.getvalue())

        result = clean_sales_order(file_path=file_name, bucket_name=bucket_name)

        assert result.empty


    def test_clean_sales_order_removes_data_if_delivery_date_or_payment_date_before_created_at(self):
        mock_s3 = boto3.client('s3', region_name='eu-west-2')

        bucket_name = "test-bucket"
        file_name = "sales_order/example.parquet"
        test_data = [{'sales_order_id' : 1,
                      'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'design_id' : 1,
                      'staff_id' : 1,
                      'counterparty_id' : 1,
                      'units_sold' : 1,
                      'unit_price' : 2.00,
                      'currency_id' : 1,
                      'agreed_delivery_date' : date.fromisoformat('2024-12-16'),
                      'agreed_payment_date' : date.fromisoformat('2025-12-16'),
                      'agreed_delivery_location_id' : 1},
                      {'sales_order_id' : 2,
                      'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                      'design_id' : 1,
                      'staff_id' : 1,
                      'counterparty_id' : 1,
                      'units_sold' : 1,
                      'unit_price' : 2.00,
                      'currency_id' : 1,
                      'agreed_delivery_date' : date.fromisoformat('2025-12-16'),
                      'agreed_payment_date' : date.fromisoformat('2024-12-16'),
                      'agreed_delivery_location_id' : 1}]
        test_df = pd.DataFrame(test_data)
        buffer = BytesIO()
        test_df.to_parquet(buffer, index=False)

        mock_s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        mock_s3.put_object(Bucket=bucket_name, Key=file_name, Body=buffer.getvalue())

        result = clean_sales_order(file_path=file_name, bucket_name=bucket_name)

        assert result.empty