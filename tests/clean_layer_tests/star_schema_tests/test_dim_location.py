from clean_layer.star_schema_tables.dim_location import create_dim_location
import pandas as pd
from datetime import datetime
from clean_layer.clean_func.clean_address import clean_address


class TestDimLocation:
    def test_returns_dataframe(self):
        test_data = [{
            "address_id": 1,
            "address_line_1": "123 Baker Street",
            "address_line_2": "Apt 4",
            "district": "Central",
            "city": "London",
            "postal_code": "NW1 6XE",
            "country": "UK",
            "phone": "0123456789",
            "created_at": datetime.fromisoformat("2022-11-03 14:20:49.962000"),
            "last_updated": datetime.fromisoformat("2022-11-03 14:20:49.962000"),
        }]
        cleaned = pd.DataFrame(test_data)

        df = create_dim_location(cleaned)

        assert isinstance(df, pd.DataFrame)

    def test_correct_columns(self):
        test_data = [{
            "address_id": 1,
            "address_line_1": "123 Baker Street",
            "address_line_2": "Apt 4",
            "district": "Central",
            "city": "London",
            "postal_code": "NW1 6XE",
            "country": "UK",
            "phone": "0123456789",
            "created_at": datetime.fromisoformat("2022-11-03 14:20:49.962000"),
            "last_updated": datetime.fromisoformat("2022-11-03 14:20:49.962000"),
        }]
        cleaned = pd.DataFrame(test_data)

        df = create_dim_location(cleaned)

        expected_cols = [
            "location_id",
            "address_line_1",
            "address_line_2",
            "district",
            "city",
            "postal_code",
            "country",
            "phone",
        ]

        assert list(df.columns) == expected_cols

    def test_correct_data_types(self):
        test_data = [{
            "address_id": 1,
            "address_line_1": "123 Baker Street",
            "address_line_2": "Apt 4",
            "district": "Central",
            "city": "London",
            "postal_code": "NW1 6XE",
            "country": "UK",
            "phone": "0123456789",
            "created_at": datetime.fromisoformat("2022-11-03 14:20:49.962000"),
            "last_updated": datetime.fromisoformat("2022-11-03 14:20:49.962000"),
        }]
        cleaned = pd.DataFrame(test_data)
        # print(cleaned)
        # cleaned = clean_address(cleaned)
        print(cleaned)
        df = create_dim_location(cleaned)
        print(df)

        assert df["location_id"].dtype == "int64"
        assert df["address_line_1"].dtype == object
        assert df["address_line_2"].dtype == object
        assert df["district"].dtype == object
        assert df["city"].dtype == object
        assert df["postal_code"].dtype == object
        assert df["country"].dtype == object
        assert df["phone"].dtype == object
        assert False
