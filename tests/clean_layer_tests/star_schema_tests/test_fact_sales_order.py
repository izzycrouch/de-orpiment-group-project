import pandas as pd
from datetime import datetime
from clean_layer.star_schema_tables.dim_location import create_dim_location


class TestDimLocation:
    def test_returns_dataframe(self):
        df = pd.DataFrame([
            {
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
            }
        ])

        result = create_dim_location(df)

        assert isinstance(result, pd.DataFrame)

    def test_correct_columns(self):
        df = pd.DataFrame([
            {
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
            }
        ])

        result = create_dim_location(df)

        expected_columns = [
            "location_id",
            "address_line_1",
            "address_line_2",
            "district",
            "city",
            "postal_code",
            "country",
            "phone",
        ]

        assert list(result.columns) == expected_columns

    def test_correct_data_types(self):
        df = pd.DataFrame([
            {
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
            }
        ])

        result = create_dim_location(df)

        assert result["location_id"].dtype == "int64"
        assert result["address_line_1"].dtype == object
        assert result["address_line_2"].dtype == object
        assert result["district"].dtype == object
        assert result["city"].dtype == object
        assert result["postal_code"].dtype == object
        assert result["country"].dtype == object
        assert result["phone"].dtype == object
