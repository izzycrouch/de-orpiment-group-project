from clean_layer.star_schema_tables.dim_staff import create_dim_staff
import pandas as pd
from datetime import datetime

class TestDimStaff:
    def test_dim_staff_returns_dataframe(self):

        test_data = [{
            'staff_id' : 1,
            'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
            'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
            'department_id' : 2,
            'first_name' : 'Harry',
            'last_name': 'Potter',
            'email_address': 'harry.potter123@gmail.com'
            }]
        cleaned_staff = pd.DataFrame(test_data)

        test_data2 = [{
            "department_id": 1,
            "department_name": "Sales",
            "location": "Manchester",
            "manager": "Richard Roma",
            "created_at": datetime.fromisoformat("2022-11-03 14:20:49.962000"),
            "last_updated": datetime.fromisoformat("2022-11-03 14:20:49.962000"),
            }]
        cleaned_department = pd.DataFrame(test_data2)

        df = create_dim_staff(cleaned_staff, cleaned_department)

        assert isinstance(df, pd.DataFrame)

    def test_correct_columns(self):
        
        test_data = [{
            'staff_id' : 1,
            'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
            'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
            'department_id' : 2,
            'first_name' : 'Harry',
            'last_name': 'Potter',
            'email_address': 'harry.potter123@gmail.com'
            }]
        cleaned_staff = pd.DataFrame(test_data)

        test_data2 = [{
            "department_id": 1,
            "department_name": "Sales",
            "location": "Manchester",
            "manager": "Richard Roma",
            "created_at": datetime.fromisoformat("2022-11-03 14:20:49.962000"),
            "last_updated": datetime.fromisoformat("2022-11-03 14:20:49.962000"),
            }]
        cleaned_department = pd.DataFrame(test_data2)

        df = create_dim_staff(cleaned_staff, cleaned_department)
        expected_cols = [
                'staff_id', 
                'first_name', 
                'last_name',
                'department_name',
                'location',
                'email_address' 
            ]

        assert (expected_cols == df.columns).any()

    def test_correct_data_types(self):

        test_data = [{
            'staff_id' : 1,
            'created_at' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
            'last_updated' : datetime.fromisoformat('2025-12-15 15:51:20.825099'),
            'department_id' : 2,
            'first_name' : 'Harry',
            'last_name': 'Potter',
            'email_address': 'harry.potter123@gmail.com'
            }]
        cleaned_staff = pd.DataFrame(test_data)

        test_data2 = [{
            "department_id": 1,
            "department_name": "Sales",
            "location": "Manchester",
            "manager": "Richard Roma",
            "created_at": datetime.fromisoformat("2022-11-03 14:20:49.962000"),
            "last_updated": datetime.fromisoformat("2022-11-03 14:20:49.962000"),
            }]
        cleaned_department = pd.DataFrame(test_data2)

        df = create_dim_staff(cleaned_staff, cleaned_department)

        assert isinstance(df, pd.DataFrame)
        assert df["staff_id"].dtypes == int
        assert df["first_name"].dtypes == object
        assert df["last_name"].dtypes == object
        assert df["department_name"].dtypes == object
        assert df["email_address"].dtypes == object
        assert df["location"].dtypes == object

