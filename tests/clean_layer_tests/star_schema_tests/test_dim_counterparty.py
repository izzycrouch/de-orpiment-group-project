import pandas as pd
from datetime import datetime
from clean_layer.star_schema_tables.dim_counterparty import create_dim_counterparty



class TestDimCounterparty:

    def test_dim_counterparty_returns_isinstance_dataframe(self):
        cp_df = pd.DataFrame([{"counterparty_id": 1,
                        "counterparty_legal_name": "Fahey and Sons",
                        "legal_address_id": 1,
                        "commercial_contact": "Micheal Toy",
                        "delivery_contact": "Mrs. Lucy Runolfsdottir",
                        "created_at": datetime.fromisoformat("2022-11-03 14:20:51.563000"),
                        "last_updated": datetime.fromisoformat("2022-11-03 14:20:51.563000")}])

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
    

        
        result = create_dim_counterparty(counterparty_df=cp_df, address_df=ad_df)
        assert isinstance(result, pd.DataFrame)
    

    def test_dim_counterparty_returns_correct_column_names(self):
   
        cp_df = pd.DataFrame([{"counterparty_id": 1,
                        "counterparty_legal_name": "Ms Jane Doe",
                        "legal_address_id": 1,
                        "commercial_contact": "test",
                        "delivery_contact": "test",
                        "created_at": datetime.fromisoformat("2022-11-03 14:20:51.563000"),
                        "last_updated": datetime.fromisoformat("2022-11-03 14:20:51.563000")}])

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
        
        expected_columns = ['counterparty_id', 'counterparty_legal_name', 'counterparty_legal_address_line_1', 'counterparty_legal_address_line_2', 'counterparty_legal_district', 'counterparty_legal_city', 'counterparty_legal_postal_code', 'counterparty_legal_country', 'counterparty_legal_phone_number']

        result = create_dim_counterparty(cp_df, ad_df)
        output_columns = result.columns.values.tolist()

        assert expected_columns == output_columns
    

    def test_dim_counterparty_returns_correct_column_datatypes(self):
        cp_df = pd.DataFrame([{"counterparty_id": 1,
                        "counterparty_legal_name": "Ms Jane Doe",
                        "legal_address_id": 1,
                        "commercial_contact": "test",
                        "delivery_contact": "test",
                        "created_at": datetime.fromisoformat("2022-11-03 14:20:51.563000"),
                        "last_updated": datetime.fromisoformat("2022-11-03 14:20:51.563000")}])

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
        
        expected_columns = ['counterparty_id', 'counterparty_legal_name', 'counterparty_legal_address_line_1', 'counterparty_legal_address_line_2', 'counterparty_legal_district', 'counterparty_legal_city', 'counterparty_legal_postal_code', 'counterparty_legal_country', 'counterparty_legal_phone_number']

        result = create_dim_counterparty(cp_df, ad_df)

        assert result["counterparty_id"].dtypes == int
        assert result["counterparty_legal_name"].dtypes == object
        assert result["counterparty_legal_address_line_1"].dtypes == object
        assert result["counterparty_legal_address_line_2"].dtypes == object
        assert result["counterparty_legal_district"].dtypes == object
        assert result["counterparty_legal_city"].dtypes == object
        assert result["counterparty_legal_postal_code"].dtypes == object
        assert result["counterparty_legal_country"].dtypes == object
        assert result["counterparty_legal_phone_number"].dtypes == object


    def test_dim_counterparty_returns_merged_data_if_address_id_the_same(self):
        cp_df = pd.DataFrame([{"counterparty_id": 1,
                        "counterparty_legal_name": "Ms Jane Doe",
                        "legal_address_id": 1,
                        "commercial_contact": "test",
                        "delivery_contact": "test",
                        "created_at": datetime.fromisoformat("2022-11-03 14:20:51.563000"),
                        "last_updated": datetime.fromisoformat("2022-11-03 14:20:51.563000")}])

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

        result = create_dim_counterparty(cp_df, ad_df)

        assert result.shape == (1,9)
    

    def test_dim_counterparty_returns_merged_data_if_address_id_the_same_with_seperate_counterparty_ids(self):
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

        result = create_dim_counterparty(cp_df, ad_df)

        assert result.shape == (2,9)