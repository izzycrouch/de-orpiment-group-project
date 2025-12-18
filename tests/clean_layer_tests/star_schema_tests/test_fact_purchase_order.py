import pandas as pd
from datetime import datetime
from clean_layer.star_schema_tables.fact_purchase_order import create_fact_purchase_order
from clean_layer.star_schema_tables.dim_date import create_dim_date

class TestFactPurchaseOrder:
    
    def test_create_fact_purchase_order_returns_isinstace_dataframe(self):
        dim_date_df = create_dim_date(start='2025-12-14', end='2025-12-21')
        dim_currency_df = pd.DataFrame([{'currency_id': 1}])        
        dim_staff_df = pd.DataFrame([{'staff_id' : 1}])        
        dim_counterparty_df = pd.DataFrame([{'counterparty_id': 1}])
        dim_location_df = pd.DataFrame([{'location_id': 1}])
        purchase_order_df = pd.DataFrame([{'purchase_order_id': 1,
                                            'created_at': datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                                            'last_updated': datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                                            'staff_id': 1,
                                            'counterparty_id': 1,
                                            'item_code': 'ZDOI5EA',
                                            'item_quantity': 1,
                                            'item_unit_price': 5,
                                            'currency_id': 1,
                                            'agreed_delivery_date': '2025-12-20',
                                            'agreed_payment_date': '2025-12-17',
                                            'agreed_delivery_location_id': 1}])

        result = create_fact_purchase_order(dim_date_df, dim_currency_df, dim_staff_df, dim_counterparty_df, dim_location_df, purchase_order_df)
        assert isinstance(result, pd.DataFrame)
    
    def test_create_fact_purchase_order_returns_correct_columns(self):
        dim_date_df = create_dim_date(start='2025-12-14', end='2025-12-21')
        dim_currency_df = pd.DataFrame([{'currency_id': 1}])
        dim_staff_df = pd.DataFrame([{'staff_id' : 1}])
        dim_counterparty_df = pd.DataFrame([{'counterparty_id': 1}])
        dim_location_df = pd.DataFrame([{'location_id': 1}])
        purchase_order_df = pd.DataFrame([{'purchase_order_id': 1,
                                            'created_at': datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                                            'last_updated': datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                                            'staff_id': 1,
                                            'counterparty_id': 1,
                                            'item_code': 'ZDOI5EA',
                                            'item_quantity': 1,
                                            'item_unit_price': 5,
                                            'currency_id': 1,
                                            'agreed_delivery_date': '2025-12-20',
                                            'agreed_payment_date': '2025-12-17',
                                            'agreed_delivery_location_id': 1}])

        result = create_fact_purchase_order(dim_date_df, dim_currency_df, dim_staff_df, dim_counterparty_df, dim_location_df, purchase_order_df)
        
        expected_columns = ['purchase_record_id',
                            'purchase_order_id',
                            'created_date',
                            'created_time',
                            'last_updated_date',
                            'last_updated_time',
                            'staff_id',
                            'counterparty_id',
                            'item_code',
                            'item_quantity',
                            'item_unit_price',
                            'currency_id',
                            'agreed_delivery_date',
                            'agreed_payment_date',
                            'agreed_delivery_location_id']
        
        output_columns = result.columns.values.tolist()
        
        assert expected_columns == output_columns
    
    def test_create_fact_purchase_order_returns_correct_column_datatyped(self):
        dim_date_df = create_dim_date(start='2025-12-14', end='2025-12-21')
        dim_currency_df = pd.DataFrame([{'currency_id': 1}])
        dim_staff_df = pd.DataFrame([{'staff_id' : 1}])
        dim_counterparty_df = pd.DataFrame([{'counterparty_id': 1}])
        dim_location_df = pd.DataFrame([{'location_id': 1}])
        purchase_order_df = pd.DataFrame([{'purchase_order_id': 1,
                                            'created_at': datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                                            'last_updated': datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                                            'staff_id': 1,
                                            'counterparty_id': 1,
                                            'item_code': 'ZDOI5EA',
                                            'item_quantity': 1,
                                            'item_unit_price': 5.99,
                                            'currency_id': 1,
                                            'agreed_delivery_date': datetime.fromisoformat('2025-12-20'),
                                            'agreed_payment_date': datetime.fromisoformat('2025-12-17'),
                                            'agreed_delivery_location_id': 1}])

        result = create_fact_purchase_order(dim_date_df, dim_currency_df, dim_staff_df, dim_counterparty_df, dim_location_df, purchase_order_df)
        
        assert result['purchase_record_id'].dtype == int 
        assert result['purchase_order_id'].dtype == int
        assert str(result['created_date'].dtype) == 'datetime64[ns]'
        assert str(result['created_time'].dtype) == 'datetime64[ns]'
        assert str(result['last_updated_date'].dtype) == 'datetime64[ns]'
        assert result['last_updated_time'].dtype == 'datetime64[ns]'
        assert result['staff_id'].dtype == int
        assert result['counterparty_id'].dtype == int
        assert result['item_code'].dtype == object
        assert result['item_quantity'].dtype == int
        assert result['item_unit_price'].dtype == float
        assert result['currency_id'].dtype == int
        assert str(result['agreed_delivery_date'].dtype) == 'datetime64[ns]'
        assert str(result['agreed_payment_date'].dtype) == 'datetime64[ns]'
        assert result['agreed_delivery_location_id'].dtype == int
    

    def test_create_fact_purchase_keeps_history_of_data(self):
        dim_date_df = create_dim_date(start='2025-12-14', end='2025-12-21')
        dim_currency_df = pd.DataFrame([{'currency_id': 1}])
        dim_staff_df = pd.DataFrame([{'staff_id' : 1}])
        dim_counterparty_df = pd.DataFrame([{'counterparty_id': 1}])
        dim_location_df = pd.DataFrame([{'location_id': 1}])
        purchase_order_df = pd.DataFrame([{'purchase_order_id': 1,
                                            'created_at': datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                                            'last_updated': datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                                            'staff_id': 1,
                                            'counterparty_id': 1,
                                            'item_code': 'ZDOI5EA',
                                            'item_quantity': 1,
                                            'item_unit_price': 5.99,
                                            'currency_id': 1,
                                            'agreed_delivery_date': datetime.fromisoformat('2025-12-20'),
                                            'agreed_payment_date': datetime.fromisoformat('2025-12-17'),
                                            'agreed_delivery_location_id': 1},
                                            {'purchase_order_id': 1,
                                            'created_at': datetime.fromisoformat('2025-12-15 15:51:20.825099'),
                                            'last_updated': datetime.fromisoformat('2025-12-16 15:51:20.825099'),
                                            'staff_id': 1,
                                            'counterparty_id': 1,
                                            'item_code': 'ZDOI5EA',
                                            'item_quantity': 2,
                                            'item_unit_price': 5.99,
                                            'currency_id': 1,
                                            'agreed_delivery_date': datetime.fromisoformat('2025-12-20'),
                                            'agreed_payment_date': datetime.fromisoformat('2025-12-17'),
                                            'agreed_delivery_location_id': 1}])

        result = create_fact_purchase_order(dim_date_df, dim_currency_df, dim_staff_df, dim_counterparty_df, dim_location_df, purchase_order_df)
        
        assert result.shape == (2, 15)