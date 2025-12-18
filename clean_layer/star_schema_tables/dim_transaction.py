def create_dim_transaction(cleaned_transaction):
    dim_transaction = (
        cleaned_transaction[
            [
                'transaction_id', 
                'transaction_type',
                'sales_order_id',
                'purchase_order_id'
            ]
        ]
    )
    return dim_transaction