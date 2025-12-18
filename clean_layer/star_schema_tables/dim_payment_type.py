def create_dim_payment_type(cleaned_payment_type):
    dim_payment_type = (
        cleaned_payment_type[
            [
                'payment_type_id',
                'payment_type_name'
            ]
        ]
    )
    return dim_payment_type