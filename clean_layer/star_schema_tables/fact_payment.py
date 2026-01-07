def create_fact_payment(payment, dim_payment_type, dim_transaction, dim_counterparty, dim_currency, dim_date):
    df = (
        payment
        .merge(dim_payment_type, on='payment_type_id')
        .merge(dim_transaction, on='transaction_id')
        .merge(dim_counterparty, on='counterparty_id')
        .merge(dim_currency, on='currency_id')
        .merge(dim_date, how='left', left_on='payment_date', right_on='date_id')
    )

    df.insert(loc=0, column='payment_record_id', value=df.index + 1)
    df.insert(loc=2, column='created_date', value=df['created_at'].dt.date)
    df.insert(loc=3, column='created_time', value=df['created_at'].dt.time)
    df.insert(loc=4, column='last_updated_date', value=df['last_updated'].dt.date)
    df.insert(loc=5, column='last_updated_time', value=df['last_updated'].dt.time)

    df = (df.merge(dim_date, left_on='created_date', right_on='date_id', how='left'))
    df = (df.merge(dim_date, left_on='last_updated_date', right_on='date_id', how='left'))


    fact_payment = (
        df[
            [
                "payment_record_id",
                "payment_id",
                "created_date",
                "created_time",
                "last_updated_date",
                "last_updated_time",
                "transaction_id",
                "counterparty_id",
                "payment_amount",
                "currency_id",
                "payment_type_id",
                "paid",
                "payment_date"
            ]
        ]
    )
    return fact_payment