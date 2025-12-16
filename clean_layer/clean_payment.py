from clean_layer.utils.connection import connect_to_db, close_db_connection
import pandas as pd
from datetime import datetime

def get_db():
    db = None
    try:
        db = connect_to_db()

        rows = db.run(f"""
            SELECT * FROM payment
            LIMIT 20;
        """)

        df = pd.DataFrame(data=rows, columns=pd.Index([
            "payment_id","created_at","last_updated", "transaction_id", "counterparty_id", "payment_amount", "currency_id",
            "payment_type_id", "paid", "payment_date", "company_ac_number", "counterparty_ac_number"
            ]))

        #print(df.head(20))
        return df

    except Exception as e:
        print(e)
    finally:
        if db:
            close_db_connection(db)

df = get_db()

def clean_payment_table(df):

    df['payment_amount'] = (
        df['payment_amount'].astype(str)
        .str.replace(',', '')
        .str.replace('£', '')
        .str.strip()
    )
    df['payment_amount'] = pd.to_numeric(df['payment_amount'], errors="coerce")


    df['payment_date'] = pd.to_datetime(df['payment_date'], format='%Y-%m-%d', errors='coerce')

    today = datetime.today()
    invalid_dates = df['payment_date'] > today
    df = df.drop(df[invalid_dates].index)

    null_mask = df.isnull().any(axis=1)
    null_rows = df[null_mask]

    print(null_rows)

    df = df.dropna(how='any',axis=0)

    #print(invalid_dates)

    #print(df['payment_date'].dtypes)

    # types = {
    #     'payment_id': 'int64',
    #     'created_at': 'datetime64[ns]',
    #     'last_updated': 'datetime64[ns]',
    #     'transaction_id': 'int64',
    #     'counterparty_id': 'int64',
    #     'payment_amount': 'object -> float64',
    #     'currency_id': 'int64',
    #     'payment_type_id': 'int64',
    #     'paid': 'bool',
    #     'payment_date': 'object -> datetime64[ns]',
    #     'company_ac_number': 'int64',
    #     'counterparty_ac_number': 'int64'
    # }

    #print(df.head(20))

clean_payment_table(df=df)