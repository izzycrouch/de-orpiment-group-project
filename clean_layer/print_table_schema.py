from extract_layer.utils.connection import connect_to_db, close_db_connection
from tabulate import tabulate

db = None
try:
    db = connect_to_db()
    tables =  ['counterparty', 'address', 'department', 'purchase_order', 'staff', 'payment_type', 'payment', 'transaction', 'design', 'sales_order', 'currency']
    # for table in tables:
    #     # rows =  db.run(f"""
    #     # SELECT *
    #     # FROM {table};
    #     # """)
    #     schema_rows = db.run(f"""
    #                             SELECT
    #                                 column_name,
    #                                 data_type,
    #                                 character_maximum_length,
    #                                 is_nullable,
    #                                 column_default
    #                             FROM information_schema.columns
    #                             WHERE table_name = '{table}'
    #                             AND table_schema = 'public'
    #                             ORDER BY ordinal_position;
    #                         """)
    #     fields = ["column_name", "data_type", "character_maximum_length", "is_nullable", "column_default"]
    #     print('table_name: ', table)
    #     print(tabulate(schema_rows, headers=fields, tablefmt="grid"))


    rows =  db.run('''
                   SELECT *
                   FROM
                   transaction
                   LIMIT 30
                   ''')



    df = pd.DataFrame(rows, columns=columns)

    print(tabulate(rows, headers= [col["name"] for col in db.columns], tablefmt="grid"))

#db_column method (pg8000)

except Exception as e:
    print(e)
finally:
    if db:
        close_db_connection(db)