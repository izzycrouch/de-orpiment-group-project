from extract_layer.utils.connection import connect_to_db, close_db_connection


db = None
try:
    db = connect_to_db()
    tables =  ['counterparty', 'address', 'department', 'purchase_order', 'staff', 'payment_type', 'payment', 'transaction', 'design', 'sales_order', 'currency']
    table = tables[0]

    rows =  db.run(f"""
    SELECT *
    FROM {table};
    """)
    column_names = [col["name"] for col in db.columns]
    print('table_name:',table)
    print("Columns:", column_names)
    print('rows:',rows)

except Exception as e:
    print(e)
finally:
    if db:
        close_db_connection(db)