from src.utils.connection import connect_to_db,close_db_connection


db = None
try:
    db = connect_to_db()
    tables = db.run("""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public';
    """)
    print(tables)

    for table in tables[1:]:
        table = table[0]
        rows =  db.run(f"""
    SELECT *
    FROM {table}
    LIMIT 1;
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