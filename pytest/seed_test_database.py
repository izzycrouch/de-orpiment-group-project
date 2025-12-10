from src.utils.connection import connect_to_db,close_db_connection
import os
from dotenv import load_dotenv
import pg8000.native

#set connection function for local postgre
load_dotenv(override=True)

def connect_to_local_db():
    return pg8000.native.Connection(
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        database=os.getenv("PG_DATABASE"),
        host=os.getenv("PG_HOST"),
        port=int(os.getenv("PG_PORT"))
    )

def close_local_db_connection(conn):
    conn.close()


def return_creat_table_sql(table_name, schema_rows):
    res = f'CREATE TABLE IF NOT EXISTS {table_name} ('
    col_list = []
    for col_name, data_type, char_len in schema_rows:

        if data_type == "character varying":
            if char_len:
                col_type = f"varchar({char_len})"
            else:
                col_type = "varchar"
        elif data_type == "USER-DEFINED":
            col_type = "text"
        else:
            col_type = data_type

        part = f"{col_name} {col_type}"
        col_list.append(part)
    for col in col_list:
        res += col
        res += ', '
    res = res[:-2]
    res += ');'

    return res


try:
    db = connect_to_db()
    local_db = connect_to_local_db()


    tables = db.run("""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public';
    """)
    tables =  [x[0] for x in tables[1:]]

    for table in tables:



        schema_rows = db.run(f"""
                                SELECT
                                    column_name,
                                    data_type,
                                    character_maximum_length
                                FROM information_schema.columns
                                WHERE table_name = '{table}'
                                AND table_schema = 'public'
                                ORDER BY ordinal_position;
                            """)

        schema_sql = return_creat_table_sql(table, schema_rows)
        local_db.run(schema_sql)




        col_names = [c[0] for c in schema_rows]


        col_sql = ", ".join(f'"{c}"' for c in col_names)
        placeholders = ", ".join(f":{c}" for c in col_names)

        rows = db.run(f"SELECT * FROM {table} LIMIT 10;")


        for row in rows:
            params = {col: value for col, value in zip(col_names, row)}
            local_db.run(
                f'INSERT INTO {table} ({col_sql}) VALUES ({placeholders});',
                **params,  )

        res = local_db.run(f"""
                    SELECT *
                    FROM {table}
                    LIMIT 1;
                """)
        print(res)


finally:
    if db:
        close_db_connection(db)
    print("Seeding Complete.")
    close_local_db_connection(local_db)
