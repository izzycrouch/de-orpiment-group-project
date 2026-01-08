from load_layer.utils.connection import connect_to_pg_db,close_pg_db_connection



def quick_check():
    conn = connect_to_pg_db()

    try:
        query = """
            SELECT
                table_name,
                (SELECT COUNT(*) FROM information_schema.tables t2
                 WHERE t2.table_name = t.table_name) as exists
            FROM information_schema.tables t
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """
        tables = conn.run(query)

        print("database:\n")
        for table in tables:
            table_name = table[0]
            count = conn.run(f"SELECT COUNT(*) FROM {table_name}")[0][0]
            print(f"{table_name}: {count} lines")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

quick_check()