from extract_layer.utils.connection import connect_to_local_db, close_db_connection

def seed_test_db():
    conn = connect_to_local_db()

    tables = ['counterparty', 'address', 'department', 'purchase_order', 'staff', 'payment_type', 'payment', 'transaction', 'design', 'sales_order', 'currency']
    
    for table in tables:
        conn.run(f'DROP TABLE IF EXISTS {table} CASCADE;')
        
    create_counterparty = '''CREATE TABLE counterparty (
                        counterparty_id SERIAL PRIMARY KEY,
                        counterparty VARCHAR(20) NOT NULL,
                        last_updated TIMESTAMP DEFAULT now());'''
    create_address = '''CREATE TABLE address (
                        address_id SERIAL PRIMARY KEY,
                        address VARCHAR(20) NOT NULL,
                        last_updated TIMESTAMP DEFAULT now());'''
    create_department = '''CREATE TABLE department (
                        department_id SERIAL PRIMARY KEY,
                        department VARCHAR(20) NOT NULL,
                        last_updated TIMESTAMP DEFAULT now());'''
    create_purchase_order = '''CREATE TABLE purchase_order (
                        purchase_order_id SERIAL PRIMARY KEY,
                        purchase_order VARCHAR(20) NOT NULL,
                        last_updated TIMESTAMP DEFAULT now());'''
    create_staff = '''CREATE TABLE staff (
                        staff_id SERIAL PRIMARY KEY,
                        staff VARCHAR(20) NOT NULL,
                        last_updated TIMESTAMP DEFAULT now());'''
    create_payment_type = '''CREATE TABLE payment_type (
                        payment_type_id SERIAL PRIMARY KEY,
                        payment_type VARCHAR(20) NOT NULL,
                        last_updated TIMESTAMP DEFAULT now());'''
    create_payments = '''CREATE TABLE payment (
                        payment_id SERIAL PRIMARY KEY,
                        payment VARCHAR(20) NOT NULL,
                        last_updated TIMESTAMP DEFAULT now());'''
    create_transaction = '''CREATE TABLE transaction (
                        transaction_id SERIAL PRIMARY KEY,
                        transaction VARCHAR(20) NOT NULL,
                        last_updated TIMESTAMP DEFAULT now());'''
    create_design = '''CREATE TABLE design (
                        design_id SERIAL PRIMARY KEY,
                        design VARCHAR(20) NOT NULL,
                        last_updated TIMESTAMP DEFAULT now());'''
    create_sales_order = '''CREATE TABLE sales_order (
                        sales_order_id SERIAL PRIMARY KEY,
                        sales_order VARCHAR(20) NOT NULL,
                        last_updated TIMESTAMP DEFAULT now());'''
    create_currency = '''CREATE TABLE currency (
                        currency_id SERIAL PRIMARY KEY,
                        currency VARCHAR(20) NOT NULL,
                        last_updated TIMESTAMP DEFAULT now());'''

    conn.run(create_counterparty)
    conn.run(create_address)
    conn.run(create_department)
    conn.run(create_purchase_order)
    conn.run(create_staff)
    conn.run(create_payment_type)
    conn.run(create_payments)
    conn.run(create_transaction)
    conn.run(create_design)
    conn.run(create_sales_order)
    conn.run(create_currency)
    
    conn.run('''INSERT INTO counterparty (counterparty) VALUES ('test');''')
    conn.run('''INSERT INTO address (address) VALUES ('test');''')
    conn.run('''INSERT INTO department (department) VALUES ('test');''')
    conn.run('''INSERT INTO purchase_order (purchase_order) VALUES ('test');''')
    conn.run('''INSERT INTO staff (staff) VALUES ('test');''')
    conn.run('''INSERT INTO payment_type (payment_type) VALUES ('test');''')
    conn.run('''INSERT INTO payment (payment) VALUES ('test');''')
    conn.run('''INSERT INTO transaction (transaction) VALUES ('test');''')
    conn.run('''INSERT INTO design (design) VALUES ('test');''')
    conn.run('''INSERT INTO sales_order (sales_order) VALUES ('test');''')
    conn.run('''INSERT INTO currency (currency) VALUES ('test');''')

    print("Seeding Complete.")
    close_db_connection(conn)