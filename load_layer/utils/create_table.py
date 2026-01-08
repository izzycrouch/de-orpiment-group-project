DIM_DATE_SQL = '''
    CREATE TABLE IF NOT EXISTS public.dim_date (
    date_id DATE PRIMARY KEY NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    day_of_week INT NOT NULL,
    day_name VARCHAR NOT NULL,
    month_name VARCHAR NOT NULL,
    quarter INT NOT NULL
);
'''
DIM_STAFF_SQL = """
    CREATE TABLE IF NOT EXISTS public.dim_staff (
        staff_id INT PRIMARY KEY NOT NULL,
        first_name VARCHAR NOT NULL,
        last_name VARCHAR NOT NULL,
        department_name VARCHAR NOT NULL,
        location VARCHAR NOT NULL,
        email_address VARCHAR NOT NULL
    );
    """


DIM_LOCATION_SQL = """
CREATE TABLE IF NOT EXISTS public.dim_location (
    location_id INT PRIMARY KEY NOT NULL,
    address_line_1 VARCHAR NOT NULL,
    address_line_2 VARCHAR,
    district VARCHAR,
    city VARCHAR NOT NULL,
    postal_code VARCHAR NOT NULL,
    country VARCHAR NOT NULL,
    phone VARCHAR NOT NULL
);
"""

DIM_CURRENCY_SQL = """
CREATE TABLE IF NOT EXISTS public.dim_currency (
    currency_id INT PRIMARY KEY NOT NULL,
    currency_code VARCHAR NOT NULL,
    currency_name VARCHAR NOT NULL
);
"""


DIM_DESIGN_SQL = """
CREATE TABLE IF NOT EXISTS public.dim_design (
    design_id INT PRIMARY KEY NOT NULL,
    design_name VARCHAR NOT NULL,
    file_location VARCHAR NOT NULL,
    file_name VARCHAR NOT NULL
);
"""

DIM_COUNTERPARTY_SQL = """
CREATE TABLE IF NOT EXISTS public.dim_counterparty (
    counterparty_id INT PRIMARY KEY NOT NULL,
    counterparty_legal_name VARCHAR NOT NULL,
    counterparty_legal_address_line_1 VARCHAR NOT NULL,
    counterparty_legal_address_line_2 VARCHAR,
    counterparty_legal_district VARCHAR,
    counterparty_legal_city VARCHAR NOT NULL,
    counterparty_legal_postal_code VARCHAR NOT NULL,
    counterparty_legal_country VARCHAR NOT NULL,
    counterparty_legal_phone_number VARCHAR NOT NULL
);
"""



DIM_PAYMENT_TYPE_SQL = """
CREATE TABLE IF NOT EXISTS public.dim_payment_type (
    payment_type_id SERIAL PRIMARY KEY NOT NULL,
    payment_type_name VARCHAR NOT NULL
);
"""



DIM_TRANSACTION_SQL = """
CREATE TABLE IF NOT EXISTS public.dim_transaction (
    transaction_id INT PRIMARY KEY NOT NULL,
    transaction_type VARCHAR NOT NULL,
    sales_order_id INT,
    purchase_order_id INT
);
"""

FACT_SALES_ORDER_SQL = """
CREATE TABLE IF NOT EXISTS public.fact_sales_order (
    sales_record_id SERIAL PRIMARY KEY,
    sales_order_id INT NOT NULL,
    created_date DATE NOT NULL,
    created_time TIME NOT NULL,
    last_updated_date DATE NOT NULL,
    last_updated_time TIME NOT NULL,
    sales_staff_id INT NOT NULL,
    counterparty_id INT NOT NULL,
    units_sold INT NOT NULL,
    unit_price NUMERIC(10,2) NOT NULL,
    currency_id INT NOT NULL,
    design_id INT NOT NULL,
    agreed_payment_date DATE NOT NULL,
    agreed_delivery_date DATE NOT NULL,
    agreed_delivery_location_id INT NOT NULL,

    CONSTRAINT fk_fso_created_date FOREIGN KEY (created_date)
        REFERENCES public.dim_date(date_id),
    CONSTRAINT fk_fso_last_updated_date FOREIGN KEY (last_updated_date)
        REFERENCES public.dim_date(date_id),
    CONSTRAINT fk_fso_staff FOREIGN KEY (sales_staff_id)
        REFERENCES public.dim_staff(staff_id),
    CONSTRAINT fk_fso_counterparty FOREIGN KEY (counterparty_id)
        REFERENCES public.dim_counterparty(counterparty_id),
    CONSTRAINT fk_fso_currency FOREIGN KEY (currency_id)
        REFERENCES public.dim_currency(currency_id),
    CONSTRAINT fk_fso_design FOREIGN KEY (design_id)
        REFERENCES public.dim_design(design_id),
    CONSTRAINT fk_fso_payment_date FOREIGN KEY (agreed_payment_date)
        REFERENCES public.dim_date(date_id),
    CONSTRAINT fk_fso_delivery_date FOREIGN KEY (agreed_delivery_date)
        REFERENCES public.dim_date(date_id),
    CONSTRAINT fk_fso_location FOREIGN KEY (agreed_delivery_location_id)
        REFERENCES public.dim_location(location_id)
);
"""


FACT_PURCHASE_ORDER_SQL = """
CREATE TABLE IF NOT EXISTS public.fact_purchase_order (
    purchase_record_id SERIAL PRIMARY KEY,
    purchase_order_id INT NOT NULL,
    created_date DATE NOT NULL,
    created_time TIME NOT NULL,
    last_updated_date DATE NOT NULL,
    last_updated_time TIME NOT NULL,
    staff_id INT NOT NULL,
    counterparty_id INT NOT NULL,
    item_code VARCHAR NOT NULL,
    item_quantity INT NOT NULL,
    item_unit_price NUMERIC NOT NULL,
    currency_id INT NOT NULL,
    agreed_delivery_date DATE NOT NULL,
    agreed_payment_date DATE NOT NULL,
    agreed_delivery_location_id INT NOT NULL,

    CONSTRAINT fk_fpo_created_date FOREIGN KEY (created_date)
        REFERENCES public.dim_date(date_id),
    CONSTRAINT fk_fpo_last_updated_date FOREIGN KEY (last_updated_date)
        REFERENCES public.dim_date(date_id),
    CONSTRAINT fk_fpo_staff FOREIGN KEY (staff_id)
        REFERENCES public.dim_staff(staff_id),
    CONSTRAINT fk_fpo_counterparty FOREIGN KEY (counterparty_id)
        REFERENCES public.dim_counterparty(counterparty_id),
    CONSTRAINT fk_fpo_currency FOREIGN KEY (currency_id)
        REFERENCES public.dim_currency(currency_id),
    CONSTRAINT fk_fpo_delivery_date FOREIGN KEY (agreed_delivery_date)
        REFERENCES public.dim_date(date_id),
    CONSTRAINT fk_fpo_payment_date FOREIGN KEY (agreed_payment_date)
        REFERENCES public.dim_date(date_id),
    CONSTRAINT fk_fpo_location FOREIGN KEY (agreed_delivery_location_id)
        REFERENCES public.dim_location(location_id)
);
"""

FACT_PAYMENT_SQL = """
CREATE TABLE IF NOT EXISTS public.fact_payment (
    payment_record_id SERIAL PRIMARY KEY,
    payment_id INT NOT NULL,
    created_date DATE NOT NULL,
    created_time TIME NOT NULL,
    last_updated_date DATE NOT NULL,
    last_updated_time TIME NOT NULL,
    transaction_id INT NOT NULL,
    counterparty_id INT NOT NULL,
    payment_amount NUMERIC NOT NULL,
    currency_id INT NOT NULL,
    payment_type_id INT NOT NULL,
    paid BOOLEAN NOT NULL,
    payment_date DATE NOT NULL,

    CONSTRAINT fk_fp_created_date FOREIGN KEY (created_date)
        REFERENCES public.dim_date(date_id),
    CONSTRAINT fk_fp_last_updated_date FOREIGN KEY (last_updated_date)
        REFERENCES public.dim_date(date_id),
    CONSTRAINT fk_fp_transaction FOREIGN KEY (transaction_id)
        REFERENCES public.dim_transaction(transaction_id),
    CONSTRAINT fk_fp_counterparty FOREIGN KEY (counterparty_id)
        REFERENCES public.dim_counterparty(counterparty_id),
    CONSTRAINT fk_fp_currency FOREIGN KEY (currency_id)
        REFERENCES public.dim_currency(currency_id),
    CONSTRAINT fk_fp_payment_type FOREIGN KEY (payment_type_id)
        REFERENCES public.dim_payment_type(payment_type_id),
    CONSTRAINT fk_fp_payment_date FOREIGN KEY (payment_date)
        REFERENCES public.dim_date(date_id)
);
"""


def creat_table(db):
    SQL_LIST = [
        DIM_DATE_SQL,
        DIM_STAFF_SQL,
        DIM_LOCATION_SQL,
        DIM_CURRENCY_SQL,
        DIM_DESIGN_SQL,
        DIM_COUNTERPARTY_SQL,
        DIM_PAYMENT_TYPE_SQL,
        DIM_TRANSACTION_SQL,
        FACT_SALES_ORDER_SQL,
        FACT_PURCHASE_ORDER_SQL,
        FACT_PAYMENT_SQL,
    ]

    for sql in SQL_LIST:
        db.run(sql)
