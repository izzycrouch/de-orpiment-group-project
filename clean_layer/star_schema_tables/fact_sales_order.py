import pandas as pd

def create_fact_sales_order(sales_order):
    so = sales_order.copy()

    fact_sales_order = pd.DataFrame({
        "sales_order_id": so["sales_order_id"],
        "created_date": so["created_at"].dt.date,
        "created_time": so["created_at"].dt.time,
        "last_updated_date": so["last_updated"].dt.date,
        "last_updated_time": so["last_updated"].dt.time,
        "sales_staff_id": so["staff_id"],
        "counterparty_id": so["counterparty_id"],
        "units_sold": so["units_sold"],
        "unit_price": so["unit_price"],
        "currency_id": so["currency_id"],
        "design_id": so["design_id"],
        "agreed_payment_date": so["agreed_payment_date"],
        "agreed_delivery_date": so["agreed_delivery_date"],
        "agreed_delivery_location_id": so["agreed_delivery_location_id"],
    })

    return fact_sales_order
