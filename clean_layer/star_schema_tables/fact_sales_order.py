import pandas as pd

def create_fact_sales_order(
    sales_order: pd.DataFrame,
    dim_date: pd.DataFrame,
    dim_staff: pd.DataFrame,
    dim_counterparty: pd.DataFrame,
    dim_currency: pd.DataFrame,
    dim_design: pd.DataFrame,
    dim_location: pd.DataFrame,
) -> pd.DataFrame:
    so = sales_order.copy()
    so["created_date"] = so["created_at"].dt.date
    so["created_time"] = so["created_at"].dt.time
    so["last_updated_date"] = so["last_updated"].dt.date
    so["last_updated_time"] = so["last_updated"].dt.time

    so = so.merge(dim_date[["date_id"]], left_on="created_date", right_on="date_id", how="inner").drop(columns=["date_id"])
    so = so.merge(dim_date[["date_id"]], left_on="last_updated_date", right_on="date_id", how="inner").drop(columns=["date_id"])
    so = so.merge(dim_date[["date_id"]], left_on="agreed_payment_date", right_on="date_id", how="inner").drop(columns=["date_id"])
    so = so.merge(dim_date[["date_id"]], left_on="agreed_delivery_date", right_on="date_id", how="inner").drop(columns=["date_id"])


    so = so.merge(dim_staff[["staff_id"]], on="staff_id", how="inner")
    so = so.merge(dim_counterparty[["counterparty_id"]], on="counterparty_id", how="inner")
    so = so.merge(dim_currency[["currency_id"]], on="currency_id", how="inner")
    so = so.merge(dim_design[["design_id"]], on="design_id", how="inner")
    so = so.merge(dim_location[["location_id"]], left_on="agreed_delivery_location_id", right_on="location_id", how="inner").drop(columns=["location_id"])


    fact_sales_order = pd.DataFrame({
        "sales_order_id": so["sales_order_id"],
        "created_date": so["created_date"],
        "created_time": so["created_time"],
        "last_updated_date": so["last_updated_date"],
        "last_updated_time": so["last_updated_time"],
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
