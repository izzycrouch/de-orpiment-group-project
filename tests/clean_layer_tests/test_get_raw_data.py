from clean_layer.utils.get_raw_data import get_df
import pandas as pd


def test_get_df():
    df = get_df('totesys-raw-data-aci','counterparty/year=2025/month=12/day=15/batch_20251215T135936Z.parquet')
    assert isinstance(df, pd.DataFrame)