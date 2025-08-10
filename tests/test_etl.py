import pandas as pd
from project.etl.etl import load_csv, clean_transform

def test_clean_transform_no_duplicates_and_total_amount():
    df = load_csv()
    df_clean = clean_transform(df)
    assert df_clean['order_id'].duplicated().sum() == 0
    assert 'total_amount' in df_clean.columns
    assert df_clean['total_amount'].isna().sum() == 0
    assert pd.api.types.is_integer_dtype(df_clean['quantity'])
    assert pd.api.types.is_float_dtype(df_clean['unit_price'])
