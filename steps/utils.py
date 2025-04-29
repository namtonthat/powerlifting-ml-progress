import logging

from polars import DataFrame


def test_counts(df_1: DataFrame, df_2: DataFrame) -> bool | None:
    logging.info(f"Row count (df_1): {len(df_1)}")
    logging.info(f"Row count (df_2): {len(df_2)}")
    assert len(df_1) == len(df_2)
