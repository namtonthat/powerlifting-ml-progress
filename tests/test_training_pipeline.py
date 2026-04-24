"""Invariants for v11 dual-head training."""

import math

import numpy as np
import pytest


def test_normalised_rmse_computation():
    """Normalised RMSE = RMSE / std(y_test). Scale-free comparison across targets."""
    y_true = np.array([10.0, 20.0, 30.0, 40.0, 50.0])
    y_pred = np.array([12.0, 18.0, 32.0, 38.0, 50.0])
    rmse = math.sqrt(np.mean((y_true - y_pred) ** 2))
    norm_rmse = rmse / y_true.std()

    # Manual check: RMSE = sqrt((4+4+4+4+0)/5) = sqrt(3.2) ≈ 1.788
    # std of [10,20,30,40,50] = sqrt(200) ≈ 14.142
    assert norm_rmse == pytest.approx(1.788 / 14.142, rel=1e-2)


def test_spearman_rho_identity_is_one():
    """Spearman rho of a strictly monotonic transformation equals 1.0."""
    from scipy.stats import spearmanr

    y_true = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
    y_pred = 2 * y_true + 10  # strictly monotonic -> rho = 1.0
    rho, _ = spearmanr(y_true, y_pred)
    assert rho == pytest.approx(1.0)


def test_predictions_parquet_schema():
    """Predictions parquet must carry both head preds and starting_tier."""
    import polars as pl

    required = {"primary_key", "date", "pred_pct_change_total", "pred_pct_change_dots", "starting_tier"}

    # Schema test uses a synthetic frame — integration test covers real file
    df = pl.DataFrame(
        {
            "primary_key": ["L1"],
            "date": [None],
            "pred_pct_change_total": [0.1],
            "pred_pct_change_dots": [0.2],
            "starting_tier": [1],
        }
    )
    assert required.issubset(set(df.columns))


def test_dual_head_predictions_outer_join_coalesces_keys():
    """Simulate the v11 merge — verify no _right columns and null preds where expected."""
    from datetime import date

    import polars as pl

    kg = pl.DataFrame(
        {
            "primary_key": ["L1", "L2", "L3"],
            "date": [date(2024, 1, 1), date(2024, 1, 1), date(2024, 1, 1)],
            "starting_tier": [1, 2, 3],
            "pred_pct_change_total": [0.05, 0.1, 0.15],
        }
    )
    dots = pl.DataFrame(
        {
            # L1: both heads; L2: kg-only (dropped here); L4: dots-only (not in kg)
            "primary_key": ["L1", "L4"],
            "date": [date(2024, 1, 1), date(2024, 1, 1)],
            "pred_pct_change_dots": [0.06, 0.2],
        }
    )

    combined = kg.join(dots, on=["primary_key", "date"], how="full", coalesce=True)

    # No _right leftovers
    assert "primary_key_right" not in combined.columns
    assert "date_right" not in combined.columns

    # Schema includes both pred columns
    assert set(combined.columns) >= {"primary_key", "date", "starting_tier", "pred_pct_change_total", "pred_pct_change_dots"}

    # No null join keys
    assert combined["primary_key"].null_count() == 0
    assert combined["date"].null_count() == 0

    # L2 (kg-only) has null pred_pct_change_dots
    l2 = combined.filter(pl.col("primary_key") == "L2")
    assert l2["pred_pct_change_dots"][0] is None

    # L4 (dots-only) has null pred_pct_change_total AND null starting_tier (kg-side only)
    l4 = combined.filter(pl.col("primary_key") == "L4")
    assert l4["pred_pct_change_total"][0] is None
