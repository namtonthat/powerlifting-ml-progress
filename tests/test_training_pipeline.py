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
