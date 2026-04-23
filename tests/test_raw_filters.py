"""Tests for data-cleaning filters added in v9."""

import polars as pl
import pytest


@pytest.fixture
def raw_module():
    # The raw script is named with a leading digit — load it by file
    import importlib.util

    spec = importlib.util.spec_from_file_location("raw_module", "steps/03_raw.py")
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_filter_bombouts_drops_rows_with_zero_squat(raw_module, bombout_rows):
    result = raw_module.filter_bombouts(bombout_rows)
    assert "bomb_squat" not in result["name"].to_list()


def test_filter_bombouts_drops_rows_with_zero_bench(raw_module, bombout_rows):
    result = raw_module.filter_bombouts(bombout_rows)
    assert "bomb_bench" not in result["name"].to_list()


def test_filter_bombouts_drops_rows_with_zero_deadlift(raw_module, bombout_rows):
    result = raw_module.filter_bombouts(bombout_rows)
    assert "bomb_dl" not in result["name"].to_list()


def test_filter_bombouts_drops_rows_with_negative_lift(raw_module, bombout_rows):
    result = raw_module.filter_bombouts(bombout_rows)
    assert "bomb_neg" not in result["name"].to_list()


def test_filter_bombouts_keeps_clean_rows(raw_module, bombout_rows):
    result = raw_module.filter_bombouts(bombout_rows)
    assert "clean" in result["name"].to_list()


def test_filter_total_consistency_keeps_within_tolerance(raw_module):
    df = pl.DataFrame(
        [
            # total matches sbd exactly
            {"name": "exact", "squat": 200.0, "bench": 100.0, "deadlift": 300.0, "total": 600.0},
            # total off by 2.0 kg — within 2.5 kg tolerance
            {"name": "within_tol", "squat": 200.0, "bench": 100.0, "deadlift": 300.0, "total": 598.0},
        ]
    )
    result = raw_module.filter_total_consistency(df)
    assert set(result["name"].to_list()) == {"exact", "within_tol"}


def test_filter_total_consistency_drops_outside_tolerance(raw_module):
    df = pl.DataFrame(
        [
            # total off by 10 kg — outside tolerance
            {"name": "way_off", "squat": 200.0, "bench": 100.0, "deadlift": 300.0, "total": 590.0},
            {"name": "exact", "squat": 200.0, "bench": 100.0, "deadlift": 300.0, "total": 600.0},
        ]
    )
    result = raw_module.filter_total_consistency(df)
    assert "way_off" not in result["name"].to_list()
    assert "exact" in result["name"].to_list()
