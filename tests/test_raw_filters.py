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


def test_filter_bw_validity_keeps_male_40_to_210(raw_module):
    df = pl.DataFrame(
        [
            {"name": "low_ok", "sex": "M", "bodyweight": 40.0, "squat": 100.0, "bench": 100.0, "deadlift": 100.0, "total": 300.0},
            {"name": "high_ok", "sex": "M", "bodyweight": 210.0, "squat": 100.0, "bench": 100.0, "deadlift": 100.0, "total": 300.0},
            {"name": "mid", "sex": "M", "bodyweight": 100.0, "squat": 100.0, "bench": 100.0, "deadlift": 100.0, "total": 300.0},
        ]
    )
    result = raw_module.filter_bw_validity(df)
    assert set(result["name"].to_list()) == {"low_ok", "high_ok", "mid"}


def test_filter_bw_validity_drops_male_outside_range(raw_module):
    df = pl.DataFrame(
        [
            {"name": "too_light", "sex": "M", "bodyweight": 35.0, "squat": 100.0, "bench": 100.0, "deadlift": 100.0, "total": 300.0},
            {"name": "too_heavy", "sex": "M", "bodyweight": 230.0, "squat": 100.0, "bench": 100.0, "deadlift": 100.0, "total": 300.0},
        ]
    )
    result = raw_module.filter_bw_validity(df)
    assert result.is_empty()


def test_filter_bw_validity_drops_female_over_150(raw_module):
    df = pl.DataFrame(
        [
            {"name": "f_ok", "sex": "F", "bodyweight": 150.0, "squat": 100.0, "bench": 100.0, "deadlift": 100.0, "total": 300.0},
            {"name": "f_over", "sex": "F", "bodyweight": 160.0, "squat": 100.0, "bench": 100.0, "deadlift": 100.0, "total": 300.0},
        ]
    )
    result = raw_module.filter_bw_validity(df)
    assert "f_ok" in result["name"].to_list()
    assert "f_over" not in result["name"].to_list()


def test_raw_filters_preserve_dots_column(raw_module):
    """Regression test — dots column must survive every v9 filter."""
    df = pl.DataFrame(
        [
            {"name": "x", "sex": "M", "bodyweight": 80.0, "squat": 200.0, "bench": 100.0, "deadlift": 250.0, "total": 550.0, "dots": 300.0},
        ]
    )
    out = df.pipe(raw_module.filter_bombouts).pipe(raw_module.filter_total_consistency).pipe(raw_module.filter_bw_validity)
    assert "dots" in out.columns
