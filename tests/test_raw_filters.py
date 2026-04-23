"""Tests for data-cleaning filters added in v9."""

import pytest


@pytest.fixture
def raw_module():
    # The raw script is named with a leading digit — load it by file
    import importlib.util

    spec = importlib.util.spec_from_file_location("raw_module", "steps/03_raw.py")
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
