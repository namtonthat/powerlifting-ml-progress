"""Tests for DOTS-parallel base-layer features added in v9."""

import importlib.util

import polars as pl
import pytest


@pytest.fixture
def base_module():
    spec = importlib.util.spec_from_file_location("base_module", "steps/03_base.py")
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_add_previous_dots_shifts_by_one(base_module, synthetic_3_lifter_5_comp):
    df = synthetic_3_lifter_5_comp.sort(["primary_key", "date"])
    out = base_module.add_previous_dots(df)
    # L1 comp 0 → null; comp 1 → L1 comp 0's dots (200.0)
    l1 = out.filter(pl.col("primary_key") == "L1").sort("date")
    assert l1["previous_dots"][0] is None
    assert l1["previous_dots"][1] == pytest.approx(200.0)
    assert l1["previous_dots"][2] == pytest.approx(215.0)


def test_add_previous_dots_no_leakage_across_lifters(base_module, synthetic_3_lifter_5_comp):
    df = synthetic_3_lifter_5_comp.sort(["primary_key", "date"])
    out = base_module.add_previous_dots(df)
    # First row of each lifter must have null previous_dots
    firsts = out.group_by("primary_key").first()
    assert firsts["previous_dots"].null_count() == firsts.height


def test_pct_change_dots_is_null_when_previous_is_zero(base_module):
    df = pl.DataFrame(
        [
            {"primary_key": "X", "dots": 300.0, "previous_dots": 0.0},
            {"primary_key": "X", "dots": 300.0, "previous_dots": 200.0},
        ]
    )
    out = base_module.add_pct_change_dots(df)
    assert out["pct_change_dots"][0] is None
    assert out["pct_change_dots"][1] == pytest.approx(50.0)


def test_min_days_between_comps_constant_is_30():
    """Sanity: the guard threshold lives in conf.MIN_DAYS_BETWEEN_COMPS."""
    import conf

    assert conf.MIN_DAYS_BETWEEN_COMPS == 30
