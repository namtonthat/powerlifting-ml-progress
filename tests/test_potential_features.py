"""Tests for v10 innate-potential features (Groups A/B/C/D)."""

import importlib.util

import polars as pl
import pytest


@pytest.fixture(scope="module")
def base_module():
    spec = importlib.util.spec_from_file_location("base_module", "steps/03_base.py")
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_first_comp_dots_constant_per_lifter(base_module, synthetic_3_lifter_5_comp):
    df = synthetic_3_lifter_5_comp.sort(["primary_key", "date"])
    out = base_module.add_first_comp_features(df)
    n_unique_per_lifter = out.group_by("primary_key").agg(pl.col("first_comp_dots").n_unique())
    assert (n_unique_per_lifter["first_comp_dots"] == 1).all()


def test_first_comp_dots_equals_dots_at_comp_1(base_module, synthetic_3_lifter_5_comp):
    df = synthetic_3_lifter_5_comp.sort(["primary_key", "date"])
    out = base_module.add_first_comp_features(df)
    l1 = out.filter(pl.col("primary_key") == "L1").sort("date")
    assert l1["first_comp_dots"][0] == pytest.approx(200.0)
    assert l1["first_comp_dots"][4] == pytest.approx(200.0)


def test_first_comp_total_per_bw(base_module, synthetic_3_lifter_5_comp):
    df = synthetic_3_lifter_5_comp.sort(["primary_key", "date"])
    out = base_module.add_first_comp_features(df)
    l1 = out.filter(pl.col("primary_key") == "L1").sort("date")
    assert l1["first_comp_total_per_bw"][0] == pytest.approx(3.75)


def test_first_comp_age_constant_per_lifter(base_module, synthetic_3_lifter_5_comp):
    df = synthetic_3_lifter_5_comp.sort(["primary_key", "date"])
    df = base_module.add_age_lifecycle_features(df)
    out = base_module.add_first_comp_features(df)
    l1 = out.filter(pl.col("primary_key") == "L1").sort("date")
    assert (l1["first_comp_age"] == l1["first_comp_age"][0]).all()


def test_first_comp_percentile_in_range(base_module, synthetic_3_lifter_5_comp):
    df = synthetic_3_lifter_5_comp.sort(["primary_key", "date"])
    df = df.with_columns(pl.lit("83").alias("ipf_weight_class"))
    out = base_module.add_first_comp_features(df).pipe(base_module.add_first_comp_percentile)
    vals = out["first_comp_percentile_vs_sex_wc"].drop_nulls().to_list()
    for v in vals:
        assert 0 <= v <= 100


def test_max_dots_so_far_uses_only_prior_comps(base_module, synthetic_3_lifter_5_comp):
    df = synthetic_3_lifter_5_comp.sort(["primary_key", "date"])
    df = base_module.add_previous_dots(df)
    out = base_module.add_rolling_career_features(df).sort(["primary_key", "date"])

    l1 = out.filter(pl.col("primary_key") == "L1").sort("date")
    # comp 1: no prior → null
    assert l1["max_dots_so_far"][0] is None
    # comp 2: prior = {200} → 200
    assert l1["max_dots_so_far"][1] == pytest.approx(200.0)
    # comp 5: prior = {200, 215, 230, 245} → 245
    assert l1["max_dots_so_far"][4] == pytest.approx(245.0)


def test_best_growth_rate_so_far_null_until_comp_3(base_module, synthetic_3_lifter_5_comp):
    df = synthetic_3_lifter_5_comp.sort(["primary_key", "date"])
    df = base_module.add_previous_dots(df)
    df = base_module.add_pct_change_dots(df)
    out = base_module.add_rolling_career_features(df).sort(["primary_key", "date"])

    l1 = out.filter(pl.col("primary_key") == "L1").sort("date")
    assert l1["best_growth_rate_so_far"][0] is None
    assert l1["best_growth_rate_so_far"][1] is None
    # comp 3: prior pct_change_dots values = {null, ~7.5} → max=7.5
    assert l1["best_growth_rate_so_far"][2] is not None


def test_best_growth_rate_so_far_does_not_leak_across_lifters(base_module, synthetic_3_lifter_5_comp):
    """Critical: shift(1) and cum_max must both be scoped to primary_key.

    If either op leaks across lifters, L3's (Elite, big growth) values would
    pollute L1's (Beginner, small growth) comp-2 feature value.
    """
    df = synthetic_3_lifter_5_comp.sort(["primary_key", "date"])
    df = base_module.add_previous_dots(df)
    df = base_module.add_pct_change_dots(df)
    out = base_module.add_rolling_career_features(df).sort(["primary_key", "date"])

    l1 = out.filter(pl.col("primary_key") == "L1").sort("date")
    assert l1["best_growth_rate_so_far"][1] is None, "Shift leaked across lifter boundary"


def test_dots_growth_trend_positive_for_improver(base_module, synthetic_3_lifter_5_comp):
    df = synthetic_3_lifter_5_comp.sort(["primary_key", "date"])
    df = base_module.add_previous_dots(df)
    out = base_module.add_dots_growth_trend(df).sort(["primary_key", "date"])

    l1 = out.filter(pl.col("primary_key") == "L1").sort("date")
    # L1 is an improver: prior dots {200, 215, 230, 245} → positive slope
    # At comp 5 we have 4 prior points → non-null slope
    assert l1["dots_growth_trend"][4] is not None
    assert l1["dots_growth_trend"][4] > 0


def test_dots_growth_trend_null_until_comp_3(base_module, synthetic_3_lifter_5_comp):
    df = synthetic_3_lifter_5_comp.sort(["primary_key", "date"])
    df = base_module.add_previous_dots(df)
    out = base_module.add_dots_growth_trend(df).sort(["primary_key", "date"])

    l1 = out.filter(pl.col("primary_key") == "L1").sort("date")
    # Need >= 2 prior points for a slope → null at comps 1 and 2
    assert l1["dots_growth_trend"][0] is None
    assert l1["dots_growth_trend"][1] is None
    assert l1["dots_growth_trend"][2] is not None
