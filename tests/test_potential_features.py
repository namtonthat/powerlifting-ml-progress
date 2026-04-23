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


def test_early_growth_rate_null_before_comp_4(base_module, synthetic_3_lifter_5_comp):
    df = synthetic_3_lifter_5_comp.sort(["primary_key", "date"]).with_columns(
        pl.cum_count("primary_key").over("primary_key").alias("cumulative_comps"),
        ((pl.col("date") - pl.col("date").shift(1)).dt.total_days() / 365.25).over("primary_key").alias("time_since_last_comp_years"),
    )
    out = base_module.add_early_growth_rate(df).sort(["primary_key", "date"])

    l1 = out.filter(pl.col("primary_key") == "L1").sort("date")
    # Comps 1-3 must be null (uses comps 1..3 data, so available from comp 4 onward)
    assert l1["early_growth_rate_dots_per_year"][0] is None
    assert l1["early_growth_rate_dots_per_year"][1] is None
    assert l1["early_growth_rate_dots_per_year"][2] is None


def test_early_growth_rate_uses_no_current_comp_dots(base_module, synthetic_3_lifter_5_comp):
    """Feature at comp 4 must depend only on comps 1..3 dots, not comp 4's."""
    df = synthetic_3_lifter_5_comp.sort(["primary_key", "date"]).with_columns(
        pl.cum_count("primary_key").over("primary_key").alias("cumulative_comps"),
        ((pl.col("date") - pl.col("date").shift(1)).dt.total_days() / 365.25).over("primary_key").alias("time_since_last_comp_years"),
    )
    out1 = base_module.add_early_growth_rate(df).sort(["primary_key", "date"])
    v1 = out1.filter(pl.col("primary_key") == "L1").sort("date")["early_growth_rate_dots_per_year"][3]

    # Mutate comp 4's dots (should NOT change feature at comp 4)
    df2 = df.with_columns(
        pl.when((pl.col("primary_key") == "L1") & (pl.col("cumulative_comps") == 4)).then(999.0).otherwise(pl.col("dots")).alias("dots"),
    )
    out2 = base_module.add_early_growth_rate(df2).sort(["primary_key", "date"])
    v2 = out2.filter(pl.col("primary_key") == "L1").sort("date")["early_growth_rate_dots_per_year"][3]

    assert v1 == v2, "early_growth_rate must not depend on current comp's dots"


def test_early_growth_rate_null_when_years_between_zero(base_module):
    """Guard: if comp 1 and comp 3 fall on the same date (years_between == 0),
    the feature must be null, not ±inf (divide-by-zero protection)."""
    from datetime import date

    df = pl.DataFrame(
        [
            {"primary_key": "X", "date": date(2024, 1, 1), "dots": 300.0, "cumulative_comps": 1},
            {"primary_key": "X", "date": date(2024, 1, 1), "dots": 310.0, "cumulative_comps": 2},
            {"primary_key": "X", "date": date(2024, 1, 1), "dots": 320.0, "cumulative_comps": 3},
            {"primary_key": "X", "date": date(2024, 2, 1), "dots": 330.0, "cumulative_comps": 4},
        ]
    )
    out = base_module.add_early_growth_rate(df)
    # Comp 4's value should be null (years_between == 0 between comp 1 and comp 3)
    assert out["early_growth_rate_dots_per_year"][3] is None


def test_starting_tier_matches_first_comp_dots(base_module, synthetic_3_lifter_5_comp):
    df = synthetic_3_lifter_5_comp.sort(["primary_key", "date"])
    df = base_module.add_previous_dots(df)
    df = base_module.add_age_lifecycle_features(df)
    df = base_module.add_first_comp_features(df)
    out = base_module.add_tier_features(df).sort(["primary_key", "date"])

    l1 = out.filter(pl.col("primary_key") == "L1").sort("date")
    # L1 first_comp_dots = 200.0 → Intermediate (half-open at 200) → ordinal 1
    assert l1["starting_tier"][0] == 1


def test_prev_tier_uses_previous_dots(base_module, synthetic_3_lifter_5_comp):
    df = synthetic_3_lifter_5_comp.sort(["primary_key", "date"])
    df = base_module.add_previous_dots(df)
    df = base_module.add_age_lifecycle_features(df)
    df = base_module.add_first_comp_features(df)
    out = base_module.add_tier_features(df).sort(["primary_key", "date"])

    l1 = out.filter(pl.col("primary_key") == "L1").sort("date")
    # comp 1 previous_dots is null → prev_tier null
    assert l1["prev_tier"][0] is None
    # comp 2 previous_dots = 200.0 → Intermediate → 1
    assert l1["prev_tier"][1] == 1


def test_tiers_climbed_so_far_non_negative(base_module, synthetic_3_lifter_5_comp):
    df = synthetic_3_lifter_5_comp.sort(["primary_key", "date"])
    df = base_module.add_previous_dots(df)
    df = base_module.add_age_lifecycle_features(df)
    df = base_module.add_first_comp_features(df)
    out = base_module.add_tier_features(df).sort(["primary_key", "date"])
    vals = out["tiers_climbed_so_far"].drop_nulls().to_list()
    for v in vals:
        assert v >= 0
