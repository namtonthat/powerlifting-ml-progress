"""Global leakage guard — runs every version.

For every `previous_*` / `prev_*` feature, asserts value at comp k equals value
of the source column at comp k-1 for same primary_key.
"""

import polars as pl
import pytest


@pytest.mark.parametrize(
    ("source_col", "shifted_col", "feature_fn_name"),
    [
        ("dots", "previous_dots", "add_previous_dots"),
        ("wilks", "previous_wilks", "add_previous_wilks"),
        ("squat", "previous_squat", "add_previous_powerlifting_records"),
        ("bench", "previous_bench", "add_previous_powerlifting_records"),
        ("deadlift", "previous_deadlift", "add_previous_powerlifting_records"),
        ("total", "previous_total", "add_previous_powerlifting_records"),
    ],
)
def test_shifted_feature_no_leakage(base_module, synthetic_3_lifter_5_comp, source_col, shifted_col, feature_fn_name):
    df = synthetic_3_lifter_5_comp.sort(["primary_key", "date"])
    fn = getattr(base_module, feature_fn_name)
    out = fn(df).sort(["primary_key", "date"])

    # For every lifter, previous_X at comp k must equal X at comp k-1
    for lifter in ["L1", "L2", "L3"]:
        lifter_df = out.filter(pl.col("primary_key") == lifter)
        source_vals = lifter_df[source_col].to_list()
        shifted_vals = lifter_df[shifted_col].to_list()
        assert shifted_vals[0] is None, f"{lifter} {shifted_col}[0] should be null"
        for k in range(1, len(source_vals)):
            assert shifted_vals[k] == source_vals[k - 1], f"{lifter} {shifted_col}[{k}] != {source_col}[{k - 1}]"


@pytest.mark.parametrize(
    "feature_col",
    ["first_comp_dots", "first_comp_total_per_bw", "first_comp_age"],
)
def test_first_comp_feature_constant_per_lifter(base_module, synthetic_3_lifter_5_comp, feature_col):
    """first_comp_* features must be constant per lifter (no per-row drift)."""
    df = synthetic_3_lifter_5_comp.sort(["primary_key", "date"])
    df = base_module.add_age_lifecycle_features(df)
    out = base_module.add_first_comp_features(df)
    unique_counts = out.group_by("primary_key").agg(pl.col(feature_col).n_unique())
    assert (unique_counts[feature_col] <= 1).all()


def test_first_comp_feature_matches_lifter_first_row(base_module, synthetic_3_lifter_5_comp):
    """Value across all rows must equal the value computed from that lifter's comp 1 data."""
    df = synthetic_3_lifter_5_comp.sort(["primary_key", "date"])
    df = base_module.add_age_lifecycle_features(df)
    out = base_module.add_first_comp_features(df).sort(["primary_key", "date"])

    for lifter in ["L1", "L2", "L3"]:
        lifter_df = out.filter(pl.col("primary_key") == lifter).sort("date")
        # first_comp_dots should equal dots at row 0 for each lifter
        assert lifter_df["first_comp_dots"][0] == lifter_df["dots"][0]
        assert (lifter_df["first_comp_dots"] == lifter_df["first_comp_dots"][0]).all()


@pytest.mark.parametrize(
    ("feature_col", "source_col"),
    [
        ("max_dots_so_far", "dots"),
        ("max_tier_so_far", "prev_tier"),
    ],
)
def test_so_far_feature_uses_only_prior_comps(base_module, synthetic_3_lifter_5_comp, feature_col, source_col):
    """Verify that *_so_far value at comp k matches max of source column restricted to comps 1..k-1.

    For max_dots_so_far: source is `dots` (but feature comes from previous_dots.cum_max — max of comps 1..k-1).
    For max_tier_so_far: source is `prev_tier` (already shift-1 of tier, so max of previous tiers).

    Note: for shifted sources like prev_tier, position 0 is null (no prior), positions 1..k
    represent comps 1..k-1. So at row k, we check max of source_vals[1..k].
    """
    df = synthetic_3_lifter_5_comp.sort(["primary_key", "date"])
    df = base_module.add_previous_dots(df)
    df = base_module.add_pct_change_dots(df)
    df = base_module.add_age_lifecycle_features(df)
    df = base_module.add_first_comp_features(df)
    df = base_module.add_tier_features(df)
    out = base_module.add_rolling_career_features(df).sort(["primary_key", "date"])

    for lifter in ["L1", "L2", "L3"]:
        lifter_df = out.filter(pl.col("primary_key") == lifter).sort("date")
        source_vals = lifter_df[source_col].to_list()
        feat_vals = lifter_df[feature_col].to_list()

        # Comp 1: no prior data → must be null
        assert feat_vals[0] is None, f"{lifter} {feature_col}[0] must be null"

        # Comp k >= 2: feature[k] must equal max of source_vals.
        # For non-shifted sources (like dots): check max(source_vals[0..k-1])
        # For shifted sources (like prev_tier): check max(source_vals[1..k]) which represent comps 1..k-1
        for k in range(1, len(source_vals)):
            prior = [v for v in source_vals[1 : k + 1] if v is not None] if source_col == "prev_tier" else [v for v in source_vals[:k] if v is not None]

            if not prior:
                assert feat_vals[k] is None, f"{lifter} {feature_col}[{k}] must be null"
            else:
                assert feat_vals[k] == max(prior), f"{lifter} {feature_col}[{k}] wrong: got {feat_vals[k]}, expected {max(prior)}"
