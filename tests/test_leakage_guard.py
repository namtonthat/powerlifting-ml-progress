"""Global leakage guard — runs every version.

For every `previous_*` / `prev_*` feature, asserts value at comp k equals value
of the source column at comp k-1 for same primary_key.
"""

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
