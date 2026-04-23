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
