"""Tests for DOTS-related constants in conf.py."""

import sys
from pathlib import Path

# Add steps directory to path
sys.path.insert(0, str(Path(__file__).parent.parent / "steps"))

import conf


def test_dots_valid_bw_range_male_is_40_to_210():
    assert conf.DOTS_VALID_BW_RANGE["M"] == (40.0, 210.0)


def test_dots_valid_bw_range_female_is_40_to_150():
    assert conf.DOTS_VALID_BW_RANGE["F"] == (40.0, 150.0)


def test_total_consistency_tolerance_is_2_5_kg():
    assert conf.TOTAL_CONSISTENCY_TOLERANCE_KG == 2.5
