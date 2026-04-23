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


def test_dots_tiers_has_5_tiers():
    assert list(conf.DOTS_TIERS.keys()) == ["Beginner", "Intermediate", "Advanced", "Elite", "World Class"]


def test_dots_tier_order_matches():
    assert conf.DOTS_TIER_ORDER == ["Beginner", "Intermediate", "Advanced", "Elite", "World Class"]


def test_dots_tiers_half_open_200_is_intermediate():
    assert conf.dots_tier_for_score(200.0) == "Intermediate"


def test_dots_tiers_half_open_500_is_world_class():
    assert conf.dots_tier_for_score(500.0) == "World Class"


def test_dots_tiers_boundaries():
    assert conf.dots_tier_for_score(0.0) == "Beginner"
    assert conf.dots_tier_for_score(199.99) == "Beginner"
    assert conf.dots_tier_for_score(300.0) == "Advanced"
    assert conf.dots_tier_for_score(400.0) == "Elite"
    assert conf.dots_tier_for_score(999.0) == "World Class"


def test_dots_tier_ordinal_returns_0_to_4():
    assert conf.dots_tier_ordinal(150.0) == 0
    assert conf.dots_tier_ordinal(250.0) == 1
    assert conf.dots_tier_ordinal(350.0) == 2
    assert conf.dots_tier_ordinal(450.0) == 3
    assert conf.dots_tier_ordinal(550.0) == 4
