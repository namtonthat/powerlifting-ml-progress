"""Smoke test — proves pytest can import from steps/."""

import conf


def test_conf_imports_and_has_expected_constants():
    assert conf.DAYS_IN_YEAR == 365.25
    assert conf.MIN_COMPETITIONS == 3
