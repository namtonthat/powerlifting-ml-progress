"""Smoke test: OpenPowerlifting's Dots column agrees with reference calculators.

Run manually after initial raw layer creation. Pick ≥10 rows spanning both sexes
and a range of bodyweights/totals. For each row:
  1. Note the upstream `dots` value.
  2. Compute DOTS for the same (sex, bodyweight, total) using liftercalc.com.
  3. Assert agreement within 1.0 DOTS (allows rounding differences).

Marking this test `@pytest.mark.manual` keeps it out of CI until reference values
are populated.
"""

import pytest


@pytest.mark.manual
def test_dots_source_matches_liftercalc_reference():
    """Populate with real (sex, bw, total, expected_dots) tuples from liftercalc.com.

    Once populated, parametrize this test over those tuples and assert the
    upstream `dots` column in the raw parquet agrees within 1.0 DOTS.
    """
    pytest.skip("Populate with real (sex, bw, total, expected_dots) values from liftercalc.com first.")
