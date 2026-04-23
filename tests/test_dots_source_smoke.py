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
@pytest.mark.parametrize(
    ("sex", "bw", "total", "expected_dots"),
    [
        # Fill with actual values from raw parquet + liftercalc.com reference.
        # Example placeholder row (replace with real data before running):
        # ("M", 83.0, 700.0, 471.5),
    ],
)
def test_dots_source_matches_liftercalc_reference(sex, bw, total, expected_dots):
    # Compare the specific row in raw parquet to expected_dots within tolerance.
    # Implementation reads conf.raw_s3_http, filters to the row, asserts match.
    pytest.skip("Populate parametrize table with real (sex, bw, total, expected) values first.")
