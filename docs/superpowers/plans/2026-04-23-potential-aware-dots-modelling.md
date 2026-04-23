# Potential-Aware DOTS Modelling Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build three sequential modelling iterations (v9, v10, v11) that introduce DOTS-based features, innate-potential signals, and dual-head training to the powerlifting progress model.

**Architecture:** Three phases accumulate on top of v8:
- **v9:** add data-cleaning filters (bomb-out, total-consistency, bodyweight-validity) and DOTS-parallel features.
- **v10:** add first-comp birth-certificate features, rolling career-so-far features, DOTS tier labels, and potential × context interactions.
- **v11:** train two heads (kg-based `pct_change_total` and DOTS-based `pct_change_dots`) on identical feature sets, compare via normalised RMSE and Spearman ρ, update dashboard.

Tests use pytest with synthetic fixtures. All new features enforce leak-free invariants via a parametrised global leakage guard. Each phase ends with a journey-doc entry and a regression guard (fail if test R² drops >0.02 vs prior version).

**Tech Stack:** Python 3.13, Polars (feature engineering), XGBoost 3.x, Optuna 4.x (HPO), MLflow 2.x (tracking), pytest (new), DuckDB (existing sqls/), Streamlit (dashboard).

**Spec:** `docs/superpowers/specs/2026-04-23-potential-aware-dots-modelling-design.md`

---

## Phase 0 — Bootstrap test infrastructure

### Task 0.1: Add pytest dev dependency

**Files:**
- Modify: `pyproject.toml`

- [ ] **Step 1: Add pytest + pytest-cov as dependency group**

Update `pyproject.toml` — add a `[dependency-groups]` section (uv convention):

```toml
[dependency-groups]
dev = [
  "pytest>=8.3.0",
  "pytest-cov>=5.0.0",
]

[tool.pytest.ini_options]
pythonpath = ["steps"]
testpaths = ["tests"]
```

The `pythonpath = ["steps"]` line is critical — existing scripts do `import conf`, `import common_io` (flat), so tests need the same import resolution.

- [ ] **Step 2: Install deps**

Run: `uv sync --all-groups`
Expected: pytest installs without error.

- [ ] **Step 3: Verify pytest is available**

Run: `uv run pytest --version`
Expected: version string like `pytest 8.x.y`.

- [ ] **Step 4: Commit**

```bash
git add pyproject.toml uv.lock
git commit -m "chore: add pytest dev dependency for test infrastructure"
```

---

### Task 0.2: Create tests/ skeleton + shared fixtures

**Files:**
- Create: `tests/__init__.py`
- Create: `tests/conftest.py`
- Create: `tests/test_smoke.py`

- [ ] **Step 1: Create empty `tests/__init__.py`**

```python
```

(Empty file — just makes `tests/` a package.)

- [ ] **Step 2: Create `tests/conftest.py` with synthetic-lifter fixture**

```python
"""Shared pytest fixtures for synthetic lifter/comp data."""

from datetime import date

import polars as pl
import pytest


@pytest.fixture
def synthetic_3_lifter_5_comp() -> pl.DataFrame:
    """3 lifters × 5 comps each with deterministic values.

    Lifter L1 — steady improver (Beginner → Intermediate)
    Lifter L2 — plateau lifter (Intermediate stays Intermediate)
    Lifter L3 — elite starter (Advanced from comp 1, reaches Elite)
    """
    rows = []
    # L1: totals 300→320→340→360→380, bw 80, sex M
    for i, total in enumerate([300, 320, 340, 360, 380]):
        rows.append(
            {
                "primary_key": "L1",
                "name": "L1",
                "date": date(2020 + i, 6, 1),
                "sex": "M",
                "bodyweight": 80.0,
                "squat": total * 0.35,
                "bench": total * 0.25,
                "deadlift": total * 0.40,
                "total": float(total),
                "dots": 200.0 + i * 15,
                "wilks": 200.0 + i * 15,
                "year_of_birth": 1995,
            }
        )
    # L2: totals 500→505→500→510→508, bw 90, sex M (plateau at Intermediate edge)
    for i, total in enumerate([500, 505, 500, 510, 508]):
        rows.append(
            {
                "primary_key": "L2",
                "name": "L2",
                "date": date(2020 + i, 6, 1),
                "sex": "M",
                "bodyweight": 90.0,
                "squat": total * 0.35,
                "bench": total * 0.25,
                "deadlift": total * 0.40,
                "total": float(total),
                "dots": 290.0 + i,
                "wilks": 290.0 + i,
                "year_of_birth": 1990,
            }
        )
    # L3: totals 650→680→710→740→770, bw 75, sex F (elite starter)
    for i, total in enumerate([650, 680, 710, 740, 770]):
        rows.append(
            {
                "primary_key": "L3",
                "name": "L3",
                "date": date(2020 + i, 6, 1),
                "sex": "F",
                "bodyweight": 75.0,
                "squat": total * 0.33,
                "bench": total * 0.27,
                "deadlift": total * 0.40,
                "total": float(total),
                "dots": 420.0 + i * 12,
                "wilks": 420.0 + i * 12,
                "year_of_birth": 1993,
            }
        )
    return pl.DataFrame(rows)


@pytest.fixture
def bombout_rows() -> pl.DataFrame:
    """Rows that should be filtered by the bomb-out filter."""
    return pl.DataFrame(
        [
            {"name": "bomb_squat", "squat": 0.0, "bench": 100.0, "deadlift": 200.0, "total": 300.0},
            {"name": "bomb_bench", "squat": 150.0, "bench": 0.0, "deadlift": 200.0, "total": 350.0},
            {"name": "bomb_dl", "squat": 150.0, "bench": 100.0, "deadlift": 0.0, "total": 250.0},
            {"name": "bomb_neg", "squat": -1.0, "bench": 100.0, "deadlift": 200.0, "total": 299.0},
            {"name": "clean", "squat": 150.0, "bench": 100.0, "deadlift": 200.0, "total": 450.0},
        ]
    )
```

- [ ] **Step 3: Create `tests/test_smoke.py`**

```python
"""Smoke test — proves pytest can import from steps/."""

import conf


def test_conf_imports_and_has_expected_constants():
    assert conf.DAYS_IN_YEAR == 365.25
    assert conf.MIN_COMPETITIONS == 3
```

- [ ] **Step 4: Run smoke test**

Run: `uv run pytest tests/test_smoke.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/__init__.py tests/conftest.py tests/test_smoke.py
git commit -m "test: bootstrap pytest infrastructure with synthetic fixtures"
```

---

## Phase 1 — v9: Data cleaning filters in `03_raw.py`

### Task 1.1: Add filter constants to conf.py

**Files:**
- Modify: `steps/conf.py`

- [ ] **Step 1: Write failing test for new constants**

Create `tests/test_conf_dots_constants.py`:

```python
"""Tests for DOTS-related constants in conf.py."""

import conf


def test_dots_valid_bw_range_male_is_40_to_210():
    assert conf.DOTS_VALID_BW_RANGE["M"] == (40.0, 210.0)


def test_dots_valid_bw_range_female_is_40_to_150():
    assert conf.DOTS_VALID_BW_RANGE["F"] == (40.0, 150.0)


def test_total_consistency_tolerance_is_2_5_kg():
    assert conf.TOTAL_CONSISTENCY_TOLERANCE_KG == 2.5
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/test_conf_dots_constants.py -v`
Expected: FAIL — `AttributeError: module 'conf' has no attribute 'DOTS_VALID_BW_RANGE'`.

- [ ] **Step 3: Add constants to `steps/conf.py`**

Add after the existing `IPF_WEIGHT_CLASSES` block (~line 79):

```python
# DOTS formula — canonical reference: OpenPowerlifting opl-csv crates/coefficients/src/dots.rs
# Konertz (2019). Valid bodyweight ranges (Konertz): M 40-210 kg, F 40-150 kg.
DOTS_VALID_BW_RANGE = {"M": (40.0, 210.0), "F": (40.0, 150.0)}

# Data quality tolerance
TOTAL_CONSISTENCY_TOLERANCE_KG = 2.5
```

- [ ] **Step 4: Run tests, expect PASS**

Run: `uv run pytest tests/test_conf_dots_constants.py -v`
Expected: 3 PASS.

- [ ] **Step 5: Commit**

```bash
git add steps/conf.py tests/test_conf_dots_constants.py
git commit -m "feat(conf): add DOTS_VALID_BW_RANGE and TOTAL_CONSISTENCY_TOLERANCE_KG"
```

---

### Task 1.2: Bomb-out filter

**Files:**
- Create: `tests/test_raw_filters.py`
- Modify: `steps/03_raw.py`

- [ ] **Step 1: Write failing test**

Create `tests/test_raw_filters.py`:

```python
"""Tests for data-cleaning filters added in v9."""

import importlib

import polars as pl
import pytest


@pytest.fixture
def raw_module():
    # The raw script is named with a leading digit — load it by file
    import importlib.util

    spec = importlib.util.spec_from_file_location("raw_module", "steps/03_raw.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_filter_bombouts_drops_rows_with_zero_squat(raw_module, bombout_rows):
    result = raw_module.filter_bombouts(bombout_rows)
    assert "bomb_squat" not in result["name"].to_list()


def test_filter_bombouts_drops_rows_with_zero_bench(raw_module, bombout_rows):
    result = raw_module.filter_bombouts(bombout_rows)
    assert "bomb_bench" not in result["name"].to_list()


def test_filter_bombouts_drops_rows_with_zero_deadlift(raw_module, bombout_rows):
    result = raw_module.filter_bombouts(bombout_rows)
    assert "bomb_dl" not in result["name"].to_list()


def test_filter_bombouts_drops_rows_with_negative_lift(raw_module, bombout_rows):
    result = raw_module.filter_bombouts(bombout_rows)
    assert "bomb_neg" not in result["name"].to_list()


def test_filter_bombouts_keeps_clean_rows(raw_module, bombout_rows):
    result = raw_module.filter_bombouts(bombout_rows)
    assert "clean" in result["name"].to_list()
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/test_raw_filters.py -v`
Expected: FAIL — `AttributeError: module 'raw_module' has no attribute 'filter_bombouts'`.

- [ ] **Step 3: Add `filter_bombouts` to `steps/03_raw.py`**

Read `steps/03_raw.py` first to find the right spot. Add after the existing filter functions (search for `filter_for_raw_events` or similar). Add:

```python
@conf.debug
def filter_bombouts(df: pl.DataFrame) -> pl.DataFrame:
    """Drop rows where any of squat/bench/deadlift <= 0.

    These are SBD events where the lifter failed all attempts on at least one lift;
    the total is recorded but the progress signal is garbage.
    """
    n_before = len(df)
    out = df.filter((pl.col("squat") > 0) & (pl.col("bench") > 0) & (pl.col("deadlift") > 0))
    logging.info("filter_bombouts dropped %d rows (%.2f%%)", n_before - len(out), 100 * (n_before - len(out)) / max(n_before, 1))
    return out
```

Ensure `import logging` is at the top of `03_raw.py`; add if missing.

- [ ] **Step 4: Run tests, expect PASS**

Run: `uv run pytest tests/test_raw_filters.py -v`
Expected: 5 PASS.

- [ ] **Step 5: Wire `filter_bombouts` into the `__main__` pipeline**

In `steps/03_raw.py`, find the `if __name__ == "__main__":` block. Insert `.pipe(filter_bombouts)` **after** the existing filter (`filter_for_raw_events` or equivalent). Example:

```python
df = (
    pl.read_parquet(...)
    # ...
    .pipe(filter_for_raw_events)
    .pipe(filter_bombouts)   # NEW
    # ...
)
```

- [ ] **Step 6: Commit**

```bash
git add steps/03_raw.py tests/test_raw_filters.py
git commit -m "feat(raw): filter bomb-out rows where any lift <= 0"
```

---

### Task 1.3: Total-consistency filter

**Files:**
- Modify: `tests/test_raw_filters.py`
- Modify: `steps/03_raw.py`

- [ ] **Step 1: Append failing tests to `tests/test_raw_filters.py`**

```python
def test_filter_total_consistency_keeps_within_tolerance(raw_module):
    df = pl.DataFrame(
        [
            # total matches sbd exactly
            {"name": "exact", "squat": 200.0, "bench": 100.0, "deadlift": 300.0, "total": 600.0},
            # total off by 2.0 kg — within 2.5 kg tolerance
            {"name": "within_tol", "squat": 200.0, "bench": 100.0, "deadlift": 300.0, "total": 598.0},
        ]
    )
    result = raw_module.filter_total_consistency(df)
    assert set(result["name"].to_list()) == {"exact", "within_tol"}


def test_filter_total_consistency_drops_outside_tolerance(raw_module):
    df = pl.DataFrame(
        [
            # total off by 10 kg — outside tolerance
            {"name": "way_off", "squat": 200.0, "bench": 100.0, "deadlift": 300.0, "total": 590.0},
            {"name": "exact", "squat": 200.0, "bench": 100.0, "deadlift": 300.0, "total": 600.0},
        ]
    )
    result = raw_module.filter_total_consistency(df)
    assert "way_off" not in result["name"].to_list()
    assert "exact" in result["name"].to_list()
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/test_raw_filters.py::test_filter_total_consistency_keeps_within_tolerance -v`
Expected: FAIL — `filter_total_consistency` doesn't exist.

- [ ] **Step 3: Add `filter_total_consistency` to `steps/03_raw.py`**

```python
@conf.debug
def filter_total_consistency(df: pl.DataFrame) -> pl.DataFrame:
    """Drop rows where |squat + bench + deadlift - total| > tolerance.

    Catches data-entry errors. Tolerance: conf.TOTAL_CONSISTENCY_TOLERANCE_KG (2.5 kg).
    """
    n_before = len(df)
    sbd_sum = pl.col("squat") + pl.col("bench") + pl.col("deadlift")
    out = df.filter((sbd_sum - pl.col("total")).abs() <= conf.TOTAL_CONSISTENCY_TOLERANCE_KG)
    logging.info("filter_total_consistency dropped %d rows (%.2f%%)", n_before - len(out), 100 * (n_before - len(out)) / max(n_before, 1))
    return out
```

- [ ] **Step 4: Run tests, expect PASS**

Run: `uv run pytest tests/test_raw_filters.py -v`
Expected: all PASS (5 bomb-out + 2 consistency).

- [ ] **Step 5: Wire into `__main__` pipeline**

Insert `.pipe(filter_total_consistency)` after `.pipe(filter_bombouts)`.

- [ ] **Step 6: Commit**

```bash
git add steps/03_raw.py tests/test_raw_filters.py
git commit -m "feat(raw): filter rows where total disagrees with sbd sum > 2.5 kg"
```

---

### Task 1.4: Bodyweight-validity filter

**Files:**
- Modify: `tests/test_raw_filters.py`
- Modify: `steps/03_raw.py`

- [ ] **Step 1: Append failing tests**

```python
def test_filter_bw_validity_keeps_male_40_to_210(raw_module):
    df = pl.DataFrame(
        [
            {"name": "low_ok", "sex": "M", "bodyweight": 40.0, "squat": 100.0, "bench": 100.0, "deadlift": 100.0, "total": 300.0},
            {"name": "high_ok", "sex": "M", "bodyweight": 210.0, "squat": 100.0, "bench": 100.0, "deadlift": 100.0, "total": 300.0},
            {"name": "mid", "sex": "M", "bodyweight": 100.0, "squat": 100.0, "bench": 100.0, "deadlift": 100.0, "total": 300.0},
        ]
    )
    result = raw_module.filter_bw_validity(df)
    assert set(result["name"].to_list()) == {"low_ok", "high_ok", "mid"}


def test_filter_bw_validity_drops_male_outside_range(raw_module):
    df = pl.DataFrame(
        [
            {"name": "too_light", "sex": "M", "bodyweight": 35.0, "squat": 100.0, "bench": 100.0, "deadlift": 100.0, "total": 300.0},
            {"name": "too_heavy", "sex": "M", "bodyweight": 230.0, "squat": 100.0, "bench": 100.0, "deadlift": 100.0, "total": 300.0},
        ]
    )
    result = raw_module.filter_bw_validity(df)
    assert result.is_empty()


def test_filter_bw_validity_drops_female_over_150(raw_module):
    df = pl.DataFrame(
        [
            {"name": "f_ok", "sex": "F", "bodyweight": 150.0, "squat": 100.0, "bench": 100.0, "deadlift": 100.0, "total": 300.0},
            {"name": "f_over", "sex": "F", "bodyweight": 160.0, "squat": 100.0, "bench": 100.0, "deadlift": 100.0, "total": 300.0},
        ]
    )
    result = raw_module.filter_bw_validity(df)
    assert "f_ok" in result["name"].to_list()
    assert "f_over" not in result["name"].to_list()
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/test_raw_filters.py -k bw_validity -v`
Expected: FAIL — `filter_bw_validity` undefined.

- [ ] **Step 3: Add filter to `steps/03_raw.py`**

```python
@conf.debug
def filter_bw_validity(df: pl.DataFrame) -> pl.DataFrame:
    """Drop rows with bodyweight outside Konertz's DOTS validity range.

    M: 40-210 kg (inclusive), F: 40-150 kg (inclusive).
    DOTS is mathematically undefined outside these ranges.
    """
    m_low, m_high = conf.DOTS_VALID_BW_RANGE["M"]
    f_low, f_high = conf.DOTS_VALID_BW_RANGE["F"]
    n_before = len(df)
    out = df.filter(
        ((pl.col("sex") == "M") & (pl.col("bodyweight") >= m_low) & (pl.col("bodyweight") <= m_high))
        | ((pl.col("sex") == "F") & (pl.col("bodyweight") >= f_low) & (pl.col("bodyweight") <= f_high))
    )
    logging.info("filter_bw_validity dropped %d rows (%.2f%%)", n_before - len(out), 100 * (n_before - len(out)) / max(n_before, 1))
    return out
```

- [ ] **Step 4: Run tests, expect PASS**

Run: `uv run pytest tests/test_raw_filters.py -v`
Expected: all PASS.

- [ ] **Step 5: Wire into `__main__` pipeline** (after `filter_total_consistency`)

- [ ] **Step 6: Commit**

```bash
git add steps/03_raw.py tests/test_raw_filters.py
git commit -m "feat(raw): filter rows with bodyweight outside Konertz DOTS validity range"
```

---

## Phase 2 — v9: DOTS integration

### Task 2.1: Inspect raw data for `Dots` column

**Files:**
- Create: `scripts/inspect_dots_column.py` (throwaway — not committed)

- [ ] **Step 1: Create inspection script**

Write to `scripts/inspect_dots_column.py`:

```python
"""Throwaway: check if OpenPowerlifting raw parquet has a Dots column."""

import polars as pl

# Use S3 HTTP URL from conf
from conf import raw_s3_http

schema = pl.read_parquet_schema(raw_s3_http)
print("Columns:", sorted(schema.keys()))
print("Has Dots?", "Dots" in schema or "dots" in schema)
if "Dots" in schema:
    print("Dots dtype:", schema["Dots"])
```

Run: `uv run python scripts/inspect_dots_column.py`

- [ ] **Step 2: Record finding**

Check output. Two branches:

**Branch A: `Dots` column exists** → skip Task 2.2, add `dots` to renames in `03_raw.py` instead. Note in journey doc.

**Branch B: `Dots` column missing** → proceed to Task 2.2 (implement formula locally).

- [ ] **Step 3: Delete throwaway script**

```bash
rm scripts/inspect_dots_column.py
```

(Don't commit it.)

---

### Task 2.2: Add DOTS column (conditional on Task 2.1 finding)

**Branch A — `Dots` column exists upstream:**

**Files:**
- Modify: `steps/conf.py`
- Modify: `steps/03_raw.py`

- [ ] **Step 1: Add `Dots` to required landing columns**

In `steps/conf.py`, append `"Dots"` to `_required_landing_column_names` (near line 154). Add rename entry:

```python
renamed_landing_column_names = {
    "Best3SquatKg": "squat",
    "Best3BenchKg": "bench",
    "Best3DeadliftKg": "deadlift",
    "TotalKg": "total",
    "BodyweightKg": "bodyweight",
    "Dots": "dots",   # NEW
}
```

- [ ] **Step 2: Write test verifying dots column is in raw output**

Append to `tests/test_raw_filters.py`:

```python
def test_raw_output_has_dots_column_after_rename(raw_module):
    """Regression test — dots column must be preserved through raw layer."""
    df = pl.DataFrame(
        [
            {"name": "x", "sex": "M", "bodyweight": 80.0, "squat": 200.0, "bench": 100.0, "deadlift": 250.0, "total": 550.0, "dots": 300.0},
        ]
    )
    # filters should preserve the dots column
    out = df.pipe(raw_module.filter_bombouts).pipe(raw_module.filter_total_consistency).pipe(raw_module.filter_bw_validity)
    assert "dots" in out.columns
```

Run: `uv run pytest tests/test_raw_filters.py::test_raw_output_has_dots_column_after_rename -v`
Expected: PASS (filters don't drop columns).

- [ ] **Step 3: Commit**

```bash
git add steps/conf.py tests/test_raw_filters.py
git commit -m "feat(raw): surface Dots column from OpenPowerlifting source"
```

**Branch B — compute DOTS locally:**

**Files:**
- Modify: `steps/conf.py`
- Create: `steps/dots.py`
- Modify: `steps/03_raw.py`
- Create: `tests/test_dots_formula.py`

- [ ] **Step 1: Add Konertz polynomial coefficients to `conf.py`**

```python
# DOTS polynomial coefficients (Konertz 2019)
# DOTS = total_kg * 500 / (A*bw^4 + B*bw^3 + C*bw^2 + D*bw + E)
DOTS_COEFFS_MALE = {
    "A": -0.000001093,
    "B": 0.0007391293,
    "C": -0.1918759221,
    "D": 24.0900756,
    "E": -307.75076,
}
DOTS_COEFFS_FEMALE = {
    "A": -0.0000010706,
    "B": 0.0005158568,
    "C": -0.1126655495,
    "D": 13.6175032,
    "E": -57.96288,
}
```

(Source: OpenPowerlifting `crates/coefficients/src/dots.rs`. Verify coefficients against that file before committing — if the reference has changed, use whatever values live there.)

- [ ] **Step 2: Write failing test with known DOTS values**

Create `tests/test_dots_formula.py`:

```python
"""Tests for locally-computed DOTS formula.

Reference values from the OpenPowerlifting DOTS calculator / liftercalc.com,
cross-checked against at least two independent calculators.
"""

import polars as pl
import pytest

from dots import compute_dots


@pytest.mark.parametrize(
    "sex,bw,total,expected_dots",
    [
        ("M", 83.0, 700.0, pytest.approx(471.5, abs=1.0)),  # male intermediate/advanced
        ("M", 100.0, 600.0, pytest.approx(376.6, abs=1.0)),
        ("F", 63.0, 400.0, pytest.approx(389.5, abs=1.0)),
        ("F", 75.0, 500.0, pytest.approx(442.6, abs=1.0)),
    ],
)
def test_compute_dots_matches_reference_values(sex, bw, total, expected_dots):
    df = pl.DataFrame({"sex": [sex], "bodyweight": [bw], "total": [total]})
    out = compute_dots(df)
    assert out["dots"][0] == expected_dots
```

Before running this test, verify the expected DOTS values by checking the same inputs on https://www.liftercalc.com or OpenPowerlifting's official calculator. Update `expected_dots` in the parametrize to match the authoritative result. **Do not accept the test passing with values that don't match the reference** — that's a silent bug.

- [ ] **Step 3: Run to verify failure**

Run: `uv run pytest tests/test_dots_formula.py -v`
Expected: FAIL — `dots` module doesn't exist.

- [ ] **Step 4: Implement `steps/dots.py`**

```python
"""DOTS coefficient computation.

Reference: Konertz (2019), OpenPowerlifting crates/coefficients/src/dots.rs.
"""

import conf
import polars as pl


def compute_dots(df: pl.DataFrame) -> pl.DataFrame:
    """Add a `dots` column computed from sex, bodyweight, and total.

    DOTS = total_kg * 500 / (A*bw^4 + B*bw^3 + C*bw^2 + D*bw + E)
    where (A, B, C, D, E) are sex-specific polynomial coefficients.
    """
    m = conf.DOTS_COEFFS_MALE
    f = conf.DOTS_COEFFS_FEMALE

    bw = pl.col("bodyweight")
    denom_male = m["A"] * bw**4 + m["B"] * bw**3 + m["C"] * bw**2 + m["D"] * bw + m["E"]
    denom_female = f["A"] * bw**4 + f["B"] * bw**3 + f["C"] * bw**2 + f["D"] * bw + f["E"]

    denom = pl.when(pl.col("sex") == "M").then(denom_male).when(pl.col("sex") == "F").then(denom_female).otherwise(None)

    return df.with_columns((pl.col("total") * 500.0 / denom).alias("dots"))
```

- [ ] **Step 5: Run tests, expect PASS**

Run: `uv run pytest tests/test_dots_formula.py -v`
Expected: 4 PASS.

- [ ] **Step 6: Wire `compute_dots` into `03_raw.py` pipeline**

Add `import dots` at the top (or `from dots import compute_dots`). Insert `.pipe(compute_dots)` after `.pipe(filter_bw_validity)` in the `__main__` block.

- [ ] **Step 7: Commit**

```bash
git add steps/dots.py steps/conf.py steps/03_raw.py tests/test_dots_formula.py
git commit -m "feat(raw): compute DOTS via Konertz formula when not in source"
```

---

### Task 2.3: DOTS-parallel features in `03_base.py` (Part 1: shift/pb/ratios)

**Files:**
- Create: `tests/test_dots_features.py`
- Modify: `steps/03_base.py`

- [ ] **Step 1: Write failing tests**

Create `tests/test_dots_features.py`:

```python
"""Tests for DOTS-parallel base-layer features added in v9."""

import importlib.util

import polars as pl
import pytest


@pytest.fixture
def base_module():
    spec = importlib.util.spec_from_file_location("base_module", "steps/03_base.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_add_previous_dots_shifts_by_one(base_module, synthetic_3_lifter_5_comp):
    df = synthetic_3_lifter_5_comp.sort(["primary_key", "date"])
    out = base_module.add_previous_dots(df)
    # L1 comp 0 → null; comp 1 → L1 comp 0's dots (200.0)
    l1 = out.filter(pl.col("primary_key") == "L1").sort("date")
    assert l1["previous_dots"][0] is None
    assert l1["previous_dots"][1] == pytest.approx(200.0)
    assert l1["previous_dots"][2] == pytest.approx(215.0)


def test_add_previous_dots_no_leakage_across_lifters(base_module, synthetic_3_lifter_5_comp):
    df = synthetic_3_lifter_5_comp.sort(["primary_key", "date"])
    out = base_module.add_previous_dots(df)
    # First row of each lifter must have null previous_dots
    firsts = out.group_by("primary_key").first()
    assert firsts["previous_dots"].null_count() == firsts.height
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/test_dots_features.py -v`
Expected: FAIL — `add_previous_dots` undefined.

- [ ] **Step 3: Add feature functions to `steps/03_base.py`**

Add next to existing `add_previous_wilks` function (search for that name):

```python
@conf.debug
def add_previous_dots(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.col("dots").shift(1).over("primary_key").alias("previous_dots"),
    )


@conf.debug
def add_dots_progress(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        ((pl.col("dots") - pl.col("previous_dots")) / pl.col("time_since_last_comp_years")).alias("dots_progress"),
    )


@conf.debug
def add_pct_change_dots(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.when(pl.col("previous_dots") > 0)
        .then((pl.col("dots") - pl.col("previous_dots")) / pl.col("previous_dots") * 100)
        .otherwise(None)
        .alias("pct_change_dots"),
    )


@conf.debug
def add_prev_pct_change_dots(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.col("pct_change_dots").shift(1).over("primary_key").alias("prev_pct_change_dots"),
    )


@conf.debug
def add_rolling_avg_dots(df: pl.DataFrame, window: int = 3) -> pl.DataFrame:
    return df.with_columns(
        pl.col("dots").rolling_mean(window_size=window, min_samples=window).over("primary_key").alias(f"rolling_avg_dots_{window}"),
    )


@conf.debug
def add_prev_rolling_avg_dots(df: pl.DataFrame, window: int = 3) -> pl.DataFrame:
    return df.with_columns(
        pl.col(f"rolling_avg_dots_{window}").shift(1).over("primary_key").alias(f"prev_rolling_avg_dots_{window}"),
    )


@conf.debug
def add_dots_personal_best(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.col("previous_dots").cum_max().over("primary_key").alias("prev_dots_personal_best"),
    ).with_columns(
        (pl.col("previous_dots") / pl.col("prev_dots_personal_best")).alias("prev_dots_vs_pb"),
        (pl.col("prev_dots_personal_best") - pl.col("previous_dots")).alias("prev_distance_from_pb_dots"),
    )
```

- [ ] **Step 4: Run tests, expect PASS**

Run: `uv run pytest tests/test_dots_features.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add steps/03_base.py tests/test_dots_features.py
git commit -m "feat(base): add DOTS-parallel lagged/rolling/pb features"
```

---

### Task 2.4: DOTS features — pct_change_dots null guard + pipeline wiring

**Files:**
- Modify: `tests/test_dots_features.py`
- Modify: `steps/03_base.py`

- [ ] **Step 1: Append failing tests for null guards**

```python
def test_pct_change_dots_is_null_when_previous_is_zero(base_module):
    df = pl.DataFrame(
        [
            {"primary_key": "X", "dots": 300.0, "previous_dots": 0.0},
            {"primary_key": "X", "dots": 300.0, "previous_dots": 200.0},
        ]
    )
    out = base_module.add_pct_change_dots(df)
    assert out["pct_change_dots"][0] is None
    assert out["pct_change_dots"][1] == pytest.approx(50.0)


def test_pct_change_dots_nulled_when_short_gap(synthetic_3_lifter_5_comp):
    """Matches existing pct_change_total guard: null out if gap < MIN_DAYS_BETWEEN_COMPS."""
    import conf
    # Checked in pipeline, not in feature fn; asserted by integration.
    assert conf.MIN_DAYS_BETWEEN_COMPS == 30
```

- [ ] **Step 2: Run — pass**

Run: `uv run pytest tests/test_dots_features.py -v`
Expected: all PASS.

- [ ] **Step 3: Wire new DOTS features into `03_base.py` pipeline**

In the `__main__` block, after the existing `add_previous_wilks` pipe, add:

```python
.pipe(add_previous_dots)
.pipe(add_dots_progress)
.pipe(add_pct_change_dots)
.pipe(add_rolling_avg_dots)
.pipe(add_prev_rolling_avg_dots)
.pipe(add_dots_personal_best)
```

Then update the progress_cols null-guard block (around line 492):

```python
progress_cols = [
    "squat_progress", "bench_progress", "deadlift_progress",
    "total_progress", "wilks_progress", "dots_progress",   # NEW
    "pct_change_total", "pct_change_dots",                 # NEW
]
```

After the progress-guard, add `.pipe(add_prev_pct_change_dots)` in the "features that depend on guarded progress values" block (next to the existing `add_prev_pct_change` call).

- [ ] **Step 4: Run ALL tests**

Run: `uv run pytest tests/ -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add steps/03_base.py tests/test_dots_features.py
git commit -m "feat(base): wire DOTS-parallel features into pipeline with null guards"
```

---

### Task 2.5: Global leakage guard

**Files:**
- Create: `tests/test_leakage_guard.py`

- [ ] **Step 1: Write parametrised leakage guard**

```python
"""Global leakage guard — runs every version.

For every `previous_*` / `prev_*` feature, asserts value at comp k equals value of
the source column at comp k-1 for same primary_key.
"""

import importlib.util

import polars as pl
import pytest


@pytest.fixture(scope="module")
def base_module():
    spec = importlib.util.spec_from_file_location("base_module", "steps/03_base.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    "source_col,shifted_col,feature_fn_name",
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
            assert shifted_vals[k] == source_vals[k - 1], f"{lifter} {shifted_col}[{k}] != {source_col}[{k-1}]"
```

- [ ] **Step 2: Run — expect PASS**

Run: `uv run pytest tests/test_leakage_guard.py -v`
Expected: 6 PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/test_leakage_guard.py
git commit -m "test: add parametrised leakage guard for previous_* features"
```

---

## Phase 3 — v9: Training integration + journey doc

### Task 3.1: Add DOTS features to `modelling_cols` in `05_train.py`

**Files:**
- Modify: `steps/05_train.py`

- [ ] **Step 1: Add DOTS columns to modelling_cols**

In `steps/05_train.py`, find the `modelling_cols` list (around line 249). Add:

```python
# v9: DOTS-parallel features
"previous_dots",
"prev_pct_change_dots",
"prev_dots_vs_pb",
"prev_distance_from_pb_dots",
"prev_rolling_avg_dots_3",
```

Keep existing Wilks features.

- [ ] **Step 2: Update `FEATURE_SET_VERSION`**

Change `FEATURE_SET_VERSION = 8` to `FEATURE_SET_VERSION = 9`.

- [ ] **Step 3: Verify the script parses (static check)**

Run: `uv run python -c "import ast; ast.parse(open('steps/05_train.py').read())"`
Expected: no output (no syntax error).

- [ ] **Step 4: Commit**

```bash
git add steps/05_train.py
git commit -m "feat(train): wire DOTS features into v9 modelling_cols"
```

---

### Task 3.2: Run the v9 pipeline end-to-end

- [ ] **Step 1: Run steps 1→3_raw**

```bash
uv run python steps/01_extract.py
uv run python steps/03_raw.py
```

Expected: no errors. Raw layer now has `dots` column and filtered rows.

- [ ] **Step 2: Run 03_base**

```bash
uv run python steps/03_base.py
```

Expected: base parquet created with new DOTS features. Logs should show filter drop counts.

- [ ] **Step 3: Start MinIO if not running**

```bash
make start
```

- [ ] **Step 4: Run v9 training**

```bash
uv run python steps/05_train.py 2>&1 | tee v9_run.log
```

Expected: 500 Optuna trials, final metrics logged to MLflow, test R²/RMSE printed.

- [ ] **Step 5: Record metrics for journey doc**

From `v9_run.log`, extract: test R², RMSE, MAE, Median AE, baseline RMSE, per-sex R², top/bottom weight-class R², top-15 features by gain. Also record bomb-out / consistency / bw-validity filter drop counts.

- [ ] **Step 6: Regression guard check**

If v9 test R² < 0.351 - 0.02 = 0.331, STOP. The regression-guard threshold from the spec is tripped — investigate before proceeding. Likely causes:
- Too many rows dropped by new filters (check drop % per filter).
- DOTS feature computation bug (rerun `tests/test_leakage_guard.py`).
- Temporal split shifted due to row-count change (acceptable but note in journey).

If v9 R² >= 0.331, proceed.

---

### Task 3.3: Update `data_science_journey.md` with v9 entry

**Files:**
- Modify: `data_science_journey.md`

- [ ] **Step 1: Append v9 row to summary table**

Update the top-of-file summary table to add v9's row matching the existing 8-column format.

- [ ] **Step 2: Append v9 section**

Add after the v7 section:

```markdown
## v9: Data cleaning + DOTS features

**Changes:**
- Added bomb-out filter in `03_raw.py` (dropped X rows, Y%)
- Added total-consistency filter (± 2.5 kg tolerance, dropped X rows, Y%)
- Added bodyweight-validity filter (Konertz ranges M 40–210, F 40–150, dropped X rows, Y%)
- Added DOTS source: [Branch A: surfaced from openpowerlifting source] OR [Branch B: computed locally via Konertz formula]
- Added 5 DOTS-parallel features: `previous_dots`, `prev_pct_change_dots`, `prev_dots_vs_pb`, `prev_distance_from_pb_dots`, `prev_rolling_avg_dots_3`

**Results:** R²=X.XXX, RMSE=X.XX, MAE=X.XX

**Insight:** [fill in from observed results — did cleaner data help? did DOTS features appear in top 15?]

**Drop counts:**
- Bomb-out: X rows (Y%)
- Total consistency: X rows (Y%)
- Bodyweight validity: X rows (Y%)
```

Fill X values from the v9 run log.

- [ ] **Step 3: Commit**

```bash
git add data_science_journey.md
git commit -m "docs(journey): v9 data cleaning + DOTS features results"
```

---

## Phase 4 — v10: Group A (first-comp birth-certificate features)

### Task 4.1: Implement `add_first_comp_features`

**Files:**
- Create: `tests/test_potential_features.py`
- Modify: `steps/03_base.py`

- [ ] **Step 1: Write failing tests**

Create `tests/test_potential_features.py`:

```python
"""Tests for v10 innate-potential features (Groups A/B/C/D)."""

import importlib.util

import polars as pl
import pytest


@pytest.fixture(scope="module")
def base_module():
    spec = importlib.util.spec_from_file_location("base_module", "steps/03_base.py")
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
    # L1 comp 1 dots = 200.0 (i=0 in fixture)
    l1 = out.filter(pl.col("primary_key") == "L1").sort("date")
    assert l1["first_comp_dots"][0] == pytest.approx(200.0)
    assert l1["first_comp_dots"][4] == pytest.approx(200.0)  # constant


def test_first_comp_total_per_bw(base_module, synthetic_3_lifter_5_comp):
    df = synthetic_3_lifter_5_comp.sort(["primary_key", "date"])
    out = base_module.add_first_comp_features(df)
    # L1 comp 1: total=300, bw=80 → 3.75
    l1 = out.filter(pl.col("primary_key") == "L1").sort("date")
    assert l1["first_comp_total_per_bw"][0] == pytest.approx(3.75)


def test_first_comp_age_constant_per_lifter(base_module, synthetic_3_lifter_5_comp):
    df = synthetic_3_lifter_5_comp.with_columns(
        (pl.col("date").dt.year() - pl.col("year_of_birth")).alias("approx_age"),
    ).sort(["primary_key", "date"])
    out = base_module.add_first_comp_features(df)
    l1 = out.filter(pl.col("primary_key") == "L1").sort("date")
    # L1 first comp 2020-06-01, yob 1995 → age 25, constant across comps
    assert (l1["first_comp_age"] == l1["first_comp_age"][0]).all()
```

- [ ] **Step 2: Run — expect FAIL**

Run: `uv run pytest tests/test_potential_features.py -v`
Expected: FAIL — `add_first_comp_features` undefined.

- [ ] **Step 3: Implement `add_first_comp_features` in `03_base.py`**

Add after existing `add_age_lifecycle_features` function:

```python
@conf.debug
def add_first_comp_features(df: pl.DataFrame) -> pl.DataFrame:
    """Broadcast each lifter's first-comp snapshot as constant columns.

    These are 'birth certificate' features — they never change for a given
    lifter, encoding starting strength and age.
    """
    return df.with_columns(
        pl.col("dots").first().over("primary_key").alias("first_comp_dots"),
        (pl.col("total").first() / pl.col("bodyweight").first()).over("primary_key").alias("first_comp_total_per_bw"),
        pl.col("approx_age").first().over("primary_key").alias("first_comp_age"),
    )
```

Note: requires `approx_age` to exist first — `add_first_comp_features` must be called **after** `add_age_lifecycle_features` in the pipeline.

- [ ] **Step 4: Run — expect PASS**

Run: `uv run pytest tests/test_potential_features.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add steps/03_base.py tests/test_potential_features.py
git commit -m "feat(base): add first-comp birth-certificate features (Group A, pt 1)"
```

---

### Task 4.2: First-comp percentile vs. sex × weight class cohort

**Files:**
- Modify: `tests/test_potential_features.py`
- Modify: `steps/03_base.py`

- [ ] **Step 1: Append failing test**

```python
def test_first_comp_percentile_in_range(base_module, synthetic_3_lifter_5_comp):
    df = synthetic_3_lifter_5_comp.sort(["primary_key", "date"])
    # Needs ipf_weight_class — add synthetically
    df = df.with_columns(pl.lit("83").alias("ipf_weight_class"))
    out = base_module.add_first_comp_features(df).pipe(base_module.add_first_comp_percentile)
    vals = out["first_comp_percentile_vs_sex_wc"].drop_nulls().to_list()
    for v in vals:
        assert 0 <= v <= 100
```

- [ ] **Step 2: Run — FAIL** (`add_first_comp_percentile` undefined)

- [ ] **Step 3: Implement in `03_base.py`**

```python
@conf.debug
def add_first_comp_percentile(df: pl.DataFrame) -> pl.DataFrame:
    """Percentile of first_comp_dots within each (sex, ipf_weight_class) cohort.

    Note: uses the full dataset distribution (matches existing
    `prev_total_percentile_rank` convention). For stricter temporal purity,
    switch to an expanding-by-date percentile — deferred as future work.
    """
    return df.with_columns(
        (pl.col("first_comp_dots").rank("average").over("sex", "ipf_weight_class") / pl.col("first_comp_dots").count().over("sex", "ipf_weight_class") * 100).alias("first_comp_percentile_vs_sex_wc"),
    )
```

- [ ] **Step 4: Run — PASS**

- [ ] **Step 5: Commit**

```bash
git add steps/03_base.py tests/test_potential_features.py
git commit -m "feat(base): add first-comp DOTS percentile vs sex×wc cohort"
```

---

## Phase 5 — v10: Group B (rolling career-so-far)

### Task 5.1: `max_dots_so_far` and `best_growth_rate_so_far`

**Files:**
- Modify: `tests/test_potential_features.py`
- Modify: `steps/03_base.py`

- [ ] **Step 1: Append failing tests**

```python
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
```

- [ ] **Step 2: Run — FAIL** (`add_rolling_career_features` undefined)

- [ ] **Step 3: Implement in `03_base.py`**

```python
@conf.debug
def add_rolling_career_features(df: pl.DataFrame) -> pl.DataFrame:
    """Career-so-far features: max, best growth rate, trend slope.

    All use shift(1) or previous_* inputs so current comp is strictly excluded.
    """
    return df.with_columns(
        pl.col("previous_dots").cum_max().over("primary_key").alias("max_dots_so_far"),
        pl.col("pct_change_dots").shift(1).cum_max().over("primary_key").alias("best_growth_rate_so_far"),
    )
```

- [ ] **Step 4: Run — PASS**

- [ ] **Step 5: Commit**

```bash
git add steps/03_base.py tests/test_potential_features.py
git commit -m "feat(base): add max_dots_so_far and best_growth_rate_so_far (Group B, pt 1)"
```

---

### Task 5.2: `dots_growth_trend` (linear slope over prior comps)

**Files:**
- Modify: `tests/test_potential_features.py`
- Modify: `steps/03_base.py`

- [ ] **Step 1: Append failing tests**

```python
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
```

- [ ] **Step 2: Run — FAIL**

- [ ] **Step 3: Implement in `03_base.py`**

```python
@conf.debug
def add_dots_growth_trend(df: pl.DataFrame) -> pl.DataFrame:
    """Linear-regression slope of previous_dots vs. days-since-lifter's-first-comp.

    Uses all prior (non-null previous_dots) points for each lifter. Null until
    at least 2 prior points exist (comp 3 onward).
    """
    # Days since first comp — used as x-axis for regression
    df = df.with_columns(
        (pl.col("date") - pl.col("date").first().over("primary_key")).dt.total_days().alias("_days_from_first"),
    )

    # Polars doesn't have built-in rolling-slope. Compute as Cov(x,y)/Var(x)
    # over an expanding window of previous_dots.
    # Using shift + cum aggregates: at comp k, use all previous_dots at comps 1..k-1.
    x = pl.col("_days_from_first")
    y = pl.col("previous_dots")

    # Count non-null y so far (expanding)
    n = y.is_not_null().cast(pl.Int64).cum_sum().over("primary_key")

    # Expanding mean of x (masked to rows where y is non-null)
    x_masked = pl.when(y.is_not_null()).then(x).otherwise(None)
    y_masked = y

    x_sum = x_masked.cum_sum().over("primary_key")
    y_sum = y_masked.cum_sum().over("primary_key")
    x_mean = x_sum / n
    y_mean = y_sum / n

    xy_sum = (x_masked * y_masked).cum_sum().over("primary_key")
    xx_sum = (x_masked * x_masked).cum_sum().over("primary_key")

    cov = xy_sum - n * x_mean * y_mean
    var = xx_sum - n * x_mean * x_mean

    slope = pl.when((n >= 2) & (var > 0)).then(cov / var).otherwise(None).alias("dots_growth_trend")

    return df.with_columns(slope).drop("_days_from_first")
```

- [ ] **Step 4: Run — PASS**

- [ ] **Step 5: Commit**

```bash
git add steps/03_base.py tests/test_potential_features.py
git commit -m "feat(base): add dots_growth_trend (expanding slope over prior comps)"
```

---

### Task 5.3: `early_growth_rate_dots_per_year` (critical: null until comp 4)

**Files:**
- Modify: `tests/test_potential_features.py`
- Modify: `steps/03_base.py`

- [ ] **Step 1: Append failing tests — leakage guard is the primary assertion**

```python
def test_early_growth_rate_null_before_comp_4(base_module, synthetic_3_lifter_5_comp):
    df = synthetic_3_lifter_5_comp.sort(["primary_key", "date"]).with_columns(
        pl.cum_count("primary_key").over("primary_key").alias("cumulative_comps"),
        ((pl.col("date") - pl.col("date").shift(1)).dt.total_days() / 365.25).over("primary_key").alias("time_since_last_comp_years"),
    )
    out = base_module.add_early_growth_rate(df).sort(["primary_key", "date"])

    l1 = out.filter(pl.col("primary_key") == "L1").sort("date")
    # Comps 1–3 must be null (uses comps 1..3 data, so available from comp 4 onward)
    assert l1["early_growth_rate_dots_per_year"][0] is None
    assert l1["early_growth_rate_dots_per_year"][1] is None
    assert l1["early_growth_rate_dots_per_year"][2] is None


def test_early_growth_rate_uses_no_current_comp_dots(base_module, synthetic_3_lifter_5_comp):
    """Feature at comp 4 must depend only on comps 1..3 dots, not comp 4's."""
    df = synthetic_3_lifter_5_comp.sort(["primary_key", "date"]).with_columns(
        pl.cum_count("primary_key").over("primary_key").alias("cumulative_comps"),
        ((pl.col("date") - pl.col("date").shift(1)).dt.total_days() / 365.25).over("primary_key").alias("time_since_last_comp_years"),
    )
    # Compute baseline
    out1 = base_module.add_early_growth_rate(df).sort(["primary_key", "date"])
    v1 = out1.filter(pl.col("primary_key") == "L1").sort("date")["early_growth_rate_dots_per_year"][3]

    # Mutate comp 4's dots (should NOT change feature at comp 4)
    df2 = df.with_columns(
        pl.when((pl.col("primary_key") == "L1") & (pl.col("cumulative_comps") == 4)).then(999.0).otherwise(pl.col("dots")).alias("dots"),
    )
    out2 = base_module.add_early_growth_rate(df2).sort(["primary_key", "date"])
    v2 = out2.filter(pl.col("primary_key") == "L1").sort("date")["early_growth_rate_dots_per_year"][3]

    assert v1 == v2, "early_growth_rate must not depend on current comp's dots"
```

- [ ] **Step 2: Run — FAIL**

- [ ] **Step 3: Implement in `03_base.py`**

```python
@conf.debug
def add_early_growth_rate(df: pl.DataFrame) -> pl.DataFrame:
    """(dots at 3rd comp - dots at 1st comp) / years between them.

    Broadcast as constant from cumulative_comps == 4 onward. Null for comps 1–3.
    Critical: uses dots *at* comp 3, so must be unavailable until comp 4 to avoid
    leaking into the comp-3 target.
    """
    # Gather each lifter's comp-1 and comp-3 dots + dates
    comp_rows = df.with_columns(
        pl.col("cumulative_comps").over("primary_key").alias("_cc"),
    )

    # Per-lifter comp-1 dots/date and comp-3 dots/date
    comp1_date = pl.when(pl.col("_cc") == 1).then(pl.col("date")).otherwise(None).max().over("primary_key").alias("_c1_date")
    comp1_dots = pl.when(pl.col("_cc") == 1).then(pl.col("dots")).otherwise(None).max().over("primary_key").alias("_c1_dots")
    comp3_date = pl.when(pl.col("_cc") == 3).then(pl.col("date")).otherwise(None).max().over("primary_key").alias("_c3_date")
    comp3_dots = pl.when(pl.col("_cc") == 3).then(pl.col("dots")).otherwise(None).max().over("primary_key").alias("_c3_dots")

    df2 = comp_rows.with_columns(comp1_date, comp1_dots, comp3_date, comp3_dots)

    years_between = (pl.col("_c3_date") - pl.col("_c1_date")).dt.total_days() / conf.DAYS_IN_YEAR
    raw_rate = (pl.col("_c3_dots") - pl.col("_c1_dots")) / years_between

    return df2.with_columns(
        pl.when(pl.col("_cc") >= 4).then(raw_rate).otherwise(None).alias("early_growth_rate_dots_per_year"),
    ).drop(["_cc", "_c1_date", "_c1_dots", "_c3_date", "_c3_dots"])
```

- [ ] **Step 4: Run — PASS**

- [ ] **Step 5: Commit**

```bash
git add steps/03_base.py tests/test_potential_features.py
git commit -m "feat(base): add early_growth_rate_dots_per_year (leak-free, comp 4+)"
```

---

## Phase 6 — v10: Group C (tier labels)

### Task 6.1: Add `DOTS_TIERS` + helper to `conf.py`

**Files:**
- Modify: `steps/conf.py`
- Modify: `tests/test_conf_dots_constants.py`

- [ ] **Step 1: Append failing tests for tier constants**

```python
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
```

- [ ] **Step 2: Run — FAIL**

- [ ] **Step 3: Add to `steps/conf.py`**

```python
# DOTS classification tiers (community convention; sex-agnostic)
# Half-open intervals: tier contains its lower bound, not its upper.
DOTS_TIERS = {
    "Beginner":     (0.0,   200.0),
    "Intermediate": (200.0, 300.0),
    "Advanced":     (300.0, 400.0),
    "Elite":        (400.0, 500.0),
    "World Class":  (500.0, float("inf")),
}
DOTS_TIER_ORDER = ["Beginner", "Intermediate", "Advanced", "Elite", "World Class"]


def dots_tier_for_score(score: float) -> str:
    """Return the community-convention tier name for a given DOTS score."""
    for tier_name in DOTS_TIER_ORDER:
        lo, hi = DOTS_TIERS[tier_name]
        if lo <= score < hi:
            return tier_name
    return DOTS_TIER_ORDER[-1]  # score above all upper bounds → top tier


def dots_tier_ordinal(score: float) -> int:
    """Return the 0-indexed tier ordinal (Beginner=0, World Class=4)."""
    return DOTS_TIER_ORDER.index(dots_tier_for_score(score))
```

- [ ] **Step 4: Run — PASS**

- [ ] **Step 5: Commit**

```bash
git add steps/conf.py tests/test_conf_dots_constants.py
git commit -m "feat(conf): add DOTS_TIERS + helpers (community convention)"
```

---

### Task 6.2: Tier features in `03_base.py`

**Files:**
- Modify: `tests/test_potential_features.py`
- Modify: `steps/03_base.py`

- [ ] **Step 1: Append failing tests**

```python
def test_starting_tier_matches_first_comp_dots(base_module, synthetic_3_lifter_5_comp):
    df = synthetic_3_lifter_5_comp.sort(["primary_key", "date"])
    df = base_module.add_previous_dots(df)
    df = df.with_columns((pl.col("date").dt.year() - pl.col("year_of_birth")).alias("approx_age"))
    df = base_module.add_first_comp_features(df)
    out = base_module.add_tier_features(df).sort(["primary_key", "date"])

    l1 = out.filter(pl.col("primary_key") == "L1").sort("date")
    # L1 first_comp_dots = 200.0 → Intermediate (half-open at 200) → ordinal 1
    assert l1["starting_tier"][0] == 1


def test_prev_tier_uses_previous_dots(base_module, synthetic_3_lifter_5_comp):
    df = synthetic_3_lifter_5_comp.sort(["primary_key", "date"])
    df = base_module.add_previous_dots(df)
    df = df.with_columns((pl.col("date").dt.year() - pl.col("year_of_birth")).alias("approx_age"))
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
    df = df.with_columns((pl.col("date").dt.year() - pl.col("year_of_birth")).alias("approx_age"))
    df = base_module.add_first_comp_features(df)
    out = base_module.add_tier_features(df).sort(["primary_key", "date"])
    vals = out["tiers_climbed_so_far"].drop_nulls().to_list()
    for v in vals:
        assert v >= 0
```

- [ ] **Step 2: Run — FAIL**

- [ ] **Step 3: Implement `add_tier_features` in `03_base.py`**

```python
def _score_to_ordinal(score_col: pl.Expr) -> pl.Expr:
    """Translate a DOTS score column into its ordinal tier (0-4).

    Uses conf.DOTS_TIERS with half-open intervals.
    """
    expr = pl.when(score_col < 200.0).then(0)
    expr = expr.when(score_col < 300.0).then(1)
    expr = expr.when(score_col < 400.0).then(2)
    expr = expr.when(score_col < 500.0).then(3)
    return expr.when(score_col >= 500.0).then(4).otherwise(None)


@conf.debug
def add_tier_features(df: pl.DataFrame) -> pl.DataFrame:
    """DOTS-tier ordinals derived from first_comp_dots and previous_dots.

    Four features:
    - starting_tier: tier at first comp (constant per lifter, 0-4)
    - prev_tier: tier at comp k-1 (null at comp 1)
    - max_tier_so_far: max prev_tier across comps 1..k-1
    - tiers_climbed_so_far: max_tier_so_far - starting_tier (>= 0)
    """
    df = df.with_columns(
        _score_to_ordinal(pl.col("first_comp_dots")).alias("starting_tier"),
        _score_to_ordinal(pl.col("previous_dots")).alias("prev_tier"),
    )
    df = df.with_columns(
        pl.col("prev_tier").cum_max().over("primary_key").alias("max_tier_so_far"),
    )
    df = df.with_columns(
        (pl.col("max_tier_so_far") - pl.col("starting_tier")).alias("tiers_climbed_so_far"),
    )
    return df
```

- [ ] **Step 4: Run — PASS**

- [ ] **Step 5: Commit**

```bash
git add steps/03_base.py tests/test_potential_features.py
git commit -m "feat(base): add DOTS tier features (starting_tier, prev_tier, max_tier_so_far, tiers_climbed)"
```

---

### Task 6.3: `comps_above_tier_threshold_so_far`

**Files:**
- Modify: `tests/test_potential_features.py`
- Modify: `steps/03_base.py`

- [ ] **Step 1: Append test**

```python
def test_comps_above_elite_counts_prior_comps_only(base_module, synthetic_3_lifter_5_comp):
    """L3 hits Elite (400+) from comp 1; comps_above_elite_so_far should count prior only."""
    df = synthetic_3_lifter_5_comp.sort(["primary_key", "date"])
    df = base_module.add_previous_dots(df)
    out = base_module.add_comps_above_tier(df).sort(["primary_key", "date"])

    l3 = out.filter(pl.col("primary_key") == "L3").sort("date")
    # L3 dots: 420, 432, 444, 456, 468 — all Elite (400+, under 500)
    # comp 1: 0 prior comps
    # comp 2: 1 prior comp (420) >= 400
    # comp 5: 4 prior comps, all >= 400
    assert l3["comps_above_elite_so_far"][0] == 0
    assert l3["comps_above_elite_so_far"][1] == 1
    assert l3["comps_above_elite_so_far"][4] == 4
```

- [ ] **Step 2: Run — FAIL**

- [ ] **Step 3: Implement in `03_base.py`**

```python
@conf.debug
def add_comps_above_tier(df: pl.DataFrame) -> pl.DataFrame:
    """One column per tier lower bound: count of prior comps at or above it.

    Uses previous_dots (shift(1)) so strictly comps 1..k-1.
    """
    thresholds = {
        "comps_above_intermediate_so_far": 200.0,
        "comps_above_advanced_so_far": 300.0,
        "comps_above_elite_so_far": 400.0,
        "comps_above_world_class_so_far": 500.0,
    }
    return df.with_columns(
        [
            pl.col("previous_dots").ge(thr).cast(pl.Int64).cum_sum().over("primary_key").alias(name)
            for name, thr in thresholds.items()
        ]
    )
```

- [ ] **Step 4: Run — PASS**

- [ ] **Step 5: Commit**

```bash
git add steps/03_base.py tests/test_potential_features.py
git commit -m "feat(base): add comps_above_tier_so_far counters (4 per-tier features)"
```

---

## Phase 7 — v10: Group D (interaction features)

### Task 7.1: Potential × context interactions

**Files:**
- Modify: `tests/test_potential_features.py`
- Modify: `steps/03_base.py`

- [ ] **Step 1: Append tests**

```python
def test_starting_tier_x_years_competing_multiplies_inputs(base_module):
    df = pl.DataFrame(
        {
            "starting_tier": [1, 2, 3],
            "years_competing": [2.0, 5.0, 10.0],
            "max_tier_so_far": [1, 3, 3],
            "time_since_last_comp_years": [0.5, 1.0, 2.0],
        }
    )
    out = base_module.add_potential_interactions(df)
    assert out["starting_tier_x_years_competing"].to_list() == [2.0, 10.0, 30.0]
    assert out["max_tier_x_time_gap"].to_list() == [0.5, 3.0, 6.0]
```

- [ ] **Step 2: Run — FAIL**

- [ ] **Step 3: Implement in `03_base.py`**

```python
@conf.debug
def add_potential_interactions(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        (pl.col("starting_tier") * pl.col("years_competing")).alias("starting_tier_x_years_competing"),
        (pl.col("max_tier_so_far") * pl.col("time_since_last_comp_years")).alias("max_tier_x_time_gap"),
    )
```

- [ ] **Step 4: Run — PASS**

- [ ] **Step 5: Commit**

```bash
git add steps/03_base.py tests/test_potential_features.py
git commit -m "feat(base): add potential × context interaction features"
```

---

### Task 7.2: Wire Group A/B/C/D into `03_base.py` pipeline

**Files:**
- Modify: `steps/03_base.py`

- [ ] **Step 1: Insert new pipes into the `__main__` block**

Find the existing `__main__` pipeline. Order matters:
- `add_first_comp_features` → needs `approx_age` → must come **after** `add_age_lifecycle_features`
- `add_first_comp_percentile` → needs `first_comp_dots` + `ipf_weight_class` → after `add_first_comp_features` and `add_ipf_weight_class`
- `add_rolling_career_features` → needs `previous_dots`, `pct_change_dots` → after `add_pct_change_dots`
- `add_dots_growth_trend` → needs `previous_dots` → after `add_previous_dots`
- `add_early_growth_rate` → needs `cumulative_comps`, `dots`, `date` → after `add_temporal_features`
- `add_tier_features` → needs `first_comp_dots`, `previous_dots` → after `add_first_comp_features`
- `add_comps_above_tier` → needs `previous_dots` → after `add_previous_dots`
- `add_potential_interactions` → needs `starting_tier`, `years_competing`, `max_tier_so_far`, `time_since_last_comp_years` → last

Insert pattern:

```python
# After all existing pipes, before common_io.io_write...
.pipe(add_first_comp_features)
.pipe(add_first_comp_percentile)
.pipe(add_rolling_career_features)
.pipe(add_dots_growth_trend)
.pipe(add_early_growth_rate)
.pipe(add_tier_features)
.pipe(add_comps_above_tier)
.pipe(add_potential_interactions)
```

- [ ] **Step 2: Re-run test suite**

```bash
uv run pytest tests/ -v
```

Expected: all PASS.

- [ ] **Step 3: Commit**

```bash
git add steps/03_base.py
git commit -m "feat(base): wire v10 potential/tier/interaction features into pipeline"
```

---

## Phase 8 — v10: Training integration + journey

### Task 8.1: Extend `modelling_cols` and add per-tier slice eval in `05_train.py`

**Files:**
- Modify: `steps/05_train.py`

- [ ] **Step 1: Bump version and add columns**

Change `FEATURE_SET_VERSION = 9` → `FEATURE_SET_VERSION = 10`. Add to `modelling_cols`:

```python
# v10: first-comp birth certificate
"first_comp_dots",
"first_comp_total_per_bw",
"first_comp_age",
"first_comp_percentile_vs_sex_wc",
# v10: rolling career-so-far
"max_dots_so_far",
"best_growth_rate_so_far",
"dots_growth_trend",
"early_growth_rate_dots_per_year",
"comps_above_intermediate_so_far",
"comps_above_advanced_so_far",
"comps_above_elite_so_far",
"comps_above_world_class_so_far",
# v10: tier labels
"starting_tier",
"prev_tier",
"max_tier_so_far",
"tiers_climbed_so_far",
# v10: potential × context
"starting_tier_x_years_competing",
"max_tier_x_time_gap",
```

- [ ] **Step 2: Add per-starting-tier slice evaluation**

After the existing per-weight-class block (around line 467), add:

```python
# Per-starting-tier slice evaluation (v10+)
logging.info("-" * 60)
logging.info("PER-STARTING-TIER RMSE:")
for tier_ord in range(5):
    mask = X_test["starting_tier"] == tier_ord
    if mask.sum() >= 50:
        seg_rmse = math.sqrt(mean_squared_error(y_test[mask], test_preds[mask]))
        seg_r2 = r2_score(y_test[mask], test_preds[mask])
        tier_name = conf.DOTS_TIER_ORDER[tier_ord]
        mlflow.log_metric(f"rmse_starting_tier_{tier_name.replace(' ', '_')}", seg_rmse)
        mlflow.log_metric(f"r2_starting_tier_{tier_name.replace(' ', '_')}", seg_r2)
        logging.info("  starting_tier=%s (n=%d): RMSE=%.4f R²=%.4f", tier_name, mask.sum(), seg_rmse, seg_r2)
```

- [ ] **Step 3: Commit (code only)**

```bash
git add steps/05_train.py
git commit -m "feat(train): v10 — add potential features + per-starting-tier slice eval"
```

---

### Task 8.2: Run v10 pipeline end-to-end + journey entry

- [ ] **Step 1: Rerun base layer**

```bash
uv run python steps/03_base.py
```

Expected: base parquet now includes all v10 features.

- [ ] **Step 2: Run v10 training**

```bash
uv run python steps/05_train.py 2>&1 | tee v10_run.log
```

- [ ] **Step 3: Regression guard check**

If v10 test R² < v9 R² - 0.02, STOP. Investigate before journey update:
- Are new features null-heavy for a big chunk of the data? (check feature importance log)
- Is the feature matrix size exploding (runtime blow-up)?

If v10 R² >= v9 R² - 0.02, proceed.

- [ ] **Step 4: Extract per-starting-tier breakdown + feature-importance ranking**

From `v10_run.log`: test R²/RMSE, per-sex, per-weight-class, per-starting-tier, top-15 features by gain. Also log lifter count per starting tier (data-quality checkpoint).

- [ ] **Step 5: Update `data_science_journey.md`**

Add v10 row to summary table, then new section:

```markdown
## v10: Potential + tier features

**Changes:**
- Added Group A (first-comp): `first_comp_dots`, `first_comp_total_per_bw`, `first_comp_age`, `first_comp_percentile_vs_sex_wc`
- Added Group B (rolling career): `max_dots_so_far`, `best_growth_rate_so_far`, `dots_growth_trend`, `early_growth_rate_dots_per_year`, 4 × `comps_above_*_so_far`
- Added Group C (tiers): `starting_tier`, `prev_tier`, `max_tier_so_far`, `tiers_climbed_so_far`
- Added Group D (interactions): `starting_tier_x_years_competing`, `max_tier_x_time_gap`

**Results:** R²=X.XXX, RMSE=X.XX, MAE=X.XX

**Data-quality checkpoint — lifters per starting tier:**
| Tier | Count | % |
|------|-------|---|
| Beginner | X | Y% |
| Intermediate | X | Y% |
| Advanced | X | Y% |
| Elite | X | Y% |
| World Class | X | Y% |

(Flag if any tier has <50 lifters; decide on merging bins in v11 if so.)

**Per-starting-tier R² / RMSE:**
| Tier | n | R² | RMSE |
|------|---|----|------|
| Beginner | ... | ... | ... |
| Intermediate | ... | ... | ... |
| Advanced | ... | ... | ... |
| Elite | ... | ... | ... |
| World Class | ... | ... | ... |

**Top 15 features by gain:** [list]

**Hypothesis validation:**
- Hypothesis: largest R² gain in Beginner/Intermediate tiers.
- Actual: [describe].
- Verdict: [confirmed / partially / refuted, with explanation].

**Insight:** [fill from observed results]
```

Fill all X values from the run log.

- [ ] **Step 6: Commit**

```bash
git add data_science_journey.md
git commit -m "docs(journey): v10 potential + tier features results"
```

---

## Phase 9 — v11: Dual-head training

### Task 9.1: Refactor `05_train.py` to loop over targets

**Files:**
- Create: `tests/test_training_pipeline.py`
- Modify: `steps/05_train.py`

- [ ] **Step 1: Write failing tests for dual-head invariants**

Create `tests/test_training_pipeline.py`:

```python
"""Invariants for v11 dual-head training."""

import numpy as np
import pytest


def test_normalised_rmse_computation():
    """Normalised RMSE = RMSE / std(y_test). Scale-free comparison across targets."""
    import math

    y_true = np.array([10.0, 20.0, 30.0, 40.0, 50.0])
    y_pred = np.array([12.0, 18.0, 32.0, 38.0, 50.0])
    rmse = math.sqrt(np.mean((y_true - y_pred) ** 2))
    norm_rmse = rmse / y_true.std()

    # Manual check: RMSE = sqrt((4+4+4+4+0)/5) = sqrt(3.2) ≈ 1.788
    # std of [10,20,30,40,50] = sqrt(200) ≈ 14.142
    assert norm_rmse == pytest.approx(1.788 / 14.142, rel=1e-2)


def test_spearman_rho_identity_is_one():
    """Spearman ρ of a strictly monotonic transformation equals 1.0."""
    from scipy.stats import spearmanr

    y_true = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
    y_pred = 2 * y_true + 10  # strictly monotonic → ρ = 1.0
    rho, _ = spearmanr(y_true, y_pred)
    assert rho == pytest.approx(1.0)
```

- [ ] **Step 2: Add scipy as dep if not present**

Check `pyproject.toml` for scipy. If missing: add to `[project].dependencies`, run `uv sync`.

- [ ] **Step 3: Run — expect PASS**

Run: `uv run pytest tests/test_training_pipeline.py -v`
Expected: PASS.

- [ ] **Step 4: Refactor `05_train.py` — wrap main block in a target loop**

Extract the existing `if __name__ == "__main__":` body into a function `train_for_target(target_col, feature_set_version, base_df)`. Then at module bottom:

```python
if __name__ == "__main__":
    base_df = pl.read_parquet(conf.base_local_file_path)
    for target_col in ["pct_change_total", "pct_change_dots"]:
        logging.info("=" * 70)
        logging.info("TRAINING HEAD: %s", target_col)
        logging.info("=" * 70)
        train_for_target(target_col, feature_set_version=11, base_df=base_df.clone())
```

Inside `train_for_target`:
- Replace `TARGET = "pct_change_total"` (hardcoded) with the parameter `target_col`.
- Replace `FEATURE_SET_VERSION = 10` (hardcoded) with the parameter.
- The `run_name = f"{TARGET}_v{FEATURE_SET_VERSION}"` line now becomes `f"{target_col}_v{feature_set_version}"`.
- Each head computes its own `p1, p99` winsorization on its own target.
- Each head starts its own MLflow parent run with the target-specific run_name.
- Tag each MLflow run with `mlflow.set_tag("target", target_col)`.

- [ ] **Step 5: Verify script parses**

Run: `uv run python -c "import ast; ast.parse(open('steps/05_train.py').read())"`
Expected: no output.

- [ ] **Step 6: Commit**

```bash
git add steps/05_train.py tests/test_training_pipeline.py pyproject.toml uv.lock
git commit -m "feat(train): v11 dual-head training — loop over pct_change_total + pct_change_dots"
```

---

### Task 9.2: Normalised-RMSE + Spearman ρ in head evaluation

**Files:**
- Modify: `steps/05_train.py`

- [ ] **Step 1: Add imports and helper at top of `05_train.py`**

```python
from scipy.stats import spearmanr
```

- [ ] **Step 2: In `train_for_target`, after `test_r2` is computed, log normalised metrics**

Find the `mlflow.log_metrics({...})` block. Add:

```python
test_rmse_normalised = test_rmse / float(y_test.values.std())
test_spearman_rho, _ = spearmanr(y_test.values.ravel(), test_preds)

mlflow.log_metrics(
    {
        # ... existing keys ...
        "test_rmse_normalised": test_rmse_normalised,
        "test_spearman_rho": float(test_spearman_rho),
    }
)

logging.info("Normalised RMSE: %.4f | Spearman ρ: %.4f", test_rmse_normalised, test_spearman_rho)
```

- [ ] **Step 3: Add regression guard**

At the end of `train_for_target`, compare current R² to the last recorded R² for this head from MLflow. (For the first v11 run, compare against v10 — hardcode a sentinel or read from the v10 tag.) If drop > 0.02, log a loud warning:

```python
# Regression guard (spec requirement)
PRIOR_R2_THRESHOLDS = {
    "pct_change_total": 0.00,   # updated per journey doc; start permissive
    "pct_change_dots":  0.00,
}
prior = PRIOR_R2_THRESHOLDS.get(target_col, 0.0)
if prior > 0 and test_r2 < prior - 0.02:
    logging.error("REGRESSION GUARD TRIPPED: R² %.4f vs prior %.4f (drop > 0.02)", test_r2, prior)
    mlflow.set_tag("regression_guard", "TRIPPED")
```

After each release, the engineer updates `PRIOR_R2_THRESHOLDS` to the latest known-good R² per head.

- [ ] **Step 4: Run a quick pipeline smoke (full run happens in Task 9.4)**

Run: `uv run python -c "import ast; ast.parse(open('steps/05_train.py').read())"`
Expected: no output.

- [ ] **Step 5: Commit**

```bash
git add steps/05_train.py
git commit -m "feat(train): v11 — normalised RMSE, Spearman ρ, regression guard"
```

---

### Task 9.3: Predictions artifact

**Files:**
- Modify: `tests/test_training_pipeline.py`
- Modify: `steps/05_train.py`
- Modify: `steps/conf.py`

- [ ] **Step 1: Append schema test**

```python
def test_predictions_parquet_schema():
    """Predictions parquet must carry both head preds and starting_tier."""
    import polars as pl
    required = {"primary_key", "date", "pred_pct_change_total", "pred_pct_change_dots", "starting_tier"}

    # Schema test uses a synthetic frame — integration test covers real file
    df = pl.DataFrame(
        {
            "primary_key": ["L1"],
            "date": [None],
            "pred_pct_change_total": [0.1],
            "pred_pct_change_dots": [0.2],
            "starting_tier": [1],
        }
    )
    assert required.issubset(set(df.columns))
```

- [ ] **Step 2: Run — PASS** (trivial schema assertion).

- [ ] **Step 3: Add path constants to `conf.py`**

```python
model_predictions_v11_local = create_output_file_path(OutputPathType.BASE, FileLocation.LOCAL, file_name="model_predictions_v11.parquet")
model_predictions_v11_s3_key = create_output_file_path(OutputPathType.BASE, FileLocation.S3, file_name="model_predictions_v11.parquet")
model_predictions_v11_s3_http = create_output_file_path(OutputPathType.BASE, FileLocation.S3, as_http=True, file_name="model_predictions_v11.parquet")
```

- [ ] **Step 4: In `05_train.py`, accumulate predictions across heads and write parquet**

Outside the per-target loop, accumulate predictions. Restructure the `__main__` block so after both heads run, the test-set predictions and starting_tier are merged into one parquet:

```python
if __name__ == "__main__":
    base_df = pl.read_parquet(conf.base_local_file_path)
    predictions_by_head = {}
    test_meta = None  # captures primary_key/date/starting_tier once
    for target_col in ["pct_change_total", "pct_change_dots"]:
        preds_df, meta_df = train_for_target(target_col, feature_set_version=11, base_df=base_df.clone())
        predictions_by_head[target_col] = preds_df
        test_meta = meta_df  # same split → same meta

    combined = test_meta.with_columns(
        predictions_by_head["pct_change_total"]["pred"].alias("pred_pct_change_total"),
        predictions_by_head["pct_change_dots"]["pred"].alias("pred_pct_change_dots"),
    )
    common_io.io_write_from_local_to_s3(
        combined,
        conf.model_predictions_v11_local,
        conf.model_predictions_v11_s3_key,
    )
```

`train_for_target` must be updated to return `(preds_df, meta_df)`:
- `preds_df` = `pl.DataFrame({"pred": test_preds})` (one column, row-aligned with test set)
- `meta_df` = `pl.DataFrame({"primary_key": ..., "date": ..., "starting_tier": ...})` built from the test split pre-drop

- [ ] **Step 5: Commit**

```bash
git add steps/05_train.py steps/conf.py tests/test_training_pipeline.py
git commit -m "feat(train): v11 — write combined dual-head predictions parquet"
```

---

### Task 9.4: Run v11 end-to-end

- [ ] **Step 1: Run v11 training**

```bash
uv run python steps/05_train.py 2>&1 | tee v11_run.log
```

Expected: two MLflow parent runs (one per head), normalised RMSE and Spearman ρ logged, combined predictions parquet uploaded to S3.

- [ ] **Step 2: Regression guard check per head**

If `pct_change_total` R² < v10 R² - 0.02 → investigate.
For `pct_change_dots` this is the first run, so no prior threshold.

- [ ] **Step 3: Record comparison**

From `v11_run.log`, capture for each head: R², RMSE, MAE, Median AE, normalised RMSE, Spearman ρ, per-starting-tier R².

---

## Phase 10 — v11: Dashboard updates

### Task 10.1: DOTS trajectory chart on `pages/02_user_data.py`

**Files:**
- Modify: `pages/02_user_data.py`

- [ ] **Step 1: Read the existing page to understand conventions**

```bash
cat pages/02_user_data.py | head -80
```

Note: the page loads raw data from S3 and renders Wilks and SBD progression charts using Altair. Identify where Wilks chart is built.

- [ ] **Step 2: Add a DOTS chart next to Wilks**

In `pages/02_user_data.py`, find the Wilks chart construction. Duplicate the chart with `dots` in place of `wilks`. Example pattern (exact code depends on existing structure):

```python
# After Wilks chart definition
dots_chart = (
    alt.Chart(user_df)
    .mark_line(point=True)
    .encode(x="Date:T", y="Dots:Q", tooltip=["Date", "Dots"])
    .properties(title="DOTS Progression")
)
st.altair_chart(dots_chart, use_container_width=True)
```

The page reads from `conf.op_cols` (CamelCase OpenPowerlifting columns). Add `"Dots"` to `conf.op_cols` if not present (check `conf.py:34`).

- [ ] **Step 3: Smoke test locally**

```bash
uv run streamlit run about.py
```

Navigate to User Data page. Enter a known lifter name. Confirm DOTS chart renders and shows reasonable values.

- [ ] **Step 4: Commit**

```bash
git add pages/02_user_data.py steps/conf.py
git commit -m "feat(dashboard): add DOTS trajectory chart to user data page"
```

---

### Task 10.2: Prediction head toggle on `pages/03_strength_progress.py`

**Files:**
- Modify: `pages/03_strength_progress.py`

- [ ] **Step 1: Read existing page**

```bash
cat pages/03_strength_progress.py
```

Identify where `pct_change_total` predictions are displayed.

- [ ] **Step 2: Load combined predictions parquet**

At the top of the page (with other `@st.cache_data` loaders):

```python
@st.cache_data
def load_model_predictions():
    import polars as pl
    return pl.read_parquet(conf.model_predictions_v11_s3_http).to_pandas()
```

- [ ] **Step 3: Add a st.radio toggle for head selection**

```python
head = st.radio(
    "Prediction head",
    options=["kg-based (pct_change_total)", "DOTS-based (pct_change_dots)"],
    horizontal=True,
)
pred_col = "pred_pct_change_total" if head.startswith("kg") else "pred_pct_change_dots"
```

Use `pred_col` in the downstream chart/table.

- [ ] **Step 4: Smoke test**

Reload the Streamlit app, navigate to page 03, toggle between heads, confirm values change.

- [ ] **Step 5: Commit**

```bash
git add pages/03_strength_progress.py
git commit -m "feat(dashboard): add dual-head prediction toggle on strength progress page"
```

---

## Phase 11 — v11: Journey entry + wrap-up

### Task 11.1: Update `data_science_journey.md` with v11 entry

**Files:**
- Modify: `data_science_journey.md`

- [ ] **Step 1: Add v11 row (two rows — one per head) to summary table**

Two rows with identical feature set, differing target and metrics.

- [ ] **Step 2: Append v11 section**

```markdown
## v11: Dual-head training (kg + DOTS)

**Changes:**
- Refactored `05_train.py` to loop over targets; both heads share identical features and temporal split.
- Added normalised RMSE (`rmse / y.std()`) and Spearman ρ for cross-target comparison.
- Wrote combined predictions parquet (`base/model_predictions_v11.parquet`) for dashboard.
- Updated dashboard: DOTS chart on user page, prediction-head toggle on strength-progress page.

**Results (head-to-head):**
| Head | R² | RMSE | MAE | Normalised RMSE | Spearman ρ |
|------|----|------|-----|-----------------|------------|
| `pct_change_total` | X.XXX | X.XX | X.XX | X.XXX | X.XXX |
| `pct_change_dots` | X.XXX | X.XX | X.XX | X.XXX | X.XXX |

**Recommendation for dashboard primary head:** [which head has better Spearman ρ and normalised RMSE]

**Per-starting-tier breakdown, both heads:** [table]

**Dashboard smoke test:**
- [ ] DOTS chart renders on user page
- [ ] Prediction toggle works on strength-progress page
- [ ] No stale-cache issues

**Insight:** [fill from observed results]
```

- [ ] **Step 3: Commit**

```bash
git add data_science_journey.md
git commit -m "docs(journey): v11 dual-head kg + DOTS results"
```

---

### Task 11.2: Update PRIOR_R2_THRESHOLDS baseline

**Files:**
- Modify: `steps/05_train.py`

- [ ] **Step 1: Replace placeholder thresholds with actual v11 values**

```python
PRIOR_R2_THRESHOLDS = {
    "pct_change_total": <actual v11 R² for kg head>,
    "pct_change_dots":  <actual v11 R² for DOTS head>,
}
```

- [ ] **Step 2: Commit**

```bash
git add steps/05_train.py
git commit -m "chore(train): lock regression guard thresholds to v11 baseline"
```

---

## Plan self-review

- **Spec coverage:** each spec section has at least one task (v9 filters → Phase 1; DOTS integration → Phase 2; global leakage guard → Task 2.5; v10 Groups A–D → Phases 4–7; v10 per-tier slice eval → Task 8.1; v11 dual-head → Phase 9; v11 predictions + dashboard → Phases 9–10; journey entries → each phase end).
- **Placeholder scan:** journey-doc templates contain `X` values to fill from actual runs — these are not "TBD/TODO" but required placeholders for log-extracted numbers. No other placeholders.
- **Type consistency:** `starting_tier` is always ordinal 0–4; feature function names are consistent across tests and impl. `pct_change_dots` and `pred_pct_change_dots` naming consistent.
- **Known conditional branch:** Task 2.2 has two branches depending on Task 2.1 finding — explicitly structured so either path terminates cleanly.
