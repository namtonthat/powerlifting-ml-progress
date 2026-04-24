"""Shared pytest fixtures for synthetic lifter/comp data."""

import importlib.util
from datetime import date

import polars as pl
import pytest


@pytest.fixture
def synthetic_3_lifter_5_comp() -> pl.DataFrame:
    """3 lifters x 5 comps each with deterministic values.

    Lifter L1 — steady improver (Beginner -> Intermediate)
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


def _load_module_by_file(name: str, path: str):
    """Load a module by file path (needed for leading-digit filenames like 03_raw.py)."""
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="session")
def raw_module():
    """Load steps/03_raw.py by file path (leading-digit filename)."""
    return _load_module_by_file("raw_module", "steps/03_raw.py")


@pytest.fixture(scope="session")
def base_module():
    """Load steps/03_base.py by file path (leading-digit filename)."""
    return _load_module_by_file("base_module", "steps/03_base.py")
