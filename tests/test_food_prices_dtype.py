"""Regression test: food_prices and food_quantities must return float64 columns.

Root cause (fixed 2026-04-14): kgs_per_other_units.json contains mixed
int/float/empty-string values.  pd.Series(dict) infers object dtype when
values are mixed; divide/multiply on an object-typed Series produces object
dtype in the result; to_parquet then converts object columns to string via
astype('string[pyarrow]').  Fix: pd.to_numeric(..., errors='coerce') forces
float64 before the arithmetic.

This test reads the cached parquet directly (dtype check) AND exercises
the Country API call (for the _finalize_result path) — skipped when the
cache is absent.
"""
import os
from pathlib import Path

import pandas as pd
import pytest

from lsms_library.paths import data_root


# ---------------------------------------------------------------------------
# Skip guard — both tests need the Uganda food_acquired cache to be warm
# ---------------------------------------------------------------------------

def _uganda_food_cache_exists() -> bool:
    root = data_root("Uganda")
    return (root / "var" / "food_prices.parquet").exists() or (
        root / "var" / "food_acquired.parquet"
    ).exists()


_SKIP_NO_CACHE = pytest.mark.skipif(
    not _uganda_food_cache_exists(),
    reason="Uganda food_prices / food_quantities parquet not cached; requires data build",
)

# Expected float64 columns for each table
_PRICE_COLS = [
    "market",
    "farmgate",
    "market_away",
    "market_home",
    "market_own",
    "unitvalue_away",
    "unitvalue_home",
    "unitvalue_inkind",
    "unitvalue_own",
]

_QUANTITY_COLS = [
    "quantity_away",
    "quantity_home",
    "quantity_inkind",
    "quantity_own",
]


# ---------------------------------------------------------------------------
# Cache-level dtype test (reads parquet directly; bypasses _finalize_result)
# ---------------------------------------------------------------------------

@_SKIP_NO_CACHE
def test_food_prices_cache_dtype_float64():
    """Cached food_prices.parquet must store numeric columns as float64, not string."""
    path = data_root("Uganda") / "var" / "food_prices.parquet"
    if not path.exists():
        pytest.skip("food_prices.parquet not in cache")
    df = pd.read_parquet(path)
    for col in _PRICE_COLS:
        if col in df.columns:
            assert pd.api.types.is_float_dtype(df[col]), (
                f"food_prices column '{col}' is {df[col].dtype}; expected float64. "
                "Root cause: kgs_per_other_units.json mixed-type values produce object "
                "dtype in pd.Series(dict), which to_parquet converts to string."
            )


@_SKIP_NO_CACHE
def test_food_quantities_cache_dtype_float64():
    """Cached food_quantities.parquet must store numeric columns as float64, not string."""
    path = data_root("Uganda") / "var" / "food_quantities.parquet"
    if not path.exists():
        pytest.skip("food_quantities.parquet not in cache")
    df = pd.read_parquet(path)
    for col in _QUANTITY_COLS:
        if col in df.columns:
            assert pd.api.types.is_float_dtype(df[col]), (
                f"food_quantities column '{col}' is {df[col].dtype}; expected float64."
            )


# ---------------------------------------------------------------------------
# API-level dtype test (exercises Country.__getattr__ + _finalize_result)
# ---------------------------------------------------------------------------

@_SKIP_NO_CACHE
def test_food_prices_api_dtype_float64():
    """Country('Uganda').food_prices() must return float64 price columns."""
    import lsms_library as ll
    uganda = ll.Country("Uganda", preload_panel_ids=False, verbose=False)
    fp = uganda.food_prices()
    assert not fp.empty, "food_prices() returned empty DataFrame"
    for col in _PRICE_COLS:
        if col in fp.columns:
            assert pd.api.types.is_float_dtype(fp[col]), (
                f"food_prices() column '{col}' is {fp[col].dtype}; expected float64."
            )


@_SKIP_NO_CACHE
def test_food_quantities_api_dtype_float64():
    """Country('Uganda').food_quantities() must return float64 quantity columns."""
    import lsms_library as ll
    uganda = ll.Country("Uganda", preload_panel_ids=False, verbose=False)
    fq = uganda.food_quantities()
    assert not fq.empty, "food_quantities() returned empty DataFrame"
    for col in _QUANTITY_COLS:
        if col in fq.columns:
            assert pd.api.types.is_float_dtype(fq[col]), (
                f"food_quantities() column '{col}' is {fq[col].dtype}; expected float64."
            )
