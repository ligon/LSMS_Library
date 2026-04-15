"""
Test that `Country('Uganda').<method>()` output is functionally equivalent
to the canonical replication-package parquet.

Complements `test_uganda_invariance.py`, which reads cached parquets from
disk and fingerprints them.  That approach silently skips features that
are derived at API time (e.g. `household_characteristics`, `locality`)
because no cached parquet exists.  This file calls the API directly and
compares to the replication package parquet as the authoritative
reference.

## Locating the replication package

Tests run only when the replication directory is accessible.  Resolution
order:

1. Env var `LSMS_UGANDA_REPLICATION_DIR` if set.
2. Fallback: `~/Projects/RiskSharing_Replication/external_data/LSMS_Library/lsms_library/countries/Uganda/var`.
3. If neither exists with parquet files, all tests in this file skip.

## Scope

17 Uganda features in the replication package, of which:
- 15 are tested here (both API and replication parquet available).
- 1 (`cluster_features`) is skipped: replication parquet is 0 bytes.
- 1 (`other_features`) is skipped: intentionally removed from the API.

## Comparison methodology

The API has evolved since the replication was generated (kinship
decomposition, `v`-join, canonical column renames, market-level lifted
out of cache, etc.).  The test normalises API output via per-feature
transforms and compares on the **intersection of common `(i, t)` keys**.
Wave-coverage expansion (API covers more waves than replication) is
expected and not a failure; only value disagreement on common rows is.

Tolerance is per-feature:
- 0.0 for string / integer columns and for features where API and
  replication should be bit-exact on common rows.
- Small non-zero `atol` for features where intentional data-quality
  improvements (e.g. `age_handler()` sentinel cleanup affecting
  `household_characteristics.log HSize`) produce small numeric drift.
"""

from __future__ import annotations

import os
import warnings
from pathlib import Path
from typing import Callable, Optional

import numpy as np
import pandas as pd
import pytest


# --------------------------------------------------------------------- setup

_DEFAULT_REPL_DIR = (
    Path.home() / "Projects/RiskSharing_Replication/external_data"
    / "LSMS_Library/lsms_library/countries/Uganda/var"
)


def _resolve_replication_dir() -> Optional[Path]:
    """Look up the Uganda replication var/ dir via env var or default."""
    env = os.environ.get("LSMS_UGANDA_REPLICATION_DIR")
    candidate = Path(env).expanduser() if env else _DEFAULT_REPL_DIR
    if candidate.is_dir() and any(candidate.glob("*.parquet")):
        return candidate
    return None


_REPL_DIR = _resolve_replication_dir()
_SKIP_REASON = (
    "Uganda replication dir not found; set LSMS_UGANDA_REPLICATION_DIR "
    f"or install the replication package at {_DEFAULT_REPL_DIR}"
)

pytestmark = pytest.mark.skipif(_REPL_DIR is None, reason=_SKIP_REASON)


# ----------------------------------------------------------- per-feature specs

_HH_CHARS_API_TO_REPLICATION = {
    # API emits 'Females 00-03' ... 'Males 51-99'.
    # Replication has 'F 00-03' ... 'M 51+' (abbreviated + last-bucket label).
    # Normalise to replication form for comparison.
    "Females 00-03": "F 00-03",
    "Females 04-08": "F 04-08",
    "Females 09-13": "F 09-13",
    "Females 14-18": "F 14-18",
    "Females 19-30": "F 19-30",
    "Females 31-50": "F 31-50",
    "Females 51-99": "F 51+",
    "Males 00-03": "M 00-03",
    "Males 04-08": "M 04-08",
    "Males 09-13": "M 09-13",
    "Males 14-18": "M 14-18",
    "Males 19-30": "M 19-30",
    "Males 31-50": "M 31-50",
    "Males 51-99": "M 51+",
}


def _tx_household_characteristics(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns=_HH_CHARS_API_TO_REPLICATION)


def _tx_food_expenditures(df: pd.DataFrame) -> pd.DataFrame:
    # API renamed 'x' → 'Expenditure' in 4dc0b351 / 51d545d7.
    return df.rename(columns={"Expenditure": "x"})


def _tx_household_roster(df: pd.DataFrame) -> pd.DataFrame:
    # API adds kinship decomposition and joins `v` from sample().  Neither
    # is in the replication roster — strip them for comparison.
    df = df.drop(columns=["Generation", "Distance", "Affinity"], errors="ignore")
    df = df.rename(columns={"Relationship": "Relation"})
    if "v" in df.index.names:
        df = df.reset_index("v", drop=True)
    return df


def _tx_locality(df: pd.DataFrame) -> pd.DataFrame:
    # Deprecated shim: API returns column 'Parish'; replication labeled it 'v'.
    return df.rename(columns={"Parish": "v"})


def _tx_shocks(df: pd.DataFrame) -> pd.DataFrame:
    # Replication had 'Shock' as a regular column; API has it in the index.
    if "Shock" in df.index.names:
        df = df.reset_index("Shock")
    return df


# Spec: (api_method, api_kwargs, transform_api, atol)
# transform_api normalises API output to match replication schema before compare.
# atol is the max absolute difference allowed on a numeric column (0.0 = bit-exact).
FEATURE_SPECS: list[tuple[str, dict, Optional[Callable], float]] = [
    ("earnings",                  {"market": "Region"}, None,                          0.0),
    ("enterprise_income",         {"market": "Region"}, None,                          0.0),
    ("fct",                       {},                   None,                          0.0),
    ("food_acquired",             {"market": "Region"}, None,                          0.0),
    ("food_expenditures",         {"market": "Region"}, _tx_food_expenditures,         0.0),
    ("food_prices",               {"market": "Region"}, None,                          0.0),
    ("food_quantities",           {"market": "Region"}, None,                          0.0),
    ("household_characteristics", {"market": "Region"}, _tx_household_characteristics, 0.02),
    ("household_roster",          {},                   _tx_household_roster,          0.0),
    ("income",                    {"market": "Region"}, None,                          0.0),
    ("interview_date",            {"market": "Region"}, None,                          0.0),
    ("locality",                  {"market": "Region"}, _tx_locality,                  0.0),
    ("nutrition",                 {"market": "Region"}, None,                          0.1),
    ("people_last7days",          {"market": "Region"}, None,                          0.0),
    ("shocks",                    {"market": "Region"}, _tx_shocks,                    0.0),
]


# ------------------------------------------------------------------- helpers

def _load_replication(name: str) -> pd.DataFrame:
    """Load a replication parquet; pytest.skip if missing or unreadable."""
    path = _REPL_DIR / f"{name}.parquet"
    if not path.is_file():
        pytest.skip(f"replication parquet missing: {path}")
    try:
        return pd.read_parquet(path, engine="pyarrow")
    except Exception as exc:
        pytest.skip(f"replication parquet unreadable ({type(exc).__name__}): {exc}")


def _call_api(name: str, kwargs: dict) -> pd.DataFrame:
    """Invoke Country('Uganda').<name>(**kwargs) with warnings silenced."""
    import lsms_library as ll
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        method = getattr(ll.Country("Uganda"), name)
        return method(**kwargs)


def _merge_on_common_index(
    api: pd.DataFrame, repl: pd.DataFrame
) -> tuple[pd.DataFrame, list[str]]:
    """
    Inner-merge on the intersection of index level names.

    Returns (merged_frame, common_levels).  Merged frame has one row per
    common key and `_api` / `_repl` suffixed columns for every shared
    column name.
    """
    common_levels = [lev for lev in repl.index.names if lev in api.index.names]
    if not common_levels:
        raise ValueError(
            f"no common index levels between API {list(api.index.names)} "
            f"and replication {list(repl.index.names)}"
        )
    api_flat = api.reset_index()
    repl_flat = repl.reset_index()
    merged = api_flat.merge(repl_flat, on=common_levels, suffixes=("_api", "_repl"))
    return merged, common_levels


def _compare_column(
    col: str,
    api_series: pd.Series,
    repl_series: pd.Series,
    atol: float,
) -> None:
    """Raise AssertionError on disagreement, with an informative message."""
    api_na = api_series.isna()
    repl_na = repl_series.isna()
    # Both NaN is agreement.
    both_na = api_na & repl_na
    # One NaN but not the other is disagreement only if there's real data.
    only_one_na = api_na ^ repl_na
    assert not only_one_na.any(), (
        f"column {col!r}: {only_one_na.sum()} rows have NaN in one side but not the other"
    )
    mask = ~both_na
    if not mask.any():
        return
    a = api_series[mask]
    r = repl_series[mask]
    if pd.api.types.is_numeric_dtype(a) and pd.api.types.is_numeric_dtype(r):
        diff = (a.astype(float) - r.astype(float)).abs()
        max_diff = diff.max()
        mean_diff = diff.mean()
        if max_diff > atol:
            # Provide context on how bad it is.
            n_bad = int((diff > atol).sum())
            raise AssertionError(
                f"column {col!r}: max |Δ| = {max_diff:.6g} > atol {atol:.6g}; "
                f"mean |Δ| = {mean_diff:.6g}; {n_bad}/{len(diff)} rows out of tolerance"
            )
    else:
        # Non-numeric: require exact string equality.
        a_str = a.astype(str)
        r_str = r.astype(str)
        mismatch = (a_str != r_str).sum()
        assert mismatch == 0, (
            f"column {col!r}: {mismatch}/{len(a)} rows differ (non-numeric)"
        )


# ------------------------------------------------------------------- the test

@pytest.mark.parametrize(
    "name,kwargs,transform,atol",
    FEATURE_SPECS,
    ids=[spec[0] for spec in FEATURE_SPECS],
)
def test_api_matches_replication(
    name: str,
    kwargs: dict,
    transform: Optional[Callable],
    atol: float,
) -> None:
    """API output must agree with the canonical replication parquet on common rows."""
    repl = _load_replication(name)

    try:
        api = _call_api(name, kwargs)
    except Exception as exc:
        pytest.fail(f"API call Country('Uganda').{name}(**{kwargs}) raised: "
                    f"{type(exc).__name__}: {exc}")

    if transform is not None:
        api = transform(api)

    merged, common_levels = _merge_on_common_index(api, repl)

    assert len(merged) > 0, (
        f"{name}: no overlap between API and replication on {common_levels}; "
        f"API shape={api.shape} index={list(api.index.names)}; "
        f"repl shape={repl.shape} index={list(repl.index.names)}"
    )

    # Enumerate columns present on both sides (after `_api`/`_repl` suffixing).
    suffixed_api = [c for c in merged.columns if c.endswith("_api")]
    pairs = []
    for c in suffixed_api:
        base = c[:-4]
        if f"{base}_repl" in merged.columns:
            pairs.append(base)

    assert pairs, (
        f"{name}: no shared data columns after merge; "
        f"API cols after transform={list(api.columns)}; "
        f"repl cols={list(repl.columns)}"
    )

    for base in pairs:
        _compare_column(base, merged[f"{base}_api"], merged[f"{base}_repl"], atol)
