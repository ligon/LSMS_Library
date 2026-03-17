"""
Sanity checks for LSMS feature DataFrames.

Usage::

    from lsms_library.diagnostics import is_this_feature_sane

    import lsms_library as ll
    uga = ll.Country('Uganda')
    report = is_this_feature_sane(uga.food_acquired(), country='Uganda', feature='food_acquired')
    report.summarize()       # prints human-readable summary
    assert report.ok         # True if no errors (warnings allowed)
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .paths import data_root, COUNTRIES_ROOT
from .yaml_utils import load_yaml


# ---------------------------------------------------------------------------
# Report dataclass
# ---------------------------------------------------------------------------

@dataclass
class Check:
    name: str
    status: str          # "pass", "warn", "fail"
    message: str = ""

    @property
    def ok(self) -> bool:
        return self.status != "fail"


@dataclass
class SanityReport:
    country: str
    feature: str
    checks: list[Check] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return all(c.ok for c in self.checks)

    @property
    def errors(self) -> list[Check]:
        return [c for c in self.checks if c.status == "fail"]

    @property
    def warnings(self) -> list[Check]:
        return [c for c in self.checks if c.status == "warn"]

    def summarize(self) -> str:
        lines = [f"Sanity report: {self.country}/{self.feature}"]
        lines.append(f"  {len(self.checks)} checks: "
                     f"{sum(1 for c in self.checks if c.status == 'pass')} pass, "
                     f"{len(self.warnings)} warn, "
                     f"{len(self.errors)} fail")
        for c in self.checks:
            icon = {"pass": "+", "warn": "~", "fail": "!"}[c.status]
            msg = f"  [{icon}] {c.name}"
            if c.message:
                msg += f": {c.message}"
            lines.append(msg)
        result = "\n".join(lines)
        print(result)
        return result


# ---------------------------------------------------------------------------
# Schema helpers
# ---------------------------------------------------------------------------

def _load_scheme(country: str) -> dict:
    """Load data_scheme.yml for a country, return {table: {index, columns}}."""
    yml = COUNTRIES_ROOT / country / "_" / "data_scheme.yml"
    if not yml.exists():
        return {}
    data = load_yaml(yml)
    if not isinstance(data, dict):
        return {}
    ds = data.get("Data Scheme", {})
    if not isinstance(ds, dict):
        return {}
    result = {}
    for name, spec in ds.items():
        if not isinstance(name, str):
            continue
        if not isinstance(spec, dict):
            result[name] = {"index": [], "columns": {}}
            continue
        idx_raw = spec.get("index", "")
        idx = [s.strip() for s in str(idx_raw).strip("()").split(",") if s.strip()] if idx_raw else []
        skip = {"index", "materialize", "backend"}
        cols = {k: v for k, v in spec.items() if k not in skip and isinstance(k, str)}
        result[name] = {"index": idx, "columns": cols}
    return result


_DTYPE_MAP = {
    "int": "numeric",
    "float": "numeric",
    "str": "string",
    "string": "string",
    "bool": "boolean",
}


# ---------------------------------------------------------------------------
# Individual checks
# ---------------------------------------------------------------------------

def _check_not_empty(df: pd.DataFrame) -> Check:
    if len(df) == 0:
        return Check("not_empty", "fail", "DataFrame is empty")
    return Check("not_empty", "pass", f"{len(df)} rows")


def _check_has_index(df: pd.DataFrame) -> Check:
    names = [n for n in df.index.names if n is not None]
    if not names:
        return Check("has_named_index", "warn", "Index has no named levels (uses default RangeIndex)")
    return Check("has_named_index", "pass", f"Index levels: {names}")


def _check_index_levels(df: pd.DataFrame, scheme: dict, feature: str) -> Check:
    spec = scheme.get(feature, {})
    expected = spec.get("index", [])
    if not expected:
        return Check("index_levels_match_scheme", "pass", "No index declared in scheme (skipped)")
    actual = list(df.index.names)
    missing = [e for e in expected if e not in actual]
    if missing:
        return Check("index_levels_match_scheme", "fail",
                     f"Missing declared levels {missing}; actual: {actual}")
    return Check("index_levels_match_scheme", "pass",
                 f"All declared levels present: {expected}")


def _check_no_null_index(df: pd.DataFrame) -> Check:
    """Check that no index level is entirely null."""
    if isinstance(df.index, pd.MultiIndex):
        for i, name in enumerate(df.index.names):
            level_vals = df.index.get_level_values(i)
            if level_vals.isna().all():
                return Check("no_null_index_levels", "fail",
                             f"Index level '{name}' is entirely null")
            if level_vals.isna().any():
                pct = level_vals.isna().mean()
                if pct > 0.1:
                    return Check("no_null_index_levels", "warn",
                                 f"Index level '{name}' has {pct:.1%} nulls")
    return Check("no_null_index_levels", "pass")


def _check_has_time_index(df: pd.DataFrame) -> Check:
    """Most LSMS features should have a time dimension 't'."""
    names = list(df.index.names)
    if "t" in names:
        n_periods = df.index.get_level_values("t").nunique()
        return Check("has_time_index", "pass", f"{n_periods} distinct periods")
    return Check("has_time_index", "warn", "No 't' level in index")


def _check_has_household_index(df: pd.DataFrame) -> Check:
    """Most features should have a household identifier 'i'."""
    names = list(df.index.names)
    if "i" in names:
        n_hh = df.index.get_level_values("i").nunique()
        return Check("has_household_index", "pass", f"{n_hh} distinct households")
    return Check("has_household_index", "warn", "No 'i' level in index")


def _check_reasonable_size(df: pd.DataFrame) -> Check:
    """Flag suspiciously small or large DataFrames."""
    n = len(df)
    if n < 10:
        return Check("reasonable_size", "warn", f"Only {n} rows — suspiciously small")
    if n > 10_000_000:
        return Check("reasonable_size", "warn", f"{n:,} rows — unusually large for LSMS data")
    return Check("reasonable_size", "pass", f"{n:,} rows")


def _check_no_all_null_columns(df: pd.DataFrame) -> Check:
    """No column should be entirely null."""
    bad = [col for col in df.columns if df[col].isna().all()]
    if bad:
        return Check("no_all_null_columns", "fail",
                     f"{len(bad)} all-null column(s): {bad[:5]}")
    return Check("no_all_null_columns", "pass", f"{len(df.columns)} columns, none all-null")


def _check_no_constant_columns(df: pd.DataFrame) -> Check:
    """Warn about columns with a single unique value (excluding NaN)."""
    constant = []
    for col in df.columns:
        nunique = df[col].dropna().nunique()
        if nunique <= 1 and len(df) > 1:
            constant.append(col)
    if constant:
        return Check("no_constant_columns", "warn",
                     f"{len(constant)} constant column(s): {constant[:5]}")
    return Check("no_constant_columns", "pass")


def _check_declared_columns(df: pd.DataFrame, scheme: dict, feature: str) -> Check:
    """Declared columns in data_scheme should exist."""
    spec = scheme.get(feature, {})
    expected_cols = spec.get("columns", {})
    if not expected_cols:
        return Check("declared_columns_present", "pass", "No columns declared (skipped)")
    all_names = set(df.columns.tolist() + list(df.index.names))
    missing = [c for c in expected_cols if c not in all_names]
    if missing:
        return Check("declared_columns_present", "fail",
                     f"Missing: {missing}")
    return Check("declared_columns_present", "pass",
                 f"All {len(expected_cols)} declared columns present")


def _check_dtype_consistency(df: pd.DataFrame, scheme: dict, feature: str) -> Check:
    """Check that column dtypes roughly match declarations."""
    spec = scheme.get(feature, {})
    expected_cols = spec.get("columns", {})
    if not expected_cols:
        return Check("dtype_consistency", "pass", "No dtype declarations (skipped)")
    issues = []
    for col_name, declared_type in expected_cols.items():
        if col_name not in df.columns:
            continue
        actual = df[col_name].dtype
        expected_kind = _DTYPE_MAP.get(str(declared_type), None)
        if expected_kind is None:
            continue
        if expected_kind == "numeric" and not pd.api.types.is_numeric_dtype(actual):
            issues.append(f"{col_name}: expected numeric, got {actual}")
        elif expected_kind == "string" and not (
            pd.api.types.is_string_dtype(actual) or pd.api.types.is_object_dtype(actual)
        ):
            issues.append(f"{col_name}: expected string, got {actual}")
    if issues:
        return Check("dtype_consistency", "warn", "; ".join(issues[:5]))
    return Check("dtype_consistency", "pass")


def _check_duplicate_index(df: pd.DataFrame) -> Check:
    """Flag high rates of duplicate index entries."""
    if not df.index.has_duplicates:
        return Check("low_duplicate_rate", "pass", "No duplicate indices")
    dup_count = df.index.duplicated().sum()
    total = len(df)
    pct = dup_count / total
    if pct > 0.50:
        return Check("low_duplicate_rate", "fail",
                     f"{dup_count}/{total} duplicates ({pct:.1%})")
    if pct > 0.05:
        return Check("low_duplicate_rate", "warn",
                     f"{dup_count}/{total} duplicates ({pct:.1%})")
    return Check("low_duplicate_rate", "pass",
                 f"{dup_count}/{total} duplicates ({pct:.1%}) — within tolerance")


def _check_index_overlap_with_spine(df: pd.DataFrame, country: str) -> Check:
    """Check that household IDs overlap with other_features (the 'spine')."""
    if "i" not in df.index.names:
        return Check("index_overlap_with_spine", "pass", "No 'i' index (skipped)")
    spine_path = data_root(country) / "var" / "other_features.parquet"
    if not spine_path.exists():
        return Check("index_overlap_with_spine", "pass",
                     "other_features not cached (skipped)")
    try:
        spine = pd.read_parquet(spine_path, engine="pyarrow")
    except Exception:
        return Check("index_overlap_with_spine", "pass", "Could not read spine (skipped)")
    if "i" not in spine.index.names:
        return Check("index_overlap_with_spine", "pass", "Spine has no 'i' index (skipped)")
    feature_ids = set(df.index.get_level_values("i").unique())
    spine_ids = set(spine.index.get_level_values("i").unique())
    if not spine_ids:
        return Check("index_overlap_with_spine", "pass", "Spine empty (skipped)")
    overlap = len(feature_ids & spine_ids) / len(spine_ids)
    if overlap < 0.5:
        return Check("index_overlap_with_spine", "warn",
                     f"Only {overlap:.1%} of spine household IDs appear in this feature")
    return Check("index_overlap_with_spine", "pass",
                 f"{overlap:.1%} overlap with other_features household IDs")


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def is_this_feature_sane(
    df: pd.DataFrame,
    country: str,
    feature: str,
) -> SanityReport:
    """Run all sanity checks on a feature DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        The feature data (e.g., from ``Country('Uganda').food_acquired()``).
    country : str
        Country name (e.g., ``'Uganda'``).
    feature : str
        Feature/table name (e.g., ``'food_acquired'``).

    Returns
    -------
    SanityReport
        Contains individual Check results.  ``report.ok`` is True if
        no checks failed (warnings are allowed).
    """
    scheme = _load_scheme(country)
    report = SanityReport(country=country, feature=feature)

    report.checks.append(_check_not_empty(df))
    report.checks.append(_check_has_index(df))
    report.checks.append(_check_index_levels(df, scheme, feature))
    report.checks.append(_check_no_null_index(df))
    report.checks.append(_check_has_time_index(df))
    report.checks.append(_check_has_household_index(df))
    report.checks.append(_check_reasonable_size(df))
    report.checks.append(_check_no_all_null_columns(df))
    report.checks.append(_check_no_constant_columns(df))
    report.checks.append(_check_declared_columns(df, scheme, feature))
    report.checks.append(_check_dtype_consistency(df, scheme, feature))
    report.checks.append(_check_duplicate_index(df))
    report.checks.append(_check_index_overlap_with_spine(df, country))

    return report
