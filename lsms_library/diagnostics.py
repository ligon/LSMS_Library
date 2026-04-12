"""
Sanity checks for LSMS feature DataFrames.

Usage::

    from lsms_library.diagnostics import is_this_feature_sane, check_panel_consistency

    import lsms_library as ll
    uga = ll.Country('Uganda')
    report = is_this_feature_sane(uga.food_acquired(), country='Uganda', feature='food_acquired')
    report.summarize()       # prints human-readable summary
    assert report.ok         # True if no errors (warnings allowed)

    # Panel consistency (for countries with panel data):
    panel_report = check_panel_consistency(uga)
    panel_report.summarize()
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml as _yaml

from .paths import data_root, COUNTRIES_ROOT
from .yaml_utils import load_yaml


# ---------------------------------------------------------------------------
# API-derived column registry
# ---------------------------------------------------------------------------

def _load_api_derived() -> dict[str, set[str]]:
    """Return {table: {col, ...}} for columns marked api_derived in data_info.yml.

    These columns are produced by ``_finalize_result()`` at API read time and
    are intentionally absent from raw cached parquets.  Sanity checks that
    operate on raw parquets must skip them.
    """
    from importlib.resources import files
    try:
        info_path = files("lsms_library") / "data_info.yml"
        with open(info_path, "r", encoding="utf-8") as f:
            info = _yaml.safe_load(f)
    except Exception:
        return {}
    result: dict[str, set[str]] = {}
    for table, cols in info.get("Columns", {}).items():
        if not isinstance(cols, dict):
            continue
        derived = {c for c, m in cols.items() if isinstance(m, dict) and m.get("api_derived")}
        if derived:
            result[table] = derived
    return result


_API_DERIVED_COLUMNS: dict[str, set[str]] = _load_api_derived()


# ---------------------------------------------------------------------------
# Report dataclass
# ---------------------------------------------------------------------------

@dataclass
class Check:
    """A single named sanity-check result.

    Produced by the individual check functions in this module and
    collected into a :class:`SanityReport`. ``status`` is one of
    ``"pass"``, ``"warn"``, or ``"fail"``; :attr:`ok` is true for
    anything except ``"fail"``.
    """
    name: str
    status: str          # "pass", "warn", "fail"
    message: str = ""

    @property
    def ok(self) -> bool:
        return self.status != "fail"


@dataclass
class SanityReport:
    """Aggregated result of running a feature's sanity checks.

    Returned by :func:`is_this_feature_sane`. Holds the country and
    feature name, the ordered list of :class:`Check` results, and
    helpers for filtering / printing. :attr:`ok` is true when every
    check passed or merely warned.
    """
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
    """Load data_scheme.yml for a country, return {table: {index, columns, optional}}.

    Column declarations support an extended dict syntax::

        panel_weight:
          type: float
          optional: true

    Columns declared with ``optional: true`` are included in ``columns`` for
    dtype and value-constraint checks (when the column IS present), but are
    collected separately in the ``optional`` set so that ``_check_declared_columns``
    can skip them when they are absent from the DataFrame.  The shorthand scalar
    syntax (``panel_weight: float``) is still supported and implies required.
    """
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
            result[name] = {"index": [], "columns": {}, "optional": set()}
            continue
        idx_raw = spec.get("index", "")
        idx = [s.strip() for s in str(idx_raw).strip("()").split(",") if s.strip()] if idx_raw else []
        skip = {"index", "materialize", "backend"}
        cols = {}
        optional_cols: set[str] = set()
        for k, v in spec.items():
            if k in skip or not isinstance(k, str):
                continue
            if isinstance(v, dict):
                # Extended syntax: {type: float, optional: true}
                dtype = v.get("type", "str")
                cols[k] = dtype
                if v.get("optional"):
                    optional_cols.add(k)
            else:
                cols[k] = v
        result[name] = {"index": idx, "columns": cols, "optional": optional_cols}
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
    extra = [a for a in actual if a not in expected]
    if missing:
        return Check("index_levels_match_scheme", "fail",
                     f"Missing declared levels {missing}; actual: {actual}")
    if extra:
        return Check("index_levels_match_scheme", "warn",
                     f"Extra index levels not in scheme: {extra}; "
                     f"expected: {expected}, actual: {actual}")
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
    """Declared columns in data_scheme should exist.

    Columns marked ``api_derived`` in data_info.yml are skipped when not
    present: they are added by ``_finalize_result()`` at API read time and are
    intentionally absent from raw cached parquets.

    Columns declared with ``optional: true`` in data_scheme.yml are also
    skipped when absent — they represent data that is genuinely unavailable
    for some countries (e.g., ``panel_weight`` in cross-sectional surveys).
    """
    spec = scheme.get(feature, {})
    expected_cols = spec.get("columns", {})
    if not expected_cols:
        return Check("declared_columns_present", "pass", "No columns declared (skipped)")
    api_derived = _API_DERIVED_COLUMNS.get(feature, set())
    optional_cols = spec.get("optional", set())
    all_names = set(df.columns.tolist() + list(df.index.names))
    missing = [
        c for c in expected_cols
        if c not in all_names and c not in api_derived and c not in optional_cols
    ]
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
        if isinstance(declared_type, list):
            # List declaration = constrained string column
            if not (pd.api.types.is_string_dtype(actual) or pd.api.types.is_object_dtype(actual)):
                issues.append(f"{col_name}: expected string (constrained), got {actual}")
            continue
        expected_kind = _DTYPE_MAP.get(str(declared_type), None)
        if expected_kind is None:
            continue
        if expected_kind == "numeric" and not pd.api.types.is_numeric_dtype(actual):
            issues.append(f"{col_name}: expected numeric, got {actual}")
        elif expected_kind == "string" and not (
            pd.api.types.is_string_dtype(actual) or pd.api.types.is_object_dtype(actual)
        ):
            issues.append(f"{col_name}: expected string, got {actual}")
        elif expected_kind == "boolean" and not (
            pd.api.types.is_bool_dtype(actual) or str(actual) == "boolean"
        ):
            issues.append(f"{col_name}: expected boolean, got {actual}")
    if issues:
        return Check("dtype_consistency", "warn", "; ".join(issues[:5]))
    return Check("dtype_consistency", "pass")


def _check_value_constraints(df: pd.DataFrame, scheme: dict, feature: str) -> Check:
    """Warn if column values fall outside a declared set (list declarations)."""
    spec = scheme.get(feature, {})
    expected_cols = spec.get("columns", {})
    if not expected_cols:
        return Check("value_constraints", "pass", "No value constraints declared (skipped)")
    issues = []
    for col_name, declared_type in expected_cols.items():
        if not isinstance(declared_type, list) or col_name not in df.columns:
            continue
        unexpected = set(df[col_name].dropna().unique()) - set(declared_type)
        if unexpected:
            sample = sorted(str(v) for v in list(unexpected)[:5])
            issues.append(f"{col_name}: {len(unexpected)} unexpected value(s): {sample}")
    if issues:
        return Check("value_constraints", "warn", "; ".join(issues))
    constrained = [c for c, t in expected_cols.items() if isinstance(t, list) and c in df.columns]
    if not constrained:
        return Check("value_constraints", "pass", "No constrained columns (skipped)")
    return Check("value_constraints", "pass",
                 f"{len(constrained)} constrained column(s) — all values valid")


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


def _check_float_stringified_index(df: pd.DataFrame) -> Check:
    """Flag index levels with values that look like float-stringified integers (.0 suffix).

    This catches a common bug where numeric IDs pass through a float
    stage (e.g., due to NaN promotion) and end up as '12345.0' instead
    of '12345', breaking joins with tables that store the clean form.
    """
    import re
    if not isinstance(df.index, pd.MultiIndex):
        return Check("no_float_stringified_index", "pass", "Single index (skipped)")

    bad_levels = []
    for level_name in df.index.names:
        if level_name is None:
            continue
        vals = df.index.get_level_values(level_name)
        # Only check string-like levels
        if not (pd.api.types.is_string_dtype(vals)
                or pd.api.types.is_object_dtype(vals)):
            continue
        sample = vals.dropna().unique()[:500]
        float_count = sum(1 for v in sample
                          if isinstance(v, str) and re.fullmatch(r'-?\d+\.0', v))
        if float_count > len(sample) * 0.5 and float_count > 5:
            bad_levels.append(f"{level_name} ({float_count}/{len(sample)} "
                              f"values have .0 suffix)")
    if bad_levels:
        return Check("no_float_stringified_index", "warn",
                     f"Float-stringified index values: {'; '.join(bad_levels)}")
    return Check("no_float_stringified_index", "pass",
                 "No float-stringified index values")


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
# ---------------------------------------------------------------------------
# Helper: load any feature from a Country (handles properties + methods)
# ---------------------------------------------------------------------------

# Features stored as properties (return dicts/non-DataFrame) rather than
# dynamic methods generated by __getattr__.
_PROPERTY_FEATURES = frozenset({"panel_ids", "updated_ids"})


def load_feature(country, feature: str):
    """Load a feature from a Country object, handling both methods and properties.

    Returns the result (DataFrame, dict, or None).  Raises on failure.
    """
    if feature in _PROPERTY_FEATURES:
        return getattr(country, feature)
    method = getattr(country, feature)
    return method()


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
    report.checks.append(_check_value_constraints(df, scheme, feature))
    report.checks.append(_check_duplicate_index(df))
    report.checks.append(_check_float_stringified_index(df))
    report.checks.append(_check_index_overlap_with_spine(df, country))

    return report


# ---------------------------------------------------------------------------
# Cross-country comparison checks
# ---------------------------------------------------------------------------

def _check_columns_match_reference(df: pd.DataFrame, ref_df: pd.DataFrame,
                                   country: str, ref_country: str,
                                   feature: str) -> Check:
    """Verify that columns match a reference country's output."""
    my_cols = set(df.columns)
    ref_cols = set(ref_df.columns)
    missing = ref_cols - my_cols
    extra = my_cols - ref_cols
    if missing:
        return Check("columns_match_reference", "fail",
                     f"Missing vs {ref_country}: {sorted(missing)}")
    if extra:
        return Check("columns_match_reference", "warn",
                     f"Extra vs {ref_country}: {sorted(extra)}")
    return Check("columns_match_reference", "pass",
                 f"Columns match {ref_country}")


def _check_string_consistency(df: pd.DataFrame) -> Check:
    """Flag string columns with inconsistent casing that suggests missing normalization."""
    issues = []
    for col in df.columns:
        if not (pd.api.types.is_string_dtype(df[col])
                or pd.api.types.is_object_dtype(df[col])):
            continue
        vals = df[col].dropna().unique()
        if len(vals) < 2:
            continue
        # Group by case-folded version; if the same concept appears in
        # multiple casings, that's a normalization problem
        from collections import Counter
        folded = Counter()
        examples = {}
        for v in vals:
            key = str(v).strip().casefold()
            folded[key] += 1
            if key not in examples:
                examples[key] = []
            examples[key].append(str(v))
        dupes = {k: examples[k] for k, c in folded.items()
                 if c > 1 and len(set(examples[k])) > 1}
        if dupes:
            sample = list(dupes.values())[:3]
            issues.append(f"{col}: {len(dupes)} label(s) with inconsistent "
                          f"casing (e.g., {sample[0][:3]})")
    if issues:
        return Check("string_consistency", "warn", "; ".join(issues[:3]))
    return Check("string_consistency", "pass",
                 "No inconsistent string casing detected")


def _check_index_structure_matches(df: pd.DataFrame, ref_df: pd.DataFrame,
                                   ref_country: str) -> Check:
    """Index level names should match the reference."""
    my_names = list(df.index.names)
    ref_names = list(ref_df.index.names)
    # Ignore 'country' level if present (from Feature class)
    my_names = [n for n in my_names if n != "country"]
    ref_names = [n for n in ref_names if n != "country"]
    if my_names != ref_names:
        return Check("index_structure_matches", "warn",
                     f"Index {my_names} vs {ref_country} {ref_names}")
    return Check("index_structure_matches", "pass",
                 f"Index structure matches {ref_country}")


def _check_value_ranges_plausible(df: pd.DataFrame, ref_df: pd.DataFrame,
                                  feature: str) -> Check:
    """Spot-check that numeric columns have plausible ranges vs reference."""
    issues = []
    for col in df.columns:
        if col not in ref_df.columns:
            continue
        if not pd.api.types.is_numeric_dtype(df[col]):
            continue
        my_mean = df[col].dropna().mean()
        ref_mean = ref_df[col].dropna().mean()
        if ref_mean == 0 or pd.isna(ref_mean) or pd.isna(my_mean):
            continue
        ratio = my_mean / ref_mean
        if ratio > 100 or ratio < 0.01:
            issues.append(f"{col}: mean {my_mean:.1f} vs ref {ref_mean:.1f} "
                          f"(ratio {ratio:.1f})")
    if issues:
        return Check("value_ranges_plausible", "warn",
                     "; ".join(issues[:3]))
    return Check("value_ranges_plausible", "pass",
                 "Numeric column ranges look plausible vs reference")


def _check_new_wave_present(df: pd.DataFrame, wave: str) -> Check:
    """Verify that the specified wave appears in the data."""
    if "t" not in df.index.names:
        return Check("new_wave_present", "warn", "No 't' in index")
    waves = sorted(df.index.get_level_values("t").unique())
    if wave not in waves:
        return Check("new_wave_present", "fail",
                     f"Wave '{wave}' not in data. Found: {waves}")
    wave_rows = (df.index.get_level_values("t") == wave).sum()
    return Check("new_wave_present", "pass",
                 f"Wave '{wave}' present with {wave_rows:,} rows")


def _check_no_regression(df: pd.DataFrame, country: str,
                         feature: str) -> Check:
    """Existing waves should still have data (adding a wave shouldn't break old ones)."""
    if "t" not in df.index.names:
        return Check("no_regression", "pass", "No 't' index (skipped)")
    waves = sorted(df.index.get_level_values("t").unique())
    empty_waves = []
    for w in waves:
        n = (df.index.get_level_values("t") == w).sum()
        if n == 0:
            empty_waves.append(w)
    if empty_waves:
        return Check("no_regression", "fail",
                     f"Empty waves (regression?): {empty_waves}")
    return Check("no_regression", "pass",
                 f"All {len(waves)} waves have data")


def _check_unmapped_labels(df: pd.DataFrame, feature: str) -> Check:
    """Check for signs of unmapped categorical labels (raw codes surviving)."""
    issues = []
    # String columns with numeric-looking values suggest unmapped codes
    for col in df.columns:
        if not (pd.api.types.is_string_dtype(df[col])
                or pd.api.types.is_object_dtype(df[col])):
            continue
        vals = df[col].dropna()
        if len(vals) == 0:
            continue
        sample = vals.head(200)
        # Check if values look like raw survey codes ("1. LABEL" or just numbers)
        coded = sample.astype(str).str.match(r'^\d+\.\s')
        if coded.mean() > 0.5:
            examples = sample[coded].unique()[:3].tolist()
            issues.append(f"{col}: {coded.mean():.0%} values look like "
                          f"raw codes (e.g., {examples})")
    if issues:
        return Check("unmapped_labels", "warn", "; ".join(issues[:3]))
    return Check("unmapped_labels", "pass", "No raw survey codes detected")


# ---------------------------------------------------------------------------
# Validation gate
# ---------------------------------------------------------------------------

def validate_feature(
    country: str,
    feature: str,
    new_wave: str | None = None,
    reference_country: str = "Uganda",
) -> SanityReport:
    """Comprehensive validation gate for a feature.

    Runs all per-country sanity checks, cross-country comparison against
    a reference, regression checks, and label mapping checks.  Intended
    as the mandatory gate before committing new feature work.

    Parameters
    ----------
    country : str
        Country name (e.g., ``'Ethiopia'``).
    feature : str
        Table name (e.g., ``'household_roster'``).
    new_wave : str, optional
        If provided, verify this wave appears in the output.
    reference_country : str
        Country to compare against (default ``'Uganda'``).

    Returns
    -------
    SanityReport
        ``report.ok`` is True only if no checks failed.

    Example
    -------
    >>> from lsms_library.diagnostics import validate_feature
    >>> report = validate_feature('Ethiopia', 'household_roster',
    ...                           new_wave='2021-22')
    >>> report.summarize()
    >>> assert report.ok
    """
    from . import Country, Feature

    report = SanityReport(country=country, feature=feature)

    # --- Load the data ------------------------------------------------------
    try:
        c = Country(country)
        method = getattr(c, feature)
        df = method()
    except Exception as e:
        report.checks.append(Check("load_data", "fail", str(e)))
        return report
    report.checks.append(Check("load_data", "pass",
                                f"Loaded {len(df):,} rows"))

    # --- Standard sanity checks ---------------------------------------------
    scheme = _load_scheme(country)
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
    report.checks.append(_check_value_constraints(df, scheme, feature))
    report.checks.append(_check_duplicate_index(df))

    # --- New wave present? --------------------------------------------------
    if new_wave:
        report.checks.append(_check_new_wave_present(df, new_wave))

    # --- No regression on existing waves ------------------------------------
    report.checks.append(_check_no_regression(df, country, feature))

    # --- Unmapped labels ----------------------------------------------------
    report.checks.append(_check_unmapped_labels(df, feature))

    # --- String consistency -------------------------------------------------
    report.checks.append(_check_string_consistency(df))

    # --- Cross-country comparison -------------------------------------------
    if reference_country != country:
        try:
            feat = Feature(feature)
            if reference_country in feat.countries:
                ref_c = Country(reference_country)
                ref_method = getattr(ref_c, feature)
                ref_df = ref_method()
                report.checks.append(
                    _check_columns_match_reference(df, ref_df, country,
                                                   reference_country, feature))
                report.checks.append(
                    _check_index_structure_matches(df, ref_df,
                                                   reference_country))
                report.checks.append(
                    _check_value_ranges_plausible(df, ref_df, feature))
            else:
                report.checks.append(
                    Check("cross_country", "pass",
                          f"{reference_country} doesn't have {feature} (skipped)"))
        except Exception as e:
            report.checks.append(
                Check("cross_country", "warn",
                      f"Could not load reference {reference_country}: {e}"))

    return report


# ---------------------------------------------------------------------------
# Panel consistency checks
# ---------------------------------------------------------------------------

def _normalize_panel_ids(country) -> list[tuple[tuple[str, str], tuple[str, str]]]:
    """Return panel_ids as a list of ``((cur_wave, cur_id), (prev_wave, prev_id))``.

    Handles both on-disk serialisation forms:

    - **JSON form** (written by country-level ``panel_ids.py`` scripts):
      dict of ``"cur_wave,cur_id" -> "prev_wave,prev_id"`` strings. The
      2021-04-10 JSON writer joins the tuples with ``,`` and loses
      tuple structure.
    - **In-memory form** (emitted by ``local_tools.panel_ids()`` from
      YAML wave aggregation): a ``RecursiveDict`` whose ``.data`` is
      ``{(cur_wave, cur_id): (prev_wave, prev_id)}``.

    The two forms are treated uniformly by this helper so downstream
    checks can walk the chain without caring which serialisation path
    the country uses.
    """
    pi = country.panel_ids
    if not pi or not isinstance(pi, dict):
        return []
    data = getattr(pi, "data", pi)

    entries: list[tuple[tuple[str, str], tuple[str, str]]] = []
    for k, v in data.items():
        if isinstance(k, tuple) and isinstance(v, tuple):
            if len(k) >= 2 and len(v) >= 2:
                entries.append(((str(k[0]), str(k[1])), (str(v[0]), str(v[1]))))
            continue
        if isinstance(k, str) and isinstance(v, str):
            k_parts = k.split(",", 1)
            v_parts = v.split(",", 1)
            if len(k_parts) == 2 and len(v_parts) == 2:
                entries.append(
                    ((k_parts[0], k_parts[1]), (v_parts[0], v_parts[1]))
                )
    return entries


def _panel_ids_chain_origins(country) -> dict[str, set[str]]:
    """Return ``{baseline_wave: {follow_up_wave, ...}}`` per chain.

    A "chain origin" is a wave whose ``(wave, id) -> (prev_wave, prev_id)``
    entries terminate — i.e. ``prev_wave`` has no further entry in
    ``panel_ids``. This lets disjoint-panel countries report multiple
    baselines: Niger's ECVMA panel chains back to 2011-12 and its
    EHCVM panel chains back to 2018-19, so this returns
    ``{'2011-12': {'2014-15'}, '2018-19': {'2021-22'}}``.

    Used by :func:`_check_updated_ids_cover_waves` and
    :func:`_check_panel_attrition_monotonic` to avoid false-positive
    warnings when a "missing" wave is the baseline of a second program.
    """
    entries = _normalize_panel_ids(country)
    if not entries:
        return {}
    source_waves = {cw for (cw, _), _ in entries}

    origins: dict[str, set[str]] = {}
    for (cw, _), (pw, _) in entries:
        # prev_wave is a baseline if no row has prev_wave as its
        # current wave — i.e. the chain terminates there.
        if pw not in source_waves:
            origins.setdefault(pw, set()).add(cw)
    return origins


def _check_has_panel_ids(country) -> Check:
    """Country must have a non-empty ``panel_ids`` chain to run panel checks.

    ``panel_ids`` is typically a :class:`.local_tools.RecursiveDict`
    (a ``UserDict`` subclass). Use a truthiness check — the
    ``isinstance(pi, dict)`` idiom misses ``UserDict`` instances.
    """
    try:
        pi = country.panel_ids
        if not pi:
            return Check("has_panel_ids", "fail", "panel_ids is empty or None")
        return Check("has_panel_ids", "pass", f"{len(pi)} panel ID mappings")
    except Exception as e:
        return Check("has_panel_ids", "fail", f"Could not load panel_ids: {e}")


def _check_has_updated_ids(country) -> Check:
    """Country must have a non-empty ``updated_ids`` mapping."""
    try:
        ui = country.updated_ids
        if not ui:
            return Check("has_updated_ids", "fail", "updated_ids is empty or None")
        waves_with_mappings = {w for w, m in ui.items() if m}
        return Check("has_updated_ids", "pass",
                     f"{len(waves_with_mappings)} waves with ID mappings")
    except Exception as e:
        return Check("has_updated_ids", "fail", f"Could not load updated_ids: {e}")


def _check_updated_ids_cover_waves(country) -> Check:
    """Every follow-up wave should have a non-empty ``updated_ids`` entry.

    In a single-chain panel the first wave is the baseline; every later
    wave should rewrite back to it. In a disjoint-panel country (Niger:
    ECVMA 2011-12↔2014-15, EHCVM 2018-19↔2021-22) there are **multiple**
    baselines. This check honors both: it infers baselines from the
    ``panel_ids`` chain structure and only warns about waves that are
    genuinely unreachable.
    """
    try:
        ui = country.updated_ids
        waves = sorted(country.waves)
        if len(waves) < 2:
            return Check("updated_ids_cover_waves", "pass",
                         "Single-wave country (skipped)")

        # Gather baselines from the panel_ids chain structure. These are
        # waves that terminate a chain rather than continue one.
        chain_origins = _panel_ids_chain_origins(country)
        baselines = set(chain_origins.keys())
        # Fall back to the first wave alphabetically if no chain data.
        if not baselines:
            baselines = {waves[0]}

        expected = set(waves) - baselines
        covered = {w for w in expected if w in ui and ui[w]}
        missing = expected - covered
        if missing:
            return Check("updated_ids_cover_waves", "warn",
                         f"No ID mappings for follow-up waves: {sorted(missing)} "
                         f"(baselines inferred: {sorted(baselines)})")
        return Check("updated_ids_cover_waves", "pass",
                     f"All {len(covered)} follow-up waves have mappings "
                     f"(baselines: {sorted(baselines)})")
    except Exception as e:
        return Check("updated_ids_cover_waves", "fail", str(e))


def _check_ids_are_self_consistent(country) -> Check:
    """Flag self-referential or null entries in ``updated_ids`` that are
    NOT backed by a corresponding ``panel_ids`` chain entry.

    A self-referential entry ``updated_ids[wave][k] == k`` is only a bug
    if ``panel_ids`` has no matching ``(wave, k) -> (prev_wave, prev_id)``
    chain that would justify it. EHCVM countries legitimately have
    identity mappings for panel households (because ``cur_i == prev_i``
    when ID construction is ``(grappe, menage)``-based), so this check
    cross-references the two outputs and only warns on orphaned
    self-refs or truly null mappings.
    """
    try:
        ui = country.updated_ids
        waves = sorted(country.waves)
        if len(waves) < 2:
            return Check("ids_self_consistent", "pass", "Single-wave (skipped)")

        # Build the set of (wave, id) tuples that have a panel_ids chain
        # entry anchoring them — these are the ones whose updated_ids
        # entry can legitimately be identity.
        entries = _normalize_panel_ids(country)
        if not entries:
            # No chain to cross-reference against. Any self-refs or null
            # entries in updated_ids are uncontextualised — report the
            # situation without spamming orphan counts.
            return Check("ids_self_consistent", "pass",
                         "No panel_ids chain to cross-reference (skipped)")
        chained: set[tuple[str, str]] = {k for k, _ in entries}

        issues = []
        for wave in waves:
            if wave not in ui or not ui[wave]:
                continue
            mappings = ui[wave]
            # Self-refs that are NOT backed by a panel_ids chain.
            orphaned_self_refs = [
                k for k, v in mappings.items()
                if k == v and (wave, k) not in chained
            ]
            if orphaned_self_refs:
                issues.append(
                    f"{wave}: {len(orphaned_self_refs)} orphaned self-referential ID(s)"
                )
            # Null mappings are always a bug.
            null_maps = [
                k for k, v in mappings.items()
                if v is None or (isinstance(v, str) and v.strip() == "")
            ]
            if null_maps:
                issues.append(f"{wave}: {len(null_maps)} null mapping(s)")

        if issues:
            return Check("ids_self_consistent", "warn", "; ".join(issues[:5]))
        return Check("ids_self_consistent", "pass")
    except Exception as e:
        return Check("ids_self_consistent", "fail", str(e))


def _compute_panel_spine(country) -> tuple[pd.DataFrame | None, str]:
    """Compute a household-level panel spine from scratch, with provenance.

    The spine is the reference for which ``(i, t)`` pairs are present,
    used by attrition and id-consistency checks. Preference order:

    1. ``household_roster`` via the ``Country`` API (applies id_walk,
       kinship expansion, and all finalize-result hooks). Collapsed to
       unique ``(i, t)`` rows.
    2. The cached ``household_roster.parquet`` under ``data_root()``
       (cheaper; skips the full finalize pipeline but still has
       canonical IDs after id_walk).
    3. ``cluster_features.parquet`` (legacy fallback, may lack ``i``).

    Returns ``(df, provenance)`` or ``(None, reason)`` if no spine is
    available. **Callers should prefer :func:`_panel_spine` which
    memoises the result on the country object to avoid rebuilding
    across multiple checks in one report.**
    """
    # Try the cached parquet first — much cheaper than the API path
    # when a country has many waves or a flaky stage-layer configuration.
    parquet_path = data_root(country.name) / "var" / "household_roster.parquet"
    if parquet_path.exists():
        try:
            df = pd.read_parquet(parquet_path, engine="pyarrow")
            if "i" in df.index.names and "t" in df.index.names:
                spine = (
                    df.reset_index()[["i", "t"]]
                    .drop_duplicates()
                    .set_index(["i", "t"])
                )
                return spine, "cached household_roster.parquet"
        except Exception:
            pass

    # Next try the API (applies the full finalize pipeline including
    # id_walk, which is what we want for a post-walk spine).
    try:
        roster = country.household_roster()
        if (isinstance(roster, pd.DataFrame)
                and "i" in roster.index.names
                and "t" in roster.index.names):
            spine = (
                roster.reset_index()[["i", "t"]]
                .drop_duplicates()
                .set_index(["i", "t"])
            )
            return spine, "household_roster (API)"
    except Exception:
        pass

    # Last-resort: legacy cluster_features parquet.
    cf_path = data_root(country.name) / "var" / "cluster_features.parquet"
    if cf_path.exists():
        try:
            df = pd.read_parquet(cf_path, engine="pyarrow")
            if "i" in df.index.names and "t" in df.index.names:
                spine = (
                    df.reset_index()[["i", "t"]]
                    .drop_duplicates()
                    .set_index(["i", "t"])
                )
                return spine, f"cached cluster_features.parquet"
        except Exception:
            pass

    return None, "no household_roster / cluster_features available"


def _panel_spine(country) -> tuple[pd.DataFrame | None, str]:
    """Memoised wrapper around :func:`_compute_panel_spine`.

    Within a single ``check_panel_consistency`` run, five checks
    consult the spine. Memoising on the country instance avoids
    rebuilding ``household_roster`` five times — critical for
    countries like Uganda whose stage-layer configuration retries
    on every fresh call.
    """
    cached = getattr(country, "_diagnostics_spine_cache", None)
    if cached is not None:
        return cached
    result = _compute_panel_spine(country)
    try:
        country._diagnostics_spine_cache = result
    except (AttributeError, TypeError):
        # Some Country-like objects may not allow attribute assignment.
        pass
    return result


def _check_panel_attrition_monotonic(country) -> Check:
    """Cross-wave overlap should be non-zero for chained waves.

    Uses ``household_roster`` as the spine (the universal cross-country
    feature). Iterates over ``panel_ids`` chains — if a country has two
    disjoint panels (e.g. Niger), both chains are checked independently
    so that "zero adjacent overlap" across a program boundary is not
    flagged.
    """
    try:
        spine, provenance = _panel_spine(country)
        if spine is None:
            return Check("attrition_monotonic", "pass", provenance)

        spine_waves = sorted(spine.index.get_level_values("t").unique())
        if len(spine_waves) < 2:
            return Check("attrition_monotonic", "pass",
                         f"Single wave (skipped; spine={provenance})")

        # Build per-wave ID sets once.
        ids_by_wave = {
            w: set(spine.xs(w, level="t").index.get_level_values("i").unique())
            for w in spine_waves
        }

        chain_origins = _panel_ids_chain_origins(country)
        baselines = set(chain_origins.keys()) or {spine_waves[0]}

        # Build chains: each baseline anchors a set of follow-up waves
        # that are transitively reachable via panel_ids. For each chain
        # we check consecutive overlap.
        entries = _normalize_panel_ids(country)
        # follows[wave] = prev_wave (from any entry)
        follows: dict[str, str] = {}
        for (cw, _), (pw, _) in entries:
            follows[cw] = pw
        # Build chains by walking backward from each non-baseline wave.
        chains: list[list[str]] = []
        seen_waves: set[str] = set()
        for baseline in sorted(baselines):
            chain = [baseline]
            seen_waves.add(baseline)
            # Extend forward by finding any wave whose "follows" points
            # into the current chain tip.
            extended = True
            while extended:
                extended = False
                for cw, pw in follows.items():
                    if pw == chain[-1] and cw not in seen_waves:
                        chain.append(cw)
                        seen_waves.add(cw)
                        extended = True
                        break
            chains.append(chain)

        issues: list[str] = []
        summaries: list[str] = []
        for chain in chains:
            if len(chain) < 2:
                continue
            # Consecutive-wave overlap within the chain.
            for a, b in zip(chain, chain[1:]):
                ids_a = ids_by_wave.get(a, set())
                ids_b = ids_by_wave.get(b, set())
                n = len(ids_a & ids_b)
                if n == 0:
                    issues.append(f"({a},{b}): zero overlap in {provenance}")
                else:
                    ratio = n / min(len(ids_a), len(ids_b)) if ids_a and ids_b else 0
                    summaries.append(f"{a}->{b}: {n} HH ({ratio:.0%})")

        # Also flag off-diagonal counts that exceed the source wave
        # (which would mean more matches than source households, a bug).
        for chain in chains:
            for i, s in enumerate(chain):
                for t in chain[i + 1:]:
                    n = len(ids_by_wave.get(s, set()) & ids_by_wave.get(t, set()))
                    if n > len(ids_by_wave.get(s, set())):
                        issues.append(
                            f"({s},{t}): {n} > {len(ids_by_wave.get(s, set()))} "
                            f"(more matches than source wave)"
                        )

        if issues:
            return Check("attrition_monotonic", "warn",
                         "; ".join(issues[:5]))
        if not summaries:
            return Check("attrition_monotonic", "pass",
                         f"No multi-wave chains found ({provenance})")
        return Check("attrition_monotonic", "pass",
                     f"Attrition chains OK ({provenance}): "
                     + "; ".join(summaries[:6]))
    except Exception as e:
        return Check("attrition_monotonic", "fail", str(e))


def _check_ids_applied_consistently(country) -> Check:
    """For the household_roster spine, check that IDs are in the canonical
    (post-``id_walk``) form rather than the raw wave-specific form.

    This catches the case where a feature was built without calling
    ``id_walk``: its IDs would match the *values* of ``updated_ids[wave]``
    (the old, wave-specific forms) rather than the *keys* (the canonical
    baseline IDs).
    """
    try:
        ui = country.updated_ids
        if not ui:
            return Check("ids_applied_consistently", "pass",
                         "No updated_ids (skipped)")

        spine, provenance = _panel_spine(country)
        if spine is None:
            return Check("ids_applied_consistently", "pass", provenance)

        issues: list[str] = []
        # For each wave with a non-empty mapping, check whether the
        # spine's IDs look pre-update. updated_ids[wave] maps
        # current_id -> canonical_id; the canonical form is the VALUES,
        # and feature IDs should be values, not keys.
        for wave, mapping in ui.items():
            if not mapping:
                continue
            wave_ids = set(
                spine.xs(wave, level="t", drop_level=False)
                .index.get_level_values("i")
                .unique()
            ) if wave in spine.index.get_level_values("t") else set()
            if not wave_ids:
                continue
            pre_update = set(mapping.keys())     # wave-specific form
            canonical = set(mapping.values())    # canonical baseline form

            # A wave with an identity mapping ({k: k}) has keys == values
            # and is ambiguous; skip.
            if pre_update == canonical:
                continue

            # Rewrites that haven't been applied: IDs in the spine match
            # the pre-update keys but NOT the canonical values.
            looks_pre = (wave_ids & pre_update) - canonical
            if looks_pre and len(looks_pre) > len(wave_ids) * 0.1:
                issues.append(
                    f"{wave}: {len(looks_pre)} IDs look pre-update "
                    f"(id_walk may not have been applied)"
                )

        if issues:
            return Check("ids_applied_consistently", "warn", "; ".join(issues[:5]))
        return Check("ids_applied_consistently", "pass",
                     f"Spine IDs look canonical ({provenance})")
    except Exception as e:
        return Check("ids_applied_consistently", "fail", str(e))


def _check_panel_ids_targets_exist(country) -> Check:
    """Every ``panel_ids`` chain entry should reference households that
    actually exist in ``household_roster`` — after accounting for
    ``id_walk``.

    Implementation subtlety: ``panel_ids`` stores **pre-walk** IDs
    (e.g. Niger's ``'1001'``), while ``household_roster`` returns
    **post-walk** IDs (e.g. Niger's canonical ``'101'``). This check
    walks each chain endpoint through ``updated_ids`` to get its
    canonical form, then verifies the canonical ID appears in the
    respective wave's roster. It also cross-checks that both
    endpoints of a chain entry resolve to the **same** canonical ID
    (otherwise the entry is internally inconsistent).

    Would have caught the Niger '10010' → '101' bug from 2026-04:
    the panel_ids.py script constructed current IDs that did not
    match the roster's actual ID form (extension vs no-extension),
    so 3211 of 3537 mappings pointed at non-existent households even
    after canonicalisation.
    """
    try:
        entries = _normalize_panel_ids(country)
        if not entries:
            return Check("panel_ids_targets_exist", "pass", "No panel_ids (skipped)")

        ui = country.updated_ids or {}

        spine, provenance = _panel_spine(country)
        if spine is None:
            return Check("panel_ids_targets_exist", "pass", provenance)

        ids_by_wave = {
            w: set(spine.xs(w, level="t").index.get_level_values("i").unique())
            for w in spine.index.get_level_values("t").unique()
        }

        missing_current = 0
        missing_prev = 0
        inconsistent = 0
        total = 0
        sample_missing: list[str] = []
        for (cw, ci), (pw, pi_) in entries:
            total += 1
            # Walk to canonical form (post-id_walk).
            canon_ci = ui.get(cw, {}).get(ci, ci)
            canon_pi = ui.get(pw, {}).get(pi_, pi_)

            # Cross-check: a chain entry's two endpoints should resolve
            # to the same canonical ID. If they don't, the chain is
            # internally inconsistent.
            if canon_ci != canon_pi:
                inconsistent += 1
                if len(sample_missing) < 3:
                    sample_missing.append(
                        f"inconsist ({cw},{ci}->{canon_ci}) != "
                        f"({pw},{pi_}->{canon_pi})"
                    )
                continue

            if canon_ci not in ids_by_wave.get(cw, set()):
                missing_current += 1
                if len(sample_missing) < 3:
                    sample_missing.append(f"cur ({cw},{canon_ci})")
            if canon_pi not in ids_by_wave.get(pw, set()):
                missing_prev += 1
                if len(sample_missing) < 3:
                    sample_missing.append(f"prev ({pw},{canon_pi})")

        if total == 0:
            return Check("panel_ids_targets_exist", "pass",
                         f"No entries to check ({provenance})")

        miss_rate_cur = missing_current / total
        miss_rate_prev = missing_prev / total
        inconsist_rate = inconsistent / total
        # Separate thresholds: the "cur" side should be exact because the
        # post-walk roster is the ground truth for the current wave, and
        # any inconsistency is a bug in the script. The "prev" side has
        # some tolerance for legitimate data drift (households whose
        # rosters drop a wave).
        cur_threshold = 0.02
        prev_threshold = 0.10
        inconsist_threshold = 0.02

        if (miss_rate_cur > cur_threshold
                or miss_rate_prev > prev_threshold
                or inconsist_rate > inconsist_threshold):
            return Check(
                "panel_ids_targets_exist", "fail",
                f"{missing_current}/{total} cur ({miss_rate_cur:.0%}), "
                f"{missing_prev}/{total} prev ({miss_rate_prev:.0%}), "
                f"{inconsistent}/{total} inconsistent ({inconsist_rate:.0%}) "
                f"vs {provenance}; sample: {sample_missing}"
            )
        if missing_current or missing_prev or inconsistent:
            return Check(
                "panel_ids_targets_exist", "warn",
                f"{missing_current}+{missing_prev}+{inconsistent}/{total} "
                f"entries have a missing or inconsistent endpoint "
                f"(cur≤{cur_threshold:.0%}, prev≤{prev_threshold:.0%}; {provenance})"
            )
        return Check("panel_ids_targets_exist", "pass",
                     f"All {total} chain endpoints present in {provenance}")
    except Exception as e:
        return Check("panel_ids_targets_exist", "fail", str(e))


def _check_id_walk_idempotent(country) -> Check:
    """Applying ``id_walk`` to an already-canonical spine must be a no-op.

    The framework sets ``df.attrs['id_converted'] = True`` after
    ``id_walk`` to prevent double-application. Operations like
    ``merge()`` and ``set_index()`` drop ``.attrs`` in pandas 2.x, and
    any downstream helper that touches a DataFrame without preserving
    ``.attrs`` can cause ``id_walk`` to run twice — which for countries
    with transitive ID chains produces duplicate rows.

    This check reruns ``id_walk`` manually on the spine and confirms
    the row count and index set are unchanged. It would have caught
    the Burkina Faso 2021-22 attrs bug from commit 4db41a27 (392
    duplicate tuples out of ~78,000).
    """
    try:
        from .local_tools import id_walk
        spine, provenance = _panel_spine(country)
        if spine is None:
            return Check("id_walk_idempotent", "pass", provenance)

        ui = country.updated_ids
        if not ui or not any(m for m in ui.values()):
            return Check("id_walk_idempotent", "pass",
                         f"No non-empty updated_ids (skipped; spine={provenance})")

        before_rows = len(spine)
        before_ids = set(zip(
            spine.index.get_level_values("i"),
            spine.index.get_level_values("t"),
        ))

        # Re-apply id_walk. Even though the spine already went through
        # _finalize_result once, running id_walk again on a clean copy
        # should produce the same set of (i, t) tuples.
        #
        # Clear .attrs so id_walk doesn't early-exit on the flag.
        replayed = spine.copy()
        replayed.attrs = {}
        replayed = id_walk(replayed, ui, hh_index="i")

        after_rows = len(replayed)
        after_ids = set(zip(
            replayed.index.get_level_values("i"),
            replayed.index.get_level_values("t"),
        ))

        delta_rows = after_rows - before_rows
        only_before = len(before_ids - after_ids)
        only_after = len(after_ids - before_ids)

        if delta_rows != 0 or only_before or only_after:
            return Check(
                "id_walk_idempotent", "fail",
                f"id_walk not idempotent: rows {before_rows}->{after_rows} "
                f"(delta {delta_rows:+d}), {only_before} disappeared, "
                f"{only_after} appeared ({provenance})"
            )
        return Check("id_walk_idempotent", "pass",
                     f"id_walk is idempotent on {provenance} "
                     f"({before_rows} rows)")
    except Exception as e:
        return Check("id_walk_idempotent", "fail", str(e))


def check_panel_consistency(country) -> SanityReport:
    """Run panel-specific sanity checks on a Country object.

    Runs eight checks grouped into three tiers:

    **Existence** — ``has_panel_ids``, ``has_updated_ids``.

    **Structural consistency** — ``updated_ids_cover_waves``,
    ``ids_self_consistent`` (both now understand disjoint panels by
    inferring baselines from ``panel_ids`` chain origins);
    ``panel_ids_targets_exist`` (every chain endpoint actually appears
    in ``household_roster``).

    **Runtime correctness** — ``attrition_monotonic`` (cross-wave
    overlap on ``household_roster`` per chain), ``ids_applied_consistently``
    (spine IDs are in the canonical post-``id_walk`` form),
    ``id_walk_idempotent`` (re-applying ``id_walk`` is a no-op).

    Parameters
    ----------
    country : Country
        A ``Country`` instance (e.g., ``ll.Country('Uganda')``).

    Returns
    -------
    SanityReport
    """
    report = SanityReport(country=country.name, feature="[panel]")

    # Existence checks first. Run both unconditionally.
    has_pi = _check_has_panel_ids(country)
    has_ui = _check_has_updated_ids(country)
    report.checks.append(has_pi)
    report.checks.append(has_ui)

    # If neither exists there's nothing useful left to check — return
    # early and skip the downstream checks so the report stays concise.
    if has_pi.status == "fail" and has_ui.status == "fail":
        return report

    # Structural + runtime checks. These rely on panel_ids and/or
    # updated_ids and the household_roster spine; they gracefully
    # skip when their prerequisites are missing.
    report.checks.append(_check_updated_ids_cover_waves(country))
    report.checks.append(_check_ids_are_self_consistent(country))
    report.checks.append(_check_panel_ids_targets_exist(country))
    report.checks.append(_check_panel_attrition_monotonic(country))
    report.checks.append(_check_ids_applied_consistently(country))
    report.checks.append(_check_id_walk_idempotent(country))

    return report
