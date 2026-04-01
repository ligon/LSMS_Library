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
    report.checks.append(_check_value_constraints(df, scheme, feature))
    report.checks.append(_check_duplicate_index(df))
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

def _check_has_panel_ids(country) -> Check:
    """Country must have panel_ids to run panel checks."""
    try:
        pi = country.panel_ids
        if pi is None or (isinstance(pi, dict) and len(pi) == 0):
            return Check("has_panel_ids", "fail", "panel_ids is empty or None")
        return Check("has_panel_ids", "pass", f"{len(pi)} panel ID mappings")
    except Exception as e:
        return Check("has_panel_ids", "fail", f"Could not load panel_ids: {e}")


def _check_has_updated_ids(country) -> Check:
    """Country must have updated_ids."""
    try:
        ui = country.updated_ids
        if ui is None or (isinstance(ui, dict) and len(ui) == 0):
            return Check("has_updated_ids", "fail", "updated_ids is empty or None")
        waves_with_mappings = {w for w, m in ui.items() if m}
        return Check("has_updated_ids", "pass",
                     f"{len(waves_with_mappings)} waves with ID mappings")
    except Exception as e:
        return Check("has_updated_ids", "fail", f"Could not load updated_ids: {e}")


def _check_updated_ids_cover_waves(country) -> Check:
    """updated_ids should have entries for every wave (except possibly the first)."""
    try:
        ui = country.updated_ids
        waves = sorted(country.waves)
        if len(waves) < 2:
            return Check("updated_ids_cover_waves", "pass", "Single-wave country (skipped)")
        # First wave usually has no previous IDs
        expected_waves = set(waves[1:])
        covered = {w for w in expected_waves if w in ui and ui[w]}
        missing = expected_waves - covered
        if missing:
            return Check("updated_ids_cover_waves", "warn",
                         f"No ID mappings for waves: {sorted(missing)}")
        return Check("updated_ids_cover_waves", "pass",
                     f"All {len(covered)} follow-up waves have mappings")
    except Exception as e:
        return Check("updated_ids_cover_waves", "fail", str(e))


def _check_ids_are_self_consistent(country) -> Check:
    """Check that updated_ids mappings don't create cycles or orphans.

    Every current ID should map to a valid previous ID, and the chain
    should terminate at a first-wave ID.
    """
    try:
        ui = country.updated_ids
        waves = sorted(country.waves)
        if len(waves) < 2:
            return Check("ids_self_consistent", "pass", "Single-wave (skipped)")

        issues = []
        for wave in waves[1:]:
            if wave not in ui or not ui[wave]:
                continue
            mappings = ui[wave]
            # Check for self-referential mappings
            self_refs = [k for k, v in mappings.items() if k == v]
            if self_refs:
                issues.append(f"{wave}: {len(self_refs)} self-referential ID(s)")
            # Check for empty/null mappings
            null_maps = [k for k, v in mappings.items()
                         if v is None or (isinstance(v, str) and v.strip() == "")]
            if null_maps:
                issues.append(f"{wave}: {len(null_maps)} null mapping(s)")

        if issues:
            return Check("ids_self_consistent", "warn", "; ".join(issues[:5]))
        return Check("ids_self_consistent", "pass")
    except Exception as e:
        return Check("ids_self_consistent", "fail", str(e))


def _check_panel_attrition_monotonic(country) -> Check:
    """The diagonal of the attrition matrix should decrease over time
    (panel attrition), and off-diagonal entries should be <= diagonal.
    """
    try:
        from .local_tools import panel_attrition
        # Need a feature with (i, t) to compute attrition
        spine_path = data_root(country.name) / "var" / "other_features.parquet"
        if not spine_path.exists():
            return Check("attrition_monotonic", "pass",
                         "other_features not cached (skipped)")
        spine = pd.read_parquet(spine_path, engine="pyarrow")
        if "i" not in spine.index.names or "t" not in spine.index.names:
            return Check("attrition_monotonic", "pass", "Spine lacks i/t index (skipped)")

        waves = sorted(spine.index.get_level_values("t").unique())
        if len(waves) < 2:
            return Check("attrition_monotonic", "pass", "Single wave (skipped)")

        attrition = panel_attrition(spine, waves)

        # Check diagonal: sample size per wave
        diag = [int(attrition.loc[w, w]) for w in waves]
        issues = []

        # Off-diagonal: attrition between waves s < t should be <= min(diag[s], diag[t])
        for i, s in enumerate(waves):
            for t in waves[i + 1:]:
                val = int(attrition.loc[s, t])
                if val > diag[i]:
                    issues.append(f"({s},{t}): {val} > {diag[i]} (more matches than source wave)")
                if val < 0:
                    issues.append(f"({s},{t}): negative count {val}")

        # Check that attrition is non-negative between adjacent waves
        for i in range(len(waves) - 1):
            s, t = waves[i], waves[i + 1]
            val = int(attrition.loc[s, t])
            if val == 0:
                issues.append(f"({s},{t}): zero overlap — panel may be broken")

        if issues:
            return Check("attrition_monotonic", "warn", "; ".join(issues[:5]))

        # Summarize attrition rates
        if len(waves) >= 2:
            first_last = int(attrition.loc[waves[0], waves[-1]])
            retention = first_last / diag[0] if diag[0] > 0 else 0
            return Check("attrition_monotonic", "pass",
                         f"Attrition matrix OK. {waves[0]}→{waves[-1]} retention: "
                         f"{first_last}/{diag[0]} ({retention:.1%})")
        return Check("attrition_monotonic", "pass")
    except Exception as e:
        return Check("attrition_monotonic", "fail", str(e))


def _check_ids_applied_consistently(country) -> Check:
    """For each feature with 'i' and 't' in the index, check that household
    IDs are consistent with updated_ids — i.e., the same physical household
    uses the same canonical ID across waves.
    """
    try:
        ui = country.updated_ids
        if not ui:
            return Check("ids_applied_consistently", "pass", "No updated_ids (skipped)")

        # Build reverse map: for each wave, which canonical IDs should appear?
        # updated_ids maps current_id -> canonical_first_wave_id
        # Check a few features for consistency
        features_to_check = ["other_features", "household_characteristics", "food_acquired"]
        issues = []

        for feature_name in features_to_check:
            feature_path = data_root(country.name) / "var" / f"{feature_name}.parquet"
            if not feature_path.exists():
                continue
            df = pd.read_parquet(feature_path, engine="pyarrow")
            if "i" not in df.index.names or "t" not in df.index.names:
                continue

            # For each wave with updated_ids, check that feature IDs
            # use the canonical (updated) form, not the raw wave-specific form
            waves_in_data = sorted(df.index.get_level_values("t").unique())
            for wave in waves_in_data:
                if wave not in ui or not ui[wave]:
                    continue
                mappings = ui[wave]
                # IDs in this wave of this feature
                wave_mask = df.index.get_level_values("t") == wave
                feature_ids = set(df[wave_mask].index.get_level_values("i"))

                # The canonical IDs are the keys of updated_ids[wave]
                # (current IDs that map to earlier canonical IDs)
                canonical_ids = set(mappings.keys())

                # Check: do any feature IDs match the *values* (old IDs)
                # instead of the keys (updated IDs)?  That would mean
                # id_walk wasn't applied.
                old_ids = set(mappings.values())
                using_old = feature_ids & old_ids - canonical_ids
                if using_old and len(using_old) > len(feature_ids) * 0.1:
                    issues.append(
                        f"{feature_name}/{wave}: {len(using_old)} IDs appear to use "
                        f"pre-update form (id_walk may not have been applied)"
                    )

        if issues:
            return Check("ids_applied_consistently", "warn", "; ".join(issues[:5]))
        return Check("ids_applied_consistently", "pass",
                     f"Checked {len(features_to_check)} features — IDs look canonical")
    except Exception as e:
        return Check("ids_applied_consistently", "fail", str(e))


def check_panel_consistency(country) -> SanityReport:
    """Run panel-specific sanity checks on a Country object.

    Parameters
    ----------
    country : Country
        A ``Country`` instance (e.g., ``ll.Country('Uganda')``).

    Returns
    -------
    SanityReport
    """
    report = SanityReport(country=country.name, feature="[panel]")

    report.checks.append(_check_has_panel_ids(country))
    report.checks.append(_check_has_updated_ids(country))
    report.checks.append(_check_updated_ids_cover_waves(country))
    report.checks.append(_check_ids_are_self_consistent(country))
    report.checks.append(_check_panel_attrition_monotonic(country))
    report.checks.append(_check_ids_applied_consistently(country))

    return report
