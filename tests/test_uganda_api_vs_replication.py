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

Per-feature tolerances live in `FeatureSpec`:

- `atol`: max absolute numeric difference (0.0 = bit-exact).
- `extra_keys`: data columns to promote to the merge key (e.g. `Shock`
  for multi-shock HHs, where the replication keeps Shock as a column
  and the API keeps it as an index level).
- `max_na_asym`: tolerated count of rows with NaN on one side and a
  value on the other.  Non-zero values document known recoveries
  (e.g. a HH whose members' ages were all sentinel-null in the old
  pipeline and are now kept with a real HSize).
- `max_outliers`: tolerated count of rows exceeding `atol`.  Non-zero
  values document known-benign drift from quality improvements
  (e.g. nutrition rows for HHs who bought Cheese, whose fct Energy
  row in the replication was ~4x too high and was correctly dropped
  from the current API fct).

Rows with a duplicate merge key are collapsed with ``groupby(key).first()``
on both sides before merging, which canonicalizes sparse multi-acquisition
rows (food_prices / food_quantities have 63 dup `(i,t,m,j,u)` rows per
side, one per acquisition mode).  The string literal ``'<NA>'`` is
treated as a genuine null on non-numeric columns — it appears in the
replication's shock-coping data from a Stata import quirk.
"""

from __future__ import annotations

import os
import warnings
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

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
    # API uses canonical single-letter Sex ('F'/'M') after spelling
    # normalisation, producing columns like 'F 00-03'.  The replication
    # predates that normalisation and has 'Females 00-03' etc.  The last
    # bucket also differs: API '51+' vs replication '51-99'.
    # Rename API → replication form for column-pairing.
    "F 00-03": "Females 00-03",
    "F 04-08": "Females 04-08",
    "F 09-13": "Females 09-13",
    "F 14-18": "Females 14-18",
    "F 19-30": "Females 19-30",
    "F 31-50": "Females 31-50",
    "F 51+": "Females 51-99",
    "M 00-03": "Males 00-03",
    "M 04-08": "Males 04-08",
    "M 09-13": "Males 09-13",
    "M 14-18": "Males 14-18",
    "M 19-30": "Males 19-30",
    "M 31-50": "Males 31-50",
    "M 51+": "Males 51-99",
}


def _tx_household_characteristics(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns=_HH_CHARS_API_TO_REPLICATION)
    # Drop log HSize from comparison.  It's a mechanical derivative of
    # the 14 age-bracket count columns (log of their sum).  The eyeball
    # diagnostic (diagnose_hsize2.py) confirmed that 1315 HHs have
    # full rosters on the API side but only 1 member on the replication
    # side — a real coverage improvement, not a regression.  Comparing
    # the brackets directly is strictly more informative than comparing
    # the log of their sum.
    return df.drop(columns=["log HSize"], errors="ignore")


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
    # API uses canonical single-letter Sex ('M'/'F'); replication has 'MALE'/'FEMALE'.
    if "Sex" in df.columns:
        df = df.assign(Sex=df["Sex"].map({"M": "MALE", "F": "FEMALE"}).astype("string"))
    return df


def _tx_locality(df: pd.DataFrame) -> pd.DataFrame:
    # Deprecated shim: API returns column 'Parish'; replication labeled it 'v'.
    return df.rename(columns={"Parish": "v"})


@dataclass(frozen=True)
class FeatureSpec:
    """Per-feature comparison parameters; see module docstring for semantics."""
    name: str
    kwargs: dict = field(default_factory=dict)
    transform: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None
    atol: float = 0.0
    extra_keys: tuple = ()
    max_na_asym: int = 0
    max_outliers: int = 0


FEATURE_SPECS: list[FeatureSpec] = [
    FeatureSpec("earnings",          kwargs={"market": "Region"}),
    FeatureSpec("enterprise_income", kwargs={"market": "Region"}),
    FeatureSpec("fct",               atol=1e-12),
    FeatureSpec("food_acquired",     kwargs={"market": "Region"}),
    FeatureSpec("food_expenditures", kwargs={"market": "Region"},
                transform=_tx_food_expenditures),
    FeatureSpec("food_prices",       kwargs={"market": "Region"}),
    FeatureSpec("food_quantities",   kwargs={"market": "Region"}),
    # household_characteristics: with MonthsSpent filter active, the large
    # 1315-HH roster-coverage drift is resolved.  Residual outliers (~211
    # per boundary bracket, max |Δ|=2) are from age_handler's DOB-derived
    # fractional ages shifting members across bucket boundaries (e.g. a
    # 3.8-year-old bins into 04-08 on the API but 00-03 on the replication).
    # log HSize is dropped (mechanical derivative of brackets; see
    # _tx_household_characteristics).  max_na_asym=1 covers H35301-04-01
    # (2015-16) whose members all had sentinel-null ages.
    FeatureSpec("household_characteristics", kwargs={"market": "Region"},
                transform=_tx_household_characteristics, max_na_asym=1,
                max_outliers=220),
    FeatureSpec("household_roster",  transform=_tx_household_roster),
    FeatureSpec("income",            kwargs={"market": "Region"}),
    FeatureSpec("interview_date",    kwargs={"market": "Region"}),
    FeatureSpec("locality",          kwargs={"market": "Region"},
                transform=_tx_locality),
    # nutrition: two HHs bought Cheese (0.5Kg and 0.05Kg).  The replication's
    # fct row for Cheese had Energy=16400 kcal/Kg — ~4x the real value and a
    # known data error.  The current API fct correctly omits that row, so the
    # two HHs' total Energy is lower by 8200 and 820 kcal respectively.
    FeatureSpec("nutrition",         kwargs={"market": "Region"}, atol=0.1,
                max_outliers=2),
    FeatureSpec("people_last7days",  kwargs={"market": "Region"}),
    # shocks: API keeps `Shock` in the index, the replication as a column.
    # Promote it to the merge key on both sides so multi-shock HHs don't
    # cartesian-merge and so the one HH with a NaN-Shock row (3213002805,
    # 2005-06) aligns 1:1 instead of producing 6 NaN-asymmetric pairings.
    FeatureSpec("shocks",            kwargs={"market": "Region"},
                extra_keys=("Shock",)),
]


# ------------------------------------------------------------------- helpers

# String dtype roundtrip via .dta / .sav / parquet occasionally preserves the
# literal four-character string '<NA>' as a genuine value.  Treat it as null
# on non-numeric comparison — otherwise `.astype(str)` compares it against
# pandas' own display for NA.  This is a replication-side quirk in the shock-
# coping strategy columns.
_STRING_NA_LITERALS = frozenset({"<NA>"})


def _load_replication(name: str) -> pd.DataFrame:
    """Load a replication parquet; pytest.skip if missing or unreadable."""
    path = _REPL_DIR / f"{name}.parquet"
    if not path.is_file():
        pytest.skip(f"replication parquet missing: {path}")
    import pyarrow.lib as _pa_lib
    try:
        return pd.read_parquet(path, engine="pyarrow")
    except (OSError, ValueError, _pa_lib.ArrowInvalid) as exc:
        pytest.skip(f"replication parquet unreadable ({type(exc).__name__}): {exc}")


def _call_api(name: str, kwargs: dict) -> pd.DataFrame:
    """Invoke Country('Uganda').<name>(**kwargs) with warnings silenced."""
    import lsms_library as ll
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        method = getattr(ll.Country("Uganda"), name)
        return method(**kwargs)


def _merge_on_common_index(
    api: pd.DataFrame,
    repl: pd.DataFrame,
    extra_keys: tuple = (),
) -> tuple[pd.DataFrame, list[str]]:
    """
    Inner-merge on the intersection of index level names plus ``extra_keys``.

    Before merging, both sides are collapsed with ``groupby(key).first()`` to
    handle the case where a single nominal key has multiple rows whose values
    occupy complementary columns (e.g. food_prices rows where one holds
    ``quantity_home`` and the sibling holds ``quantity_inkind``).  Without this
    dedupe, the merge cartesian-explodes such groups and manufactures spurious
    NaN asymmetries.

    Returns (merged_frame, full_merge_key).
    """
    common_levels = [lev for lev in repl.index.names if lev in api.index.names]
    if not common_levels:
        raise ValueError(
            f"no common index levels between API {list(api.index.names)} "
            f"and replication {list(repl.index.names)}"
        )

    api_flat = api.reset_index()
    repl_flat = repl.reset_index()

    merge_key = list(common_levels) + [k for k in extra_keys if k not in common_levels]
    missing_api = [k for k in merge_key if k not in api_flat.columns]
    missing_repl = [k for k in merge_key if k not in repl_flat.columns]
    assert not missing_api, f"extra_keys missing on API side: {missing_api}"
    assert not missing_repl, f"extra_keys missing on replication side: {missing_repl}"

    # Collapse duplicate merge-key groups: groupby.first() skips NaN per column.
    api_ded = api_flat.groupby(merge_key, dropna=False, sort=False,
                               as_index=False).first()
    repl_ded = repl_flat.groupby(merge_key, dropna=False, sort=False,
                                 as_index=False).first()

    merged = api_ded.merge(repl_ded, on=merge_key, suffixes=("_api", "_repl"))
    return merged, merge_key


def _coerce_string_na(s: pd.Series) -> pd.Series:
    """Replace known literal-string NA sentinels with real nulls on string data."""
    if s.dtype == object or pd.api.types.is_string_dtype(s):
        return s.mask(s.isin(_STRING_NA_LITERALS))
    return s


def _compare_column(
    col: str,
    api_series: pd.Series,
    repl_series: pd.Series,
    atol: float,
    max_na_asym: int = 0,
    max_outliers: int = 0,
) -> None:
    """Raise AssertionError on disagreement, with an informative message."""
    api_series = _coerce_string_na(api_series)
    repl_series = _coerce_string_na(repl_series)

    api_na = api_series.isna()
    repl_na = repl_series.isna()
    both_na = api_na & repl_na
    only_one_na = api_na ^ repl_na
    n_asym = int(only_one_na.sum())
    if n_asym > max_na_asym:
        raise AssertionError(
            f"column {col!r}: {n_asym} rows have NaN in one side but not the "
            f"other (max_na_asym={max_na_asym})"
        )

    mask = ~(both_na | only_one_na)
    if not mask.any():
        return
    a = api_series[mask]
    r = repl_series[mask]
    if pd.api.types.is_numeric_dtype(a) and pd.api.types.is_numeric_dtype(r):
        diff = (a.astype(float) - r.astype(float)).abs()
        n_bad = int((diff > atol).sum())
        if n_bad > max_outliers:
            raise AssertionError(
                f"column {col!r}: max |Δ| = {diff.max():.6g} > atol {atol:.6g}; "
                f"mean |Δ| = {diff.mean():.6g}; "
                f"{n_bad}/{len(diff)} rows out of tolerance "
                f"(max_outliers={max_outliers})"
            )
    else:
        a_str = a.astype(str)
        r_str = r.astype(str)
        n_bad = int((a_str != r_str).sum())
        if n_bad > max_outliers:
            raise AssertionError(
                f"column {col!r}: {n_bad}/{len(a)} rows differ (non-numeric; "
                f"max_outliers={max_outliers})"
            )


# ------------------------------------------------------------------- the test

@pytest.mark.parametrize(
    "spec",
    FEATURE_SPECS,
    ids=[spec.name for spec in FEATURE_SPECS],
)
def test_api_matches_replication(spec: FeatureSpec) -> None:
    """API output must agree with the canonical replication parquet on common rows."""
    repl = _load_replication(spec.name)

    try:
        api = _call_api(spec.name, spec.kwargs)
    except Exception as exc:  # broad catch intentional: any API failure is a test failure
        pytest.fail(f"API call Country('Uganda').{spec.name}(**{spec.kwargs}) raised: "
                    f"{type(exc).__name__}: {exc}")

    if spec.transform is not None:
        api = spec.transform(api)

    merged, merge_key = _merge_on_common_index(api, repl, extra_keys=spec.extra_keys)

    assert len(merged) > 0, (
        f"{spec.name}: no overlap between API and replication on {merge_key}; "
        f"API shape={api.shape} index={list(api.index.names)}; "
        f"repl shape={repl.shape} index={list(repl.index.names)}"
    )

    suffixed_api = [c for c in merged.columns if c.endswith("_api")]
    pairs = []
    for c in suffixed_api:
        base = c[:-4]
        if f"{base}_repl" in merged.columns:
            pairs.append(base)

    assert pairs, (
        f"{spec.name}: no shared data columns after merge; "
        f"API cols after transform={list(api.columns)}; "
        f"repl cols={list(repl.columns)}"
    )

    for base in pairs:
        _compare_column(
            base, merged[f"{base}_api"], merged[f"{base}_repl"],
            atol=spec.atol,
            max_na_asym=spec.max_na_asym,
            max_outliers=spec.max_outliers,
        )
