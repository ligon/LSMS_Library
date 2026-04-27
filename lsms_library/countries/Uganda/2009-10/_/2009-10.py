"""Wave-level formatting helpers for Uganda 2009-10.

* ``District`` — coerces float-stringified district code to int-string.
* ``Age`` / ``household_roster`` — adopt ``age_handler`` to reconstruct
  Age from h2q9a/b/c when h2q8 is missing (GH #177).  Sentinel cleanup
  and the per-row glue live in ``../../_/_age_helpers.py``.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd

_HELPERS = Path(__file__).resolve().parent.parent.parent / "_" / "_age_helpers.py"
_spec = importlib.util.spec_from_file_location("_uganda_age_helpers", _HELPERS)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

INTERVIEW_YEAR = 2009


def District(x):
    """Coerce numeric District code (float-stringified from Stata) to int-string.

    h1aq1 in GSEC1.dta is stored as a numeric code (e.g. 101.0, 102.0).
    Without explicit coercion, df_data_grabber leaves myvars as float and the
    result stringifies to '101.0'.  This function strips the .0 suffix.
    """
    if pd.isna(x):
        return pd.NA
    try:
        return str(int(float(x)))
    except (ValueError, TypeError):
        return str(x)


def Age(value):
    """Pre-process the YAML ``Age:`` list per row (sentinel cleanup)."""
    return _mod.age_components(value)


def household_roster(df):
    """Reduce list-valued ``Age`` to a scalar via age_handler."""
    return _mod.run_household_roster(df, interview_year=INTERVIEW_YEAR)
