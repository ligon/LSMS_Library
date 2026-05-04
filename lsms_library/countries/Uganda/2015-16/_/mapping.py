"""Wave-level formatting helpers for Uganda 2015-16.

Reconstructs ``Age`` from h2q9a/b/c via the shared ``age_handler``
glue in ``../../_/_age_helpers.py``.  Month sentinel in this wave
is the lowercase string ``'dk'``; the helper accepts that case-
insensitively.  GH #177.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

_HELPERS = Path(__file__).resolve().parent.parent.parent / "_" / "_age_helpers.py"
_spec = importlib.util.spec_from_file_location("_uganda_age_helpers", _HELPERS)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

INTERVIEW_YEAR = 2015


def Age(value):
    return _mod.age_components(value)


def household_roster(df):
    return _mod.run_household_roster(df, interview_year=INTERVIEW_YEAR)
