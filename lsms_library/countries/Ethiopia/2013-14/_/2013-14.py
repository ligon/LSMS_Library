"""Wave-level formatting helpers for Ethiopia 2013-14.

Falls back to ``age_in_months // 12`` when ``hh_s1q04_a`` (years) is
missing.  All cleanup logic lives in ``../../_/_age_helpers.py``.

GH #178.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

_HELPERS = Path(__file__).resolve().parent.parent.parent / "_" / "_age_helpers.py"
_spec = importlib.util.spec_from_file_location("_ethiopia_age_helpers", _HELPERS)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)


def Age(value):
    return _mod.age_components(value)


def household_roster(df):
    return _mod.run_household_roster(df)
