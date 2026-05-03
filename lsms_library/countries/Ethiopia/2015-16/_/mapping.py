"""Wave-level formatting helpers for Ethiopia 2015-16 (GH #178).

Falls back to ``age_in_months // 12`` when ``hh_s1q04a`` (years) is
missing.  See ``data_info.yml`` for why the ``hh_s1q04g_*`` DOB
triplet is *not* used: the year is Ethiopian-calendar (e.g. 1999)
while the month is English Gregorian ('November', 'February') --
mixed-calendar entry the survey codebook does not disambiguate.

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
