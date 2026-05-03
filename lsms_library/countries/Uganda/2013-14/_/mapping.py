"""Wave-level formatting helpers for Uganda 2013-14.

Reconstructs ``Age`` from h2q9a/b/c (day/month/year of birth) when the
direct h2q8 is missing.  All sentinel handling and the ``age_handler``
call live in the shared module ``../../_/_age_helpers.py``; this file
just supplies the wave-specific ``INTERVIEW_YEAR``.

GH #177.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

_HELPERS = Path(__file__).resolve().parent.parent.parent / "_" / "_age_helpers.py"
_spec = importlib.util.spec_from_file_location("_uganda_age_helpers", _HELPERS)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

INTERVIEW_YEAR = 2013


def Age(value):
    """Pre-process the YAML ``Age:`` list per row (sentinel cleanup)."""
    return _mod.age_components(value)


def household_roster(df):
    """Reduce list-valued ``Age`` to a scalar via age_handler."""
    return _mod.run_household_roster(df, interview_year=INTERVIEW_YEAR)
