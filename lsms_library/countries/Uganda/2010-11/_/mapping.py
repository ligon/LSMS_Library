"""Wave-level formatting helpers for Uganda 2010-11.

Reconstructs ``Age`` from h2q9a/b/c (day/month/year of birth) when
the direct h2q8 is missing.  Sentinel handling and the per-row
``age_handler`` glue live in ``../../_/_age_helpers.py``; this file
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

_EDU_HELPERS = Path(__file__).resolve().parent.parent.parent / "_" / "_education_helpers.py"
_edu_spec = importlib.util.spec_from_file_location("_uganda_education_helpers", _EDU_HELPERS)
_edu_mod = importlib.util.module_from_spec(_edu_spec)
_edu_spec.loader.exec_module(_edu_mod)

INTERVIEW_YEAR = 2010


def Age(value):
    return _mod.age_components(value)


def household_roster(df):
    return _mod.run_household_roster(df, interview_year=INTERVIEW_YEAR)


def individual_education(df):
    # GH #171 df_edit hook: fold unlabeled float junk codes -> Unknown.
    return _edu_mod.coerce_unmapped_to_unknown(df)
