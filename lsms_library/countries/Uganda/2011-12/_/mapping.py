"""Wave-level formatting helpers for Uganda 2011-12.

Reconstructs ``Age`` from h2q9a/b/c via the shared ``age_handler``
glue in ``../../_/_age_helpers.py``.  GH #177.
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

INTERVIEW_YEAR = 2011


def Age(value):
    return _mod.age_components(value)


def household_roster(df):
    return _mod.run_household_roster(df, interview_year=INTERVIEW_YEAR)


def individual_education(df):
    # GH #171 df_edit hook: fold unlabeled float junk codes -> Unknown.
    return _edu_mod.coerce_unmapped_to_unknown(df)
