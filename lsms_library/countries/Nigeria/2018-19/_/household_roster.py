"""Build Nigeria 2018-19 household_roster from pp + ph rounds (GH #179).

Adopts ``age_handler`` for DOB-derived Age precision; see the
``Nigeria/_/_age_helpers.py`` module docstring for context.

Source files:
  - Post Planting: sect1_plantingw4.dta  (t=2018Q3)  — year-of-birth only (s1q7_year)
  - Post Harvest:  sect1_harvestw4.dta   (t=2019Q1)  — DOB at s1q6_* (sparse, ~25%)
"""
import importlib.util
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append('../../../_/')
from lsms_library.local_tools import df_data_grabber, to_parquet

_HELPERS = Path(__file__).resolve().parent.parent.parent / '_' / '_age_helpers.py'
_spec = importlib.util.spec_from_file_location('_nigeria_age_helpers', _HELPERS)
_age = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_age)


def extract_string(x):
    try:
        return x.split('. ')[-1].title()
    except AttributeError:
        return ''


# ---- Post planting (t=2018Q3): year-of-birth only --------------------------

idxvars = dict(
    i='hhid',
    t=('hhid', lambda x: '2018Q3'),
    v='ea',
    pid='indiv',
)

myvars = dict(
    Sex=('s1q2', lambda s: extract_string(s).title()),
    Age='s1q6',
    _dob_year='s1q7_year',
    Relationship=('s1q3', lambda s: extract_string(s).title()),
)

pp = df_data_grabber('../Data/sect1_plantingw4.dta', idxvars, **myvars)

# ---- Post harvest (t=2019Q1): DOB triplet at s1q6_* ------------------------

idxvars = dict(
    i='hhid',
    t=('hhid', lambda x: '2019Q1'),
    v='ea',
    pid='indiv',
)

myvars = dict(
    Sex=('s1q2', lambda s: extract_string(s).title()),
    Age='s1q4',
    _dob_day='s1q6_day',
    _dob_month='s1q6_month',
    _dob_year='s1q6_year',
    Relationship=('s1q3', lambda s: extract_string(s).title()),
)

ph = df_data_grabber('../Data/sect1_harvestw4.dta', idxvars, **myvars)

df = pd.concat([pp, ph])
df = df.replace('', pd.NA).sort_index().dropna(how='all')

# pp has only year; ph has full triplet.  apply_age_handler handles
# the missing day/month columns gracefully (they're absent from the pp
# rows after the concat — pd.NA in those slots — and age_handler falls
# through to the year-math fallback).
df = _age.apply_age_handler(
    df, age_col='Age',
    day_col='_dob_day', month_col='_dob_month', year_col='_dob_year',
    interview_year=2018,
)

to_parquet(df, 'household_roster.parquet')
