"""Build Nigeria 2023-24 household_roster from pp + ph rounds (GH #179).

This is the wave-5 GHS-Panel.  The wave folder existed in the repo
with raw .dta files but no roster script -- this script wires it up
following the pp/ph skill (.claude/skills/add-feature/pp-ph/SKILL.md).

Adopts ``age_handler`` for DOB-derived Age precision; see the
``Nigeria/_/_age_helpers.py`` module docstring for context.

Source files:
  - Post Planting Wave 5/Household/sect1_plantingw5.dta  (t=2023Q3)
  - Post Harvest  Wave 5/Household/sect1_harvestw5.dta   (t=2024Q1)

DOB layout (different from earlier waves -- s1q10 is month, s1q11
is "calculated year of birth"):
  - s1q6: Age in completed years
  - s1q10: Month of birth ('9. SEP' / '10. OCTOBER' Stata categorical)
  - s1q11: Year of birth (numeric, calculated from age + interview)

Day-of-birth is not collected in 2023-24, so day_col is None.
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
    """Strip Stata 'n. NAME' categorical prefix and titlecase."""
    try:
        return x.split('. ')[-1].title()
    except AttributeError:
        return ''


# ---- Post planting (t=2023Q3) ----------------------------------------------

idxvars = dict(
    i='hhid',
    t=('hhid', lambda x: '2023Q3'),
    v='ea',
    pid='indiv',
)

myvars = dict(
    Sex=('s1q2', lambda s: extract_string(s).title()),
    Age='s1q6',
    _dob_month='s1q10',
    _dob_year='s1q11',
    Relationship=('s1q3', lambda s: extract_string(s).title()),
)

pp = df_data_grabber(
    '../Data/Post Planting Wave 5/Household/sect1_plantingw5.dta',
    idxvars, **myvars,
)

# ---- Post harvest (t=2024Q1) -----------------------------------------------

idxvars = dict(
    i='hhid',
    t=('hhid', lambda x: '2024Q1'),
    v='ea',
    pid='indiv',
)

myvars = dict(
    Sex=('s1q2', lambda s: extract_string(s).title()),
    Age='s1q6',
    _dob_month='s1q10',
    _dob_year='s1q11',
    Relationship=('s1q3', lambda s: extract_string(s).title()),
)

ph = df_data_grabber(
    '../Data/Post Harvest Wave 5/Household/sect1_harvestw5.dta',
    idxvars, **myvars,
)

df = pd.concat([pp, ph])

# Drop rows for individuals not in household any longer.
df = df.replace('', pd.NA).sort_index().dropna(how='all')

# DOB triplet -> Age via age_handler (no day column in this wave).
df = _age.apply_age_handler(
    df, age_col='Age',
    day_col=None, month_col='_dob_month', year_col='_dob_year',
    interview_year=2023,
)

to_parquet(df, 'household_roster.parquet')
