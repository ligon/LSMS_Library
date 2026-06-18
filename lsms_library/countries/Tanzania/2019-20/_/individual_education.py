#!/usr/bin/env python
"""Individual educational attainment for Tanzania 2019-20.

Source file: HH_SEC_C.dta (education module).
Variables:
    sdd_hhid   household id (matches household_roster i)
    sdd_indid  individual id (matches household_roster pid)
    hh_c07     "What is the highest grade completed by [NAME]?" -- a
               value-labelled text code (PP, ADULT, D1..D8, F1..F6,
               'O'+COURSE, DIPLOMA, U1..U5&+, ...).

We emit the raw text label; the canonical ordinal mapping onto
``Educational Attainment`` happens in the country-level aggregator
``_/individual_education.py`` via the ``harmonize_education`` table in
``_/categorical_mapping.org`` (GH #171).

hh_c07 is missing for members who never attended school (hh_c03 == 'NO');
those rows carry no attainment value and are dropped so the canonical
column has no NaN.  i/pid are formatted with format_id to match the
YAML-path household_roster.  v is joined from sample() at API time.
"""
import sys
sys.path.append('../../_')
from lsms_library.local_tools import get_dataframe, to_parquet, format_id
from tanzania import harmonize_education_labels
import pandas as pd

t = '2019-20'

df = get_dataframe('../Data/HH_SEC_C.dta')

edu = pd.DataFrame({
    't': t,
    'i': df['sdd_hhid'].apply(format_id),
    'pid': df['sdd_indid'].apply(format_id),
    'Educational Attainment': df['hh_c07'].astype(str).str.strip(),
})

# Drop members with no attainment recorded so the column is NaN-free, then
# map the raw NPS grade codes onto the canonical ordinal vocabulary.
edu['Educational Attainment'] = edu['Educational Attainment'].replace(
    {'nan': pd.NA, 'NaN': pd.NA, '': pd.NA, '<NA>': pd.NA})
edu = edu.dropna(subset=['Educational Attainment'])
edu['Educational Attainment'] = harmonize_education_labels(edu['Educational Attainment'])

edu = edu.set_index(['t', 'i', 'pid'])

if not edu.index.is_unique:
    edu = edu.groupby(level=edu.index.names).first()

to_parquet(edu, 'individual_education.parquet')
