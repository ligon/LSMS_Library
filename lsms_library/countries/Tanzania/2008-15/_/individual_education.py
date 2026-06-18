#!/usr/bin/env python
"""Individual educational attainment for Tanzania 2008-15 (rounds 1-4).

Source file: upd4_hh_c.dta (multi-round education module).
Variables:
    r_hhid  household id (matches household_roster i)
    UPI     individual panel id (matches household_roster pid)
    round   1..4 -> waves 2008-09, 2010-11, 2012-13, 2014-15
    hc_07   "What is the highest grade completed by [NAME]?" -- a
            value-labelled text code (PP, ADULT, D1..D8, F1..F6,
            'O'+COURSE, DIPLOMA, U1..U5&+, ...).

This is a multi-round file: one wave-level parquet carries all four NPS
rounds with the logical wave in the ``t`` index level (Wave.grab_data()
filters to the requested year).  We emit the raw text label; the canonical
ordinal mapping onto ``Educational Attainment`` happens in the
country-level aggregator ``_/individual_education.py`` via the
``harmonize_education`` table in ``_/categorical_mapping.org`` (GH #171).

hc_07 is missing for members who never attended school (hc_03 == 'NO');
those rows carry no attainment value and are dropped so the canonical
column has no NaN.  Cluster identity (v) is joined from sample() at API
time, not baked into this parquet.
"""
import sys
sys.path.append('../../_')
from lsms_library.local_tools import get_dataframe, to_parquet
from tanzania import harmonize_education_labels
import pandas as pd

round_match = {1: '2008-09', 2: '2010-11', 3: '2012-13', 4: '2014-15'}

df = get_dataframe('../Data/upd4_hh_c.dta')

edu = pd.DataFrame({
    'i': df['r_hhid'].astype(str),
    't': df['round'].map(round_match),
    'pid': df['UPI'].astype(float).astype(int).astype(str),
    'Educational Attainment': df['hc_07'].astype(str).str.strip(),
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
