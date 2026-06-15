#!/usr/bin/env python
"""Individual educational attainment for Tanzania 2008-15 (rounds 1-4).

Source file: upd4_hh_c.dta (multi-round education module).
Variables:
    r_hhid  household id (matches household_roster i)
    UPI     individual panel id (matches household_roster pid)
    hc_04   highest level of education attained (numeric code)

hc_04 is missing (~33%) for members who never attended school
(hc_03 == 'NO'); those rows carry no attainment value and are dropped
so the canonical column has no NaN.  Raw per-wave codes are preserved
(no cross-country harmonization).  Cluster identity (v) is joined from
sample() at API time, not baked into this parquet.
"""
from lsms_library.local_tools import get_dataframe, to_parquet
import pandas as pd

round_match = {1: '2008-09', 2: '2010-11', 3: '2012-13', 4: '2014-15'}

df = get_dataframe('../Data/upd4_hh_c.dta')

edu = pd.DataFrame({
    'i': df['r_hhid'].astype(str),
    't': df['round'].map(round_match),
    'pid': df['UPI'].astype(float).astype(int).astype(str),
    'Educational Attainment': df['hc_04'],
})

# Preserve raw codes as clean strings ("9.0" -> "9"); drop members with
# no attainment recorded (never attended school) so the column is NaN-free.
edu['Educational Attainment'] = (
    edu['Educational Attainment']
    .astype(float)
    .astype('Int64')
    .astype(str)
    .replace('<NA>', pd.NA)
)
edu = edu.dropna(subset=['Educational Attainment'])

edu = edu.set_index(['t', 'i', 'pid'])

if not edu.index.is_unique:
    edu = edu.groupby(level=edu.index.names).first()

to_parquet(edu, 'individual_education.parquet')
