#!/usr/bin/env python
"""Individual educational attainment for Tanzania 2019-20.

Source file: HH_SEC_C.dta (education module).
Variables:
    sdd_hhid   household id (matches household_roster i)
    sdd_indid  individual id (matches household_roster pid)
    hh_c04     highest level of education attained (numeric code)

hh_c04 is missing (~29%) for members who never attended school; those
rows carry no attainment value and are dropped so the canonical column
has no NaN.  Raw per-wave codes are preserved (no cross-country
harmonization).  i/pid are formatted with format_id to match the
YAML-path household_roster.  v is joined from sample() at API time.
"""
from lsms_library.local_tools import get_dataframe, to_parquet, format_id
import pandas as pd

t = '2019-20'

df = get_dataframe('../Data/HH_SEC_C.dta')

edu = pd.DataFrame({
    't': t,
    'i': df['sdd_hhid'].apply(format_id),
    'pid': df['sdd_indid'].apply(format_id),
    'Educational Attainment': df['hh_c04'],
})

# Preserve raw codes as clean strings ("9.0" -> "9"); drop members with
# no attainment recorded so the column is NaN-free.
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
