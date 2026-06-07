#!/usr/bin/env python
"""Tajikistan 2009 life_satisfaction.

Source: ../Data/m8a.dta -- Module 8a "Subjective Poverty and Food Security"
(household level, one row per HHID, i matches the roster's i: HHID).

Genuine satisfaction-rating items kept (variable labels confirmed via
pyreadstat metadata):
  M8AQ9  "Overall how satisfied are you with your life?"          -> Domain 'Overall'
  M8AQ2  "How satisfied are you with your current financial       -> Domain 'Finances'
          situation?"

Deliberately EXCLUDED (per the #331 brief):
  - M8AQ1/M8AQ5 (meals/income amounts), M8AQ3/M8AQ4 (direction of change /
    expectation of financial situation -- not a satisfaction rating),
    M8AQ6/M8AQ7 (adequacy of food consumption/expenditure), M8AQ8/M8AQ10
    (level of concern / aspect of concern -- not satisfaction).
  - M8AQ11-M8AQ34_* are the HFIAS food-(in)security battery -> food-security family.
  - This module has NO subjective-poverty / Cantril ladder item here (the ladder
    lives in the 2007 m9 module as m9aq9a/b and belongs to subjective_well_being).

Native ordinal labels are preserved verbatim as the Satisfaction value (no numeric
scale invented). Non-substantive responses ("don't know", "refuse to answer") and
missing are dropped. Output is LONG-form, index (t, i, Domain).
"""
import pandas as pd
from lsms_library.local_tools import get_dataframe, format_id, to_parquet

ITEMS = {
    'M8AQ9': 'Overall',
    'M8AQ2': 'Finances',
}

NON_SUBSTANTIVE = {"don't know", 'refuse to answer'}

df = get_dataframe('../Data/m8a.dta')[['HHID'] + list(ITEMS)].copy()
df['i'] = df['HHID'].apply(format_id)

long = df.melt(id_vars='i', value_vars=list(ITEMS),
               var_name='item', value_name='Satisfaction')
long['Domain'] = long['item'].map(ITEMS)
long['Satisfaction'] = long['Satisfaction'].astype(str).str.strip()

long = long[~long['Satisfaction'].str.lower().isin(NON_SUBSTANTIVE)]
long = long[~long['Satisfaction'].isin(['nan', 'None', ''])]

long['t'] = '2009'

out = (long[['t', 'i', 'Domain', 'Satisfaction']]
       .drop_duplicates(subset=['t', 'i', 'Domain'])
       .set_index(['t', 'i', 'Domain'])
       .sort_index())

to_parquet(df=out, fn='life_satisfaction.parquet')
