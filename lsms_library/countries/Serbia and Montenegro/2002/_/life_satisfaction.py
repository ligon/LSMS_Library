"""
Serbia and Montenegro 2002 life_satisfaction.

Source: ../Data/2002 5 non-food consumption.dta (the household-level non-food
consumption module).  Variable ``bm2`` carries the variable label
"How would you describe current financial status of your household?" -- a
self-rated 5-point ordinal:
  {1: 'Very bad', 2: 'Bad', 3: 'Neither good nor bad', 4: 'Good', 5: 'Very good'}
plus a non-substantive {6: 'Doesn t know'} option.

This is the subjective financial-position rating, so it maps to the canonical
life_satisfaction Domain 'Finances'.  We preserve the survey's native ordinal
label as the Satisfaction value (do NOT invent a numeric scale).

The file is already HOUSEHOLD level: one row per (mesto, rbd), no duplicates.
The household id ``i`` matches the roster's ``i`` (idxvars i: [mesto, rbd],
formatted as 'mesto-rbd' via format_id) -- see ../_/mapping.py and
household_roster in data_info.yml.

Output is LONG-form, index (t, i, Domain), single column Satisfaction.
"Doesn t know" / missing responses are dropped (non-substantive).
"""
import pandas as pd
from lsms_library.local_tools import get_dataframe, format_id, to_parquet

DOMAIN = 'Finances'

# Native 5-point ordinal labels we keep (drop 'Doesn t know' and missing).
SUBSTANTIVE = {'Very bad', 'Bad', 'Neither good nor bad', 'Good', 'Very good'}

df = get_dataframe('../Data/2002 5 non-food consumption.dta')[['mesto', 'rbd', 'bm2']].copy()

# Build i to match the roster: 'mesto-rbd' via format_id.
df['i'] = df.apply(
    lambda r: format_id('-'.join([str(int(r['mesto'])), str(int(r['rbd']))])),
    axis=1,
)

df['Satisfaction'] = df['bm2'].astype(str).str.strip()
df = df[df['Satisfaction'].isin(SUBSTANTIVE)]

df['t'] = '2002'
df['Domain'] = DOMAIN

out = (df[['t', 'i', 'Domain', 'Satisfaction']]
       .drop_duplicates(subset=['t', 'i', 'Domain'])
       .set_index(['t', 'i', 'Domain'])
       .sort_index())

to_parquet(df=out, fn='life_satisfaction.parquet')
