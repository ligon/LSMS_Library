"""
Serbia and Montenegro 2003 life_satisfaction.

Source: ../Data/2003 5 non-food consumption.dta (the household-level non-food
consumption module).  Variable ``bm2`` is the same self-rated household
financial-status item used in 2002:
  {'Very bad', 'Bad', 'Neither bad nor good', 'Good', 'Very good'}
plus a non-substantive 'Dont know' option.

Maps to the canonical life_satisfaction Domain 'Finances'.  We preserve the
survey's native ordinal label as Satisfaction.  The 2003 wording of the middle
category is "Neither bad nor good"; it is normalized to the 2002 canonical
"Neither good nor bad" so the ordinal label set is consistent across waves.

Household level (one row per (mesto, rbd)); ``i`` matches the roster
(idxvars i: [mesto, rbd], formatted 'mesto-rbd' via format_id).

Output is LONG-form, index (t, i, Domain), single column Satisfaction.
"Dont know" / missing responses are dropped (non-substantive).
"""
import pandas as pd
from lsms_library.local_tools import get_dataframe, format_id, to_parquet

DOMAIN = 'Finances'

# 2003 raw label -> canonical 5-point ordinal (drop 'Dont know' / missing).
LABEL_MAP = {
    'Very bad': 'Very bad',
    'Bad': 'Bad',
    'Neither bad nor good': 'Neither good nor bad',
    'Good': 'Good',
    'Very good': 'Very good',
}

df = get_dataframe('../Data/2003 5 non-food consumption.dta')[['mesto', 'rbd', 'bm2']].copy()

df['i'] = df.apply(
    lambda r: format_id('-'.join([str(int(r['mesto'])), str(int(r['rbd']))])),
    axis=1,
)

df['Satisfaction'] = df['bm2'].astype(str).str.strip().map(LABEL_MAP)
df = df[df['Satisfaction'].notna()]

df['t'] = '2003'
df['Domain'] = DOMAIN

out = (df[['t', 'i', 'Domain', 'Satisfaction']]
       .drop_duplicates(subset=['t', 'i', 'Domain'])
       .set_index(['t', 'i', 'Domain'])
       .sort_index())

to_parquet(df=out, fn='life_satisfaction.parquet')
