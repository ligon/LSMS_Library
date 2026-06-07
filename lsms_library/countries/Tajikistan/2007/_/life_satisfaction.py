#!/usr/bin/env python
"""Tajikistan 2007 life_satisfaction.

Source: ../Data/r1m9.dta -- Module 9a (round 1), the round whose roster
(r1m1.dta) is the canonical household_roster source for the 2007 wave, so
i: hhid matches the roster's i.

The 2007 wave was fielded in multiple rounds; only round 1 (r1m9) and the
supplementary round (sm9) carry the satisfaction items, and round 2 (r2m9)
carries none.  We use r1m9 alone because it is the round aligned with the
canonical roster/sample (t='2007'); folding sm9 in under the same t would
duplicate (t, i, Domain) for panel households re-interviewed in the supplement.

Genuine satisfaction-rating items kept (variable labels confirmed via
pyreadstat metadata):
  m9aq10 "Overall how satisfied are you with your life?"          -> Domain 'Overall'
  m9aq2  "How satisfied are you with your current financial       -> Domain 'Finances'
          situation?"

Deliberately EXCLUDED (per the #331 brief):
  - m9aq9a/m9aq9b are the 6-step subjective-poverty (Cantril-style) ladder ->
    that is the separate subjective_well_being construct, not a satisfaction rating.
  - m9aq1/m9aq5 (meals/income), m9aq3/m9aq4 (direction/expectation of financial
    situation), m9aq6/m9aq7 (adequacy of food consumption/expenditure), m9aq8/m9aq11
    (concern), m9aq12*-m9aq33* (HFIAS / dietary-diversity / food-security battery),
    and the Module 9b intra-household decision-making block.

Native ordinal labels are preserved verbatim. Non-substantive responses and
missing are dropped. Output is LONG-form, index (t, i, Domain).
"""
import pandas as pd
from lsms_library.local_tools import get_dataframe, format_id, to_parquet

ITEMS = {
    'm9aq10': 'Overall',
    'm9aq2': 'Finances',
}

NON_SUBSTANTIVE = {"don't know", 'refuse to answer'}

df = get_dataframe('../Data/r1m9.dta')[['hhid'] + list(ITEMS)].copy()
df['i'] = df['hhid'].apply(format_id)

long = df.melt(id_vars='i', value_vars=list(ITEMS),
               var_name='item', value_name='Satisfaction')
long['Domain'] = long['item'].map(ITEMS)
long['Satisfaction'] = long['Satisfaction'].astype(str).str.strip()

long = long[~long['Satisfaction'].str.lower().isin(NON_SUBSTANTIVE)]
long = long[~long['Satisfaction'].isin(['nan', 'None', ''])]

long['t'] = '2007'

out = (long[['t', 'i', 'Domain', 'Satisfaction']]
       .drop_duplicates(subset=['t', 'i', 'Domain'])
       .set_index(['t', 'i', 'Domain'])
       .sort_index())

to_parquet(df=out, fn='life_satisfaction.parquet')
