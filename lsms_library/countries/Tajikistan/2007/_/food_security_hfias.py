#!/usr/bin/env python
"""Tajikistan 2007 food_security_hfias (HFIAS 9-item Household Food Insecurity
Access Scale).

Source: ../Data/r1m9.dta -- Module 9a (round 1), the round whose roster
(r1m1.dta) is the canonical household_roster source for the 2007 wave, so
i: hhid matches the roster's i.  r1m9 has exactly one row per hhid (4644 HH),
covering every household in the round-1 roster.

The 9 standard HFIAS occurrence questions, each paired with a frequency-of-
occurrence follow-up.  Variable labels confirmed via pyreadstat metadata; value
labels are: occurrence {1: yes, 2: no}; frequency {1: rarely (1-2 times),
2: sometimes (3-10 times), 3: often (more than 10 times)}.  Recall: past 4 weeks.

  HFIAS item                          occurrence  frequency
  1 worried not enough food           m9aq14      m9aq15
  2 unable to eat preferred foods     m9aq17      m9aq18
  3 ate limited variety of foods      m9aq19      m9aq20
  4 ate foods did not want to eat     m9aq21      m9aq22
  5 ate a smaller meal than needed    m9aq23      m9aq24
  6 ate fewer meals in a day          m9aq25      m9aq26
  7 no food of any kind in house      m9aq27      m9aq28
  8 went to sleep hungry              m9aq29      m9aq30
  9 went a whole day & night w/o food m9aq31      m9aq32

Each item is coded 0-3: 0 if the occurrence answer is no; otherwise the
frequency code (1=Rarely, 2=Sometimes, 3=Often).  HFIAS_score is the sum of the
9 items (0-27).  HFIAS_category follows the FANTA (Coates, Swindale & Bilinsky
2007) algorithm.  Output index (t, i).
"""
import pandas as pd
from lsms_library.local_tools import get_dataframe, format_id, to_parquet

ID = 'hhid'

# (occurrence column, frequency column) for HFIAS items 1..9, in order.
PAIRS = [
    ('m9aq14', 'm9aq15'),
    ('m9aq17', 'm9aq18'),
    ('m9aq19', 'm9aq20'),
    ('m9aq21', 'm9aq22'),
    ('m9aq23', 'm9aq24'),
    ('m9aq25', 'm9aq26'),
    ('m9aq27', 'm9aq28'),
    ('m9aq29', 'm9aq30'),
    ('m9aq31', 'm9aq32'),
]

YES = {'yes'}
NO = {'no'}
FREQ = {
    'rarely (1-2 times)': 1,
    'sometimes (3-10 times)': 2,
    'often (more than 10 times)': 3,
}


def _item_score(occ, freq):
    """HFIAS item score 0-3 from an occurrence/frequency label pair."""
    o = str(occ).strip().lower()
    if o in NO:
        return 0
    if o in YES:
        f = str(freq).strip().lower()
        return FREQ.get(f, pd.NA)
    return pd.NA


def hfias_category(row):
    """FANTA HFIAS prevalence category from items a1..a9 (each 0-3)."""
    a = [row[f'HFIAS{k}'] for k in range(1, 10)]
    if any(pd.isna(v) for v in a):
        return pd.NA
    a1, a2, a3, a4, a5, a6, a7, a8, a9 = a
    if a5 == 3 or a6 == 3 or a7 in (1, 2, 3) or a8 in (1, 2, 3) or a9 in (1, 2, 3):
        return 'Severely food insecure'
    if a3 in (2, 3) or a4 in (2, 3) or a5 in (1, 2) or a6 in (1, 2):
        return 'Moderately food insecure'
    if a1 in (2, 3) or a2 in (1, 2, 3) or a3 == 1 or a4 == 1:
        return 'Mildly food insecure'
    return 'Food secure'


cols = [ID] + [c for pair in PAIRS for c in pair]
df = get_dataframe('../Data/r1m9.dta')[cols].copy()
df['i'] = df[ID].apply(format_id)

out = pd.DataFrame({'i': df['i']})
for k, (occ, freq) in enumerate(PAIRS, start=1):
    out[f'HFIAS{k}'] = [_item_score(o, f) for o, f in zip(df[occ], df[freq])]

item_cols = [f'HFIAS{k}' for k in range(1, 10)]
present = out[item_cols].notna().all(axis=1)
out['HFIAS_score'] = pd.NA
out.loc[present, 'HFIAS_score'] = out.loc[present, item_cols].sum(axis=1)
out['HFIAS_category'] = out.apply(hfias_category, axis=1)

out = out[out[item_cols].notna().any(axis=1)].copy()

for c in item_cols + ['HFIAS_score']:
    out[c] = out[c].astype('Int64')

out['t'] = '2007'
out = (out[['t', 'i'] + item_cols + ['HFIAS_score', 'HFIAS_category']]
       .drop_duplicates(subset=['t', 'i'])
       .set_index(['t', 'i'])
       .sort_index())

to_parquet(df=out, fn='food_security_hfias.parquet')
