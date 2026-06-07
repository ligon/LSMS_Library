#!/usr/bin/env python
"""Tajikistan 2009 food_security_hfias (HFIAS 9-item Household Food Insecurity
Access Scale).

Source: ../Data/m8a.dta -- Module 8a "Subjective Poverty and Food Security"
(household level, one row per HHID; i matches the roster's i: HHID).

The 9 standard HFIAS occurrence questions, each paired with a frequency-of-
occurrence follow-up.  Variable labels confirmed via pyreadstat metadata; value
labels are: occurrence {1: YES, 2: NO}; frequency {1: RARELY (1-2 times),
2: SOMETIMES (3-10 times), 3: OFTEN (more than 10 times)}.  Recall: past 4 weeks.

  HFIAS item                          occurrence  frequency
  1 worried not enough food           M8AQ11      M8AQ12
  2 unable to eat preferred foods     M8AQ14      M8AQ15
  3 ate limited variety of foods      M8AQ16      M8AQ17
  4 ate foods did not want to eat     M8AQ18      M8AQ19
  5 ate a smaller meal than needed    M8AQ20      M8AQ21
  6 ate fewer meals in a day          M8AQ22      M8AQ23
  7 no food of any kind in house      M8AQ24      M8AQ25
  8 went to sleep hungry              M8AQ26      M8AQ27
  9 went a whole day & night w/o food M8AQ28      M8AQ29

Each item is coded 0-3: 0 if the occurrence answer is NO; otherwise the
frequency code (1=Rarely, 2=Sometimes, 3=Often).  HFIAS_score is the sum of the
9 items (0-27).  HFIAS_category follows the FANTA (Coates, Swindale & Bilinsky
2007) algorithm.  Output index (t, i).
"""
import pandas as pd
from lsms_library.local_tools import get_dataframe, format_id, to_parquet

ID = 'HHID'

# (occurrence column, frequency column) for HFIAS items 1..9, in order.
PAIRS = [
    ('M8AQ11', 'M8AQ12'),
    ('M8AQ14', 'M8AQ15'),
    ('M8AQ16', 'M8AQ17'),
    ('M8AQ18', 'M8AQ19'),
    ('M8AQ20', 'M8AQ21'),
    ('M8AQ22', 'M8AQ23'),
    ('M8AQ24', 'M8AQ25'),
    ('M8AQ26', 'M8AQ27'),
    ('M8AQ28', 'M8AQ29'),
]

# Occurrence labels mean "did this happen": YES -> use frequency; NO -> 0.
YES = {'yes'}
NO = {'no'}
# Frequency labels -> ordinal 1/2/3.
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
        return FREQ.get(f, pd.NA)  # YES but freq missing/odd -> NA
    return pd.NA  # occurrence missing / don't know / refuse


def hfias_category(row):
    """FANTA HFIAS prevalence category from items a1..a9 (each 0-3)."""
    a = [row[f'HFIAS{k}'] for k in range(1, 10)]
    if any(pd.isna(v) for v in a):
        return pd.NA
    a1, a2, a3, a4, a5, a6, a7, a8, a9 = a
    # Severe first (categories are mutually exclusive; severe dominates).
    if a5 == 3 or a6 == 3 or a7 in (1, 2, 3) or a8 in (1, 2, 3) or a9 in (1, 2, 3):
        return 'Severely food insecure'
    if a3 in (2, 3) or a4 in (2, 3) or a5 in (1, 2) or a6 in (1, 2):
        return 'Moderately food insecure'
    if a1 in (2, 3) or a2 in (1, 2, 3) or a3 == 1 or a4 == 1:
        return 'Mildly food insecure'
    return 'Food secure'


cols = [ID] + [c for pair in PAIRS for c in pair]
df = get_dataframe('../Data/m8a.dta')[cols].copy()
df['i'] = df[ID].apply(format_id)

out = pd.DataFrame({'i': df['i']})
for k, (occ, freq) in enumerate(PAIRS, start=1):
    out[f'HFIAS{k}'] = [_item_score(o, f) for o, f in zip(df[occ], df[freq])]

# Score is the sum of the 9 items where all items are present.
item_cols = [f'HFIAS{k}' for k in range(1, 10)]
present = out[item_cols].notna().all(axis=1)
out['HFIAS_score'] = pd.NA
out.loc[present, 'HFIAS_score'] = out.loc[present, item_cols].sum(axis=1)
out['HFIAS_category'] = out.apply(hfias_category, axis=1)

# Drop households where the whole battery is missing (not administered).
out = out[out[item_cols].notna().any(axis=1)].copy()

for c in item_cols + ['HFIAS_score']:
    out[c] = out[c].astype('Int64')

out['t'] = '2009'
out = (out[['t', 'i'] + item_cols + ['HFIAS_score', 'HFIAS_category']]
       .drop_duplicates(subset=['t', 'i'])
       .set_index(['t', 'i'])
       .sort_index())

to_parquet(df=out, fn='food_security_hfias.parquet')
