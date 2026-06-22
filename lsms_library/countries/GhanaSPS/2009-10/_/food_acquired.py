#!/usr/bin/env python
"""GhanaSPS 2009-10 food_acquired -> canonical (t, i, j, u, s).

Source: 2009-10/Data/S11A.dta (Section 11A food consumption), one row per
(household, food item):
  - hhno     -> i  (household)
  - itname   -> j  (food item text; harmonized to a Preferred Label via
                    food_items.org '2009-10' column)
  - s11a_f   -> u  (numeric unit code; decoded via units.org 'unit09' then
                    folded to the harmonized Preferred Label via 'harmonizedunit')

Three acquisition sources, EACH carrying its own quantity AND value (cedis +
pesewas, value = cedis + pesewas/100):
  - purchased (s='purchased'): qty s11a_ci ; value s11a_cii + s11a_ciii/100
  - produced  (s='produced') : qty s11a_bi ; value s11a_bii + s11a_biii/100
  - received gift (s='inkind'): qty s11a_di ; value s11a_dii + s11a_diii/100

Unlike the stock food_acquired_to_canonical (which NaNs produced/inkind
Expenditure), GhanaSPS records value on all three sources, so each emitted
source row carries both Quantity and Expenditure.

The legacy script had the i/j SWAP (j=household, i=item); this fixes it to
i=household, j=item.  Price is NOT written -- it is api-derived
(food_prices = Expenditure/Quantity).  v is omitted: GhanaSPS has no sample
table (cluster identity unavailable; accepted data gap) so no cluster is joined.
"""
import sys

import numpy as np
import pandas as pd

sys.path.append('../../../_/')
from lsms_library.local_tools import to_parquet, get_dataframe, df_from_orgfile

t = '2009-10'

df = get_dataframe('../Data/S11A.dta', convert_categoricals=False)

df = df.rename({'hhno': 'i', 'itname': 'j', 's11a_f': 'u'}, axis=1)
df['i'] = df['i'].astype(str)
df['j'] = df['j'].astype(str).str.strip()

# Harmonize food item text -> Preferred Label via food_items.org '2009-10' col.
food_items = df_from_orgfile('../../_/food_items.org', name=None, to_numeric=False, encoding='ISO-8859-1')
food_items = food_items[['2009-10', 'Preferred Label']].apply(lambda s: s.astype(str).str.strip())
food_items = food_items[~food_items['2009-10'].isin(['', '---', 'nan'])]
food_items = food_items[~food_items['Preferred Label'].isin(['', '---', 'nan'])]
item_map = dict(zip(food_items['2009-10'], food_items['Preferred Label']))
df['j'] = df['j'].replace(item_map)

# Decode numeric unit code -> label (unit09) -> harmonized Preferred Label.
unit09 = df_from_orgfile('../../_/units.org', name='unit09', encoding='ISO-8859-1')
unit09['Code'] = unit09['Code'].astype(str).str.replace('�', '', regex=False).str.strip()
unit09['Preferred Label'] = unit09['Preferred Label'].astype(str).str.strip()
code2label = dict(zip(unit09['Code'], unit09['Preferred Label']))

harmonized = df_from_orgfile('../../_/units.org', name='harmonizedunit', encoding='ISO-8859-1')
harmonized = harmonized[['2009-10', 'Preferred Label']].apply(lambda s: s.astype(str).str.strip())
harmonized = harmonized[~harmonized['2009-10'].isin(['', '---', 'nan'])]
label2pref = dict(zip(harmonized['2009-10'], harmonized['Preferred Label']))

u_code = df['u'].astype('Int64').astype(str).replace('<NA>', pd.NA)
u_label = u_code.map(code2label)
df['u'] = u_label.map(label2pref).fillna(u_label)

# Build value (cedis + pesewas/100) for each source.  Cedis is the major
# denomination and pesewas the minor; a missing pesewa entry means zero pesewas,
# so fillna(0) the pesewa fraction before adding -- otherwise a present-cedi /
# absent-pesewa row (the majority here) would NaN out and silently drop the
# whole cedi value (~74% of value loss vs. the raw source total).  A row with
# neither cedi nor pesewa nor quantity is dropped downstream by the (qty|exp)!=0
# filter, so fillna-ing both components is safe.
for cedi, pes, val in [('s11a_cii', 's11a_ciii', 'purchased_value'),
                       ('s11a_bii', 's11a_biii', 'produced_value'),
                       ('s11a_dii', 's11a_diii', 'inkind_value')]:
    df[val] = (pd.to_numeric(df[cedi], errors='coerce').fillna(0.0)
               + pd.to_numeric(df[pes], errors='coerce').fillna(0.0) / 100)

sources = [
    ('purchased', 's11a_ci', 'purchased_value'),
    ('produced',  's11a_bi', 'produced_value'),
    ('inkind',    's11a_di', 'inkind_value'),
]

base = {'t': t, 'i': df['i'].to_numpy(), 'j': df['j'].to_numpy(), 'u': df['u'].to_numpy()}
parts = []
for s, qcol, vcol in sources:
    parts.append(pd.DataFrame({**base, 's': s,
                               'Quantity': pd.to_numeric(df[qcol], errors='coerce').to_numpy(),
                               'Expenditure': df[vcol].to_numpy()}))

out = pd.concat(parts, ignore_index=True)
# Keep a source row only if it carries quantity or expenditure.
qty = out['Quantity'].fillna(0.0)
exp = out['Expenditure'].fillna(0.0)
out = out[(qty != 0) | (exp != 0)].copy()
out = out[out['j'].notna() & (out['j'].astype(str).str.strip() != '')]

# Collapse duplicate canonical keys (item repeated within a hh), summing the
# additive measures (min_count=1 keeps an all-NaN group NaN).
out = (out.groupby(['t', 'i', 'j', 'u', 's'], dropna=False)
          .agg(Quantity=('Quantity', lambda s: s.sum(min_count=1)),
               Expenditure=('Expenditure', lambda s: s.sum(min_count=1)))
          .sort_index())

to_parquet(out, 'food_acquired.parquet')
