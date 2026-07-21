#!/usr/bin/env python
"""Cambodia 2019-20 food_acquired -> canonical (t, i, j, u, s).

Source: hh_sec_5.dta (CSES 2019-20 food-consumption roster), one row per
(household, item):
  - HHID                            -> i  (household)
  - food_consumption_roster_1__id   -> j  (food item, 1-64; harmonized to a
                                           Preferred Label via food_items.org)
  - s05q03                          -> u  (unit: Kg, Piece, ...)
  - s05q04                          -> Quantity  (TOTAL consumed; ONE figure)
  - s05q05                          -> "total spent"   (value PURCHASED)
  - s05q06                          -> "value obtained" (value obtained WITHOUT
                                           purchase: own production + in-kind)

The module splits *value* by acquisition source but reports only ONE total
quantity, so canonical (t,i,j,u,s) needs a quantity-apportionment rule.  Per
the #108 decision (2026-06-20) we **split by value share**: each item becomes
up to two rows --

  s='purchased': Expenditure = s05q05; Quantity = total * spent/(spent+obtained)
  s='produced' : Expenditure = s05q06; Quantity = total * obtained/(spent+obtained)

The quantity split is therefore an imputation (the survey does not split
quantity); the reported Expenditure values are preserved exactly.  When a row
has quantity but no value (spent+obtained == 0) the quantity is assigned to
'produced' (consumed-but-unvalued is almost always own production).  's=produced'
here lumps own-production + in-kind/gifts (s05q06 is "value obtained otherwise",
not strictly farm output).

Price is NOT written -- it is api-derived (food_prices = Expenditure/Quantity).
`v` is omitted: script-path waves let _join_v_from_sample add the cluster at
API time (post-2026-04-10 design).
"""
import sys

import numpy as np
import pandas as pd

sys.path.append('../../../_/')
from lsms_library.local_tools import to_parquet, get_dataframe, df_from_orgfile

t = '2019-20'

df = get_dataframe('../Data/hh_sec_5.dta', convert_categoricals=False)
df = df.rename({'HHID': 'i',
                'food_consumption_roster_1__id': 'j',
                's05q03': 'u',
                's05q04': 'Quantity',
                's05q05': 'spent',
                's05q06': 'obtained'}, axis=1)

# Harmonize the numeric item code (1-64) to a Preferred Label via food_items.org.
# Key on the code, not the categorical text: some raw Stata value labels carry
# mojibake (e.g. a zero-width space in item 41) that does not round-trip.
df['j'] = pd.to_numeric(df['j'], errors='coerce').astype('Int64').astype(str)
food_items = df_from_orgfile('../../_/food_items.org', name='food_label', to_numeric=False)
food_items = food_items.loc[:, ['Preferred Label', 'Code']]
food_items['Code'] = food_items['Code'].str.strip()
food_items = food_items.replace(['', '---'], pd.NA).dropna()
code_to_label = food_items.set_index('Code')['Preferred Label'].str.strip().to_dict()
df['j'] = df['j'].replace(code_to_label)

for c in ['Quantity', 'spent', 'obtained']:
    df[c] = pd.to_numeric(df[c], errors='coerce')

df['u'] = df['u'].astype(str)

# Drop rows that record no consumption at all (no quantity and no value).
qty = df['Quantity'].fillna(0.0)
spent = df['spent'].fillna(0.0)
obtained = df['obtained'].fillna(0.0)
value = spent + obtained
df = df[(qty > 0) | (value > 0)].copy()

qty = df['Quantity'].fillna(0.0).to_numpy()
spent = df['spent'].fillna(0.0).to_numpy()
obtained = df['obtained'].fillna(0.0).to_numpy()
value = spent + obtained
# Purchased share of the single total quantity by value; if value==0 (quantity
# but no recorded value) assign it all to 'produced'.
share_purchased = np.divide(spent, value,
                            out=np.zeros_like(spent, dtype=float),
                            where=value > 0)

base = {'t': t, 'i': df['i'].to_numpy(), 'j': df['j'].to_numpy(), 'u': df['u'].to_numpy()}
purchased = pd.DataFrame({**base, 's': 'purchased',
                          'Quantity': qty * share_purchased, 'Expenditure': spent})
produced = pd.DataFrame({**base, 's': 'produced',
                         'Quantity': qty * (1.0 - share_purchased), 'Expenditure': obtained})

out = pd.concat([purchased, produced], ignore_index=True)
# Keep a source row only if it carries quantity or expenditure.
out = out[(out['Quantity'] > 0) | (out['Expenditure'] > 0)]
# Collapse any duplicate canonical keys (e.g. an item repeated within a hh),
# summing the additive measures (min_count=1 keeps an all-NaN group NaN).
out = (out.groupby(['t', 'i', 'j', 'u', 's'], dropna=False)
          .agg(Quantity=('Quantity', lambda s: s.sum(min_count=1)),
               Expenditure=('Expenditure', lambda s: s.sum(min_count=1)))
          .sort_index())

to_parquet(out, 'food_acquired.parquet')
