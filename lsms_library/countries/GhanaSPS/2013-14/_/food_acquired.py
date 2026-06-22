#!/usr/bin/env python
"""GhanaSPS 2013-14 food_acquired -> canonical (t, i, j, u, s).

Source: 2013-14/Data/11a_foodcomsumption_prod_purch.dta, one row per
(household, food item):
  - FPrimary       -> i  (household)
  - foodlongname   -> j  (food item text; harmonized via food_items.org '2013-14')
  - unit           -> u  (clean text unit; harmonized via units.org
                          'harmonizedunit' '2013-14' col.  NOTE: the file also
                          has a free-text 'unitname'; 'unit' is the clean one.)

Three acquisition sources, EACH carrying its own quantity AND value (cedis):
  - purchased (s='purchased')  : qty purchasedquant     ; value purchasedcedis
  - produced  (s='produced')   : qty producedquant      ; value producedcedis
  - received gift (s='inkind') : qty receivedgiftquant  ; value receivedgiftcedis

GhanaSPS records value on all three sources, so each emitted source row carries
both Quantity and Expenditure.  The legacy script had the i/j SWAP
(j=household, i=item); this fixes it to i=household, j=item.  Price is
api-derived; v omitted (no sample table -- accepted data gap).
"""
import sys

import numpy as np
import pandas as pd

sys.path.append('../../../_/')
from lsms_library.local_tools import to_parquet, get_dataframe, df_from_orgfile

t = '2013-14'

df = get_dataframe('../Data/11a_foodcomsumption_prod_purch.dta', convert_categoricals=False)

df = df.rename({'FPrimary': 'i', 'foodlongname': 'j', 'unit': 'u'}, axis=1)
df['i'] = df['i'].astype(str)
df['j'] = df['j'].astype(str).str.strip()
df['u'] = df['u'].astype(str).str.strip().replace({'': pd.NA, 'nan': pd.NA})

# Harmonize food item text -> Preferred Label via food_items.org '2013-14' col.
food_items = df_from_orgfile('../../_/food_items.org', name=None, to_numeric=False, encoding='ISO-8859-1')
food_items = food_items[['2013-14', 'Preferred Label']].apply(lambda s: s.astype(str).str.strip())
food_items = food_items[~food_items['2013-14'].isin(['', '---', 'nan'])]
food_items = food_items[~food_items['Preferred Label'].isin(['', '---', 'nan'])]
item_map = dict(zip(food_items['2013-14'], food_items['Preferred Label']))
df['j'] = df['j'].replace(item_map)

# Harmonize native unit text -> Preferred Label via units.org '2013-14' col.
harmonized = df_from_orgfile('../../_/units.org', name='harmonizedunit', encoding='ISO-8859-1')
harmonized = harmonized[['2013-14', 'Preferred Label']].apply(lambda s: s.astype(str).str.strip())
harmonized = harmonized[~harmonized['2013-14'].isin(['', '---', 'nan'])]
unit_map = dict(zip(harmonized['2013-14'], harmonized['Preferred Label']))
df['u'] = df['u'].replace(unit_map)

sources = [
    ('purchased', 'purchasedquant',    'purchasedcedis'),
    ('produced',  'producedquant',     'producedcedis'),
    ('inkind',    'receivedgiftquant', 'receivedgiftcedis'),
]

base = {'t': t, 'i': df['i'].to_numpy(), 'j': df['j'].to_numpy(), 'u': df['u'].to_numpy()}
parts = []
for s, qcol, vcol in sources:
    parts.append(pd.DataFrame({**base, 's': s,
                               'Quantity': pd.to_numeric(df[qcol], errors='coerce').to_numpy(),
                               'Expenditure': pd.to_numeric(df[vcol], errors='coerce').to_numpy()}))

out = pd.concat(parts, ignore_index=True)
qty = out['Quantity'].fillna(0.0)
exp = out['Expenditure'].fillna(0.0)
out = out[(qty != 0) | (exp != 0)].copy()
out = out[out['j'].notna() & (out['j'].astype(str).str.strip() != '')]

out = (out.groupby(['t', 'i', 'j', 'u', 's'], dropna=False)
          .agg(Quantity=('Quantity', lambda s: s.sum(min_count=1)),
               Expenditure=('Expenditure', lambda s: s.sum(min_count=1)))
          .sort_index())

to_parquet(out, 'food_acquired.parquet')
