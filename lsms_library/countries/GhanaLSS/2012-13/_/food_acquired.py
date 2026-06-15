#!/usr/bin/env python
"""GhanaLSS 2012-13 food_acquired -> canonical long form.

Emits index (t, i, j, u, s, visit) with columns [Quantity, Expenditure, Price].

- s='purchased' from sec9b (food code = freqcd, decoded via harmonize_food Code_9b).
  2012-13 purchases are VALUE-ONLY (no quantity, no unit): per the food-acquired
  skill's LCU convention we emit u='Value', Expenditure=value, Quantity=Expenditure.
  No physical quantity is fabricated (Phase-3 price-imputation is out of scope).
- s='produced' from sec8h (food label = foodcd, decoded via harmonize_food Label_8h).
  Produced rows carry a real Quantity (sum of the visit columns s8hq3..s8hq8), a
  real unit u (s8hq9), and a farmgate Price (s8hq10).  Expenditure is NaN.

visit is KEPT as its own index level (decision D1 -- do NOT fold into t).  The
derived tables sum it out at API time.

i = household id ('hid'), pre-composed "clust/nh" matching sample()'s HID.
"""
import sys
import numpy as np
import pandas as pd
sys.path.append('../../../_/')
from lsms_library.local_tools import (get_categorical_mapping, df_data_grabber,
                                       format_id, _to_numeric, to_parquet,
                                       get_dataframe)

w = '2012-13'

# Food-item harmonization.
# Purchases (sec9b) carry NUMERIC food codes (freqcd) -> map via Code_9b.
# Produced (sec8h) carries TEXT food labels (foodcd) -> map via Label_8h.
purchase_food = get_categorical_mapping(tablename='harmonize_food',
                                        idxvars={'Code': ('Code_9b', format_id)},
                                        **{'Label': 'Preferred Label'})
produced_food = get_categorical_mapping(tablename='harmonize_food',
                                        idxvars={'Code': 'Label_8h'},
                                        **{'Label': 'Preferred Label'})

############################################################
# Purchases  (s = 'purchased')  -- value only, u = 'Value'
############################################################
idxvars = dict(i='hid',
               j=('freqcd', lambda x: purchase_food.get(format_id(x), '')))

myvars = {f"Expenditure_{i}": (f"s9bq{i}", _to_numeric) for i in range(1, 7)}

x = df_data_grabber('../Data/PARTB/sec9b.dta', idxvars,
                    convert_categoricals=False, **myvars)

purch_visits = []
for i in range(1, 7):
    di = x.loc[:, [f"Expenditure_{i}"]].copy()
    di.columns = ['Expenditure']
    di['visit'] = i
    di = di.reset_index().replace({r'': pd.NA, 0: np.nan})
    purch_visits.append(di)

purch = pd.concat(purch_visits, ignore_index=True)
# Drop non-food / unmapped purchase codes (j == '').  ~30% of the combined
# Section-9B module is the legitimate NON-FOOD part -- expected, not data loss.
purch = purch[(purch['j'] != '') & purch['j'].notna()]
# Collapse any multiple records for the same (i, j, visit).
purch = purch.groupby(['i', 'j', 'visit'], as_index=False)['Expenditure'].sum()
purch = purch[purch['Expenditure'].notna()]

purch['u'] = 'Value'
purch['s'] = 'purchased'
# LCU convention: no physical quantity, so Quantity == Expenditure.
purch['Quantity'] = purch['Expenditure']
purch['Price'] = np.nan

############################################################
# Home produced  (s = 'produced')  -- real Quantity, u, Price
############################################################
prod = get_dataframe('../Data/PARTB/sec8h.dta', convert_categoricals=True)
# keep only households that consumed own-produced food in the past 12 months
prod = prod[prod['s8hq1'] == 'yes'].copy()

prod['j'] = prod['foodcd'].map(produced_food)
prod = prod[(prod['j'] != '') & prod['j'].notna()]

prod = prod.rename(columns={'hid': 'i', 's8hq9': 'u', 's8hq10': 'Price'})
qty_cols = {f"s8hq{i}": f"Quantity_{i}" for i in range(3, 9)}
prod = prod.rename(columns=qty_cols)

keep = ['i', 'j', 'u', 'Price'] + list(qty_cols.values())
prod = prod[keep].copy()
prod = prod.replace({r'': pd.NA, 0: np.nan})
# Non-string unit codes -> string (mirrors the legacy guard).
prod['u'] = prod['u'].astype(str)
# Collapse duplicate (i, j, u) records, summing visit quantities; Price = mean.
agg = {c: 'sum' for c in qty_cols.values()}
agg['Price'] = 'mean'
prod = prod.groupby(['i', 'j', 'u'], as_index=False).agg(agg)

# Wide visit columns -> long.
prod = pd.wide_to_long(prod, stubnames=['Quantity'], i=['i', 'j', 'u'],
                       j='visit', sep='_', suffix=r'\d+').reset_index()
prod = prod[prod['Quantity'].notna() & (prod['Quantity'] != 0)]

prod['s'] = 'produced'
prod['Expenditure'] = np.nan

############################################################
# Stack the two sources into the s index level.
############################################################
cols = ['i', 'j', 'u', 's', 'visit', 'Quantity', 'Expenditure', 'Price']
fa = pd.concat([purch[cols], prod[cols]], ignore_index=True)

fa['t'] = w
fa = fa.set_index(['t', 'i', 'j', 'u', 's', 'visit'])
fa = fa.reorder_levels(['t', 'i', 'j', 'u', 's', 'visit'])

# Drop all-empty rows.
fa = fa.replace(0, np.nan).dropna(how='all')

if __name__ == '__main__':
    to_parquet(fa, 'food_acquired.parquet')
