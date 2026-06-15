#!/usr/bin/env python
"""GhanaLSS 2005-06 canonical food_acquired.

Emits the canonical long form with a GhanaLSS-local ``visit`` index level:

    index  : (t, i, j, u, s, visit)      -- NO v (joined from sample() at API time)
    columns: [Quantity, Expenditure, Price]
    s      : {purchased, produced}

Purchases (Section 9B / Code_9b) are value-only: Expenditure = recorded value,
u = 'Value', Quantity = Expenditure (no fabricated physical quantity; price-based
imputation is a later, out-of-scope phase).  Production (Section 8H / Code_8h)
carries a real Quantity in a native unit ``u`` plus a farmgate Price; its
Expenditure is left NaN (no produced value is recorded).

Food codes map to canonical ``j`` via the harmonize_food table; rows whose label
is empty ('') are the non-food Section-9B block (codes ~165-277) and are dropped.

Household id ``i`` is built EXACTLY as sample()/household_roster() build it -- by
running this wave's mapping.i() over the pre-composed source ``hhid`` column --
so food.i matches sample.i and v joins cleanly (fixes the GH #256 NaN-v bug).
"""
import sys
sys.path.append('../../_')
sys.path.append('../../../_/')

import numpy as np
import pandas as pd

from lsms_library.local_tools import to_parquet, df_from_orgfile, get_dataframe
import mapping

t = '2005-06'

# --- food-item harmonization (purchases: Code_9b; production: Code_8h) ----------
labels = df_from_orgfile('./categorical_mapping.org', name='harmonize_food',
                         encoding='ISO-8859-1')
labelsd = {}
for column in ['Code_9b', 'Code_8h']:
    labelsd[column] = (labels[['Preferred Label', column]]
                       .dropna(subset=[column])
                       .set_index(column)['Preferred Label'].to_dict())


def _drop_nonfood(s):
    """Keep only rows whose harmonized j is a non-empty string label."""
    return s.apply(lambda x: isinstance(x, str) and x.strip() != '')


# ================================ PURCHASES (s9b) ==============================
# Value-only.  Visits 1..10 each carry a recorded value in s9bq{visit}.
df = get_dataframe('../Data/partb/sec9b.dta', convert_categoricals=False)

# i exactly as sample()/roster() build it: mapping.i() over the *pre-composed*
# source 'hhid' column (sample reads idxvars i: hhid -> format_id(hhid)).
df['i'] = df['hhid'].apply(mapping.i)
df['j'] = df['freqcd'].replace(labelsd['Code_9b'])
df = df[_drop_nonfood(df['j'])]          # drop non-food Section-9B block (j == '')

pur_visit_cols = {f's9bq{v}': f'Expenditure_v{v}' for v in range(1, 11)}
x = df.rename(columns=pur_visit_cols)[['i', 'j'] + list(pur_visit_cols.values())]
x = x.replace({r'': pd.NA, 0: np.nan})
# Several distinct freqcd codes harmonize to one j (e.g. 'Other Cereal'); sum
# their per-visit values so (i, j) uniquely identifies a row for the melt.
x = x.groupby(['i', 'j']).sum(min_count=1).reset_index()
x = pd.wide_to_long(x, ['Expenditure'], ['i', 'j'], 'visit', sep='_v')
x = x.dropna(subset=['Expenditure'])     # keep only visits with a recorded value
x['s'] = 'purchased'
x['u'] = 'Value'
x['Quantity'] = x['Expenditure']         # value-only: Quantity carries the value
x['Price'] = np.nan
x = x.reset_index()

# ================================ PRODUCED (s8h) ==============================
# Real quantity (visits 4..12) in a native unit (s8hq13), with a farmgate Price
# (s8hq14).  Expenditure left NaN -- no produced value is recorded.
prod = get_dataframe('../Data/partb/sec8h.dta', convert_categoricals=True)
prod = prod[prod['s8hq1'] == 'yes']      # only HH that consumed own produce
prod['i'] = prod['hhid'].apply(mapping.i)
prod['j'] = prod['foodcd'].replace(labelsd['Code_8h'])
prod = prod[_drop_nonfood(prod['j'])]

# Native unit label (decoded text, e.g. 'basket', 'kilogram') and farmgate price.
prod['u'] = prod['s8hq13'].astype(str)
prod['Price'] = prod['s8hq14']

pro_visit_cols = {f's8hq{v}': f'Quantity_v{v}' for v in range(4, 13)}
keep = ['i', 'j', 'u', 'Price'] + list(pro_visit_cols.values())
y = prod.rename(columns=pro_visit_cols)[keep]
y = y.replace({r'': pd.NA, 0: np.nan})
# As with purchases, distinct foodcd may share a j; sum per-visit quantities so
# (i, j, u, Price) uniquely identifies a row for the melt.
y = y.groupby(['i', 'j', 'u', 'Price']).sum(min_count=1).reset_index()
y = pd.wide_to_long(y, ['Quantity'], ['i', 'j', 'u', 'Price'], 'visit', sep='_v')
y = y.dropna(subset=['Quantity'])        # keep only visits with a recorded quantity
y = y.reset_index()
y['s'] = 'produced'
y['Expenditure'] = np.nan

# =============================== COMBINE & WRITE ==============================
idx = ['t', 'i', 'j', 'u', 's', 'visit']
fa = pd.concat([x, y], ignore_index=True)
fa['t'] = t
fa = fa.set_index(idx)[['Quantity', 'Expenditure', 'Price']]
fa = fa.dropna(how='all')

to_parquet(fa, 'food_acquired.parquet')
