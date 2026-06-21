#!/usr/bin/env python
"""Panama 2008 food_acquired -> canonical long form.

Index   : (t, i, j, u, s)   -- NO `v` (joined from sample() at API time)
Columns : [Quantity, Expenditure, Price]
i = household (hogar), j = harmonized food item, u = native unit,
s in {purchased, produced, inkind, other} (transformations.S_VALUES).

Source: 2008/Data/05alimentos.dta (Section 11A food, one row per
household x food item).  Per item:

  Purchases (last 15 days):
    s11a6a = quantity bought                 -> Quantity (s='purchased')
    s11a6b = unit code (numeric)             -> u
    s11a6c = total comprado (total paid)     -> Expenditure (s='purchased')

  Acquisition WITHOUT purchase (last 15 days):
    s11a10a = quantity obtained (15 days)    -> Quantity
    s11a10b = unit code (numeric)            -> u
  The SOURCE of the non-purchased quantity is the yes/no flag block:
    s11a11a = producción propia (own production) -> s='produced'
    s11a11b = regalo o donación (gift)           -> s='inkind'
    s11a11c = parte del pago (part of payment)   -> s='other'
    s11a11d = del negocio (own business)         -> s='other'
  One canonical row per flagged source.

NOTE on recall periods (round-1 refuting defect): s11a9a is the MONTHLY
recall of the same home-consumption quantity (parallels 1997 ga109a /
2003 gai09a) and overlaps the 15-day s11a10a; summing both double-counts.
We use ONLY s11a10a so the non-purchased quantity shares the 15-day recall
basis of purchased s11a6a.  s11a10a is NOT 'produced' wholesale: it is total
non-purchased acquisition; the split is driven by the s11a11* source flags.
"""
import sys

sys.path.append('../../_/')
sys.path.append('../../../_/')

import numpy as np
import pandas as pd

from lsms_library.local_tools import (df_from_orgfile, format_id,
                                       get_dataframe, to_parquet)

t = '2008'

# Read numerically: producto/units/flags are all numeric codes here.
df = get_dataframe('../Data/05alimentos.dta', convert_categoricals=False)

cols = ['hogar', 'producto',
        's11a6a', 's11a6b', 's11a6c',          # purchased
        's11a10a', 's11a10b',                   # non-purchased (15d)
        's11a11a', 's11a11b', 's11a11c', 's11a11d']  # source flags
df = df.loc[:, cols].copy()

# Numeric amounts.  Sentinel 99999 == missing; 0 amounts treated as missing.
for c in ['s11a6a', 's11a6c', 's11a10a']:
    df[c] = pd.to_numeric(df[c], errors='coerce')
    df.loc[df[c] >= 99999, c] = np.nan

# --- unit decode: numeric code -> English label ----------------------------
# Codes from the Stata value labels on s11a6b/s11a10b.
unit_code = {12: 'gallon', 13: 'gram', 20: 'pound', 21: 'liter',
             27: 'ounce', 39: 'unit', 41: 'milliliter'}


def decode_unit(code):
    try:
        return unit_code.get(int(code), pd.NA)
    except (ValueError, TypeError):
        return pd.NA


df['u_bought'] = df['s11a6b'].apply(decode_unit)
df['u_obtained'] = df['s11a10b'].apply(decode_unit)

# --- harmonize food item j -------------------------------------------------
# 2008 food_items.org column holds the lower-case product TEXT; decode the
# numeric producto code to text first via the source value labels.
dfc = get_dataframe('../Data/05alimentos.dta', convert_categoricals=True)
prod_text = dfc['producto'].astype(str).str.strip()

food_items = df_from_orgfile('../../_/food_items.org')
food_items = food_items.loc[:, ['Preferred Label', t]]
food_items[t] = food_items[t].astype(str).str.strip()
food_items = food_items.replace(['', '---', 'nan'], pd.NA).dropna()
item_map = dict(zip(food_items[t], food_items['Preferred Label'].str.strip()))


def harmonize_item(label):
    if pd.isna(label) or str(label).strip().lower() in ('', 'nan'):
        return pd.NA
    return item_map.get(str(label).strip(), pd.NA)


df['i'] = df['hogar'].apply(format_id)
df['j'] = prod_text.apply(harmonize_item).values

# --- build canonical rows --------------------------------------------------
records = []

# Purchased rows
buy = df.loc[df['s11a6a'].notna() | df['s11a6c'].notna(),
             ['i', 'j', 'u_bought', 's11a6a', 's11a6c']].copy()
buy = buy.rename(columns={'u_bought': 'u', 's11a6a': 'Quantity',
                          's11a6c': 'Expenditure'})
buy['s'] = 'purchased'
buy['Price'] = np.nan
records.append(buy)

# Non-purchased rows split by source flags.  1 = yes (sí), 2 = no.
flag_to_s = {'s11a11a': 'produced',   # producción propia
             's11a11b': 'inkind',     # regalo o donación
             's11a11c': 'other',      # parte del pago
             's11a11d': 'other'}      # del negocio

obtained = df.loc[df['s11a10a'].notna() & (df['s11a10a'] > 0)].copy()
for flag, s in flag_to_s.items():
    sub = obtained.loc[obtained[flag] == 1,
                       ['i', 'j', 'u_obtained', 's11a10a']].copy()
    if sub.empty:
        continue
    sub = sub.rename(columns={'u_obtained': 'u', 's11a10a': 'Quantity'})
    sub['Expenditure'] = np.nan
    sub['Price'] = np.nan
    sub['s'] = s
    records.append(sub)

# Obtained quantity with no flag -> default 'produced' (small residual).
flagset = pd.Series(False, index=obtained.index)
for flag in flag_to_s:
    flagset = flagset | (obtained[flag] == 1)
unflagged = obtained.loc[~flagset, ['i', 'j', 'u_obtained', 's11a10a']].copy()
if not unflagged.empty:
    unflagged = unflagged.rename(columns={'u_obtained': 'u',
                                          's11a10a': 'Quantity'})
    unflagged['Expenditure'] = np.nan
    unflagged['Price'] = np.nan
    unflagged['s'] = 'produced'
    records.append(unflagged)

out = pd.concat(records, ignore_index=True)
out['t'] = t

out = out[out['j'].notna()]
out['u'] = out['u'].astype('string')

# Coerce value columns to plain float64 (defensive against arrow-backed dtypes).
for c in ['Quantity', 'Expenditure', 'Price']:
    out[c] = pd.to_numeric(out[c], errors='coerce').astype('float64')

# Zero amounts are "reported nothing" -> treat as missing.
out.loc[out['Quantity'] == 0, 'Quantity'] = np.nan
out.loc[out['Expenditure'] == 0, 'Expenditure'] = np.nan

# Value-only convention (food-acquired skill / GhanaLSS): a purchase with a
# monetary value but no physical quantity/unit is carried as u='Value' with
# Quantity = Expenditure, so the framework can still sum it.
value_only = (out['Quantity'].isna() & out['u'].isna()
              & out['Expenditure'].notna())
out.loc[value_only, 'u'] = 'Value'
out.loc[value_only, 'Quantity'] = out.loc[value_only, 'Expenditure']

# Drop rows that carry no amount at all.
out = out[out['Quantity'].notna() | out['Expenditure'].notna()]

buy_mask = out['s'] == 'purchased'
with np.errstate(divide='ignore', invalid='ignore'):
    out.loc[buy_mask, 'Price'] = (out.loc[buy_mask, 'Expenditure']
                                  / out.loc[buy_mask, 'Quantity'])
out.loc[~np.isfinite(out['Price']), 'Price'] = np.nan

idx = ['t', 'i', 'j', 'u', 's']
agg = {'Quantity': 'sum', 'Expenditure': 'sum', 'Price': 'mean'}
out = out.groupby(idx, dropna=False).agg(agg)
out = out.dropna(how='all')

if __name__ == '__main__':
    to_parquet(out, 'food_acquired.parquet')
