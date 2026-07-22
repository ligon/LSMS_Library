#!/usr/bin/env python
"""Panama 2003 food_acquired -> canonical long form.

Index   : (t, i, j, u, s)   -- NO `v` (joined from sample() at API time)
Columns : [Quantity, Expenditure, Price]
i = household (form), j = harmonized food item, u = native unit,
s in {purchased, produced, inkind, other} (transformations.S_VALUES).

Source: 2003/Data/E03GA10B.DTA (consumption module GAI, one row per
household x food item).  Per item:

  Purchases (last 15 days):
    gai06a  = number of presentations bought
    gai06b1 = size per presentation (conversion to the unit in gai06b2)
    gai06b2 = unit (LITRO/GRAMO/LIBRA/...)   -> u
    gai06c  = total paid                     -> Expenditure (s='purchased')
  Quantity = gai06a * gai06b1 in the unit gai06b2.

  Acquisition WITHOUT purchase (last 15 days):
    gai10a  = number of presentations obtained
    gai10b1 = size per presentation
    gai10b2 = unit                           -> u
  Quantity = gai10a * gai10b1.
  The SOURCE of the non-purchased quantity is the yes/no flag block
  (parallels 1997 ga111* and 2008 s11a11*):
    gai111 = own production    -> s='produced'
    gai112 = gift / donation   -> s='inkind'
    gai113 = part of payment   -> s='other'
    gai114 = own business      -> s='other'
  One canonical row per flagged source.

NOTE on recall periods (round-1 refuting defect): gai09a is the MONTHLY
recall of the same home-consumption quantity and OVERLAPS ~100% with the
15-day gai10a; summing both double-counts.  We use ONLY gai10a so the
non-purchased quantity shares the 15-day recall basis of purchased gai06a.
"""
import sys

sys.path.append('../../_/')
sys.path.append('../../../_/')

import numpy as np
import pandas as pd

from lsms_library.local_tools import (df_from_orgfile, format_id,
                                       get_dataframe, to_parquet)

t = '2003'

# Read categorically so gai00 (item) and gai06b2/gai10b2 (unit text) decode.
df = get_dataframe('../Data/E03GA10B.DTA', convert_categoricals=True)

cols = ['form', 'gai00',
        'gai06a', 'gai06b1', 'gai06b2', 'gai06c',     # purchased
        'gai10a', 'gai10b1', 'gai10b2',               # non-purchased (15d)
        'gai111', 'gai112', 'gai113', 'gai114']        # source flags
df = df.loc[:, cols].copy()

# Numeric amounts.  Sentinel 99999 == "NR".
for c in ['gai06a', 'gai06b1', 'gai06c', 'gai10a', 'gai10b1']:
    df[c] = pd.to_numeric(df[c], errors='coerce')
    df.loc[df[c] >= 99999, c] = np.nan

# Source flags come back as categoricals (Sí/No/NR) under convert_categoricals;
# normalize to a yes-boolean.  In the raw data 1=Sí, 2=No, 9=NR.
for c in ['gai111', 'gai112', 'gai113', 'gai114']:
    df[c] = df[c].astype(str).str.strip().str.lower()


def is_yes(series):
    return series.isin(['1', '1.0', 'sí', 'si'])


# --- unit decode: Spanish text -> English label ----------------------------
unit_dict = {'GALON': 'gallon',
             'GRAMO': 'gram',
             'FRAMO': 'gram',          # typo for GRAMO in the raw data
             'KILOGRAMO': 'kilogram',
             'LIBRA': 'pound',
             'LITRO': 'liter',
             'ONZA': 'ounce',
             'MILILITRO': 'milliliter'}


def decode_unit(label):
    if pd.isna(label):
        return pd.NA
    return unit_dict.get(str(label).strip().upper(), pd.NA)


df['u_bought'] = df['gai06b2'].apply(decode_unit)
df['u_obtained'] = df['gai10b2'].apply(decode_unit)

# --- harmonize food item j -------------------------------------------------
food_items = df_from_orgfile('../../_/food_items.org')
food_items = food_items.loc[:, ['Preferred Label', t]]
food_items[t] = food_items[t].astype(str).str.strip()
food_items = food_items.replace(['', '---', 'nan'], pd.NA).dropna()
item_map = dict(zip(food_items[t], food_items['Preferred Label'].str.strip()))


def harmonize_item(label):
    if pd.isna(label):
        return pd.NA
    return item_map.get(str(label).strip(), pd.NA)


df['i'] = df['form'].apply(format_id)
df['j'] = df['gai00'].apply(harmonize_item)

# Total quantity in native units = count * size-per-presentation.
df['qty_bought'] = df['gai06a'] * df['gai06b1']
df['qty_obtained'] = df['gai10a'] * df['gai10b1']

# --- build canonical rows --------------------------------------------------
records = []

# Purchased rows
buy = df.loc[df['qty_bought'].notna() | df['gai06c'].notna(),
             ['i', 'j', 'u_bought', 'qty_bought', 'gai06c']].copy()
buy = buy.rename(columns={'u_bought': 'u', 'qty_bought': 'Quantity',
                          'gai06c': 'Expenditure'})
buy['s'] = 'purchased'
buy['Price'] = np.nan
records.append(buy)

# Non-purchased rows split by source flags.
flag_to_s = {'gai111': 'produced',   # own production
             'gai112': 'inkind',     # gift / donation
             'gai113': 'other',      # part of payment
             'gai114': 'other'}      # own business

obtained = df.loc[df['qty_obtained'].notna() & (df['qty_obtained'] > 0)].copy()
for flag, s in flag_to_s.items():
    sub = obtained.loc[is_yes(obtained[flag]),
                       ['i', 'j', 'u_obtained', 'qty_obtained']].copy()
    if sub.empty:
        continue
    sub = sub.rename(columns={'u_obtained': 'u', 'qty_obtained': 'Quantity'})
    sub['Expenditure'] = np.nan
    sub['Price'] = np.nan
    sub['s'] = s
    records.append(sub)

# Obtained quantity with no flag -> default 'produced' (small residual).
flagset = pd.Series(False, index=obtained.index)
for flag in flag_to_s:
    flagset = flagset | is_yes(obtained[flag])
unflagged = obtained.loc[~flagset, ['i', 'j', 'u_obtained', 'qty_obtained']].copy()
if not unflagged.empty:
    unflagged = unflagged.rename(columns={'u_obtained': 'u',
                                          'qty_obtained': 'Quantity'})
    unflagged['Expenditure'] = np.nan
    unflagged['Price'] = np.nan
    unflagged['s'] = 'produced'
    records.append(unflagged)

out = pd.concat(records, ignore_index=True)
out['t'] = t

out = out[out['j'].notna()]
out['u'] = out['u'].astype('string')

# Coerce value columns to plain float64 (the categorical read can hand back
# arrow-backed float32, which rejects scalar/array .loc assignment below).
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
