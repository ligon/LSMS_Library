#!/usr/bin/env python
"""Panama 1997 food_acquired -> canonical long form.

Index   : (t, i, j, u, s)   -- NO `v` (joined from sample() at API time)
Columns : [Quantity, Expenditure, Price]
i = household (form), j = harmonized food item, u = native unit,
s in {purchased, produced, inkind, other} (transformations.S_VALUES).

Source: 1997/Data/GAST-A.DTA (consumption module GA, one row per
household x food item).  The questionnaire records, per item:

  Purchases (last 15 days):
    ga106a = quantity bought   -> Quantity (s='purchased')
    ga106b = unit code         -> u
    ga106c = total paid        -> Expenditure (s='purchased')

  Acquisition WITHOUT purchase (last 15 days):
    ga110a = quantity obtained -> Quantity (non-purchased)
    ga110b = unit code         -> u
  The SOURCE of that non-purchased quantity is recorded in yes/no flags:
    ga111a = OBT PROPI  (own production)        -> s='produced'
    ga111b = OBT DONAC  (gift / donation)       -> s='inkind'
    ga111c = OBT NEGO   (own business)          -> s='other'
    ga111d = (unlabeled, parallels 2008 negocio)-> s='other'
  A given obtained item may carry more than one flag; we emit one canonical
  row per flagged source (the obtained quantity attributed to that source).

NOTE on recall periods (the refuting defect from round 1):
  ga109a ('NOR SINCO', unit ga109b='UNIDA MES') is the MONTHLY recall of the
  same home-consumption quantity and OVERLAPS ~100% with ga110a (the 15-day
  recall).  Summing both double-counts.  We use ONLY ga110a (15 days) so the
  non-purchased quantity is on the SAME recall basis as the purchased ga106a.
  ga110a is NOT 'produced' wholesale: it is total non-purchased acquisition;
  the produced/inkind/other split is driven by the ga111* source flags.
"""
import sys

sys.path.append('../../_/')
sys.path.append('../../../_/')

import json

import numpy as np
import pandas as pd

from lsms_library.local_tools import (df_from_orgfile, format_id,
                                       get_dataframe, to_parquet)

t = '1997'

# --- source ----------------------------------------------------------------
df = get_dataframe('../Data/GAST-A.DTA', convert_categoricals=False)

cols = ['form', 'ga100',
        'ga106a', 'ga106b', 'ga106c',          # purchased qty / unit / value
        'ga110a', 'ga110b',                     # non-purchased qty / unit (15d)
        'ga111a', 'ga111b', 'ga111c', 'ga111d']  # source flags
df = df.loc[:, cols].copy()

# Sentinel / missing handling.  In 1997 the amount fields use 9999.x (e.g.
# 9999.5, 9999.9, 9999.99) as the explicit "NR"/missing sentinel (and >= 1e99
# is Stata-missing).  Mask everything >= 9999 -- matches the legacy script's
# 9999-band convention and reconciles purchased value to the raw < 9999 total.
for c in ['ga106a', 'ga106c', 'ga110a']:
    df[c] = pd.to_numeric(df[c], errors='coerce')
    df.loc[df[c] >= 9999, c] = np.nan

# --- unit decode: numeric code -> English label via units.json -------------
with open('../../_/units.json', 'r') as f:
    units = pd.DataFrame(json.load(f)['units'])
unit_dict = (units.loc[:, ['Unitcode', 'Translation']]
             .set_index('Unitcode')['Translation'].to_dict())


def decode_unit(code):
    try:
        return unit_dict.get(int(code), pd.NA)
    except (ValueError, TypeError):
        return pd.NA


df['u_bought'] = df['ga106b'].apply(decode_unit)
df['u_obtained'] = df['ga110b'].apply(decode_unit)

# --- harmonize food item j -------------------------------------------------
food_items = df_from_orgfile('../../_/food_items.org')
food_items = food_items.loc[:, ['Preferred Label', t]]
food_items[t] = food_items[t].astype(str).str.strip()
food_items = food_items.replace(['', '---', 'nan'], pd.NA).dropna()
# 1997 column holds numeric item codes; key the lookup on the integer code.
item_map = {}
for code, label in zip(food_items[t], food_items['Preferred Label']):
    try:
        item_map[int(float(code))] = str(label).strip()
    except (ValueError, TypeError):
        continue


def harmonize_item(code):
    try:
        return item_map.get(int(code), pd.NA)
    except (ValueError, TypeError):
        return pd.NA


df['i'] = df['form'].apply(format_id)
df['j'] = df['ga100'].apply(harmonize_item)

# --- build canonical rows --------------------------------------------------
records = []

# Purchased rows
buy = df.loc[df['ga106a'].notna() | df['ga106c'].notna(),
             ['i', 'j', 'u_bought', 'ga106a', 'ga106c']].copy()
buy = buy.rename(columns={'u_bought': 'u', 'ga106a': 'Quantity',
                          'ga106c': 'Expenditure'})
buy['s'] = 'purchased'
buy['Price'] = np.nan
records.append(buy)

# Non-purchased rows, split by the source flags.
# Flag value 1 = "Sí" (yes); 2 = "No"; >=9 / NaN = not applicable.
flag_to_s = {'ga111a': 'produced',   # OBT PROPI  (own production)
             'ga111b': 'inkind',     # OBT DONAC  (gift / donation)
             'ga111c': 'other',      # OBT NEGO   (own business)
             'ga111d': 'other'}      # unlabeled  (parallels 2008 negocio)

obtained = df.loc[df['ga110a'].notna() & (df['ga110a'] > 0)].copy()
for flag, s in flag_to_s.items():
    sub = obtained.loc[obtained[flag] == 1,
                       ['i', 'j', 'u_obtained', 'ga110a']].copy()
    if sub.empty:
        continue
    sub = sub.rename(columns={'u_obtained': 'u', 'ga110a': 'Quantity'})
    sub['Expenditure'] = np.nan
    sub['Price'] = np.nan
    sub['s'] = s
    records.append(sub)

# Obtained quantity with NO source flag set -> default to 'produced'
# (own consumption with unrecorded source; small residual).
flagset = pd.Series(False, index=obtained.index)
for flag in flag_to_s:
    flagset = flagset | (obtained[flag] == 1)
unflagged = obtained.loc[~flagset, ['i', 'j', 'u_obtained', 'ga110a']].copy()
if not unflagged.empty:
    unflagged = unflagged.rename(columns={'u_obtained': 'u',
                                          'ga110a': 'Quantity'})
    unflagged['Expenditure'] = np.nan
    unflagged['Price'] = np.nan
    unflagged['s'] = 'produced'
    records.append(unflagged)

out = pd.concat(records, ignore_index=True)
out['t'] = t

# Drop rows whose food item did not harmonize.
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
# Quantity = Expenditure, so the framework can still sum it.  This is what was
# previously leaking as a null `u` on ~zero-quantity purchase rows.
value_only = (out['Quantity'].isna() & out['u'].isna()
              & out['Expenditure'].notna())
out.loc[value_only, 'u'] = 'Value'
out.loc[value_only, 'Quantity'] = out.loc[value_only, 'Expenditure']

# Drop rows that carry no amount at all.
out = out[out['Quantity'].notna() | out['Expenditure'].notna()]

# Purchased Price = total spent / quantity bought (per native unit).
buy_mask = out['s'] == 'purchased'
with np.errstate(divide='ignore', invalid='ignore'):
    out.loc[buy_mask, 'Price'] = (out.loc[buy_mask, 'Expenditure']
                                  / out.loc[buy_mask, 'Quantity'])
out.loc[~np.isfinite(out['Price']), 'Price'] = np.nan

idx = ['t', 'i', 'j', 'u', 's']

# Collapse duplicate (t,i,j,u,s) keys (same item/unit/source reported twice);
# sum the quantities/expenditures, mean the per-unit price.
agg = {'Quantity': 'sum', 'Expenditure': 'sum', 'Price': 'mean'}
out = out.groupby(idx, dropna=False).agg(agg)
out = out.dropna(how='all')

if __name__ == '__main__':
    to_parquet(out, 'food_acquired.parquet')
