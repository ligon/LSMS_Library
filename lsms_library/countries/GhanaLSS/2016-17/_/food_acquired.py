#!/usr/bin/env python
"""GhanaLSS 2016-17 food_acquired -> canonical long form.

Emits the canonical schema (per DESIGN_ghanalss_food_acquired_2026-06-15.org,
decisions D1/D5/D6):

  Index   : (t, i, j, u, s, visit)   -- NO `v` (joined from sample() at API time)
  Columns : [Quantity, Expenditure, Price]
  s       : {'purchased', 'produced'} as an index level (the two source modules
            are STACKED into rows, not kept as wide produced_/purchased_ columns)
  visit   : KEPT as a country-local index level (D1 override -- do NOT fold into t)

Sources:
  - Purchases : g7sec9b.dta, Section 9B.  Visits 1-6.  Per visit i:
        s9bq{i}a = amount spent  -> Expenditure
        s9bq{i}b = quantity      -> Quantity
        s9bq{i}c = unit code     -> u (decoded via Stata value labels + unit_label)
    -> s='purchased'.  No reported Price.
  - Production: g7sec8h.dta, Section 8H.  Visits 3-8.  Per visit i:
        s8hq{i}q = quantity      -> Quantity
        s8hq{i}u = unit code     -> u (label is code-prefixed "N. Label"; stripped)
        s8hq{i}p = selling price -> Price (farmgate)
    -> s='produced'.  No produced expenditure column -> Expenditure = NaN.

Unit decode (D-units / GH #348):
  The full g7sec9b/g7sec8h .dta carry Stata value labels on the unit columns, so
  convert_categoricals=True yields native unit *label strings* (e.g. 'Heap').
  Production labels are prefixed with the numeric code ("7. Bowl"); the prefix is
  stripped.  Both are then mapped through the country-level unit_label org table
  (raw label string -> canonical Preferred Label) so `u` holds clean labels with
  no leading digits (leak audit clean).  NB: g7sec9b_small.dta does NOT carry the
  value labels, so we read the full g7sec9b.dta here.
"""
import re
import warnings

import numpy as np
import pandas as pd

from lsms_library.local_tools import (df_from_orgfile, get_categorical_mapping,
                                       get_dataframe, format_id, _to_numeric,
                                       to_parquet)

t = '2016-17'


def hh_id(clust, nh):
    """Canonical household id: clust + '/' + zero-padded nh (matches sample/roster)."""
    c = format_id(clust)
    n = format_id(nh, zeropadding=2)
    if c is None or n is None:
        return pd.NA
    return c + '/' + n


# --- unit decode: native label string -> canonical Preferred Label ----------
_ul = df_from_orgfile('../../_/unit_labels.org', name='unit_label').dropna()
_unit_map = dict(zip(_ul['u'].astype(str).str.strip(),
                     _ul['Preferred Label'].astype(str).str.strip()))


def decode_unit(label):
    """Strip any leading 'N. ' code prefix, then map the native label -> PL."""
    if pd.isna(label):
        return pd.NA
    s = re.sub(r'^\s*\d+\.\s*', '', str(label)).strip()
    if s == '':
        return pd.NA
    return _unit_map.get(s, pd.NA)


def _stack_visits(df_num, df_cat, visits, qty_stem, unit_stem, label_map,
                  item_col, exp_stem=None, price_stem=None):
    """Reshape one source module (one row per HH x item) into long form.

    `df_num` (convert_categoricals=False) supplies the numeric food codes and
    amounts; `df_cat` (convert_categoricals=True) supplies the decoded unit
    *label strings* for the unit columns.  The two reads are the same source
    rows in the same order, so columns line up positionally.

    Stacks each visit into rows; emits columns Quantity/u (+ optional
    Expenditure, Price) with i (household), j (harmonized item), visit.
    """
    df_num = df_num.copy()
    i = pd.Series([hh_id(c, n) for c, n in zip(df_num['clust'], df_num['nh'])])
    j = df_num[item_col].apply(lambda x: label_map.get(format_id(x), ''))

    frames = []
    for v in visits:
        cols = {'Quantity': _to_numeric(df_num[qty_stem.format(v=v)]).values,
                'u': df_cat[unit_stem.format(v=v)].apply(decode_unit).values}
        if exp_stem is not None:
            cols['Expenditure'] = _to_numeric(df_num[exp_stem.format(v=v)]).values
        if price_stem is not None:
            cols['Price'] = _to_numeric(df_num[price_stem.format(v=v)]).values
        part = pd.DataFrame(cols)
        part['i'] = i.values
        part['j'] = j.values
        part['visit'] = v
        frames.append(part)
    out = pd.concat(frames, ignore_index=True)
    # Treat 0 as missing for amounts (matches the legacy script's convention).
    out = out.replace({'': pd.NA, 0: np.nan})
    return out


####################
# Purchases (Section 9B) -> s='purchased'
####################
# Food codes (freqcd) stay numeric for the harmonize_food lookup, so read the
# numeric view; read the categorical view of the full g7sec9b.dta (NOT _small,
# which carries no value labels) only to decode the unit columns.
purch_labels = get_categorical_mapping(
    tablename='harmonize_food',
    idxvars={'Code': ('Code_9b', format_id)}, **{'Label': 'Preferred Label'})

num9b = get_dataframe('../Data/g7sec9b.dta', convert_categoricals=False)
cat9b = get_dataframe('../Data/g7sec9b.dta', convert_categoricals=True)
fe = _stack_visits(num9b, cat9b, range(1, 7),
                   qty_stem='s9bq{v}b', unit_stem='s9bq{v}c',
                   label_map=purch_labels, item_col='freqcd',
                   exp_stem='s9bq{v}a')
fe['s'] = 'purchased'


####################
# Home production (Section 8H) -> s='produced'
####################
prod_labels = get_categorical_mapping(
    tablename='harmonize_food',
    idxvars={'Code': ('Code_8h', format_id)}, **{'Label': 'Preferred Label'})

num8h = get_dataframe('../Data/g7sec8h.dta', convert_categoricals=False)
cat8h = get_dataframe('../Data/g7sec8h.dta', convert_categoricals=True)
hp = _stack_visits(num8h, cat8h, range(3, 9),
                   qty_stem='s8hq{v}q', unit_stem='s8hq{v}u',
                   label_map=prod_labels, item_col='foodcd',
                   price_stem='s8hq{v}p')
hp['s'] = 'produced'


####################
# Combine, harmonize, canonicalize
####################
df = pd.concat([fe, hp], ignore_index=True)

# Drop rows whose food code did not harmonize (no '' / NA j leakage; D6).
df = df[df['j'].notna() & (df['j'] != '')]

# LCU-only convention: a monetary value with no physical quantity/unit -> u='Value'
# with Quantity = Expenditure (food-acquired skill).  2016-17 records quantities,
# so this is typically empty, but apply it where it occurs.
if 'Expenditure' in df.columns:
    value_only = df['Quantity'].isna() & df['u'].isna() & df['Expenditure'].notna()
    df.loc[value_only, 'u'] = 'Value'
    df.loc[value_only, 'Quantity'] = df.loc[value_only, 'Expenditure']

df['t'] = t

# Sentinel guard: some Stata extracts use a huge value for missing numerics.
num = df.select_dtypes(exclude=['object', 'category'])
if len(num.columns):
    na = num.max().max()
    if pd.notna(na) and na > 1e99:
        warnings.warn(f"Large number used for missing?  Replacing {na} with NaN.")
        df = df.replace(na, np.nan)

idx = ['t', 'i', 'j', 'u', 's', 'visit']
value_cols = [c for c in ['Quantity', 'Expenditure', 'Price'] if c in df.columns]

# Collapse duplicate (t,i,j,u,s,visit) rows (multiple source records of the same
# item/unit within a visit) by summing the amounts.
df = df.groupby(idx, dropna=False)[value_cols].sum(min_count=1)

df = df.dropna(how='all')

if __name__ == '__main__':
    to_parquet(df, 'food_acquired.parquet')
