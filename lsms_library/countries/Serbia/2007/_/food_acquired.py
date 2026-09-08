#!/usr/bin/env python
"""Serbia 2007 food_acquired -> canonical (t, i, j, u, s).

Source: m5_1_diary.dta -- the LSMS 7-day food-consumption *diary*.  One row
per (household, product); each row carries up to seven daily triples:

  opstina + popkrug + dom   -> i  (household id; concatenation)
  proizvod                  -> j  (numeric product code 01xx-11xx; harmonized
                                   to a Preferred Label via
                                   categorical_mapping.org harmonize_food)
  mera                      -> u  (native unit: kg / gr / litar / komad, and
                                   `dinar` -> the canonical `Value`; see below)
  kol_1 .. kol_7            -> daily Quantity (in unit `mera`)
  din_1 .. din_7            -> daily value in dinars
  izvor_1 .. izvor_7        -> daily acquisition SOURCE (see `s` below)

`s` source -- VERIFIED from the source (per #218 brief).  The diary records a
per-day-entry `izvor` ("source/origin") flag, so `s` is NOT a blanket
'purchased'.  The Stata value labels are:

  izvor == 1  "bought in the shop"     -> s = 'purchased'
  izvor == 2  "own production"         -> s = 'produced'
  izvor == 3  "received as a gift"     -> s = 'inkind'
  izvor is NaN (kol==0, din>0)         -> s = 'purchased'

The izvor-NaN entries are exclusively the prefix-11 "food/drink away from home"
items (restaurant / work / school): pure cash outlays with no physical
quantity and no source flag, so they are purchased.  All three labeled sources
record an (imputed) dinar value, so Expenditure is meaningful for produced /
inkind rows too; we preserve it.

We sum kol -> Quantity and din -> Expenditure over the 7 diary days within each
canonical (t, i, j, u, s) key.  An optional `Price` is the median daily unit
value (din/kol) over the diary days; it is purely informational -- the derived
food_prices table is computed at API time by _FOOD_DERIVED.

`u` -- the currency code is canonicalised (GH #770).  `mera == 'dinar'` is not
a unit of measure: it is the diary's marker for a pure cash outlay, used by the
prefix-11 "food/drink away from home" items.  It is emitted as the framework's
canonical currency-denominated sentinel **`Value`**, NOT as the currency's
name.  The name of the currency is redundant with the country and the wave, and
`currency=` (lsms_library/currency.py) is the lever for currency
*representation*; `Value` is what `country._RESERVED_U_SENTINELS` and
`transformations._CURRENCY_DENOMINATED_UNITS` already recognise.  Spelled
`dinar`, the label was invisible to both, and
`transformations.conversion_to_kgs` inferred a kg-per-dinar factor of
4.209166724149965 for it -- currency served as mass.

Note that Serbia's `Value` rows do NOT follow the GhanaLSS/Panama value-only
convention of setting `Quantity = Expenditure`: 3,767 of the 3,772 carry
`Quantity` NaN with the cash amount in `Expenditure` alone (the remaining 5
carry a stray `kol` punch).  Both shapes exist in the corpus; the label means
"denominated in local currency", not "Quantity is the expenditure".  No
quantity is synthesised here -- that would be a data change, not a relabel.

`v` is omitted: script-path waves let _join_v_from_sample add the cluster at
API time (post-2026-04-10 design).
"""
import sys

import numpy as np
import pandas as pd

sys.path.append('../../../_/')
sys.path.append('.')
import mapping
from lsms_library.local_tools import to_parquet, get_dataframe, df_from_orgfile

t = '2007'

df = get_dataframe('../Data/m5_1_diary.dta', convert_categoricals=False)

# Household id = opstina + popkrug + dom.  Build it with the wave's mapping.i
# (hyphen-joined, leading zeros stripped via format_id) so the key matches the
# `i` produced by the YAML-path tables (sample, roster, assets); otherwise the
# _join_v_from_sample at API time finds 0 overlap and `v` is 100% NaN.
hh_cols = ['opstina', 'popkrug', 'dom']
df['i'] = df[hh_cols].apply(mapping.i, axis=1)

# Native unit (string) and the numeric product code for harmonization.
# `dinar` is currency, not a measure -> canonical `Value` sentinel (GH #770).
U_CANONICAL = {'dinar': 'Value'}
df['u'] = df['mera'].astype(str).replace(U_CANONICAL)
df['j'] = df['proizvod'].astype(str).str.strip()

# Melt the seven daily (kol, din, izvor) triples into long form.
frames = []
for k in range(1, 8):
    sub = df[['i', 'j', 'u', f'kol_{k}', f'din_{k}', f'izvor_{k}']].copy()
    sub = sub.rename(columns={f'kol_{k}': 'kol',
                              f'din_{k}': 'din',
                              f'izvor_{k}': 'izvor'})
    frames.append(sub)
L = pd.concat(frames, ignore_index=True)

for c in ['kol', 'din', 'izvor']:
    L[c] = pd.to_numeric(L[c], errors='coerce')

# Keep only diary entries that record consumption (quantity or value).
L = L[(L['kol'] > 0) | (L['din'] > 0)].copy()

# Map the per-entry source flag to the canonical s axis.
#   1 -> purchased, 2 -> produced, 3 -> inkind, NaN (meals away) -> purchased.
s_map = {1.0: 'purchased', 2.0: 'produced', 3.0: 'inkind'}
L['s'] = L['izvor'].map(s_map).fillna('purchased')

# Harmonize the numeric proizvod code to a Preferred Label via the
# harmonize_food table in categorical_mapping.org.
# Key on the clean numeric code: the textual `nsifra` field uses a legacy YUSCII
# transliteration ('|' for "đ") that cannot round-trip through an org table.
# GH #783: the vocabulary moved out of the standalone food_items.org into
# the country's categorical_mapping.org as `harmonize_food` -- the only file
# Country.categorical_mapping opens, so labels= can now see it too.
food_items = df_from_orgfile('../../_/categorical_mapping.org', name='harmonize_food', to_numeric=False)
food_items = food_items.loc[:, ['Preferred Label', 'proizvod']]
food_items['proizvod'] = food_items['proizvod'].str.strip()
food_items = food_items.replace(['', '---'], pd.NA).dropna()
code_to_label = food_items.set_index('proizvod')['Preferred Label'].str.strip().to_dict()
L['j'] = L['j'].replace(code_to_label)

# Daily unit value for an informational Price (NaN where quantity is 0).
L['daily_price'] = np.where(L['kol'] > 0, L['din'] / L['kol'], np.nan)

# Collapse to canonical (t, i, j, u, s): sum the additive measures over the
# diary days; carry the median daily unit value as Price.
L['t'] = t
out = (L.groupby(['t', 'i', 'j', 'u', 's'], dropna=False)
        .agg(Quantity=('kol', lambda x: x.sum(min_count=1)),
             Expenditure=('din', lambda x: x.sum(min_count=1)),
             Price=('daily_price', 'median'))
        .sort_index())

to_parquet(out, 'food_acquired.parquet')
