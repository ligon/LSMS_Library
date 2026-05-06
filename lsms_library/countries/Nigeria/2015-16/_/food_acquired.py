#!/usr/bin/env python
"""Extract raw food acquisition data for Nigeria 2015-16.

Source files:
  - sect10b_harvestw3.csv  (post-harvest, 2016Q1)
  - sect7b_plantingw3.csv  (post-planting, 2015Q3)
"""
import sys
import numpy as np
import pandas as pd

sys.path.append('../../../_/')
from lsms_library.local_tools import to_parquet, get_categorical_mapping, get_dataframe
from lsms_library.transformations import food_acquired_to_canonical

food_labels = get_categorical_mapping(tablename='harmonize_food',
                                       idxvars='Code',
                                       **{'Preferred Label': 'Preferred Label'})

_unit_raw = get_categorical_mapping(tablename='unit',
                                     idxvars='Code',
                                     **{'2015-16': '2015-16'})
unitcodes = {k: v for k, v in _unit_raw.items() if isinstance(v, str)}

zone_labels = {1: 'North central',
               2: 'North east',
               3: 'North west',
               4: 'South east',
               5: 'South south',
               6: 'South west'}


def extract_food(fn, varmap, t, food_labels):
    """Read a food consumption file and produce a standardized DataFrame."""
    df = get_dataframe(fn)

    df = df.rename(columns=varmap)

    df['j'] = df['j'].replace(food_labels)
    df['u'] = df['u'].replace(unitcodes).fillna('None')
    df['m'] = df['m'].replace(zone_labels)

    df['t'] = t

    keep = ['t', 'i', 'j', 'u', 'm',
            'Quantity', 'Expenditure', 'Produced']
    keep = [c for c in keep if c in df.columns]
    df = df[keep]

    value_cols = [c for c in ['Quantity', 'Expenditure', 'Produced'] if c in df.columns]
    df = df.replace('', pd.NA)
    df = df.dropna(subset=value_cols, how='all')

    return df


# --- Harvest (2016Q1) ---
harvest_vars = {
    'hhid': 'i',
    'item_cd': 'j',
    's10bq2a': 'Quantity',
    's10bq2b': 'u',
    's10bq4': 'Expenditure',
    's10bq5a': 'Produced',
    'zone': 'm',
}
harvest = extract_food('../Data/sect10b_harvestw3.csv',
                        harvest_vars, '2016Q1', food_labels)

# --- Planting (2015Q3) ---
planting_vars = {
    'hhid': 'i',
    'item_cd': 'j',
    's7bq2a': 'Quantity',
    's7bq2b': 'u',
    's7bq4': 'Expenditure',
    's7bq5a': 'Produced',
    'zone': 'm',
}
planting = extract_food('../Data/sect7b_plantingw3.csv',
                         planting_vars, '2015Q3', food_labels)

# --- Combine and index ---
df = pd.concat([harvest, planting], ignore_index=True)

df['i'] = df['i'].astype(int).astype(str)

df = df.set_index(['t', 'i', 'j', 'u'])
df = df.sort_index()

# Canonical reshape (GH #169): drop `m` (lives in cluster_features) and
# melt (Quantity, Produced, Expenditure) onto the `s` (acquisition source)
# axis.  `v` is a required input to food_acquired_to_canonical but does
# NOT belong in the wave-level parquet (it's joined at API time from
# sample()), so we add a placeholder and strip it from the output index.
if 'm' in df.columns:
    df = df.drop(columns=['m'])
df = df.reset_index()
df['v'] = pd.NA
df = food_acquired_to_canonical(df.set_index(['t', 'v', 'i', 'j', 'u']),
                                drop_columns=())
df = df.reset_index('v', drop=True)
df = df.sort_index()

to_parquet(df, 'food_acquired.parquet')
