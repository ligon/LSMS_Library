#!/usr/bin/env python
"""Extract raw food acquisition data for Nigeria 2018-19.

Produces item-level food consumption, purchase, and production data
before any unit conversion.  Downstream scripts derive food_expenditures,
food_prices, and food_quantities from this table.

Source files:
  - sect10b_harvestw4.csv  (post-harvest, 2019Q1)
  - sect7b_plantingw4.csv  (post-planting, 2018Q3)
"""
import sys
import numpy as np
import pandas as pd
import dvc.api

sys.path.append('../../../_/')
from lsms_library.local_tools import to_parquet, get_categorical_mapping

food_labels = get_categorical_mapping(tablename='harmonize_food',
                                       idxvars='Code',
                                       **{'Preferred Label': 'Preferred Label'})

_unit_raw = get_categorical_mapping(tablename='unit',
                                     idxvars='Code',
                                     **{'2018-19': '2018-19'})
unitcodes = {k: v for k, v in _unit_raw.items() if isinstance(v, str)}

zone_labels = {1: 'North central',
               2: 'North east',
               3: 'North west',
               4: 'South east',
               5: 'South south',
               6: 'South west'}


def extract_food(fn, varmap, t, food_labels):
    """Read a food consumption file and produce a standardized DataFrame."""
    with dvc.api.open(fn, mode='rb') as csv:
        df = pd.read_csv(csv)

    df = df.rename(columns=varmap)

    # Apply food item labels from categorical_mapping.org
    df['j'] = df['j'].replace(food_labels)

    # Apply unit labels and zone labels
    df['u'] = df['u'].replace(unitcodes).fillna('None')
    df['m'] = df['m'].replace(zone_labels)

    df['t'] = t

    # Keep relevant columns
    keep = ['t', 'i', 'j', 'u', 'm',
            'Quantity', 'Expenditure', 'Produced']
    # Only keep columns that exist (some may be absent in some waves)
    keep = [c for c in keep if c in df.columns]
    df = df[keep]

    # Drop rows with no data at all
    value_cols = [c for c in ['Quantity', 'Expenditure', 'Produced'] if c in df.columns]
    df = df.replace('', pd.NA)
    df = df.dropna(subset=value_cols, how='all')

    return df


# --- Harvest (2019Q1) ---
harvest_vars = {
    'hhid': 'i',
    'item_cd': 'j',
    's10bq2a': 'Quantity',         # total quantity consumed past 7 days
    's10bq2b': 'u',                # unit of consumption
    's10bq10': 'Expenditure',      # total value purchased
    's10bq5a': 'Produced',         # home produced quantity
    'zone': 'm',
}
harvest = extract_food('../Data/sect10b_harvestw4.csv',
                        harvest_vars, '2019Q1', food_labels)

# --- Planting (2018Q3) ---
planting_vars = {
    'hhid': 'i',
    'item_cd': 'j',
    's7bq2a': 'Quantity',
    's7bq2b': 'u',
    's7bq10': 'Expenditure',
    's7bq5a': 'Produced',
    'zone': 'm',
}
planting = extract_food('../Data/sect7b_plantingw4.csv',
                         planting_vars, '2018Q3', food_labels)

# --- Combine and index ---
df = pd.concat([harvest, planting], ignore_index=True)

# Convert ID to string for consistency with household_roster
df['i'] = df['i'].astype(int).astype(str)

df = df.set_index(['t', 'i', 'j', 'u'])

# Sort and save
df = df.sort_index()

to_parquet(df, 'food_acquired.parquet')
