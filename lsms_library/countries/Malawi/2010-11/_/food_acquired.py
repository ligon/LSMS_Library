#!/usr/bin/env python
"""Malawi 2010-11 food_acquired -- wide-form extraction + canonical melt.

The wave script extracts per-source quantities and units (purchased /
own-produced / in-kind) from ``hh_mod_g1.dta``, applies Malawi's
region-keyed unit-conversion CSV (``ihs3_conversions.csv``) and the
inline ``"300 grams"``-style fallback once per source, then hands the
wide-form DataFrame to :func:`malawi.food_acquired_to_canonical` for
the long-form melt onto the ``s`` (acquisition source) axis and the
legacy ``j↔i`` swap.

Phase 3 of GH #169 / DESIGN_food_acquired_canonical_2026-05-05.org.
Pilot wave for the four-wave fan-out (2004-05, 2013-14, 2016-17,
2019-20 follow this template, with per-wave column-code differences).
"""
from lsms_library.local_tools import to_parquet
from lsms_library.local_tools import get_dataframe

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
from malawi import (conversion_table_matching, food_acquired_to_canonical,
                    normalize_food_label)

wave = "2010-11"

df = get_dataframe('../Data/Full_Sample/Household/hh_mod_g1.dta',
                   convert_categoricals=True)

conversions = pd.read_csv('ihs3_conversions.csv')

# Read region directly from household module for conversion table merge.
# 2010-11 has no 'region' column.  hh_a01 is the district *name* (string,
# e.g. "Chitipa"), not a numeric district code; derive Region from the
# first character of case_id instead (1=North, 2=Central, 3=Southern).
hh = get_dataframe('../Data/Full_Sample/Household/hh_mod_a_filt.dta')
_region_map = {'1': 'North', '2': 'Central', '3': 'Southern'}
regions = (hh[['case_id']].drop_duplicates()
           .assign(region=lambda d: d['case_id'].astype(str).str[0]
                   .map(_region_map))
           .dropna(subset=['region'])
           .set_index('case_id')['region'])
regions.index.name = 'j'
regions.name = 'm'

columns_dict = {
    'case_id': 'j', 'hh_g02': 'i',
    'hh_g03a': 'quantity_consumed', 'hh_g03b': 'unitcode_consumed',
    'hh_g03b_os': 'unitsdetail_consumed',
    'hh_g05': 'expenditure',
    'hh_g04a': 'quantity_bought',  'hh_g04b': 'unitcode_bought',
    'hh_g04b_os': 'unitsdetail_bought',
    'hh_g06a': 'quantity_produced', 'hh_g06b': 'unitcode_produced',
    'hh_g06b_os': 'unitsdetail_produced',
    'hh_g07a': 'quantity_gifted',   'hh_g07b': 'unitcode_gifted',
    'hh_g07b_os': 'unitsdetail_gifted',
}

df = df.rename(columns_dict, axis=1)
df = df.loc[:, list(columns_dict.values())]
df['i'] = normalize_food_label(df['i'].astype(str).str.capitalize())

cols = df.loc[:, ['quantity_consumed', 'expenditure', 'quantity_bought',
                  'quantity_produced', 'quantity_gifted']].columns
df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')


match_df, D = conversion_table_matching(df, conversions,
                                        conversion_label_name='item_name')
conversions['item_name'] = conversions['item_name'].map(D)

df = df.set_index(['j', 'i'])
df = df.join(regions).replace(r'^\s*$', pd.NA, regex=True)

# Uppercase unit codes for the (region, item, unit) merge below.
for src in ('consumed', 'bought', 'produced', 'gifted'):
    col = f'unitcode_{src}'
    df[col] = df[col].str.upper()

conversions = conversions.set_index(['region', 'item_name', 'unit_code'])

# Region-keyed unit conversion.  Merge once per source.
df = df.reset_index()
for src in ('consumed', 'bought', 'produced', 'gifted'):
    df = df.merge(
        conversions, how='left',
        left_on=['i', 'm', f'unitcode_{src}'],
        right_on=['item_name', 'region', 'unit_code'],
    ).rename({'factor': f'cfactor_{src}'}, axis=1)
df = df.set_index(['j', 'i'])

# Inline "300 grams"-style fallback per source.  Same regex as 2004-05.
grams_re = r'(\d+)\s*g(?:\s+|r)'
kgs_re = r'(\d+)\s*k(?:g|ilo)'

for src in ('consumed', 'bought', 'produced', 'gifted'):
    detail_col = f'unitsdetail_{src}'
    cfactor_col = f'cfactor_{src}'
    quant_col = f'quantity_{src}'
    u_col = f'u_{src}'
    code_col = f'unitcode_{src}'

    detail_lower = df[detail_col].astype(str).str.lower()
    fallback = pd.concat([
        detail_lower.str.extract(grams_re).astype(float) * 0.01,
        detail_lower.str.extract(kgs_re).astype(float),
    ], axis=0).dropna()

    # Fill missing cfactor from inline regex extraction.
    df[cfactor_col] = df.apply(lambda x, c=cfactor_col, f=fallback:
                               x[c] or f, axis=1)
    # Apply conversion factor (NaN cfactor -> identity).
    df[quant_col] = df[quant_col].mul(df[cfactor_col].fillna(1))
    # Canonical unit string: 'kg' if a cfactor was applied, else the
    # raw unit-detail text, falling back to the unitcode if absent.
    df[u_col] = np.where(~df[cfactor_col].isna(), 'kg', df[detail_col])
    df[u_col] = df[u_col].replace('nan', pd.NA).fillna(df[code_col])

df['t'] = wave
df = df.reset_index()

# Hand off to the canonical-melt + i/j-swap helper.
out = food_acquired_to_canonical(df.set_index(['j', 't', 'i']), wave=wave)
to_parquet(out, "food_acquired.parquet")
