#!/usr/bin/env python
from lsms_library.local_tools import to_parquet
from lsms_library.local_tools import get_dataframe, get_categorical_mapping

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
import json
from malawi import conversion_table_matching, food_acquired_to_canonical, normalize_food_label

wave = "2013-14"

df = get_dataframe('../Data/HH_MOD_G1_13.dta', convert_categoricals=True)

conversions = pd.read_csv('../../2010-11/_/ihs3_conversions.csv')

# Read region directly from household module for conversion table merge
hh = get_dataframe('../Data/HH_MOD_A_FILT_13.dta', convert_categoricals=True)
regions = hh[['y2_hhid', 'region']].drop_duplicates().set_index('y2_hhid')['region']
regions = regions.replace({'South': 'Southern'})
regions.index.name = 'j'
regions.name = 'm'

columns_dict = {'y2_hhid': 'j', 'hh_g02' : 'i', 'hh_g03a': 'quantity_consumed', 'hh_g03b' : 'unitcode_consumed',
                'hh_g05': 'expenditure', 'hh_g04a': 'quantity_bought', 'hh_g04b': 'unitcode_bought',
                'hh_g06a': 'quantity_produced', 'hh_g06b': 'unitcode_produced',
                'hh_g07a': 'quantity_gifted', 'hh_g07b': 'unitcode_gifted'
                }

df = df.rename(columns_dict, axis=1)
df = df.loc[:, list(columns_dict.values())]
df['i'] = normalize_food_label(df['i'].astype(str).str.capitalize())

unitsdetail_convertions = get_categorical_mapping(tablename='unit',
                                                  idxvars={'j':'Code'},
                                                **{'Label':'Unit'})

df['unitsdetail_consumed'] = df['unitcode_consumed'].astype(str).str.lower().map(unitsdetail_convertions).fillna(pd.NA)
df['unitsdetail_bought'] = df['unitcode_bought'].astype(str).str.lower().map(unitsdetail_convertions).fillna(pd.NA)
df['unitsdetail_produced'] = df['unitcode_produced'].astype(str).str.lower().map(unitsdetail_convertions).fillna(pd.NA)
df['unitsdetail_gifted'] = df['unitcode_gifted'].astype(str).str.lower().map(unitsdetail_convertions).fillna(pd.NA)

cols = df.loc[:, ['quantity_consumed', 'expenditure', 'quantity_bought',
                  'quantity_produced', 'quantity_gifted']].columns
df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')


match_df, D = conversion_table_matching(df, conversions, conversion_label_name = 'item_name')
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

# Inline "300 grams"-style fallback per source.
grams = r'(\d+)\s*g(?:\s+|r)'
kgs = r'(\d+)\s*k(?:g|ilo)'

for src in ('consumed', 'bought', 'produced', 'gifted'):
    detail_col = f'unitsdetail_{src}'
    cfactor_col = f'cfactor_{src}'
    quant_col = f'quantity_{src}'
    u_col = f'u_{src}'
    code_col = f'unitcode_{src}'

    detail_lower = df[detail_col].astype(str).str.lower()
    fallback = pd.concat([
        detail_lower.str.extract(grams).astype(float) * 0.01,
        detail_lower.str.extract(kgs).astype(float),
    ], axis=0).dropna()
    df[cfactor_col] = df.apply(lambda x, c=cfactor_col, f=fallback: x[c] or f, axis=1)
    df[quant_col] = df[quant_col].mul(df[cfactor_col].fillna(1))
    df[u_col] = np.where(~df[cfactor_col].isna(), 'kg', df[detail_col])
    df[u_col] = df[u_col].replace('nan', pd.NA).fillna(df[code_col])

df['t'] = wave
df = df.reset_index()
out = food_acquired_to_canonical(df.set_index(['j', 't', 'i']), wave=wave)
# 2013-14 source lacks the _os (other-specify) free-text column that 2010-11
# uses to disambiguate two rows with i='OTHER (SPECIFY)' for the same HH.
# Collapse the resulting duplicate canonical-index rows by summing.
out = out.groupby(level=out.index.names).sum(min_count=1)
to_parquet(out, 'food_acquired.parquet')
