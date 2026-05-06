#!/usr/bin/env python
from lsms_library.local_tools import to_parquet
from lsms_library.local_tools import get_dataframe

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
import json
from malawi import food_acquired_to_canonical, normalize_food_label

wave = "2004-05"

df = get_dataframe('../Data/sec_i.dta', convert_categoricals=True)

columns_dict = {'case_id': 'j', 'i0a' : 'i', 'i03a': 'quantity_consumed', 'i03b' : 'u_consumed',
                'i05': 'expenditure', 'i04a': 'quantity_bought', 'i04b' : 'u_bought',
                'i06a': 'quantity_produced', 'i06b' : 'u_produced',
                'i07a': 'quantity_gifted', 'i07b' : 'u_gifted'
                }

df = df.astype(str).replace('nan', pd.NA)
df = df.rename(columns_dict, axis=1)
df = df.loc[:, list(columns_dict.values())]
# Normalize case + en-dash mojibake on the food-label column to match
# the form that apply_harmonize_food expects (same shape as other waves).
df['i'] = normalize_food_label(df['i'].astype(str).str.capitalize())

cols = df.loc[:, ['quantity_consumed', 'expenditure', 'quantity_bought',
                  'quantity_produced', 'quantity_gifted']].columns
df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')

df = df.set_index(['j', 'i']).replace(r'^\s*$', pd.NA, regex=True)

#custom convert some units in formats such as "300 grams" into kg, typically handled by handling_unusual_units in malawi.py for data with conversion tables
grams = r'(\d+)\s*g(?:\s+|r)'
kgs   = r'(\d+)\s*k(?:g|ilo)'
for src in ('consumed', 'bought', 'produced', 'gifted'):
    u_col = f'u_{src}'
    quant_col = f'quantity_{src}'
    cfactor_col = f'cfactor_{src}'
    lower = df[u_col].astype(str).str.lower()
    fallback = pd.concat([
        lower.str.extract(grams).astype(float) * 0.01,
        lower.str.extract(kgs).astype(float),
    ], axis=0).dropna()
    df[cfactor_col] = fallback
    df[quant_col] = df[quant_col].mul(df[cfactor_col].fillna(1))
    df[u_col] = np.where(~df[cfactor_col].isna(), 'kg', df[u_col])

df['t'] = wave
df = df.reset_index()
out = food_acquired_to_canonical(df.set_index(['j', 't', 'i']), wave=wave)
to_parquet(out, 'food_acquired.parquet')
