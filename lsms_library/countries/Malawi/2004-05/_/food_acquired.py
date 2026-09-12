#!/usr/bin/env python
from lsms_library.local_tools import to_parquet
from lsms_library.local_tools import get_dataframe

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
import json
from malawi import (food_acquired_to_canonical, normalize_food_label,
                    _extract_kg_conversion)

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

# IHS2 ships NO unit-conversion table (see Malawi/_/CONTENTS.org), so the only
# per-row kg factor available is a magnitude carried in the unit label itself.
# The label set here is closed and 22 values wide: '50kg bag' -> 50 and
# '90 kg bag' -> 90 are the only two that carry one.  Shared with the other
# waves' helper module rather than copied inline (GH #878, which also fixed
# that regex's grams factor: 0.01 -> 0.001).
for src in ('consumed', 'bought', 'produced', 'gifted'):
    df[f'cfactor_{src}'] = _extract_kg_conversion(df[f'u_{src}'])
    # Keep native quantity + native unit; food_acquired_to_canonical computes
    # the summable Quantity_kg = quantity x cfactor (GH #378 / Malawi migration).

df['t'] = wave
df = df.reset_index()
out = food_acquired_to_canonical(df.set_index(['j', 't', 'i']), wave=wave)
to_parquet(out, 'food_acquired.parquet')
