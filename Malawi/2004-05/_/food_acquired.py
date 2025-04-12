from lsms_library.local_tools import to_parquet
from lsms_library.local_tools import get_dataframe
#!/usr/bin/env python

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
import json
import dvc.api
from lsms import from_dta

with dvc.api.open('../Data/sec_i.dta', mode='rb') as dta:
    df = from_dta(dta, convert_categoricals=True)

columns_dict = {'case_id': 'j', 'i0a' : 'i', 'i03a': 'quantity_consumed', 'i03b' : 'u_consumed',
                'i05': 'expenditure', 'i04a': 'quantity_bought', 'i04b' : 'u_bought',
                'i06a': 'quantity_produced', 'i06b' : 'u_produced',
                'i07a': 'quantity_gifted', 'i07b' : 'u_gifted'
                }
regions = get_dataframe('other_features.parquet').reset_index().set_index(['j'])['m']

df = df.astype(str).apply(lambda x: x.astype(str).str.lower()).replace('nan', np.NaN)
df = df.rename(columns_dict, axis=1)
df = df.loc[:, list(columns_dict.values())]

cols = df.loc[:, ['quantity_consumed', 'expenditure', 'quantity_bought',
                  'quantity_produced', 'quantity_gifted']].columns
df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')

df = df.set_index(['j', 'i'])
df = df.join(regions).set_index('m', append=True).replace(r'^\s*$', np.nan, regex=True)

df = df.reset_index().set_index(['j', 'm', 'i']).replace(r'^\s*$', np.NaN, regex=True)

#custom convert some units in formats such as "300 grams" into kg, typically handled by handling_unusual_units in malawi.py for data with conversion tables
grams = r'(\d+)\s*g(?:\s+|r)'
kgs =r'(\d+)\s*k(?:g|ilo)'

conv_kgrams_consumed = pd.concat([df['u_consumed'].str.extract(grams).astype(float)*0.01,
                                  df['u_consumed'].str.extract(kgs).astype(float)], axis= 0).dropna()
conv_kgrams_bought = pd.concat([df['u_bought'].str.extract(grams).astype(float)*0.01,
                                df['u_bought'].str.extract(kgs).astype(float)], axis=0).dropna()

df['cfactor_consumed'] = conv_kgrams_consumed
df['cfactor_bought'] = conv_kgrams_bought

df["quantity_consumed"] = df['quantity_consumed'].mul(df['cfactor_consumed'].fillna(1))
df["quantity_bought"] = df['quantity_bought'].mul(df['cfactor_bought'].fillna(1))

df['u_consumed'] = np.where(~df['cfactor_consumed'].isna(), 'kg', df['u_consumed'])
df['u_bought'] = np.where(~df['cfactor_bought'].isna(), 'kg', df['u_bought'])

#prices
df['price per unit'] = df['expenditure']/df['quantity_bought']

df['t'] = '2004-05'
df = df.reset_index().set_index(['j','t','i']).dropna(how='all')

final = df.loc[:, ['quantity_consumed', 'u_consumed', 'quantity_bought', 'u_bought', 'price per unit', 'expenditure', 'cfactor_consumed', 'cfactor_bought']]
to_parquet(final, "food_acquired.parquet")
