#!/usr/bin/env python

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
import json
import dvc.api
from lsms import from_dta
from malawi import clean_text

with dvc.api.open('../Data/Full_Sample/Household/hh_mod_g1.dta', mode='rb') as dta:
    df = from_dta(dta, convert_categoricals=True)

conversions = pd.read_csv('ihs3_conversions.csv')
regions = pd.read_parquet('other_features.parquet').reset_index().set_index(['j'])['m']

columns_dict = {'case_id': 'j', 'hh_g02' : 'i', 'hh_g03a': 'quantity_consumed', 'hh_g03b' : 'unitcode_consumed', 'hh_g03b_os': 'unitsdetail_consumed',
                'hh_g05': 'expenditure', 'hh_g04a': 'quantity_bought', 'hh_g04b': 'unitcode_bought', 'hh_g04b_os': 'unitsdetail_bought',
                'hh_g06a': 'quantity_produced', 'hh_g06b': 'unitcode_produced', 'hh_g06b_os': 'unitsdetail_produced',
                'hh_g07a': 'quantity_gifted', 'hh_g07b': 'unitcode_gifted', 'hh_g07b_os': 'unitsdetail_gifted',
                }

df = df.rename(columns_dict, axis=1)
df = df.loc[:, list(columns_dict.values())]
df['i'] = df['i'].astype(str).apply(clean_text)

cols = df.loc[:, ['quantity_consumed', 'expenditure', 'quantity_bought',
                  'quantity_produced', 'quantity_gifted']].columns
df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')

df = df.set_index(['j', 'i'])
df = df.join(regions).set_index('m', append=True).replace(r'^\s*$', np.nan, regex=True)

conversions['item_name'] = conversions['item_name'].apply(clean_text)
conversions = conversions.set_index(['region', 'item_name', 'unit_code'])
df = df.reset_index().merge(conversions, how='left', left_on=['i', 'm', 'unitcode_consumed'], right_on=['item_name', 'region', 'unit_code']).rename({'factor' : 'cfactor_consumed'}, axis=1)
df = df.merge(conversions, how='left', left_on=['i', 'm', 'unitcode_bought'], right_on=['item_name', 'region', 'unit_code']).rename({'factor' : 'cfactor_bought'}, axis = 1)
df = df.set_index(['j', 'm', 'i'])

grams = r'(\d+)\s*g(?:\s+|r)'
kgs =r'(\d+)\s*k(?:g|ilo)'

conv_kgrams_consumed = pd.concat([df['unitsdetail_consumed'].str.lower().str.extract(grams).astype(float)*0.01,
                                  df['unitsdetail_consumed'].str.lower().str.extract(kgs).astype(float)], axis= 0).dropna()
conv_kgrams_bought = pd.concat([df['unitsdetail_bought'].str.lower().str.extract(grams).astype(float)*0.01,
                                df['unitsdetail_bought'].str.lower().str.extract(kgs).astype(float)], axis=0).dropna()

df['cfactor_consumed'] = df.apply(lambda x: x['cfactor_consumed'] or conv_kgrams_consumed, axis = 1)
df['cfactor_bought'] = df.apply(lambda x: x['cfactor_bought'] or conv_kgrams_bought, axis = 1)

df["quantity_consumed"] = df['quantity_consumed'].mul(df['cfactor_consumed'].fillna(1))
df["quantity_bought"] = df['quantity_bought'].mul(df['cfactor_bought'].fillna(1))

df['u_consumed'] = np.where(~df['cfactor_consumed'].isna(), 'kg', df['unitsdetail_consumed'])
df['u_consumed'] = df['u_consumed'].replace('nan', np.NaN).fillna(df['unitcode_consumed'])
df['u_bought'] = np.where(~df['cfactor_bought'].isna(), 'kg', df['unitsdetail_bought'])
df['u_bought'] = df['u_bought'].replace('nan', np.NaN).fillna(df['unitcode_bought'])

df['price per unit'] = df['expenditure']/df['quantity_bought']

final = df.loc[:, ['quantity_consumed', 'u_consumed', 'quantity_bought', 'u_bought', 'price per unit', 'expenditure', 'cfactor_consumed', 'cfactor_bought']]
final.to_parquet("food_acquired.parquet")
