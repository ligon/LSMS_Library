#!/usr/bin/env python

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
import json
import dvc.api
from lsms import from_dta

with dvc.api.open('../Data/hh_mod_g1.dta', mode='rb') as dta:
    df = from_dta(dta, convert_categoricals=True)

columns_dict = {'case_id': 'j', 'hh_g02' : 'i', 'hh_g06b_label' : 'units','hh_g03a' :'quantity','hh_g05': 'expenditure', 'hh_g04a': 'quantity_bought', 'hh_g04b_label': 'units_bought'}
df = df.rename(columns_dict, axis=1)

cols = df.loc[:, ['quantity', 'expenditure', 'quantity_bought']].columns
df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')

df = df.loc[:, list(columns_dict.values())]
df['price per unit'] = (df['expenditure'])/df['quantity_bought']

df['t'] = '2016-17'
df = df.set_index(['j','t', 'i'])

# Deal with some problematic units which are floats
df['units'] = df.units.astype(str)
df['units_bought'] = df.units_bought.astype(str)

df = df.dropna(how='all')

df.to_parquet("food_acquired.parquet")
