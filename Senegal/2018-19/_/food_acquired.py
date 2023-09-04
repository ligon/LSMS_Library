#!/usr/bin/env python

import sys
sys.path.append('../../_/')
sys.path.append('../../../_/')
import pandas as pd
import numpy as np
import json
import dvc.api
from lsms import from_dta
from local_tools import df_from_orgfile

with dvc.api.open('../Data/s07b_me_sen2018.dta', mode='rb') as dta:
    df = from_dta(dta, convert_categoricals=True)

df['j'] = df['vague'].astype(str) + df['grappe'].astype(str) + df['menage'].astype(str)
waves = {1: 2018, 2: 2019}
df['t'] = df['vague'].map(waves)

col = {'j': 'j', 't': 't', 's07bq01': 'i', 's07bq03a': 'quantity', 's07bq03b': 'units',
       's07bq08': 'last expenditure', 's07bq07a': 'last purchase quantity', 's07bq07b': 'last purchase units'}
df = df.rename(col, axis = 1).reset_index()

final = df.loc[:, list(col.values())]

# Get cleaned up names of units
final['units'] = final.units.astype(str)
final['last purchase units'] = final['last purchase units'].astype(str)

units = df_from_orgfile('../../_/units.org',name='unitlabels',encoding='ISO-8859-1')
unitsd = units.set_index('Code').squeeze().to_dict()
final = final.replace({'units':unitsd,'last purchase units':unitsd})
final['price'] = final['last expenditure']/final['last purchase quantity']

final = final.set_index(['j', 't', 'i'])
final.to_parquet('food_acquired.parquet')
