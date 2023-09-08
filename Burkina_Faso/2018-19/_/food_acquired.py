#!/usr/bin/env python

import sys
sys.path.append('../../_/')
from burkina_faso import age_sex_composition
import pandas as pd
import numpy as np
import json
import dvc.api
from lsms import from_dta

with dvc.api.open('../Data/s07b_me_bfa2018.dta', mode='rb') as dta:
    df = from_dta(dta, convert_categoricals=True)

df["j"] = df["grappe"].astype(int).astype(str) + '-'  + df["menage"].astype(int).astype(str) #concatenate menage and grappe

df['t'] = df['vague']
df.loc[df['vague'] == 1.0, 't'] = '2018'
df.loc[df['vague'] == 2.0, 't'] = '2019'


df = df.rename({"s07bq01": "i", "s07bq03a" : "quantity", "s07bq03b" : "units", "s07bq08" : "total expenses", "s07bq07a" : "amount bought"}, axis = 1)
df['price per unit'] = df['total expenses']/df['amount bought']
df = df.loc[:, ['j', 'i', 'quantity', 'units', 'total expenses', 'price per unit', 't']]

df = df.set_index(['j', 'i', 't'])
df.to_parquet('food_acquired.parquet')
