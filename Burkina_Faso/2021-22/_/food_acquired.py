#!/usr/bin/env python

import sys
sys.path.append('../../_/')
sys.path.append('../../../_')
from local_tools import to_parquet
import pandas as pd
import numpy as np
import dvc.api
from lsms import from_dta

#data recorded for two periods: seven days before the survey and thirty days before the survey.

with dvc.api.open('../Data/s07b_me_bfa2021.dta', mode='rb') as dta:
    df = from_dta(dta, convert_categoricals=True, encoding='iso-8859-1')

df["j"] = df["grappe"].astype(int).astype(str) + '-'  + df["menage"].astype(int).astype(str) #concatenate menage and grappe

df['t'] = df['vague'].map({1: '2021', 2:'2022'})

df = df.rename({"s07bq01": "i", "s07bq03a" : "quantity", "s07bq03b" : "units", "s07bq08" : "total expenses", "s07bq07a" : "amount bought"}, axis = 1)
df['price per unit'] = df['total expenses']/df['amount bought']
df = df.loc[:, ['j', 'i', 'quantity', 'units', 'total expenses', 'price per unit', 't']]

df = df.set_index(['j', 'i', 't'])
#inspect missing encoding for units
df = df.dropna(subset=['quantity', 'total expenses', 'price per unit'])

to_parquet(df,'food_acquired.parquet')
