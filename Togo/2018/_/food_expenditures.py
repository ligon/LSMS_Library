#!/usr/bin/env python
from lsms_library.local_tools import to_parquet
import sys
sys.path.append('../../_')
import numpy as np
import pandas as pd
import json
import dvc.api
from lsms import from_dta
from cfe.df_utils import broadcast_binary_op

with dvc.api.open('../Data/Togo_survey2018_fooditems_forEthan.dta',mode='rb') as dta:
    food = from_dta(dta)

vars={'hhid': 'j',
      's07bq01' : 'i',
      's07bq02' : 'last week?', # Has your household consumed [XX] in the past 7 days. 
      's07bq03a' : 'c',    # What is the total quantity of [XX] consumed in the past 7 days. Quantity. 
      's07bq03b' : 'unit', # What is the total quantity of [XX] consumed in the past 7 days. Unit
      's07bq03c' : 'unit modifier', # What is the total quantity of [XX] consumed in the past 7 days. Unit size. 
      's07bq04' : 'home produced',  # What quantity of [XX] was home produced? (In the same unit)
      's07bq05' : 'gift received', # What quantity of [XX] comes from a gift? (In the same unit)
      's07bq06' : 'last purchase',  # When was the last time you bought [XX]?
      's07bq07a' : 'purchased quantity', # What quantity of [XX] did you buy last time? Quantity
      's07bq07b' : 'purchased unit', # What quantity of [XX] did you buy last time? Unit
      's07bq07c' : 'purchased unit modifier', # What quantity of [XX] did you buy last time? Unit size
      's07bq08': 'purchase value', # What was the value of [XX] bought the last time?
      'region_survey': 'm',
      'vague':'t'
      }

food = food.rename(columns=vars).set_index(['j','t','m','i','unit','unit modifier'])

# Get prices implied last purchase in previous 30 days

purchases = food.reset_index().set_index(['j','t','m','i'])[['purchase value','purchased quantity','purchased unit','purchased unit modifier']]

purchases['unit value'] = purchases['purchase value']/purchases['purchased quantity']

unit_values = purchases.groupby(['t','m','i','purchased unit','purchased unit modifier']).median()['unit value'].dropna()

c = food['c'].unstack(['t','m','i','unit','unit modifier'])

idx = list(set(c.columns.tolist()).intersection(unit_values.index.tolist()))

c = c[idx].stack(['t','m','i','unit','unit modifier'])

p = unit_values[idx]
p.index.names = ['t','m','i','unit','unit modifier']

x = broadcast_binary_op(c,lambda x,y: x*y, p)

x = x.groupby(['j','t','m','i']).sum().dropna().reset_index()

x['j'] = x['j'].astype(int).astype(str)
x['t'] = x['t'].astype(int).astype(str)

x = x.set_index(['j','t','m','i']).squeeze()

x = x.unstack('i')

labels = json.load(open('food_items.json'))

x = x.rename(columns=labels)
x = x.groupby('i',axis=1).sum()

x = x.replace(0,np.nan)


x = x.reset_index().set_index(['j','t','m'])

x = x.iloc[:,2:] # Drop two funky columns with numeric labels

x = x.drop_duplicates()

to_parquet(x, 'food_expenditures.parquet',compression='gzip')


