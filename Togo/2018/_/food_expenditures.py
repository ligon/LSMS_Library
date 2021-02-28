#!/usr/bin/env python
import sys
sys.path.append('../../_')
from togo import food_expenditures
import numpy as np
import pandas as pd
import json
from cfe.df_utils import broadcast_binary_op

food = pd.read_stata('../Data/Togo_survey2018_fooditems_forEthan.dta')

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
      's07bq08': 'purchase value' # What was the value of [XX] bought the last time?
      }

food = food.rename(columns=vars).set_index(['j','i','unit','unit modifier'])

# Get prices implied last purchase in previous 30 days

purchases = food.reset_index().set_index(['j','i'])[['purchase value','purchased quantity','purchased unit','purchased unit modifier']]

purchases['unit value'] = purchases['purchase value']/purchases['purchased quantity']

unit_values = purchases.groupby(['i','purchased unit','purchased unit modifier']).median()['unit value'].dropna()

c = food['c'].unstack(['i','unit','unit modifier'])

idx = list(set(c.columns.tolist()).intersection(unit_values.index.tolist()))

c = c[idx].stack(['i','unit','unit modifier'])

p = unit_values[idx]
p.index.names = ['i','unit','unit modifier']

x = broadcast_binary_op(c,lambda x,y: x*y, p)

x = x.groupby(['j','i']).sum().dropna().reset_index()

x['j'] = x['j'].astype(int).astype(str)

x = x.set_index(['j','i']).squeeze()

x = x.unstack('i')

labels = json.load(open('food_items.json'))

x = x.rename(columns=labels)
x = x.groupby('i',axis=1).sum()

x = x.replace(0,np.nan)

region =  pd.read_stata('../Data/Togo_survey2018_fooditems_forEthan.dta').set_index('hhid')['region_survey']
region.index.name = 'j'
region = region.groupby('j').head(1)
region = region.reset_index('j')
region['j'] = region['j'].astype(int).astype(str)
region = region.set_index('j').squeeze()
region.name = 'm'

x = x.join(region,how='left')
x['t'] = 2018

x = x.reset_index().set_index(['j','t','m'])

x = x.iloc[:,2:] # Drop two funky columns with numeric labels

x = x.drop_duplicates()

x.to_parquet('food_expenditures.parquet')


