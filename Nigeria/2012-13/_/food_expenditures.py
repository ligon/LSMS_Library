from lsms_library.local_tools import to_parquet
#!/usr/bin/env python
import sys
sys.path.append('../../_')
import numpy as np
import pandas as pd
import json
import dvc.api
from cfe.df_utils import broadcast_binary_op
from units import conv, unitcodes

def rectify(food,units=unitcodes,conv=conv):

    food['m'] = food['m'].replace({1:'North central',
                                   2:'North east',
                                   3:'North west',
                                   4:'South east',
                                   5:'South south',
                                   6:'South west'})

    food['u'] = food['u'].replace(units)
    food['u'] = food['u'].fillna('None')

    food['purchased unit'] = food['purchased unit'].replace(units)
    food['purchased unit'] = food['purchased unit'].fillna('None')

    food['home produced unit'] = food['home produced unit'].replace(units)
    food['home produced unit'] = food['home produced unit'].fillna('None')

    food['gift unit'] = food['gift unit'].replace(units)
    food['gift unit'] = food['gift unit'].fillna('None')

    food.set_index(['j','t','m','i','u'],inplace=True)

    # Get prices implied by purchases

    purchases = food.reset_index().set_index(['j','t','m','i'])[['purchased value','purchased quantity','purchased unit']]

    purchases['unit value'] = purchases['purchased value']/purchases['purchased quantity']

    unit_values = purchases[['purchased unit','unit value']].dropna()
    unit_values.rename(columns={'purchased unit':'u'},inplace=True)
    unit_values = unit_values.reset_index().set_index(['j','t','m','i','u']).squeeze()
    p_per_kg = broadcast_binary_op(unit_values,lambda x,y:x/y,conv).dropna()

    p_per_kg = p_per_kg.loc[p_per_kg.index.isin(conv.index.levels[conv.index.names.index('u')].tolist(),level='u')]

    p = p_per_kg.squeeze().groupby(['t','m','i']).median()

    n = p_per_kg.squeeze().groupby(['t','m','i']).count()

    c = food['c']

    c_in_kg = broadcast_binary_op(c,lambda x,y:x*y,conv).dropna()

    c_in_kg = c_in_kg.loc[c_in_kg.index.isin(conv.index.levels[conv.index.names.index('u')].tolist(),level='u')]

    c = c_in_kg.squeeze().groupby(['j','t','m','i']).sum()

    return c,p,n

C = []
P = []
N = []

lbls = json.load(open('../../_/food_items.json'))

#########################
# Harvest(2013Q1)

with dvc.api.open('../Data/sect10b_harvestw2.csv',mode='rb') as csv:
    harvest = pd.read_csv(csv)

vars={'hhid': 'j',
      'item_cd' : 'i',
      's10bq1' : 'last week?', # Has your household consumed [XX] in the past 7 days. 
      's10bq2a' : 'c',    # What is the total quantity of [XX] consumed in the past 7 days. Quantity. 
      's10bq2b' : 'u', # What is the total quantity of [XX] consumed in the past 7 days. Unit
      's10bq3a' : 'purchased quantity', 
      's10bq3b' : 'purchased unit', 
      's10bq4'  : 'purchased value', 
      's10bq5a' : 'home produced quantity', 
      's10bq5b' : 'home produced unit', 
      's10bq6a' : 'gift quantity', 
      's10bq6b' : 'gift unit', 
      'zone': 'm',
      'sector':'rural' # 1=Urban; 2=Rural
      }

t = '2013Q1'
harvest = harvest.rename(columns=vars)

harvest['t'] = t
harvest = harvest.replace({'i':{int(k):v for k,v in lbls[t].items()}})

c,p,n = rectify(harvest)

C.append(c)
P.append(p)
N.append(n)
##################
# Planting (2012Q3)
with dvc.api.open('../Data/sect7b_plantingw2.csv',mode='rb') as csv:
    planting = pd.read_csv(csv)

vars={'hhid': 'j',
      'item_cd' : 'i',
      's7bq1' : 'last week?', # Has your household consumed [XX] in the past 7 days. 
      's7bq2a' : 'c',    # What is the total quantity of [XX] consumed in the past 7 days. Quantity. 
      's7bq2b' : 'u', # What is the total quantity of [XX] consumed in the past 7 days. Unit
      's7bq2c' : 'other unit', # Specify other unit
      's7bq3a' : 'purchased quantity', 
      's7bq3b' : 'purchased unit', 
      's7bq3c' : 'purchased other unit', 
      's7bq4'  : 'purchased value', 
      's7bq5a' : 'home produced quantity', 
      's7bq5b' : 'home produced unit', 
      's7bq5c' : 'produced other unit',
      's7bq6a' : 'gift quantity', 
      's7bq6b' : 'gift unit', 
      's7bq6c' : 'gift other unit',
      'zone': 'm',
      'sector':'rural' # 1=Urban; 2=Rural
      }

t = '2012Q3'
planting = planting.rename(columns=vars)

planting['t'] = t
planting = planting.replace({'i':{int(k):v for k,v in lbls[t].items()}})

c,p,n = rectify(planting)

C.append(c)
P.append(p)
N.append(n)
#####################

c = pd.concat(C,axis=0)
p = pd.concat(P,axis=0)
n = pd.concat(N,axis=0)

x = broadcast_binary_op(c,lambda x,y: x*y, p)

x = x.groupby(['j','t','m','i']).sum().dropna().reset_index()

x['j'] = x['j'].astype(int).astype(str)

x = x.set_index(['j','t','m','i']).squeeze()

x = x.replace(0,np.nan)

x = x.unstack('i')

x = x.groupby('i',axis=1).sum()

x = x.reset_index().set_index(['j','t','m'])

x = x.drop_duplicates()

to_parquet(x, 'food_expenditures.parquet',compression='gzip')

p = p.unstack('i')

to_parquet(p, 'unitvalues.parquet')

to_parquet(c.unstack('i'), 'food_quantities.parquet')
