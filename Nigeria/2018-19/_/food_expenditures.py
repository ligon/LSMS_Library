from lsms_library.local_tools import to_parquet
#!/usr/bin/env python
import sys
import numpy as np
import pandas as pd
import json
import dvc.api
from cfe.df_utils import broadcast_binary_op
from units import conv, unitcodes

# Replace t
conv = conv.groupby(['m','i','u']).mean()
conv = pd.DataFrame({'2019Q1':conv,
                     '2018Q3':conv})
conv.columns.name = 't'
conv = conv.stack('t')

def rectify(food,units=unitcodes,conv=conv):

    food['m'] = food['m'].replace({1:'North central',
                                   2:'North east',
                                   3:'North west',
                                   4:'South east',
                                   5:'South south',
                                   6:'South west'})

    food['u'] = food['u'].replace(units)
    food['u'] = food['u'].fillna('None')
    food['other unit'] = food['other unit'].fillna('None')

    food['purchased unit'] = food['purchased unit'].replace(units)
    food['purchased unit'] = food['purchased unit'].fillna('None')
    food['purchased other unit'] = food['purchased other unit'].fillna('None')

    food.set_index(['j','t','m','i','u'],inplace=True)

    # Get prices implied by purchases

    purchases = food.reset_index().set_index(['j','t','m','i'])[['purchased value','purchased quantity','purchased unit']]
    #purchases['purchased other unit'] = purchases['purchased other unit'].fillna('None')

    purchases['unit value'] = purchases['purchased value']/purchases['purchased quantity']

    unit_values = purchases[['purchased unit','unit value']].dropna()
    unit_values.rename(columns={'purchased unit':'u'},inplace=True)
    unit_values = unit_values.reset_index().set_index(['j','t','m','i','u']).squeeze()
    p_per_kg = broadcast_binary_op(unit_values,lambda x,y:x/y,conv)

    p_per_kg = p_per_kg.loc[p_per_kg.index.isin(conv.index.levels[2].tolist(),level='u')]

    p_per_kg = p_per_kg.dropna()

    p = p_per_kg.squeeze().groupby(['t','m','i']).median()

    n = p_per_kg.squeeze().groupby(['t','m','i']).count()
    
    c = food['c']

    c_in_kg = broadcast_binary_op(c,lambda x,y:x*y,conv)

    c_in_kg = c_in_kg.loc[c_in_kg.index.isin(conv.index.levels[2].tolist(),level='u')]

    c_in_kg = c_in_kg.dropna()

    c = c_in_kg.squeeze().groupby(['j','t','m','i']).sum()

    return c,p,n

C = []
P = []
N = []

lbls = json.load(open('../../_/food_items.json'))

#########################
# Harvest
t = '2019Q1'

with dvc.api.open('../Data/sect10b_harvestw4.csv',mode='rb') as csv:
    harvest = pd.read_csv(csv)

vars={'hhid': 'j',
      'item_cd' : 'i',
      's10bq1' : 'last week?', # Has your household consumed [XX] in the past 7 days. 
      's10bq2a' : 'c',    # What is the total quantity of [XX] consumed in the past 7 days. Quantity. 
      's10bq2b' : 'u', # What is the total quantity of [XX] consumed in the past 7 days. Unit
      's10bq2c' : 'unit size',
      's10bq2b_os':'other unit',
      's10bq10'  : 'purchased value', 
      's10bq9a' : 'purchased quantity',
      's10bq9b' : 'purchased unit',
      's10bq9b_os':'purchased other unit',
      's10bq9c' : 'purchased unit size',
      'zone': 'm',
      'sector':'rural' # 1=Urban; 2=Rural
      }

harvest = harvest.rename(columns=vars)

harvest['t'] = t

harvest = harvest.replace({'i':{int(k):v for k,v in lbls[t].items()}})

c,p,n = rectify(harvest)

C.append(c)
P.append(p)
N.append(n)

##################
# Planting (2018Q3)

t = '2018Q3'

with dvc.api.open('../Data/sect7b_plantingw4.csv',mode='rb') as csv:
    planting = pd.read_csv(csv)

vars={'hhid': 'j',
      'item_cd' : 'i',
      's7bq1' : 'last week?', # Has your household consumed [XX] in the past 7 days. 
      's7bq2a' : 'c',    # What is the total quantity of [XX] consumed in the past 7 days. Quantity. 
      's7bq2b' : 'u', # What is the total quantity of [XX] consumed in the past 7 days. Unit
      's7bq2c' : 'unit size',
      's7bq2b_os':'other unit',
      's7bq10'  : 'purchased value', 
      's7bq9a' : 'purchased quantity',
      's7bq9b' : 'purchased unit',
      's7bq9b_os':'purchased other unit',
      's7bq9c' : 'purchased unit size',
      'zone': 'm',
      'sector':'rural' # 1=Urban; 2=Rural
      }

planting = planting.rename(columns=vars)
planting = planting[vars.values()]

planting['t'] = t
planting = planting.replace({'i':{int(k):v for k,v in lbls[t].items()}})

use = planting[['c','purchased value']].dropna(how='all').index

c,p,n = rectify(planting.loc[use])

C.append(c)
P.append(p)
N.append(n)
#####################

c = pd.concat(C,axis=0)
p = pd.concat(P,axis=0)

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
