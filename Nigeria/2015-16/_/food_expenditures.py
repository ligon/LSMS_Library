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
    food['other unit'] = food['other unit'].fillna('None')

    food['purchased unit'] = food['purchased unit'].replace(units)
    food['purchased unit'] = food['purchased unit'].fillna('None')
    food['purchased other unit'] = food['purchased other unit'].fillna('None')

    food['home produced unit'] = food['home produced unit'].replace(units)
    food['home produced unit'] = food['home produced unit'].fillna('None')
    food['produced other unit'] = food['produced other unit'].fillna('None')

    food['gift unit'] = food['gift unit'].replace(units)
    food['gift unit'] = food['gift unit'].fillna('None')
    food['gift other unit'] = food['gift other unit'].fillna('None')

    food.set_index(['j','t','m','i','u'],inplace=True)

    # Get prices implied by purchases

    purchases = food.reset_index().set_index(['j','t','m','i'])[['purchased value','purchased quantity','purchased unit']]

    purchases['unit value'] = purchases['purchased value']/purchases['purchased quantity']

    unit_values = purchases[['purchased unit','unit value']].dropna()
    unit_values.rename(columns={'purchased unit':'u'},inplace=True)
    unit_values = unit_values.reset_index().set_index(['j','t','m','i','u']).squeeze()
    p_per_kg = broadcast_binary_op(unit_values,lambda x,y:x/y,conv)

    p_per_kg = p_per_kg.loc[p_per_kg.index.isin(conv.index.get_level_values('u').tolist(),level='u')]

    p = p_per_kg.squeeze().groupby(['t','m','i']).median()

    c = food['c']

    c_in_kg = broadcast_binary_op(c,lambda x,y:x*y,conv)

    c_in_kg = c_in_kg.loc[c_in_kg.index.isin(conv.index.get_level_values('u').tolist(),level='u')]

    c = c_in_kg.squeeze().groupby(['j','t','m','i']).sum()

    return c,p

C = []
P = []

lbls = json.load(open('../../_/food_items.json'))

#########################
# Harvest(2016Q1)

with dvc.api.open('../Data/sect10b_harvestw3.csv',mode='rb') as csv:
    harvest = pd.read_csv(csv)

vars={'hhid': 'j',
      'item_cd' : 'i',
      's10bq1' : 'last week?', # Has your household consumed [XX] in the past 7 days. 
      's10bq2a' : 'c',    # What is the total quantity of [XX] consumed in the past 7 days. Quantity. 
      's10bq2b' : 'u', # What is the total quantity of [XX] consumed in the past 7 days. Unit
      's10bq2b_os':'other unit',
      's10bq3a' : 'purchased quantity', 
      's10bq3b' : 'purchased unit', 
      's10bq3b_os':'purchased other unit',
      's10bq4'  : 'purchased value', 
      's10bq5a' : 'home produced quantity', 
      's10bq5b' : 'home produced unit', 
      's10bq5b_os':'produced other unit',
      's10bq6a' : 'gift quantity', 
      's10bq6b' : 'gift unit', 
      's10bq6b_os':'gift other unit',
      'zone': 'm',
      'sector':'rural' # 1=Urban; 2=Rural
      }
t = '2016Q1'
harvest = harvest.rename(columns=vars)

harvest['t'] = t
harvest = harvest.replace({'i':{int(k):v for k,v in lbls[t].items()}})

c,p = rectify(harvest)

C.append(c)
P.append(p)

##################
# Planting (2015Q3)
with dvc.api.open('../Data/sect7b_plantingw3.csv',mode='rb') as csv:
    planting = pd.read_csv(csv)

vars={'hhid': 'j',
      'item_cd' : 'i',
      's7bq1' : 'last week?', # Has your household consumed [XX] in the past 7 days. 
      's7bq2a' : 'c',    # What is the total quantity of [XX] consumed in the past 7 days. Quantity. 
      's7bq2b' : 'u', # What is the total quantity of [XX] consumed in the past 7 days. Unit
      's7bq2b_os' : 'other unit', # Specify other unit
      's7bq3a' : 'purchased quantity', 
      's7bq3b' : 'purchased unit', 
      's7bq3b_os' : 'purchased other unit', 
      's7bq4'  : 'purchased value', 
      's7bq5a' : 'home produced quantity', 
      's7bq5b' : 'home produced unit', 
      's7bq5b_os' : 'produced other unit',
      's7bq6a' : 'gift quantity', 
      's7bq6b' : 'gift unit', 
      's7bq6b_os' : 'gift other unit',
      'zone': 'm',
      'sector':'rural' # 1=Urban; 2=Rural
      }
t = '2015Q3'
planting = planting.rename(columns=vars)

planting['t'] = t
planting = planting.replace({'i':{int(k):v for k,v in lbls[t].items()}})

c,p = rectify(planting)

C.append(c)
P.append(p)
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

x.to_parquet('food_expenditures.parquet',compression='gzip')

p = p.unstack('i')

p.to_parquet('unitvalues.parquet')

c.unstack('i').to_parquet('food_quantities.parquet')
