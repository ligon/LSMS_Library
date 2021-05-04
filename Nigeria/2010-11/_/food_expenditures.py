#!/usr/bin/env python
import sys
sys.path.append('../../_')
import numpy as np
import pandas as pd
import json
import dvc.api
from cfe.df_utils import broadcast_binary_op

units = {1:'Kg',
         2:'g',
         3:'l',
         4:'ml',
         5:'piece',
         6:'other'}

def rectify(food,units=units):

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



    food.set_index(['j','t','m','i','u','other unit'],inplace=True)

    # Get prices implied by purchases

    purchases = food.reset_index().set_index(['j','t','m','i'])[['purchased value','purchased quantity','purchased unit','purchased other unit']]
    purchases['purchased other unit'] = purchases['purchased other unit'].fillna('None')

    purchases['unit value'] = purchases['purchased value']/purchases['purchased quantity']

    unit_values = purchases.groupby(['t','m','i','purchased unit','purchased other unit']).median()['unit value'].dropna()
    unit_values.index.names = ['t','m','i','u','other unit']

    c = food['c'].unstack(['t','m','i','u','other unit'])

    idx = c.columns.intersection(unit_values.index)

    c = c[idx]

    c = c.stack(['u'])
    c = c.stack(['i'])
    c = c.stack(['other unit'])
    c = c.stack(['t','m'])

    p = unit_values[idx]
    p.index.names = ['t','m','i','u','other unit']

    return c,p

C = []
P = []

lbls = json.load(open('../../_/food_items.json'))

#########################
# Harvest

with dvc.api.open('Nigeria/2010-11/Data/sect10b_harvestw1.csv',mode='rb') as csv:
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

harvest = harvest.rename(columns=vars)

harvest['t'] = '2010Q3'

harvest['other unit'] = 'None'
harvest['purchased other unit'] = 'None'
harvest['produced other unit'] = 'None'
harvest['gift other unit'] = 'None'

c,p = rectify(harvest)

c = c.rename(index={int(k):v for k,v in lbls['2010Q3'].items()},level='i')
p = p.rename(index={int(k):v for k,v in lbls['2010Q3'].items()},level='i')

C.append(c)
P.append(p)

##################
# Planting (2011Q1)
with dvc.api.open('Nigeria/2010-11/Data/sect7b_plantingw1.csv',mode='rb') as csv:
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

planting = planting.rename(columns=vars)

planting['t'] = '2011Q1'

c,p = rectify(planting)

c = c.rename(index={int(k):v for k,v in lbls['2011Q1'].items()},level='i')
p = p.rename(index={int(k):v for k,v in lbls['2011Q1'].items()},level='i')

C.append(c)
P.append(p)
###################

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

p = p.xs('Kg',level='u')
p = p.xs('None',level='other unit')

p = p.unstack('i')

p.to_parquet('unitvalues.parquet')

