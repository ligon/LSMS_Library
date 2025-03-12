#!/usr/bin/env python
import sys
import numpy as np
import pandas as pd
import json
import dvc.api
from cfe.df_utils import broadcast_binary_op

sys.path.append('../../2015-16/_')
from units import unitcodes

def rectify(food,units=unitcodes):

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
 

    food.set_index(['j','t','m','i','u'],inplace=True)

    # Get prices implied by purchases

    purchases = food.reset_index().set_index(['j','t','m','i'])[['purchased value','purchased quantity','purchased unit',]]
 
    purchases['unit value'] = purchases['purchased value']/purchases['purchased quantity']

    unit_values = purchases[['purchased unit','unit value']].dropna()
    unit_values.rename(columns={'purchased unit':'u'},inplace=True)
    unit_values = unit_values.reset_index().set_index(['j','t','m','i','u']).squeeze()


    return unit_values


U = []

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

unit_values = rectify(harvest)

U.append(unit_values)


##################
# Planting (2018Q3)

t = '2018Q3'

with dvc.api.open('Nigeria/2018-19/Data/sect7b_plantingw4.csv',mode='rb') as csv:
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

planting['t'] = t
planting = planting.replace({'i':{int(k):v for k,v in lbls[t].items()}})

unit_values = rectify(planting)

U.append(unit_values)

###################

u = pd.concat(U,axis=0)

u.to_pickle('individual_unit_values.pickle')

