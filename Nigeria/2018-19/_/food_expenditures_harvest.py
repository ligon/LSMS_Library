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
         4:'cl',
         10:'bin/basket',
         11:'paint rubber',
         12:'milk cup',
         13:'cigarette cup',
         14:'tin',
         20:'small congo',
         21:'large congo',
         30:'small mudu',
         31:'large mudu',
         40:'small derica',
         41:'medium derica',
         42:'large derica',
         43:'very large derica',
         50:'small tiya',
         51:'medium tiya',
         52:'large tiya',
         60:'small kobiowu',
         61:'medium kobiowu',
         62:'large kobiowu',
         70:'small bowl',
         71:'medium bowl',
         72:'large bowl',
         80:'small piece',
         81:'medium piece',
         82:'large piece',
         90:'small heap',
         91:'medium heap',
         92:'large heap',
         100:'small bunch',
         101:'medium bunch',
         102:'large bunch',
         110:'small stalk',
         111:'medium stalk',
         112:'large stalk',
         120:'small packet/sachet',
         121:'medium packet/sachet',
         122:'large packet/sachet',
         900:'other specify'}

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

t = '2018Q3'

with dvc.api.open('Nigeria/2018-19/Data/sect10b_harvestw4.csv',mode='rb') as csv:
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

c,p = rectify(harvest)

c = c.rename(index={int(k):v for k,v in lbls[t].items()},level='i')
p = p.rename(index={int(k):v for k,v in lbls[t].items()},level='i')

x = broadcast_binary_op(c,lambda x,y: x*y, p)

x = x.groupby(['j','t','m','i']).sum().dropna().reset_index()

x['j'] = x['j'].astype(int).astype(str)

x = x.set_index(['j','t','m','i']).squeeze()

x = x.replace(0,np.nan)

x = x.unstack('i')

x = x.groupby('i',axis=1).sum()

x = x.reset_index().set_index(['j','t','m'])

x = x.drop_duplicates()

x.to_parquet('food_expenditures_harvest.parquet',compression='gzip')


