#!/usr/bin/env python
import sys
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

    food['myu'] = list(zip([str(u) for u in food['purchased unit']],[str(u) for u in food['purchased other unit']]))
    
    food.set_index(['j','t','m','i','myu'],inplace=True)

    # Get prices implied by purchases

    purchases = food.reset_index().set_index(['j','t','m','i'])[['purchased value','purchased quantity','myu']]

    purchases['unit value'] = purchases['purchased value']/purchases['purchased quantity']

    unit_values = purchases.groupby(['t','m','i','myu']).median()['unit value'].dropna()
    unit_values.index.names = ['t','m','i','u']

    food.index.names = ['j','t','m','i','u']
    c = food['c'].unstack(['t','m','i','u'])

    idx = c.columns.intersection(unit_values.index)

    c = c[idx]

    c = c.stack(['t','m'])
    c = c.stack(['u'])
    c = c.stack(['i'])

    p = unit_values[idx]
    p.index.names = ['t','m','i','u']

    return c,p

lbls = json.load(open('../../_/food_items.json'))


##################
# Planting (2019Q1)

t = '2019Q1'

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
planting = planting[vars.values()]

planting['t'] = t

use = planting.iloc[:,3:11].dropna(how='all').index

c,p = rectify(planting.loc[use])

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

x.to_parquet('food_expenditures_planting.parquet',compression='gzip')


