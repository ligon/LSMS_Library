#!/usr/bin/env python
import sys
sys.path.append('../../_')
import numpy as np
import dvc.api
import pandas as pd


t = '2013-14'

myvars = dict(fn='../Data/11a_foodcomsumption_prod_purch.dta')

with dvc.api.open(myvars['fn'],mode='rb') as dta:
    df = pd.read_stata(dta, convert_categoricals=True)

# Values recorded as cedis

x = df[['FPrimary','foodlongname',
        'purchasedquant', 'purchasedcedis',
        'producedquant', 'producedcedis',
        'receivedgiftquant', 'receivedgiftcedis',
        'unit']]

col = {'FPrimary': 'j', 
       'foodlongname': 'i', 
       'purchasedquant': 'purchased_quantity', 
       'purchasedcedis': 'purchased_value',
       'producedquant': 'produced_quantity',
       'producedcedis': 'produced_value',
       'receivedgiftquant': 'inkind_quantity',
       'receivedgiftcedis': 'inkind_value',
       'unit': 'unit'}

x = x.rename(col, axis = 1)
x['price'] = x['purchased_value']/x['purchased_quantity']
x['t'] = t
x['j'] = x['j'].astype(str)
x = x.set_index(['j','t','i'])
x['unit'] = x['unit'].replace('', np.nan)
x = x.dropna(how='all')

x.to_parquet('food_acquired.parquet')
