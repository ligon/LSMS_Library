#!/usr/bin/env python
from lsms_library.local_tools import to_parquet
import sys
sys.path.append('../../_')
import numpy as np
import dvc.api
import pandas as pd

t = '2017-18'

myvars = dict(fn='../Data/11a_foodconsumption_prod_purch.dta')

with dvc.api.open(myvars['fn'],mode='rb') as dta:
    df = pd.read_stata(dta, convert_categoricals=True)

# Values recorded as cedis

x = df[['FPrimary','foodname',
        'purchasedquant', 'purchasedcedis',
        'producedquant', 'producedcedis',
        'receivedgiftquant', 'receivedgiftcedis',
        'unitname']]

col = {'FPrimary': 'j', 
       'foodname': 'i', 
       'purchasedquant': 'purchased_quantity', 
       'purchasedcedis': 'purchased_value',
       'producedquant': 'produced_quantity',
       'producedcedis': 'produced_value',
       'receivedgiftquant': 'inkind_quantity',
       'receivedgiftcedis': 'inkind_value',
       'unitname': 'unit'}

x = x.rename(col, axis = 1)
x['price'] = x['purchased_value']/x['purchased_quantity']
x['t'] = t
x['j'] = x['j'].astype(str)
x = x.set_index(['j','t','i'])
x['unit'] = x['unit'].replace('', np.nan)
x = x.dropna(how='all')

to_parquet(x, 'food_acquired.parquet')


