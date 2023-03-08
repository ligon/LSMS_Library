#!/usr/bin/env python
import sys
sys.path.append('../../_/')
from tanzania import food_acquired
import numpy as np

fn='../Data/hh_sec_j1.dta'

myvars = dict(item='itemcode',
              HHID='y5_hhid',
              #year ='round',
              quant_ttl_consume='hh_j02_2',
              unit_ttl_consume = 'hh_j02_1',
              quant_purchase = 'hh_j03_2',
              unit_purchase = 'hh_j03_1',
              value_purchase = 'hh_j04',
              #place_purchase = 'hj_05', 
              quant_own = 'hh_j05_2',
              unit_own = 'hh_j05_1', 
              quant_inkind = 'hh_j06_2', 
              unit_inkind = 'hh_j06_1'
              )

d = food_acquired(fn,myvars)
d['t'] = '2020-21'
df = d.reset_index().set_index(['j','t','i'])


pair = {'quant': ['quant_ttl_consume', 'quant_purchase', 'quant_own', 'quant_inkind'] ,
        'unit': ['unit_ttl_consume', 'unit_purchase', 'unit_own', 'unit_inkind']}

unit_conversion = {'kilogram': 1,
                   'gram': 0.001,
                   'litre': 1,
                   'mililitre': 0.001,
                   'piece': 'p'}

df = df.fillna(0).replace(unit_conversion).replace('none', 0).replace('hakuna', 0)
pattern = r"[p+]"
for i in range(4):
    df[pair['quant'][i]] = df[pair['quant'][i]].astype(np.int64) * df[pair['unit'][i]]
    df[pair['quant'][i]].replace('', 0, inplace=True)
    if df[pair['quant'][i]].dtype != 'O':
        df[pair['unit'][i]] = 'kg'
    else: 
        df[pair['unit'][i]] = np.where(df[pair['quant'][i]].str.contains(pattern).to_frame() == True, 'piece', 'kg')
        df[pair['quant'][i]] = df[pair['quant'][i]].apply(lambda x: x if str(x).count('p') == 0 else str(x).count('p'))

df['agg_u'] = df[pair['unit']].apply(lambda x: max(x) if min(x) == max(x) else min(x) + '+' + max(x), axis = 1)

df.to_parquet('food_acquired.parquet')
