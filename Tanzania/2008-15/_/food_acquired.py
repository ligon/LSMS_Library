#!/usr/bin/env python
import sys
sys.path.append('../../_/')
from tanzania import food_acquired
import numpy as np

fn='../Data/upd4_hh_j1.dta'

myvars = dict(item='hj_00',
              HHID='UPHI',
              year ='round',
              quant_ttl_consume='hj_02_2',
              unit_ttl_consume = 'hj_02_1',
              quant_purchase = 'hj_03_2',
              unit_purchase = 'hj_03_1',
              value_purchase = 'hj_04',
              #place_purchase = 'hj_05', 
              quant_own= 'hj_06_2',
              unit_own = 'hj_06_1', 
              quant_inkind = 'hj_07_2', 
              unit_inkind = 'hj_07_1'
              )

df = food_acquired(fn,myvars)

df = df.reset_index().rename(columns = {'j':'UPHI'}).set_index(['UPHI','i', 't'])

pair = {'quant': ['quant_ttl_consume', 'quant_purchase', 'quant_own', 'quant_inkind'] ,
        'unit': ['unit_ttl_consume', 'unit_purchase', 'unit_own', 'unit_inkind']}

unit_conversion = {'Kg': 1,
                   'Gram': 0.001,
                   'Litre': 1,
                   'Millilitre': 0.001,
                   'Piece': 'p'}

df = df.fillna(0).replace(unit_conversion).replace('NONE', 0)
pattern = r"[p+]"
for i in range(4):
    df[pair['quant'][i]] = df[pair['quant'][i]].astype(np.int64) * df[pair['unit'][i]]
    df[pair['quant'][i]].replace('', 0, inplace=True)
    df[pair['unit'][i]] = np.where(df[pair['quant'][i]].str.contains(pattern).to_frame() == True, 'piece', 'kg')
    df[pair['quant'][i]] = df[pair['quant'][i]].apply(lambda x: x if str(x).count('p') == 0 else str(x).count('p'))

df['agg_u'] = df[pair['unit']].apply(lambda x: max(x) if min(x) == max(x) else min(x) + '+' + max(x), axis = 1)


df.to_parquet('food_acquired.parquet')
