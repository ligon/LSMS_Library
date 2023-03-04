#!/usr/bin/env python
import sys
sys.path.append('../../_/')
from tanzania import food_acquired

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

df = food_acquired(fn,myvars)
df['t'] = '2020-21'
df = df.reset_index().set_index(['j','t','i'])

df.to_parquet('food_acquired.parquet')
