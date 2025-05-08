#!/usr/bin/env python
from lsms_library.local_tools import to_parquet
import sys
sys.path.append('../../_/')
from tanzania import food_acquired, new_harmonize_units
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

d = food_acquired(fn, myvars)
d['t'] = '2020-21'
df = d.reset_index().set_index(['j','t','i'])

unit_conversion = {'kilogram': 1,
                   'gram': 0.001,
                   'litre': 1,
                   'mililitre': 0.001,
                   'piece': 'p'}
df = new_harmonize_units(df, unit_conversion)

assert df.index.is_unique, "Non-unique index!  Fix me!"

assert len(df[['quant_purchase','quant_own','quant_inkind']].dropna(how='all'))>0

to_parquet(df, 'food_acquired.parquet')
