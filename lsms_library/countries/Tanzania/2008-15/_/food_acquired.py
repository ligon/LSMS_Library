#!/usr/bin/env python
from lsms_library.local_tools import to_parquet
import sys
sys.path.append('../../_/')
from tanzania import food_acquired, new_harmonize_units
import numpy as np

fn='../Data/upd4_hh_j1.dta'

myvars = dict(item='hj_00',
              HHID='r_hhid',
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

df = df.reset_index()
# Manual correction of a missing food label mapping 
# Refer to page 87 of the questionnaire
df.i = df.i.replace(1083.0, 'WHEAT, BARLEY, GRAIN, AND OTHER CEREALS')

df = df.set_index(['j','t','i'])

unit_conversion = {'Kg': 1,
                   'Gram': 0.001,
                   'Litre': 1,
                   'Millilitre': 0.001,
                   'Piece': 'p'}

df = new_harmonize_units(df, unit_conversion)

assert df.index.is_unique, "Non-unique index!  Fix me!"
assert len(df[['quant_purchase','quant_own','quant_inkind']].dropna(how='all'))>0

to_parquet(df, 'food_acquired.parquet')
