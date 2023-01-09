#!/usr/bin/env python
import sys
sys.path.append('../../_/')
from tanzania import food_acquired

fn='../Data/upd4_hh_j1.dta'
import dvc.api
from lsms import from_dta

#with dvc.api.open('../Data/upd4_hh_j1.dta',mode='rb') as dta:
    #df = from_dta(dta)

myvars = dict(item='hj_00',
              HHID='r_hhid',
              year ='round',
              quant_ttl_consume='hj_02_2',
              unit_ttl_consume = 'hj_02_1',
              quant_purchase = 'hj_03_2',
              unit_purchase = 'hj_03_1',
              value_purchase = 'hj_04',
              place_purchase = 'hj_05', 
              quant_own = 'hj_06_2',
              unit_own = 'hj_06_1', 
              quant_inkind = 'hj_07_2', 
              unit_inkind = 'hj_07_1'
              )

df = food_acquired(fn,myvars)

df.to_parquet('food_acquired.parquet')
