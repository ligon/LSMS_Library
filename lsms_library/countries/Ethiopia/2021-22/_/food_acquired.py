#!/usr/bin/env python
import sys
sys.path.append('../../_/')
from ethiopia import food_acquired
sys.path.append('../../../_/')
from lsms_library.local_tools import to_parquet

fn='../Data/sect6a_hh_w5.dta'

myvars = dict(item='item_cd',
              HHID='household_id',
              quantity = 's6aq02a',
              units = 's6aq02b',
              value_purchased  = 's6aq04',
              quantity_purchased = 's6aq03a',
              units_purchased = 's6aq03b')

df = food_acquired(fn,myvars)

to_parquet(df, 'food_acquired.parquet')
