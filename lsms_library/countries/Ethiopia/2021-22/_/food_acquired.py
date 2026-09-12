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
              units_purchased = 's6aq03b',
              quantity_produced = 's6aq05a',  # Q5 own production (quantity)
              units_produced = 's6aq05b',     # Q5 own production (unit)
              quantity_inkind = 's6aq06a',    # Q6 gifts / other sources (quantity)
              units_inkind = 's6aq06b')       # Q6 gifts / other sources (unit)

df = food_acquired(fn,myvars)

to_parquet(df, 'food_acquired.parquet')
