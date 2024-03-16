#!/usr/bin/env python
import sys
sys.path.append('../../_/')
from ethiopia import food_acquired
sys.path.append('../../../_/')
from local_tools import to_parquet

fn='../Data/sect6a_hh_w4.dta'

myvars = dict(item='item_cd',                  # Code label uniquely identifying food
              HHID='household_id',                # Unique household id
              quantity = 's6aq02a',           # Quantity of food consumed
              units = 's6aq02b',              # Units for food consumed
              value_purchased  = 's6aq04',     # Total value of food purchased
              quantity_purchased = 's6aq03a', # Quantity of food purchased
              units_purchased = 's6aq03b')

df = food_acquired(fn,myvars)

to_parquet(df, 'food_acquired.parquet')
