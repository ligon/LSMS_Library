#!/usr/bin/env python
import sys
sys.path.append('../../_/')
from ethiopia import food_acquired
sys.path.append('../../../_/')
from lsms_library.local_tools import to_parquet

fn='../Data/sect6a_hh_w4.dta'

myvars = dict(item='item_cd',                  # Code label uniquely identifying food
              HHID='household_id',                # Unique household id
              quantity = 's6aq02a',           # Quantity of food consumed
              units = 's6aq02b',              # Units for food consumed
              value_purchased  = 's6aq04',     # Total value of food purchased
              quantity_purchased = 's6aq03a', # Quantity of food purchased
              units_purchased = 's6aq03b',
              quantity_produced = 's6aq05a',  # Q5 own production (quantity)
              units_produced = 's6aq05b',     # Q5 own production (unit)
              quantity_inkind = 's6aq06a',    # Q6 gifts / other sources (quantity)
              units_inkind = 's6aq06b')       # Q6 gifts / other sources (unit)

df = food_acquired(fn,myvars)

to_parquet(df, 'food_acquired.parquet')
