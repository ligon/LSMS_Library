#!/usr/bin/env python
import sys
sys.path.append('../../_/')
from ethiopia import food_acquired
sys.path.append('../../../_/')
from lsms_library.local_tools import to_parquet

fn='../Data/sect5a_hh_w1.dta'

myvars = dict(item='hh_s5aq00',                  # Code label uniquely identifying food
              HHID='household_id',                # Unique household id
              quantity = 'hh_s5aq02_a',           # Quantity of food consumed
              units = 'hh_s5aq02_b',              # Units for food consumed
              value_purchased  = 'hh_s5aq04',     # Total value of food purchased
              quantity_purchased = 'hh_s5aq03_a', # Quantity of food purchased
              units_purchased = 'hh_s5aq03_b',
              quantity_produced = 'hh_s5aq05_a', # Q5 own production (quantity)
              units_produced = 'hh_s5aq05_b',     # Q5 own production (unit)
              quantity_inkind = 'hh_s5aq06_a',    # Q6 gifts / other sources (quantity)
              units_inkind = 'hh_s5aq06_b')       # Q6 gifts / other sources (unit)

df = food_acquired(fn,myvars)

to_parquet(df, 'food_acquired.parquet')
