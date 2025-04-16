from lsms_library.local_tools import to_parquet
#!/usr/bin/env python
import sys
sys.path.append('../../_/')
from uganda import food_acquired

fn='../Data/GSEC14A.dta'

myvars = dict(item='h14aq2',               # Code label uniquely identifying food
              HHID='HHID',                 # Unique household id
              market='h14aq12',            # Market price
              farmgate='h14aq13',          # Farmgate price
              value_home='h14aq5',         # Total value of food purchased consumed at home
              value_away='h14aq7',         # Total value of food consumed away from home
              value_own='h14aq9',          # Value of food consumed out of own production
              value_inkind='h14aq11',      # Value of food received (and consumed) in kind
              quantity_home='h14aq4',      # Quantity of food consumed at home
              quantity_away='h14aq6',      # Quantity of food consumed away from home
              quantity_own='h14aq8',       # Quantity of food consumed out of own production
              quantity_inkind='h14aq10',   # Quantity of consumed food received in kind
              units='h14aq3')              # Units in which quantities are measured

df = food_acquired(fn,myvars)

to_parquet(df, 'food_acquired.parquet')
