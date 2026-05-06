#!/usr/bin/env python
import sys
sys.path.append('../../_/')
from uganda import food_acquired, food_acquired_to_canonical
from lsms_library.local_tools import to_parquet

fn='../Data/GSEC14A.dta'
round = '2005-06'

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

d = food_acquired(fn,myvars)
d['t'] = round
df = d.reset_index().set_index(['t','i','j','u'])

# Canonical (t, i, j, u, s) long form (Phase 3 of GH #169).  See
# uganda.food_acquired_to_canonical for the suffix-melt rules.
df = food_acquired_to_canonical(df)

assert df.index.is_unique, "Non-unique (t,i,j,u,s) index!  Fix me!"
assert len(df) > 0, "food_acquired produced no rows after canonical reshape"

to_parquet(df, 'food_acquired.parquet')
