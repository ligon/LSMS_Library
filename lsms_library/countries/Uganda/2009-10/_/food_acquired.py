#!/usr/bin/env python
from lsms_library.local_tools import to_parquet
import sys
sys.path.append('../../_/')
from uganda import food_acquired, food_acquired_to_canonical

fn='../Data/GSEC15b.dta'
round = '2009-10'

myvars = dict(item='itmcd',                   # Code label uniquely identifying food
              HHID='hh',                      # Unique household id
              market='h15bq12',               # Market price
              farmgate='h15bq13',             # Farmgate price
              value_home='h15bq5',            # Total value of food purchased consumed at home
              value_away='h15bq7',            # Total value of food consumed away from home
              value_own='h15bq9',             # Value of food consumed out of own production
              value_inkind='h15bq11',         # Value of food received (and consumed) in kind
              quantity_home='h15bq4',         # Quantity of food consumed at home
              quantity_away='h15bq6',         # Quantity of food consumed away from home
              quantity_own='h15bq8',          # Quantity of food consumed out of own production
              quantity_inkind='h15bq10',      # Quantity of consumed food received in kind
              units='untcd')                  # Units in which quantities are measured

d = food_acquired(fn,myvars)
d['t'] = round
df = d.reset_index().set_index(['t','i','j','u'])

# Canonical (t, i, j, u, s) long form (Phase 3 of GH #169).  See
# uganda.food_acquired_to_canonical for the suffix-melt rules.
df = food_acquired_to_canonical(df)

assert df.index.is_unique, "Non-unique (t,i,j,u,s) index!  Fix me!"
assert len(df) > 0, "food_acquired produced no rows after canonical reshape"

to_parquet(df, 'food_acquired.parquet')
