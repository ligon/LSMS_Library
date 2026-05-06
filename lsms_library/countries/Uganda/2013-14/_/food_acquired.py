#!/usr/bin/env python
from lsms_library.local_tools import to_parquet
import sys
sys.path.append('../../_/')
from uganda import food_acquired, food_acquired_to_canonical

fn='../Data/GSEC15B.dta'
round = '2013-14'

myvars = dict(item='itmcd',
              HHID='HHID',
              market='h15bq12',
              farmgate='h15bq13',
              value_home='h15bq5',
              value_away='h15bq7',
              value_own='h15bq9',
              value_inkind='h15bq11',
              quantity_home='h15bq4',
              quantity_away='h15bq6',
              quantity_own='h15bq8',
              quantity_inkind='h15bq10',
              units='untcd')

d = food_acquired(fn,myvars)
d['t'] = round
df = d.reset_index().set_index(['t','i','j','u'])

# Canonical (t, i, j, u, s) long form (Phase 3 of GH #169).  See
# uganda.food_acquired_to_canonical for the suffix-melt rules.
df = food_acquired_to_canonical(df)

assert df.index.is_unique, "Non-unique (t,i,j,u,s) index!  Fix me!"
assert len(df) > 0, "food_acquired produced no rows after canonical reshape"

to_parquet(df, 'food_acquired.parquet')
