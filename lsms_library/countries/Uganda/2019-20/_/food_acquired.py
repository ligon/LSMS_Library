#!/usr/bin/env python
from lsms_library.local_tools import to_parquet
import sys
sys.path.append('../../_/')
from uganda import food_acquired, food_acquired_to_canonical

fn = '../Data/HH/gsec15b.dta'
round = '2019-20'

myvars = {'units':'CEB03C',
          'item':'CEB01',
          'HHID':'hhid',
          'market_home':'CEB14a',
          'market_away':'CEB14b',
          'market_own':'CEB14c',
          'farmgate':'CEB15',
          'value_home':'CEB07',
          'value_away':'CEB09',
          'value_own':'CEB11',
          'value_inkind':'CEB013',
          'quantity_home':'CEB06',
          'quantity_away':'CEB08',
          'quantity_own':'CEB10',
          'quantity_inkind':'CEB012'}

d = food_acquired(fn,myvars)
d['t'] = round
df = d.reset_index().set_index(['t','i','j','u'])

# Canonical (t, i, j, u, s) long form (Phase 3 of GH #169).  See
# uganda.food_acquired_to_canonical for the suffix-melt rules.
df = food_acquired_to_canonical(df)

assert df.index.is_unique, "Non-unique (t,i,j,u,s) index!  Fix me!"
assert len(df) > 0, "food_acquired produced no rows after canonical reshape"

to_parquet(df, 'food_acquired.parquet')
