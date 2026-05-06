#!/usr/bin/env python
from lsms_library.local_tools import to_parquet
import sys
sys.path.append('../../_/')
from tanzania import food_acquired, new_harmonize_units, food_acquired_to_canonical
import numpy as np

fn='../Data/HH_SEC_J1.dta'
round = '2019-20'

myvars = dict(item='itemcode',
              HHID='sdd_hhid',
              #year ='round',
              quant_ttl_consume='hh_j02_2',
              unit_ttl_consume = 'hh_j02_1',
              quant_purchase = 'hh_j03_2',
              unit_purchase = 'hh_j03_1',
              value_purchase = 'hh_j04',
              #place_purchase = 'hj_05',
              quant_own = 'hh_j05_2',
              unit_own = 'hh_j05_1',
              quant_inkind = 'hh_j06_2',
              unit_inkind = 'hh_j06_1'
              )

d = food_acquired(fn,myvars)
d['t'] = round
df = d.reset_index().set_index(['j','t','i'])

assert df.index.is_unique, "Non-unique index!  Fix me!"

unit_conversion = {'Kg': 1,
                   'Gram': 0.001,
                   'Litre': 1,
                   'Millilitre': 0.001,
                   'Piece': 'p'}
df = new_harmonize_units(df, unit_conversion)

# Canonical (t, i, j, u, s) long form (Phase 3 of GH #169).  See
# tanzania.food_acquired_to_canonical for the suffix-melt rules.
df = food_acquired_to_canonical(df)

assert df.index.is_unique, "Non-unique (t,i,j,u,s) index!  Fix me!"
assert len(df) > 0, "food_acquired produced no rows after canonical reshape"

to_parquet(df, 'food_acquired.parquet')
