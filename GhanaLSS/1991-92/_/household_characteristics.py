#!/usr/bin/env python
import sys
sys.path.append('../../_')
from ghana import household_characteristics

t = '1991-92'

myvars = dict(fn='../Data/S1.DTA', HHID=['clust', 'nh'],
              age='agey',sex=('sex',lambda s: 'male' if s == 1 else 'female'),
              months_spent=None)

z = household_characteristics(**myvars)

z.columns.name = 'k'
z.index.name = 'j'

z['t'] = t
z['m'] = "Ghana"

z = z.reset_index()
z['j'] = z['j'].astype(str)
z = z.set_index(['j','t','m'])
z.to_parquet('household_characteristics.parquet')

