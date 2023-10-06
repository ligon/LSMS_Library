#!/usr/bin/env python
import sys
sys.path.append('../../_')
from ghana import household_characteristics

t = '1998-99'

myvars = dict(fn='../Data/SEC1.DTA', HHID=['clust', 'nh'],
              age='agey',sex=('sex',lambda s: s.lower()[0]),
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

