#!/usr/bin/env python
import sys
sys.path.append('../../_')
from ghana_panel import household_demographics

t = '2009-10'

myvars = dict(fn='Ghana/%s/Data/S1D.dta' % t, HHID='hhno',
              age='s1d_4i',sex=('s1d_1',lambda s: s.lower()[0]),
              months_spent=None)

z = household_demographics(**myvars)

z.columns.name = 'k'
z.index.name = 'j'

z['t'] = t
z['m'] = "Ghana"

z = z.reset_index().set_index(['j','t','m'])

z.to_parquet('household_demographics.parquet')

