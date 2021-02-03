#!/usr/bin/env python
import sys
sys.path.append('../../_')
from ghana_panel import household_demographics

t = '2013-14'

myvars = dict(fn='Ghana/%s/Data/01b2_roster.dta' % t, HHID='FPrimary',
              age='ageyears',sex=('gender',lambda s: s.lower()[0]),
              months_spent=None)

z = household_demographics(**myvars)

z.columns.name = 'k'
z.index.name = 'j'

z['t'] = t
z['m'] = "Ghana"

z = z.reset_index().set_index(['j','t','m'])

z.to_parquet('household_demographics.parquet')

