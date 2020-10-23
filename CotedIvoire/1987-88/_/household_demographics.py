#!/usr/bin/env python
import sys
sys.path.append('../../_')
from cotedivoire import household_demographics

t = '1987-88'

myvars = dict(fn='CotedIvoire/%s/Data/F01A.DAT' % t, HHID='HID',
              age='AGEY',sex=('SEX',lambda s: 'm' if s==1 else 'f'),
              months_spent='MON')

z = household_demographics(**myvars)

z.columns.name = 'k'
z.index.name = 'j'

z['t'] = t
z['m'] = "Cote d'Ivoire"

z = z.reset_index().set_index(['j','t','m'])

z.to_parquet('household_demographics.parquet')

