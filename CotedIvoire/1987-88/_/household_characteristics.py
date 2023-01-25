#!/usr/bin/env python
import sys
sys.path.append('../../_')
from cotedivoire import household_characteristics

t = '1987-88'

myvars = dict(fn='../Data/F01A.DAT', HHID='HID',
              age='AGEY',sex=('SEX',lambda s: 'm' if s==1 else 'f'),
              months_spent='MON')

z = household_characteristics(**myvars)

z.columns.name = 'k'
z.index.name = 'j'

z['t'] = t
z['m'] = "Cote d'Ivoire"

z = z.reset_index().set_index(['j','t','m'])

z.to_parquet('household_characteristics.parquet')

