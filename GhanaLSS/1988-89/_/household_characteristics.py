#!/usr/bin/env python
import sys
sys.path.append('../../_')
from ghana import household_characteristics

t = '1988-89'

myvars = dict(fn='../Data/Y01A.DAT', HHID= 'HID',
              age='AGEY',sex=('SEX',lambda s: 'male' if s == 1 else 'female'),
              months_spent=None, fn_type='csv')

z = household_characteristics(**myvars)

z.columns.name = 'k'
z.index.name = 'j'

z['t'] = t
z['m'] = "Ghana"

z = z.reset_index()
z['j'] = z['j'].astype(str)
z = z.set_index(['j','t','m'])
z.to_parquet('household_characteristics.parquet')
