#!/usr/bin/env python
import sys
sys.path.append('../../_')
from cotedivoire import food_expenditures

t = '1986-87'

myvars = dict(fn='CotedIvoire/%s/Data/F12A.DAT' % t,item='FOODCD',HHID='HID',
              purchased='CFOODB')

x = food_expenditures(**myvars)

x['t'] = t
x['m'] = "Cote d'Ivoire"

x = x.reset_index().set_index(['j','t','m'])

x.to_parquet('food_expenditures.parquet')

