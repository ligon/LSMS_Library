#!/usr/bin/env python
import sys
sys.path.append('../../_')
from cotedivoire import food_expenditures

t = '1988-89'

myvars = dict(fn='CotedIvoire/%s/Data/SEC12A.DAT' % t,item='FOODCD',HHID='NH',
              purchased='CFOODB')

x = food_expenditures(**myvars)

x['t'] = t
x['m'] = "Cote d'Ivoire"

x = x.reset_index().set_index(['j','t','m'])

x.to_parquet('food_expenditures.parquet')

