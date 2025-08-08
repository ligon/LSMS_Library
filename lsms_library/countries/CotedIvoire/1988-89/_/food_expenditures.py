#!/usr/bin/env python
from lsms_library.local_tools import to_parquet
import sys
sys.path.append('../../_')
from cotedivoire import food_expenditures

t = '1988-89'

myvars = dict(fn='../Data/SEC12A.DAT',item='FOODCD',HHID='NH',
              purchased='CFOODB')

x = food_expenditures(**myvars)

x['t'] = t
x['m'] = "Cote d'Ivoire"

x = x.reset_index().set_index(['j','t','m'])

to_parquet(x, 'food_expenditures.parquet')

