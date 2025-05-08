#!/usr/bin/env python
from lsms_library.local_tools import to_parquet
import sys
sys.path.append('../../_')
from cotedivoire import food_expenditures

myvars = dict(fn='../Data/F12A.DAT',item='FOODCD',HHID='HID',
              purchased='CFOODB')

x = food_expenditures(**myvars)

x['t'] = '1985-86'
x['m'] = "Cote d'Ivoire"

x = x.reset_index().set_index(['j','t','m'])

to_parquet(x, 'food_expenditures.parquet')

