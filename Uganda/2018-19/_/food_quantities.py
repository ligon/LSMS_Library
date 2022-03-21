#!/usr/bin/env python

import sys
sys.path.append('../../_')
from uganda import food_quantities

myvars = dict(fn='Uganda/2018-19/Data/GSEC15B.dta',item='CEB01',HHID='hhid',
              purchased='CEB04',
              away='CEB06',
              produced='CEB08',
              given='CEB010',
              units='CEB03C')

q = food_quantities(**myvars)

q.to_parquet('food_quantities.parquet')

