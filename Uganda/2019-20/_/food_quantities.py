#!/usr/bin/env python

import sys
sys.path.append('../../_')
from uganda import food_quantities

myvars = dict(fn='../Data/HH/gsec15b.dta',
              item='CEB01',
              HHID='hhid',
              purchased='CEB06',
              away='CEB08',
              produced='CEB10',
              given='CEB012',
              units='CEB03C')

q = food_quantities(**myvars)

q.to_parquet('food_quantities.parquet')

