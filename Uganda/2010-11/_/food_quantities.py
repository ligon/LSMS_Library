#!/usr/bin/env python

import sys
sys.path.append('../../_')
from uganda import food_quantities

myvars = dict(fn='Uganda/2010-11/Data/GSEC15b.dta',item='itmcd',HHID='hh',
              purchased='h15bq4',
              away='h15bq6',
              produced='h15bq8',
              given='h15bq10',units='untcd')

q = food_quantities(**myvars)

q.to_parquet('food_quantities.parquet')

