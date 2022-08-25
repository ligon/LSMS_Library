#!/usr/bin/env python

import sys
sys.path.append('../../_')
from uganda import food_quantities

myvars = dict(fn='../Data/GSEC14A.dta',item='h14aq2',HHID='hh',
              purchased='h14aq4',
              away='h14aq6',
              produced='h14aq8',
              given='h14aq10',
              units='h14aq3')

q = food_quantities(**myvars)

q.to_parquet('food_quantities.parquet')

