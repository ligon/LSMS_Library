#!/usr/bin/env python
import sys
sys.path.append('../../_')
from uganda import food_expenditures

myvars = dict(fn='../Data/GSEC14A.dta',item='h14aq2',HHID='hh',
              purchased='h14aq5',
              away='h14aq7',
              produced='h14aq9',
              given='h14aq11')

x = food_expenditures(**myvars)

x.to_parquet('food_expenditures.parquet')

