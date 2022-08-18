#!/usr/bin/env python

import sys
sys.path.append('../../_')
from uganda import prices_and_units

myvars = dict(fn='../Data/GSEC15b.dta',item='h15bq2',HHID='hh',market='h15bq12',farmgate='h15bq13',units='untcd')

prices = prices_and_units(**myvars)

prices.to_parquet('./food_prices.parquet')

