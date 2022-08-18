#!/usr/bin/env python

import sys
sys.path.append('../../_')
from uganda import prices_and_units


myvars = {'fn':'../Data/gsec15b.dta',
          'units':'untcd',
          'item':'itmcd',
          'HHID':'HHID',
          'market':'h15bq12',
          'farmgate':'h15bq13'}
        

prices = prices_and_units(**myvars)

prices.to_parquet('./food_prices.parquet')

