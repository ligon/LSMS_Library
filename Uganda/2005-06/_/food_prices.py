#!/usr/bin/env python

import sys
sys.path.append('../../_')
from uganda import prices_and_units

myvars = dict(fn='Uganda/2005-06/Data/GSEC14A.dta',item='h14aq2',HHID='hh', market='h14aq12',farmgate='h14aq13',units='h14aq3')

prices = prices_and_units(**myvars)

prices.to_parquet('./food_prices.parquet')

