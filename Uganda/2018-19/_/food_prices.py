#!/usr/bin/env python

import sys
sys.path.append('../../_')
from uganda import prices_and_units


myvars = {'fn':'../Data/GSEC15B.dta',
          'units':'CEB03C',
          'item':'CEB01',
          'HHID':'hhid',
          'market':'CEB012',
          'farmgate':'CEB013'}
        

prices = prices_and_units(**myvars)

# Stray float in itmcd index...
prices = prices.loc[prices.index.get_level_values('itmcd').map(type)==str,:]

prices.to_parquet('./food_prices.parquet')
