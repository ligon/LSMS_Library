#!/usr/bin/env python
"""
Read food expenditures; use harmonized food labels.
"""

import pandas as pd

x={}
for t in ['2005-06','2009-10','2010-11','2011-12','2013-14','2015-16']:
    x[t] = pd.read_parquet('../'+t+'/_/food_expenditures.parquet')
    x[t] = x[t].stack('itmcd')
    x[t] = x[t].reset_index().set_index(['HHID','itmcd']).squeeze()

x = pd.DataFrame(x)
x = x.stack().unstack('itmcd')
x.columns.name='i'
x.index.names=['j','t']

x.to_parquet('food_expenditures.parquet')
