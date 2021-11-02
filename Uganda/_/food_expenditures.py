

#!/usr/bin/env python
"""
Read food expenditures; use harmonized food labels.
"""

import pandas as pd

x={}

for t in ['2005-06','2009-10','2010-11','2011-12','2013-14','2015-16','2018-19','2019-20']:
    x[t] = pd.read_parquet('../'+t+'/_/food_expenditures.parquet')
    x[t] = x[t].stack('i').dropna()
    x[t] = x[t].reset_index().set_index(['j','i']).squeeze()

x = pd.DataFrame(x)
x = x.stack().unstack('i')

x.index.names=['j','t']
x['m'] = 'Uganda'
x = x.reset_index().set_index(['j','t','m'])

x.to_parquet('../var/food_expenditures.parquet')
