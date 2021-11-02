#!/usr/bin/env python
"""
Concatenate data on household characteristics across rounds.
"""

import pandas as pd

z={}
for t in ['2005-06','2009-10','2010-11','2011-12','2013-14','2015-16','2018-19','2019-20']:
    z[t] = pd.read_parquet('../'+t+'/_/household_characteristics.parquet')
    z[t] = z[t].stack('k')
    z[t] = z[t].reset_index().set_index(['j','k']).squeeze()

z = pd.DataFrame(z)
z = z.stack().unstack('k')
z.index.names=['j','t']

z['m'] = 'Uganda'
z = z.reset_index().set_index(['j','t','m'])

z.to_parquet('../var/household_characteristics.parquet')
