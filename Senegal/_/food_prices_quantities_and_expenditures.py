#!/usr/bin/env python3

import pandas as pd
import sys
sys.path.append('../../_/')
from local_tools import to_parquet

df = pd.read_parquet('../var/food_acquired.parquet')
df.index = df.index.rename({'units':'u'})

x = df[['last expenditure']].groupby(['j','t','m','i']).sum()
to_parquet(x, '../var/food_expenditures.parquet')

p = df['price'].groupby(['t','m','i','u']).median()
p = p.reset_index()
p['t'] = p['t'].astype(str)
p = p.set_index(['t','m','i','u'])
to_parquet(p.unstack('t'), '../var/food_prices.parquet')

q = df['quantity']
q = q.dropna()

to_parquet(pd.DataFrame({'Quantity':q}), '../var/food_quantities.parquet')
