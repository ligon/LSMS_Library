#!/usr/bin/env python
from lsms_library.local_tools import to_parquet
from lsms_library.local_tools import get_dataframe

import pandas as pd
import numpy as np

df = get_dataframe('../var/food_acquired.parquet')
df.index = df.index.rename({'units':'u'})

x = df[['expenditure']].groupby(['j','t','m','i']).sum()
x = x.replace(0,np.nan).dropna()
to_parquet(x, '../var/food_expenditures.parquet')

p = df['price per unit'].groupby(['t','m','i','u']).median()
p = p.reset_index()
p['t'] = p['t'].astype(str)
p = p.set_index(['t','m','i','u'])
to_parquet(p.unstack('t'), '../var/food_prices.parquet')

q = df['quantity_consumed']
q = q.replace(0,np.nan).dropna()

to_parquet(pd.DataFrame({'Quantity':q}), '../var/food_quantities.parquet')
q.squeeze().unstack('i').to_csv('~/Downloads/food_quantities.csv')
