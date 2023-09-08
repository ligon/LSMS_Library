#!/usr/bin/env python3

import pandas as pd

df = pd.read_parquet('../var/food_acquired.parquet')
df.index = df.index.rename({'units':'u'})

x = df[['last expenditure']].groupby(['j','t','m','i']).sum()
x.to_parquet('../var/food_expenditures.parquet')

p = df['price'].groupby(['t','m','i','u']).median()
p = p.reset_index()
p['t'] = p['t'].astype(str)
p = p.set_index(['t','m','i','u'])
p.unstack('t').to_parquet('../var/food_prices.parquet')

q = df['quantity']
q = q.dropna()

pd.DataFrame({'Quantity':q}).to_parquet('../var/food_quantities.parquet')
