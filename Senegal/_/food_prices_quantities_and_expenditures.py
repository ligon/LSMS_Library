#!/usr/bin/env python3

import pandas as pd

df = pd.read_parquet('../var/food_acquired.parquet')
df.index = df.index.rename({'units':'u'})

x = df[['last expenditure']]
x = x.groupby(['j','t','i']).sum()
x.to_parquet('../var/food_expenditures.parquet')
x.unstack('i').to_csv('~/Downloads/food_expenditures.csv')

p = df['price'].groupby(['t','m','i','u']).median()
p = p.reset_index()
p['t'] = p['t'].astype(str)
p = p.set_index(['t','m','i','u'])
p.unstack('t').to_parquet('../var/food_prices.parquet')
p.unstack('t').to_csv('~/Downloads/food_prices.csv')

q = df['quantity']
q = q.dropna()

pd.DataFrame({'Quantity':q}).to_parquet('../var/food_quantities.parquet')
q.squeeze().unstack('i').to_csv('~/Downloads/food_quantities.csv')
