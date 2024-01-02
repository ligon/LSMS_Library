#!/usr/bin/env python3

import pandas as pd
import numpy as np

df = pd.read_parquet('../var/food_acquired.parquet')
df.index = df.index.rename({'units':'u'})

x = df[['expenditure']].groupby(['j','t','m','i']).sum()
x.to_parquet('../var/food_expenditures.parquet')

p = df['price per unit'].groupby(['t','m','i','u']).median()
p = p.reset_index()
p['t'] = p['t'].astype(str)
p = p.set_index(['t','m','i','u'])
p.unstack('t').to_parquet('../var/food_prices.parquet')

q = df['quantity_consumed']
q = q.dropna()

pd.DataFrame({'Quantity':q}).to_parquet('../var/food_quantities.parquet')
q.squeeze().unstack('i').to_csv('~/Downloads/food_quantities.csv')
