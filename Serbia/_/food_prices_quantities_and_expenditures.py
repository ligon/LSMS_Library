#!/usr/bin/env python3

import pandas as pd

df = pd.read_parquet('../var/food_acquired.parquet')
df.index = df.index.rename({'units':'u'})

x = df[['Total Expenditure']]
x.droplevel('u').to_parquet('../var/food_expenditures.parquet')
x.droplevel('u').unstack('i').to_csv('~/Downloads/food_expenditures.csv')

p = df['Price'].groupby(['t','m','i','u']).median()
p.unstack('t').to_parquet('../var/food_prices.parquet')
p.unstack('t').to_csv('~/Downloads/food_prices.csv')

q = x.join(p,on=['t','m','i','u'])
q = q['Total Expenditure']/q['Price']
q = q.dropna()

pd.DataFrame({'Quantity':q}).to_parquet('../var/food_quantities.parquet')
q.squeeze().unstack('i').to_csv('~/Downloads/food_quantities.csv')
