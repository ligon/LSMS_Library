#!/usr/bin/env python3

import pandas as pd

df = pd.read_parquet('../var/food_acquired.parquet')
df.index = df.index.rename({'units':'u'})

x = df[['Total Expenditure']]
x = x.swaplevel(1,2)
x.droplevel('u').to_parquet('../var/food_expenditures.parquet')

p = df['Price'].groupby(['t','m','i','u']).median()
p.unstack('t').to_parquet('../var/food_prices.parquet')

q = x.join(p,on=['t','m','i','u'])
q = q['Total Expenditure']/q['Price']
q = q.dropna()

pd.DataFrame({'Quantity':q}).to_parquet('../var/food_quantities.parquet')
