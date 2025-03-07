#!/usr/bin/env python3

import pandas as pd

df = pd.read_parquet('../var/food_acquired.parquet')
df.index = df.index.rename({'units':'u'})

x = df[['Total Expenditure']]
x.droplevel('u').to_parquet('../var/food_expenditures.parquet')

p = df['Unit Value'].groupby(['t','m','i','u']).median()
p.to_frame('Prices').to_parquet('../var/food_prices.parquet')

q = x.join(p,on=['t','m','i','u'])
q = q['Total Expenditure']/q['Unit Value']
q = q.dropna()

pd.DataFrame({'Quantity':q}).to_parquet('../var/food_quantities.parquet')
