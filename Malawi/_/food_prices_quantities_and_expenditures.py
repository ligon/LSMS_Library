#!/usr/bin/env python3

import pandas as pd
import numpy as np

df = pd.read_parquet('../var/food_acquired.parquet')
df.index = df.index.rename({'units':'u'})

x = df[['expenditure']]

x.droplevel('u').to_parquet('../var/food_expenditures.parquet')
x.droplevel('u').groupby(['j','t','m','i']).sum().unstack('i').to_csv('~/Downloads/food_expenditures.csv')

p = df['price per unit'].groupby(['t','m','i','u']).median()
p.unstack('t').to_parquet('../var/food_prices.parquet')
p.unstack('t').to_csv('~/Downloads/food_prices.csv')

q = x.join(p,on=['t','m','i','u'])
q = q['expenditure']/q['price per unit']
q = q.dropna()

pd.DataFrame({'Quantity':q}).to_parquet('../var/food_quantities.parquet')
q.squeeze().unstack('i').to_csv('~/Downloads/food_quantities.csv')
