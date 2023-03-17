#!/usr/bin/env python3

import pandas as pd

df = pd.read_parquet('../var/food_acquired.parquet')

x = df[['Total Expenditure']]
x.to_parquet('../var/food_expenditures.parquet')
x.unstack('i').to_csv('~/Downloads/food_expenditures.csv')

p = df['Unit Value'].groupby(['t','m','i']).median()
p.unstack('t').to_parquet('../var/unit_values.parquet')
p.unstack('t').to_csv('~/Downloads/unit_values.csv')

q = x.join(p,on=['t','m','i'])
q = q['Total Expenditure']/q['Unit Value']
q = q.dropna()

pd.DataFrame({'Quantity':q}).to_parquet('../var/food_quantities.parquet')
q.unstack('i').to_csv('~/Downloads/food_quantities.csv')
