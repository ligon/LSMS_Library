#!/usr/bin/env python3

import pandas as pd

df = pd.read_parquet('../var/food_acquired.parquet')

x = df[['Total Expenditure']]
x.to_parquet('../var/food_expenditures.parquet')
x.unstack('i').to_csv('~/Downloads/food_expenditures.csv')

p = final['Unit Value'].groupby(['t','i']).median().unstack('t')
