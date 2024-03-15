"""Calculate food prices for different items across rounds; allow
different prices for different units.
"""
import pandas as pd
import numpy as np
import json
import sys
sys.path.append('../../_/')
from local_tools import to_parquet

df = pd.read_parquet('../var/food_acquired.parquet')

x = df[['total spent']].rename({'total spent': 'total expenditure'})
to_parquet(x.droplevel('u'), '../var/food_expenditures.parquet')
x.droplevel('u').unstack('i').to_csv('~/Downloads/food_expenditures.csv')

p = df['price per unit'].groupby(['t','m','i','u']).median()
p.unstack('t').to_parquet('../var/food_prices.parquet')
p.unstack('t').to_csv('~/Downloads/food_prices.csv')

q = x.join(p,on=['t','m','i','u'])
q = q['total spent']/q['price per unit']
q = q.dropna()

to_parquet(pd.DataFrame({'Quantity':q}), '../var/food_quantities.parquet')
q.squeeze().unstack('i').to_csv('~/Downloads/food_quantities.csv')
