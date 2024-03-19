"""Calculate food prices for different items across rounds; allow
different prices for different units.
"""
import pandas as pd
import numpy as np
import json

df = pd.read_parquet('../var/food_acquired.parquet')

x = df[['total spent']].rename({'total spent': 'total expenditure'})
x = x.replace(0,np.nan).dropna()
z = x.droplevel('u').groupby(['j','m','t', 'i']).sum()
z.to_parquet('../var/food_expenditures.parquet')

p = df['price per unit'].replace(0,np.nan).dropna()

p = p.groupby(['t','m','i','u']).median()
p.to_frame('Prices').to_parquet('../var/food_prices.parquet')

q = x.join(p,on=['t','m','i','u'])
q = q['total spent']/q['price per unit']
q = q.dropna()

pd.DataFrame({'Quantity':q}).to_parquet('../var/food_quantities.parquet')
