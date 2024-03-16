"""Calculate food prices for different items across rounds; allow
different prices for different units.
"""
import sys
sys.path.append('../../_/')
from local_tools import to_parquet
import pandas as pd
import numpy as np
import json

df = pd.read_parquet('../var/food_acquired.parquet')

x = df[['total expenses']].rename({'total expenses': 'total expenditure'})
x = x.replace(0,np.nan).dropna()
x.droplevel('u').to_parquet('../var/food_expenditures.parquet')

p = df['price per unit'].groupby(['t','m','i','u']).median()
p.unstack('t').to_parquet('../var/food_prices.parquet')

q = x.join(p,on=['t','m','i','u'])
q = q['total expenses']/q['price per unit']
q = q.dropna()

to_parquet(pd.DataFrame({'Quantity':q}),'../var/food_quantities.parquet')
