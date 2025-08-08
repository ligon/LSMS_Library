from lsms_library.local_tools import to_parquet
from lsms_library.local_tools import get_dataframe
"""Calculate food prices for different items across rounds; allow
different prices for different units.
"""
import pandas as pd
import numpy as np
import json

df = get_dataframe('../var/food_acquired.parquet')

x = df[['total spent']].rename({'total spent': 'total expenditure'})
x = x.replace(0,np.nan).dropna()
z = x.droplevel('u').groupby(['j','t','m', 'i']).sum()
to_parquet(z, '../var/food_expenditures.parquet')

p = df['price per unit'].replace(0,np.nan).dropna()

p = p.groupby(['t','m','i','u']).median()
to_parquet(p.to_frame('Prices'), '../var/food_prices.parquet')

q = x.join(p,on=['t','m','i','u'])
q = q['total spent']/q['price per unit']
q = q.dropna()

to_parquet(pd.DataFrame({'Quantity':q}), '../var/food_quantities.parquet')
