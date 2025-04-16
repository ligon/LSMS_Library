from lsms_library.local_tools import to_parquet
from lsms_library.local_tools import get_dataframe
#!/usr/bin/env python3

import pandas as pd

df = get_dataframe('../var/food_acquired.parquet')
df.index = df.index.rename({'units':'u'})

x = df[['Total Expenditure']]
to_parquet(x.droplevel('u'), '../var/food_expenditures.parquet')

p = df['Unit Value'].groupby(['t','m','i','u']).median()
to_parquet(p.to_frame('Prices'), '../var/food_prices.parquet')

q = x.join(p,on=['t','m','i','u'])
q = q['Total Expenditure']/q['Unit Value']
q = q.dropna()

to_parquet(pd.DataFrame({'Quantity':q}), '../var/food_quantities.parquet')
