"""Calculate food prices for different items across rounds; allow
different prices for different units.
"""

import pandas as pd
import numpy as np

x = []
for t in ['2014', '2018-19']:
    df = pd.read_parquet('../'+t+'/_/food_acquired.parquet').squeeze()
    df = df.reset_index()
    df['units'] = df['units'].astype(str)
    df = df.set_index(['j', 't', 'i', 'units'])
    df = df.loc[:, ['quantity', 'total expenses', 'price per unit']]
    df.index = df.index.rename({'units': 'u'})
    df.columns.name = 'k'
    x.append(df)

fa = pd.concat(x)

of = pd.read_parquet('../var/other_features.parquet')

fa = fa.join(of, on=['j', 't'])
fa = fa.reset_index().set_index(['j','t','m','i','u'])

fa = fa.replace(0.0, np.nan)
fa = fa.groupby(['j','m','t','i','u']).sum()
fa.to_parquet('../var/food_acquired.parquet')
