"""Calculate food prices for different items across rounds; allow
different prices for different units.
"""

import pandas as pd
import numpy as np

fa = []
for t in ['2004-05', '2010-11', '2016-17', '2019-20']:
    df = pd.read_parquet('../'+t+'/_/food_acquired.parquet').squeeze()
    df['t'] = t
    df = df.rename({'u_consumed': 'units'}, axis = 1).reset_index()
    df['units'] = df['units'].str.lower()
    # There may be occasional repeated reports of purchases of same food
    df = df.groupby(['j','t','m','i','units']).sum()
    df = df.reset_index().set_index(['j','t','m', 'i','units'])
    fa.append(df)

fa = pd.concat(fa)

fa = fa.replace(np.inf, 0)
fa = fa.replace(0,np.NaN)

fa.to_parquet('../var/food_acquired.parquet')
