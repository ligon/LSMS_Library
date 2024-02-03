
#!/usr/bin/env python
"""
Calculate food prices for different items across rounds; allow different prices for different units.
"""

import pandas as pd
import numpy as np

years = ['2004-05', '2010-11', '2016-17', '2019-20']
fa = []

for t in years:
    df = pd.read_parquet('../'+t+'/_/food_acquired.parquet').squeeze()
    df = df.rename({'u_consumed': 'units'}, axis = 1).reset_index()
    df['units'] = df['units'].str.lower()
    # There may be occasional repeated reports of purchases of same food
    df = df.groupby(['j','t','i','units']).agg({'quantity_consumed': 'sum',
                                                'expenditure': 'sum',
                                                'quantity_bought':'sum',
                                                'price per unit': 'first'})
    fa.append(df)

fa = pd.concat(fa)
fa = fa.replace(np.inf, 0)
fa = fa.replace(0,np.NaN)

of = pd.read_parquet('../var/other_features.parquet')

if 'm' in of.index.names:
    fa = fa.join(of.reset_index('m')['m'], on=['j','t'])
else:
    fa = fa.join(of['m'], on=['j','t'])

fa = fa.reset_index().set_index(['j','t','m','i','units'])

fa.to_parquet('../var/food_acquired.parquet')
