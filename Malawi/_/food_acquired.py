#!/usr/bin/env python
"""Calculate food prices for different items across rounds; allow
different prices for different units.  
"""

import pandas as pd
import numpy as np

years = ['2004-05', '2010-11', '2016-17', '2019-20']  # 2004-05 not complete
years = ['2010-11', '2016-17', '2019-20']

fa = []
for t in years:
    df = pd.read_parquet('../'+t+'/_/food_acquired.parquet').squeeze()
    df = df.groupby(['j','t','i','units']).agg({'quantity': 'sum',
                                                'expenditure': 'sum',
                                                'quantity_bought':'sum',
                                                'units_bought':'first'})
    df['price'] = df['expenditure']/df['quantity_bought']
    df = df.reset_index().set_index(['j','t','i','units'])
    #df = id_walk(df,t,Waves)
    fa.append(df)

fa = pd.concat(fa)

of = pd.read_parquet('../var/other_features.parquet')

if 'm' in of.index.names:
    fa = fa.join(of.reset_index('m'), on=['j','t'])
else:
    fa = fa.join(of, on=['j','t'])

fa = fa.reset_index().set_index(['j','t','m','i','units'])

fa = fa.replace(0,np.nan)
fa.to_parquet('../var/food_acquired.parquet')
