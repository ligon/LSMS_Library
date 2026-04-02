#!/usr/bin/env python
"""
Concatenate wave-level food_acquired parquets into a single country-level file.

Each wave-level parquet already has index (j, t, m, i) with region 'm' included,
so no join with other_features is needed.
"""
from lsms_library.local_tools import to_parquet, get_dataframe

import pandas as pd
import numpy as np

years = ['2004-05', '2010-11', '2013-14', '2016-17', '2019-20']
fa = []

for t in years:
    df = get_dataframe('../' + t + '/_/food_acquired.parquet').squeeze()
    df = df.rename({'u_consumed': 'units'}, axis=1).reset_index()
    df['units'] = df['units'].str.lower()
    # There may be occasional repeated reports of purchases of same food
    df = df.groupby(['j', 't', 'm', 'i', 'units']).agg({
        'quantity_consumed': 'sum',
        'expenditure': 'sum',
        'quantity_bought': 'sum',
        'price per unit': 'first',
    })
    fa.append(df)

fa = pd.concat(fa)
fa = fa.replace(np.inf, 0)
fa = fa.replace(0, np.nan)

fa = fa.reset_index().set_index(['j', 't', 'm', 'i', 'units'])

to_parquet(fa, '../var/food_acquired.parquet')
