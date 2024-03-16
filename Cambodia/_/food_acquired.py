"""Calculate food prices for different items across rounds; allow
different prices for different units.
"""

import pandas as pd
import numpy as np
import sys
sys.path.append('../../_/')
from local_tools import to_parquet


fa = []
for t in ['2019-20']:
    df = pd.read_parquet('../'+t+'/_/food_acquired.parquet').squeeze()
    df['t'] = t
    df = df.reset_index()
    df['units'] = df['units'].astype(str)
    df = df.set_index(['j', 't', 'i', 'units'])
    df.index = df.index.rename({'units': 'u'})
    df.columns.name = 'k'
    fa.append(df)

fa = pd.concat(fa)

of = pd.read_parquet('../var/other_features.parquet')

fa = fa.join(of, on=['j','t'])
fa = fa.reset_index().set_index(['j','t','m','i','u'])

fa = fa.replace(0,np.nan)
fa = fa.groupby(['j','m','t','i','u']).sum()

to_parquet(fa, '../var/food_acquired.parquet')
