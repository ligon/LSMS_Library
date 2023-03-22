"""Calculate food prices for different items across rounds; allow
different prices for different units.  
"""

import pandas as pd
import numpy as np

fa = []
for t in ['2000']:
    df = pd.read_parquet('../'+t+'/_/food_acquired.parquet').squeeze()
    df['t'] = t
    df['units'] = 'lbs'
    # There may be occasional repeated reports of purchases of same food
    df = df.groupby(['j','t','i','units']).sum()
    df = df.reset_index().set_index(['j','t','i','units'])
    #df = id_walk(df,t,Waves)
    fa.append(df)

fa = pd.concat(fa)

of = pd.read_parquet('../var/other_features.parquet')

fa = fa.join(of.reset_index('m')['m'],on=['j','t'])
fa = fa.reset_index().set_index(['j','t','m','i','units'])

fa = fa.replace(0,np.nan)
fa.to_parquet('../var/food_acquired.parquet')
