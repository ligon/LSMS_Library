"""Calculate food prices for different items across rounds; allow
different prices for different units.  
"""

import pandas as pd
import numpy as np
from ethiopia import change_id, Waves
import warnings

def id_walk(df,wave,waves):

    use_waves = list(waves.keys())
    T = use_waves.index(wave)
    for t in use_waves[T::-1]:
        if len(waves[t]):
            df = change_id(df,'../%s/Data/%s' % (t,waves[t][0]),*waves[t][1:])
        else:
            df = change_id(df)

    return df

p = []
for t in Waves.keys():
    df = pd.read_parquet('../'+t+'/_/food_acquired.parquet').squeeze()
    df['t'] = t
    # There may be occasional repeated reports of purchases of same food
    df = df.groupby(['j','t','i','units','units_purchased']).sum()
    df = df.reset_index().set_index(['j','t','i','units'])
    df = id_walk(df,t,Waves)
    p.append(df)

p = pd.concat(p)

try:
    of = pd.read_parquet('../var/other_features.parquet')

    p = p.join(of.reset_index('m')['m'],on=['j','t'])
    p = p.reset_index().set_index(['j','t','m','i','units'])
except FileNotFoundError:
    warnings.warn('No other_features.parquet found.')
    pass

p.to_parquet('../var/food_acquired.parquet')
