"""Calculate food prices for different items across rounds; allow
different prices for different units.  
"""
import sys
sys.path.append('../../_/')
from lsms_library.local_tools import to_parquet

import pandas as pd
import numpy as np
from ethiopia import change_id, Waves, harmonized_food_labels
import warnings
import sys
sys.path.append('../../_/')
from lsms_library.local_tools import to_parquet


def fix_food_labels():
    D = {}
    for w in Waves.keys():
        D.update(harmonized_food_labels(fn='./food_items.org',key=w))

    return D

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
    df0 = df.groupby(['j','t','i','units','units_purchased']).sum()
    #df = df.reset_index().set_index(['j','t','i','units','units_purchased'])
    df1 = id_walk(df0,t,Waves)
    p.append(df1)

p = pd.concat(p)

try:
    of = pd.read_parquet('../var/other_features.parquet')

    p = p.join(of.reset_index('m')['m'],on=['j','t'])
    p = p.reset_index()
    p = p.set_index(['j','t','m','i','units','units_purchased'])
except FileNotFoundError:
    warnings.warn('No other_features.parquet found.')
    p['m'] = 'Ethiopia'
    p = p.reset_index().set_index(['j','t','m','i','units','units_purchased'])

p = p.rename(index=fix_food_labels(),level='i')

to_parquet(p, '../var/food_acquired.parquet')
