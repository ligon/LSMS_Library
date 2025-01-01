#!/usr/bin/env python
"""
Concatenate data on other household features across rounds.
"""

import pandas as pd
from ghana import change_id, Waves
import numpy as np
import sys
sys.path.append('../../_/')
from local_tools import to_parquet

def id_walk(df,wave,waves):
    
    use_waves = list(waves.keys())
    T = use_waves.index(wave)
    for t in use_waves[T::-1]:
        if len(waves[t]):
            df = change_id(df,'../%s/Data/%s' % (t,waves[t][0]),*waves[t][1:])
        else:
            df = change_id(df)

    return df
    
x = {}

for t in Waves.keys():
    x[t] = pd.read_parquet('../'+t+'/_/other_features.parquet')
    if 't' in x[t].index.names:
        x[t] = x[t].droplevel('t')
    x[t] = id_walk(x[t],t,Waves)
    x[t].columns.name ='k'
    x[t] = x[t].fillna('nan')
    x[t] = x[t].stack('k')
    x[t] = x[t].reset_index().set_index(['j','m', 'k']).squeeze()

z = pd.DataFrame(x)
z.columns.name = 't'

z = z.stack().unstack('m')

z = z.stack().unstack('k')

z['Rural']= z['Rural'].replace('nan', np.nan)
z = z.reset_index().set_index(['j','t','m'])

if __name__=='__main__':
    to_parquet(z,'../var/other_features.parquet')
