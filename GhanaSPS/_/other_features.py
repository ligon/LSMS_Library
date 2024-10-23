#!/usr/bin/env python
"""
Concatenate data on other household features across rounds.
"""

import pandas as pd
from ghana_panel import change_id, Waves
import numpy as np

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

z = z.stack().unstack('k').reset_index()

z['Rural']= z['Rural'].replace('nan', np.nan)
z['m'] = z['m'].str.replace(' Region', '')
z['m'] = z['m'].str.replace('-', ' ')
z['m'] = np.where(z['t'] == '2013-14', 'Ghana', z['m'])

z = z.set_index(['j','t','m'])

z.to_parquet('../var/other_features.parquet')
