#!/usr/bin/env python
"""
Compile data on reported household assets.
"""
import pandas as pd
import numpy as np
from uganda import change_id, Waves

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

for t in list(Waves.keys()):
    print(t)
    x[t] = pd.read_parquet('../'+t+'/_/assets.parquet')
    x[t] = id_walk(x[t],t,Waves).squeeze()

x = pd.DataFrame(x)
x.columns.names = ['t']
x = pd.DataFrame({'assets':x.stack()})

x['m'] = 'Uganda'
x = x.reset_index().set_index(['j','t','m'])

x.to_parquet('../var/assets.parquet')
