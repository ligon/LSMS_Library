#!/usr/bin/env python
"""
Compile data on reported shocks.
"""
import sys
sys.path.append('../../_/')
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
    x[t] = pd.read_parquet('../'+t+'/_/shocks.parquet')
    x[t] = id_walk(x[t],t,Waves)

x = pd.concat(x.values())

x['m'] = 'Uganda'
x = x.reset_index().set_index(['j','t','m'])

x.to_parquet('../var/shocks.parquet')
