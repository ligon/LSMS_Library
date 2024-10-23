#!/usr/bin/env python
"""
Compile data on interview dates.
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
    try: 
        x[t] = pd.read_parquet('../'+t+'/_/interview_date.parquet')
        x[t] = id_walk(x[t],t,Waves)
    except FileNotFoundError:
        print(f"No parquet foound for {t}")

x = pd.concat(x.values())

try:
    of = pd.read_parquet('../var/other_features.parquet')

    x = x.join(of.reset_index('m')['m'],on=['j','t'])

except FileNotFoundError:
    x['m'] ='Uganda'

x = x.reset_index().set_index(['j','t','m'])

x.to_parquet('../var/interview_date.parquet')
