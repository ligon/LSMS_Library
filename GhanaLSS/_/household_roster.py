#!/usr/bin/env python
"""
Concatenate household rosters across rounds.
"""

import pandas as pd
try:
    from .ghana import change_id, Waves  # Used as part of a package
except ImportError:
    from ghana import change_id, Waves # Running standalone

def id_walk(df,wave,waves):
    
    use_waves = list(waves.keys())
    T = use_waves.index(wave)
    for t in use_waves[T::-1]:
        if len(waves[t]):
            df = change_id(df,'../%s/Data/%s' % (t,waves[t][0]),*waves[t][1:])
        else:
            df = change_id(df)

    return df

x = []

for t in Waves.keys():
    print(t)
    df = pd.read_parquet('../'+t+'/_/household_roster.parquet')
    df1 = id_walk(df,t,Waves)
    df1.columns.name ='k'
    df2 = df1.stack('k').dropna()
    df2 = df2.reset_index().set_index(['j','indiv','t','k']).squeeze()
    try:
        df2 = df2.drop(columns = 'm')
    except KeyError:
        pass
    x.append(df2)

z = pd.concat(x)

z = z.unstack('k')

try:
    of = pd.read_parquet('../var/other_features.parquet')
    z = z.join(of.reset_index('m')['m'],on=['j','t'])
    z = z.reset_index().set_index(['j','indiv','t','m'])
except FileNotFoundError:
    warnings.warn('No other_features.parquet found.')
    z['m'] = 'Ghana'
    z = z.reset_index().set_index(['j','indiv','t','m'])

z.to_parquet('../var/household_roster.parquet')
