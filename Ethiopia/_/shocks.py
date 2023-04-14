#!/usr/bin/env python
"""
Compile data on reported shocks.
"""
import sys
sys.path.append('../../_/')
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

x = {}

for t in list(Waves.keys()):
    print(t)
    x[t] = pd.read_parquet('../'+t+'/_/shocks.parquet')
    x[t] = id_walk(x[t],t,Waves)

x = pd.concat(x.values())

try:
    of = pd.read_parquet('../var/other_features.parquet')

    x = x.join(of.reset_index('m')['m'],on=['j','t'])

except FileNotFoundError:
    warnings.warn('No other_features.parquet found.')
    x['m'] ='Ethiopia'

x = x.reset_index().set_index(['j','t','m'])

def harmonize_shocks(df_column):
    df_column = df_column.str.title()
    return df_column.str.replace(r".+\.\s", '').str.replace(r"(\s)*\/(\s)*", '/')

category_labels = {'Death Of Household Member' : 'Death Of Hh Member (Main Bread Earner)',
          'Death Of A Child Under 5 Including Miscarriage Or Stillbirth' : 'Death/Abortion/Still Birth Of Children Under 5',
          'Death Of Other Household Member' : 'Death Of Other Hh Member',
          'Displacement (Due To Government Development Projects)' : 'Displacement (Due To Gov Dev Project)',
          'Local Unrest/Violence ' : 'Local Unrest/Violence',
          'Increase In Price Of Inputs' : 'Price Raise Of Inputs',
          'Unusual Increase In Price Of Inputs (Seed, Fertilizer)' : 'Price Raise Of Inputs',
          'Unusual Price Rise Of Food Items Agriculture Produces ' : 'Price Raise Of Food Item',
          'Unusual Price Fall  Of Food Items Agriculture Produces ' : 'Price Fall Of Food Items',
          'Loss Of Non-Farm Jobs Of Household Member' : 'Loss Of Non-Farm Jobs Of Hh Member',
          'Involuntary Loss Of House/Farm' : 'Involuntary Loss Of House/Land',
          'Sold Durable Assests' : 'Sold Durable Assets'}

#harmonize shocks and coping strategies labels
x.loc[:, ['Shock', 'HowCoped0', 'HowCoped1','HowCoped2']] = x.loc[:, ['Shock', 'HowCoped0', 'HowCoped1','HowCoped2']].apply(harmonize_shocks)
x = x.replace(category_labels)
x.Occurrence = x.Occurrence.fillna(0)


x.to_parquet('../var/shocks.parquet')
