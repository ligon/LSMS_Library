#!/usr/bin/env python
"""
Read non-food expenditures; use harmonized non-food labels.
"""
import pandas as pd
import numpy as np
from uganda import change_id, Waves, harmonized_food_labels

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
    x[t] = pd.read_parquet('../'+t+'/_/nonfood_expenditures.parquet')
    x[t] = id_walk(x[t],t,Waves)
    x[t] = x[t].stack('i').dropna()
    x[t] = x[t].reset_index().set_index(['j','i']).squeeze()
    x[t] = x[t].replace(0,np.nan).dropna()

df = pd.DataFrame(x)
df.columns.name = 't'

x = df.stack().unstack('i')

agg_labels = harmonized_food_labels(fn='./nonfood_items.org',
                                    key='Preferred Label',
                                    value='Aggregate Label')
#x = x.rename(columns=agg_labels)

x = x.groupby('i',axis=1).sum()

x['m'] = 'Uganda'
x = x.reset_index().set_index(['j','t','m'])

x = x.fillna(0)

x.to_parquet('../var/nonfood_expenditures.parquet')
