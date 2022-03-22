#!/usr/bin/env python
"""
Compile data on food quantities across all rounds, with harmonized units & food names.
"""

import pandas as pd
import numpy as np
import json
from uganda import change_id, Waves

unitlabels = pd.read_csv('unitlabels.csv',index_col=0).squeeze().to_dict()

def id_walk(df,wave,waves):
    
    use_waves = list(waves.keys())
    T = use_waves.index(wave)
    for t in use_waves[T::-1]:
        if len(waves[t]):
            df = change_id(df,'Uganda/%s/Data/%s' % (t,waves[t][0]),*waves[t][1:])
        else:
            df = change_id(df)

    return df

q={}
for t in Waves.keys():
    print(t)
    q[t] = pd.read_parquet('../'+t+'/_/food_quantities.parquet')
    q[t] = id_walk(q[t],t,Waves)
    q[t] = q[t].reset_index().set_index(['j','u'])
    q[t] = q[t].stack('i')
    q[t] = q[t].reset_index().set_index(['j','i','u']).squeeze()
    q[t] = q[t].replace(0,np.nan).dropna()

q = pd.DataFrame(q)
q.columns.name='t'

q = q.stack()
q = q.reset_index().replace({'u':unitlabels})
q = q.set_index(['j','t','u','i'])
q.rename(columns={0:'quantities'},inplace=True)

conv = json.load(open('conversion_to_kgs.json'))

# Convert amenable units to Kg
def to_kgs(x):
    try:
        x['quantities'] = x['quantities']*conv[x['u']]
        x['u'] = 'Kg'
    except KeyError:
        x['u'] = '%s' % x['u']
 
    return x

q = q.reset_index().apply(to_kgs,axis=1).set_index(['j','t','u','i'])

q = q.groupby(['j','t','u','i']).sum()

#q = q.unstack('i')

q.to_parquet('../var/food_quantities.parquet')
