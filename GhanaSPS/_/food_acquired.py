"""Calculate food prices for different items across rounds; allow
different prices for different units.  
"""

import pandas as pd
import numpy as np
from ghana_panel import change_id, Waves, harmonized_food_labels
import warnings
import sys
sys.path.append('../../_/')
from lsms_library.local_tools import df_from_orgfile

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

#harmonize unit labels 
units = df_from_orgfile('./units.org',name='harmonizedunit',encoding='ISO-8859-1')
unitsd = units.set_index('Preferred Label').squeeze().to_dict('dict')
for k in unitsd.keys():
    unitsd[k] = {v: k for k, v in unitsd[k].items()}

p = []
for t in Waves.keys():
    df = pd.read_parquet('../'+t+'/_/food_acquired.parquet').squeeze()
    print(t)
    df = df.replace({'unit': unitsd[t]})
    # There may be occasional repeated reports of purchases of same food
    df = df.drop(columns = 'price')
    df0 = df.groupby(['j','t','i','unit']).sum()
    df0['purchased_value'] = df0['purchased_value'].replace(0, np.nan)
    df0['price'] = df0['purchased_value']/df0['purchased_quantity']
    #df = df.reset_index().set_index(['j','t','i','units','units_purchased'])
    df1 = id_walk(df0,t,Waves)
    p.append(df1)

p = pd.concat(p)

try:
    of = pd.read_parquet('../var/other_features.parquet')

    p = p.join(of.reset_index('m')['m'],on=['j','t']).reset_index()
    p['m'] = np.where(p['t'] == '2013-14', 'Ghana', p['m']) #unneeded once 2013-14 fix is found
    p = p.set_index(['j','t','m','i','unit'])
except FileNotFoundError:
    warnings.warn('No other_features.parquet found.')
    p['m'] = 'Ghana'
    p = p.reset_index().set_index(['j','t','m','i','unit'])

p = p.rename(index=fix_food_labels(),level='i')

p.to_parquet('../var/food_acquired.parquet')
