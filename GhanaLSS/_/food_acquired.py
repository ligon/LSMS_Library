"""Calculate food prices for different items across rounds; allow
different prices for different units.  
"""

import pandas as pd
import numpy as np
from ghana import change_id, Waves, harmonized_food_labels
import warnings
import sys
sys.path.append('../../_/')
from local_tools import df_from_orgfile

# def fix_food_labels():
#     D = {}
#     for w in Waves.keys():
#         D.update(harmonized_food_labels(fn='./food_items.org',key=w))

#     return D

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
# units = df_from_orgfile('./units.org',name='harmonizedunit',encoding='ISO-8859-1')
# unitsd = units.set_index('Preferred Label').squeeze().to_dict('dict')
# for k in unitsd.keys():
#     unitsd[k] = {v: k for k, v in unitsd[k].items()}

dfs = []
for t in Waves.keys():
    df = pd.read_parquet('../'+t+'/_/food_acquired.parquet').squeeze()
    print(t)
    #df = df.replace({'unit': unitsd[t]})
    if 'purchased_value' in df.columns and 'purchased_quantity' in df.columns:
        df['purchased_value'] = df['purchased_value'].replace(0, np.nan)
        df['purchased_price'] = df['purchased_value']/df['purchased_quantity']
    #df = df.reset_index().set_index(['j','t','i','units','units_purchased'])
    df1 = id_walk(df,t,Waves)
    df1 = df1.reset_index()
    df1['t_temp'] = df1['t']
    df1['t'] = t
    df1 = df1.set_index(['j', 't', 'i', 'u'])
    print(df1)
    dfs.append(df1)

p = pd.concat(dfs)

try:
    of = pd.read_parquet('../var/other_features.parquet')
    p = p.reset_index()
    p = p.join(of.reset_index('m')['m'],on=['j','t'])
    p['t'] = p['t_temp']
    p = p.drop(columns = 't_temp')
    p = p.set_index(['j','t','m','i','u'])
except FileNotFoundError:
    warnings.warn('No other_features.parquet found.')
    p['m'] = 'Ghana'
    p = p.reset_index()
    p['t'] = p['t_temp']
    p = p.drop(columns = 't_tempt')
    p = p.set_index(['j','t','m','i','u'])
    p.join()

#p = p.rename(index=fix_food_labels(),level='i')

p.to_parquet('../var/food_acquired.parquet')
