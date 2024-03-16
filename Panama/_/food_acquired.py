"""Calculate food prices for different items across rounds; allow
different prices for different units.
"""
import sys
sys.path.append('../../_')
from local_tools import to_parquet
import pandas as pd
import numpy as np

fa = []
for t in ['1997', '2003', '2008']:
    df = pd.read_parquet('../'+t+'/_/food_acquired.parquet').squeeze()
    df['t'] = t
    df = df.reset_index()
    df['j'] = t + df['j'].map(str)
    df['unitcode (bought)'] = df['unitcode (bought)'].astype(str)
    df = df.set_index(['j', 't', 'i', 'unitcode (bought)'])
    df.index = df.index.rename({'unitcode (bought)': 'u'})
    df.columns.name = 'k'
    #df = id_walk(df,t,Waves)
    fa.append(df)

fa = pd.concat(fa)

of = pd.read_parquet('../var/other_features.parquet')

fa = fa.join(of, on=['j','t'])
fa = fa.reset_index().set_index(['j','t','m','i','u'])

fa = fa.replace(0,np.nan)
fa = fa.groupby(['j','t','m','i','u']).sum()
to_parquet(fa,'../var/food_acquired.parquet')
