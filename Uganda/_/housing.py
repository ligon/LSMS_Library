#!/usr/bin/env python
"""
Read and conglomerate housing data
"""
import pandas as pd
import numpy as np
from uganda import change_id, Waves, id_walk
import json

x = {}

for t in list(Waves.keys()):
    print(t)
    x[t] = pd.read_parquet('../'+t+'/_/housing.parquet')
    x[t] = x[t].stack('k').dropna()
    x[t] = x[t].reset_index().set_index(['j','k']).squeeze()
    x[t] = x[t].replace(0,np.nan).dropna()

df = pd.DataFrame(x)
df.columns.name = 't'

x = df.stack().unstack('k')


x = x.groupby('k',axis=1).sum()

x['m'] = 'Uganda'
x = x.reset_index().set_index(['j','t','m'])

x = x.fillna(0)
panel_id_json = json.load(open('../var/panel_id.json'))
x = id_walk(x, Waves, panel_id_json)

x.to_parquet('../var/housing.parquet')
