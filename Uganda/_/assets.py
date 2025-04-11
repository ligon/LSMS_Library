#!/usr/bin/env python
"""
Compile data on reported household assets.
"""
import pandas as pd
import numpy as np
from uganda import id_walk, Waves
import json


x = {}

for t in list(Waves.keys()):
    print(t)
    x[t] = pd.read_parquet('../'+t+'/_/assets.parquet')
    x[t] = x[t].stack().squeeze()


x = pd.DataFrame(x)
x.columns.names = ['t']
x = pd.DataFrame({'assets':x.stack()})

x['m'] = 'Uganda'
x = x.reset_index().set_index(['j','t','m'])

updated_ids = json.load(open('updated_ids.json'))
x = id_walk(x, updated_ids)

x.to_parquet('../var/assets.parquet')
