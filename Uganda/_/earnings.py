#!/usr/bin/env python
"""
Compile data on reported labor earnings.
"""
import pandas as pd
import numpy as np
from uganda import change_id, Waves, id_walk
import json


x = {}

for t in list(Waves.keys()):
    print(t)
    x[t] = pd.read_parquet('../'+t+'/_/earnings.parquet')
    x[t] = x[t].stack().squeeze()

x = pd.DataFrame(x)
x.columns.names = ['t']
x = pd.DataFrame({'earnings':x.stack()})

x['m'] = 'Uganda'
x = x.reset_index().set_index(['j','t','m'])

panel_id_json = json.load(open('panel_ids.json'))
x = id_walk(x, Waves, panel_id_json)

x.to_parquet('../var/earnings.parquet')
