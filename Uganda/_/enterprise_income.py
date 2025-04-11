#!/usr/bin/env python
"""
Compile data on reported household assets.
"""
import pandas as pd
import numpy as np
from uganda import Waves, id_walk
import json

x = {}

for t in list(Waves.keys()):
    print(t)
    x[t] = pd.read_parquet('../'+t+'/_/enterprise_income.parquet')
    x[t].columns.name = 'k'
    x[t]= x[t].stack().squeeze()


x = pd.DataFrame(x)
x.columns.names = ['t']
x = x.stack('t').unstack('k')

x['m'] = 'Uganda'
x = x.reset_index().set_index(['j','t','m'])

updated_ids = json.load(open('updated_ids.json'))
x = id_walk(x, updated_ids)

x.to_parquet('../var/enterprise_income.parquet')
