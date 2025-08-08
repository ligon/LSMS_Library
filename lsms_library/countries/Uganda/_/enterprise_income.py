#!/usr/bin/env python
from lsms_library.local_tools import to_parquet
from lsms_library.local_tools import get_dataframe
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
    x[t] = get_dataframe('../'+t+'/_/enterprise_income.parquet')
    x[t].columns.name = 'k'
    x[t]= x[t].stack().squeeze()


x = pd.DataFrame(x)
x.columns.names = ['t']
x = x.stack('t').unstack('k')

updated_ids = json.load(open('updated_ids.json'))
x = id_walk(x, updated_ids)

try:
    of = get_dataframe('../var/other_features.parquet')

    x = x.join(of.reset_index('m')['m'],on=['j','t'])

except FileNotFoundError:
    x['m'] ='Uganda'

x = x.reset_index().set_index(['j','t','m'])

to_parquet(x, '../var/enterprise_income.parquet')
