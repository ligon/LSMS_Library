#!/usr/bin/env python
from lsms_library.local_tools import to_parquet
from lsms_library.local_tools import get_dataframe
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
    x[t] = get_dataframe('../'+t+'/_/assets.parquet')
    x[t] = x[t].stack().squeeze()


x = pd.DataFrame(x)
x.columns.names = ['t']
x = pd.DataFrame({'assets':x.stack()})

x['m'] = 'Uganda'
x = x.reset_index().set_index(['j','t','m'])

updated_ids = json.load(open('updated_ids.json'))
x = id_walk(x, updated_ids)

to_parquet(x, '../var/assets.parquet')
