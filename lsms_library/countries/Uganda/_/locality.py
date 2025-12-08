#!/usr/bin/env python
"""
Concatenate locality (parish/village) identifiers across rounds.
"""
from lsms_library.local_tools import to_parquet
from lsms_library.local_tools import get_dataframe
import sys
import pandas as pd
from uganda import Waves, id_walk
import json


x = {}

for t in Waves.keys():
    print(t, file=sys.stderr)
    x[t] = get_dataframe('../'+t+'/_/locality.parquet')
    if 't' in x[t].index.names:
        x[t] = x[t].droplevel('t')

z = pd.concat(x)
z.index.names = ['t', 'i', 'm']

z = z.reorder_levels(['i', 't', 'm']).sort_index()

with open('updated_ids.json', 'r') as f:
    updated_ids = json.load(f)

z = id_walk(z, updated_ids)

to_parquet(z, '../var/locality.parquet')
