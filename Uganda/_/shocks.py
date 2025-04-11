#!/usr/bin/env python
"""
Compile data on reported shocks.
"""
import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
from uganda import Waves, id_walk
import json
x = {}

for t in list(Waves.keys()):
    print(t)
    x[t] = pd.read_parquet('../'+t+'/_/shocks.parquet')

x = pd.concat(x.values())

try:
    of = pd.read_parquet('../var/other_features.parquet')

    x = x.join(of.reset_index('m')['m'],on=['j','t'])

except FileNotFoundError:
    x['m'] ='Uganda'

x = x.reset_index().set_index(['j','t','m'])
updated_ids = json.load(open('updated_ids.json'))
x= id_walk(x, updated_ids)

x.to_parquet('../var/shocks.parquet')
