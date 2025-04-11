#!/usr/bin/env python
"""
Concatenate data on shocks across rounds.
"""

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
from tanzania import Waves, add_markets_from_other_features, id_walk, waves
import warnings
import dvc.api
from lsms import from_dta
import json

s={}
for t in Waves.keys():
    s[t] = pd.read_parquet('../'+t+'/_/shocks.parquet')

s = pd.concat(s.values())

try:
    s = add_markets_from_other_features('',s).reset_index().set_index(['j','t','m','Shock'])
except FileNotFoundError:
    warnings.warn('No other_features.parquet found.')
    s['m'] = 'Tanzania'
    s = s.reset_index().set_index(['j','t','m','Shock'])

with open('updated_ids.json','r') as f:
    updated_ids =json.load(f)

s = id_walk(s, updated_ids)


s.to_parquet('../var/shocks.parquet')
