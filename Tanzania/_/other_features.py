#!/usr/bin/env python
"""
Concatenate data on household characteristics across rounds.
"""

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
from tanzania import Waves, waves, id_walk
import dvc.api
from lsms import from_dta
import json

z = {}
for t in Waves.keys():
    z[t] = pd.read_parquet('../'+t+'/_/other_features.parquet')

foo = z.copy()
z = pd.concat(z.values())

z = z.reset_index().set_index(['j','t','m'])
z.columns.name = 'k'

with open('panel_ids.json','r') as f:
    panel_id_json =json.load(f)

z = id_walk(z, waves, panel_id_json)

assert z.index.is_unique, "Non-unique index!  Fix me!"

z.to_parquet('../var/other_features.parquet')
