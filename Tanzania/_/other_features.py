#!/usr/bin/env python
from lsms_library.local_tools import to_parquet
from lsms_library.local_tools import get_dataframe
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
    z[t] = get_dataframe('../'+t+'/_/other_features.parquet')

foo = z.copy()
z = pd.concat(z.values())

z = z.reset_index().set_index(['j','t','m'])
z.columns.name = 'k'

with open('updated_ids.json','r') as f:
    updated_ids =json.load(f)

# z = id_walk(z, updated_ids)

# assert z.index.is_unique, "Non-unique index!  Fix me!"

# to_parquet(z, '../var/other_features.parquet')
