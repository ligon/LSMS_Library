#!/usr/bin/env python
"""
Compile data on reported shocks.
"""
from lsms_library.local_tools import to_parquet
from lsms_library.local_tools import get_dataframe
import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
from uganda import Waves, id_walk
import json
x = {}

for t in list(Waves.keys()):
    print(t, file=sys.stderr)
    x[t] = get_dataframe('../'+t+'/_/shocks.parquet')

x = pd.concat(x.values())

updated_ids = json.load(open('updated_ids.json'))
x= id_walk(x, updated_ids)

x = x.reset_index().set_index(['i','t'])

to_parquet(x, '../var/shocks.parquet')
