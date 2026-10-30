#!/usr/bin/env python
"""
Compile data on reported shocks.
"""
from lsms_library import Country
from lsms_library.local_tools import to_parquet
from lsms_library.local_tools import get_dataframe
import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
from uganda import id_walk
import json
x = {}

country = Country('Uganda')
for t in country.waves:
    wave = country[t]
    if 'shocks' not in wave.data_scheme:
        continue
    print(t, file=sys.stderr)
    x[t] = get_dataframe('../'+wave.wave_folder+'/_/shocks.parquet')

x = pd.concat(x.values())

updated_ids = json.load(open('updated_ids.json'))
x= id_walk(x, updated_ids)

x = x.reset_index().set_index(['i','t'])

to_parquet(x, '../var/shocks.parquet')
