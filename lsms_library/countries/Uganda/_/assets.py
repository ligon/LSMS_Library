#!/usr/bin/env python
"""
Compile the real wave observations into a (t, i, j) assets panel.

Keep the item axis and Value column intact.  Stacking each wave into a Series
and aligning those Series in a DataFrame manufactures absent wave/item rows
under pandas 3; squeezing also broadcasts a one-row wave as a scalar.
"""
from lsms_library import Country
from lsms_library.local_tools import to_parquet, get_dataframe

import sys
import pandas as pd
from uganda import id_walk
import json


x = {}

country = Country('Uganda')
for t in country.waves:
    wave = country[t]
    if 'assets' not in wave.data_scheme:
        continue
    print(t, file=sys.stderr)
    x[t] = get_dataframe('../'+wave.wave_folder+'/_/assets.parquet')


x = pd.concat(x, names=['t'])

updated_ids = json.load(open('updated_ids.json'))
x = id_walk(x, updated_ids)

to_parquet(x, '../var/assets.parquet')
