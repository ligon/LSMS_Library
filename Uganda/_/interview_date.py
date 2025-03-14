#!/usr/bin/env python
"""
Compile data on interview dates.
"""
import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
from uganda import change_id, Waves, id_walk
import json

x = {}
for t in list(Waves.keys()):
    print(t)
    try: 
        x[t] = pd.read_parquet('../'+t+'/_/interview_date.parquet')
    except FileNotFoundError:
        print(f"No parquet foound for {t}")

x = pd.concat(x.values())

try:
    of = pd.read_parquet('../var/other_features.parquet')

    x = x.join(of.reset_index('m')['m'],on=['j','t'])

except FileNotFoundError:
    x['m'] ='Uganda'

x = x.reset_index().set_index(['j','t','m'])

# panel_id_json = json.load(open('panel_ids.json'))
# x = id_walk(x, Waves, panel_id_json)

x.to_parquet('../var/interview_date.parquet')
