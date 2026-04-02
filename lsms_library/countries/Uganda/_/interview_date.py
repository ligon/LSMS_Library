#!/usr/bin/env python
from lsms_library.local_tools import to_parquet, get_dataframe

"""
Compile data on interview dates.
"""
import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
from uganda import Waves, id_walk
import json

x = {}
for t in list(Waves.keys()):
    print(t, file=sys.stderr)
    try: 
        x[t] = get_dataframe('../'+t+'/_/interview_date.parquet')
    except FileNotFoundError:
        print(f"No parquet foound for {t}", file=sys.stderr)

x = pd.concat(x.values())

updated_ids = json.load(open('updated_ids.json'))
x = id_walk(x, updated_ids)

x = x.reset_index().set_index(['i','t'])

to_parquet(x.dropna(), '../var/interview_date.parquet')
