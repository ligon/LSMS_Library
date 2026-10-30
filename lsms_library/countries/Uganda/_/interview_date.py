#!/usr/bin/env python
from lsms_library import Country
from lsms_library.local_tools import to_parquet, get_dataframe

"""
Compile data on interview dates.
"""
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
    # GH #1287; ledger §§2–4: only undeclared output may be skipped.
    if 'interview_date' not in wave.data_scheme:
        continue
    print(t, file=sys.stderr)
    fn = f'../{wave.wave_folder}/_/interview_date.parquet'
    try: 
        x[t] = get_dataframe(fn)
    except FileNotFoundError as error:
        raise RuntimeError(
            f'Uganda/{t}: declared interview_date failed to load '
            f'{fn}; refusing a partial country table.'
        ) from error

x = pd.concat(x.values())

updated_ids = json.load(open('updated_ids.json'))
x = id_walk(x, updated_ids)

x = x.reset_index().set_index(['i','t'])

# Canonical column name is Int_t (datetime); wave scripts emit `date`.
# Rename so the cross-country Feature('interview_date') aligns (GH #325).
x = x.rename(columns={'date': 'Int_t'})

to_parquet(x.dropna(), '../var/interview_date.parquet')
