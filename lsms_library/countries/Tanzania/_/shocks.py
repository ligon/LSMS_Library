#!/usr/bin/env python
from lsms_library.local_tools import to_parquet
from lsms_library.local_tools import get_dataframe
from lsms_library.paths import data_root
"""
Concatenate data on shocks across rounds.
"""

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
from tanzania import Waves, id_walk, waves
import warnings
import json

s = {}
for t in Waves.keys():
    # Try data_root first (where _resolve_data_path writes), then in-tree fallback
    candidates = [
        str(data_root('Tanzania') / t / '_' / 'shocks.parquet'),
        '../' + t + '/_/shocks.parquet',
    ]
    loaded = False
    for path in candidates:
        try:
            s[t] = get_dataframe(path)
            loaded = True
            break
        except (FileNotFoundError, Exception):
            continue
    if not loaded:
        warnings.warn(f'Could not load shocks for {t}')

if not s:
    raise RuntimeError('No shocks data found for any wave.')

s = pd.concat(s.values())

# Ensure index uses 'i' (not 'j')
if 'j' in s.index.names and 'i' not in s.index.names:
    s.index = s.index.rename({'j': 'i'})

# Ensure proper index structure
idx_names = s.index.names
target_idx = ['t', 'i', 'Shock']
if list(idx_names) != target_idx:
    s = s.reset_index()
    # Drop any extra index columns
    for col in s.columns:
        if col not in target_idx and col not in ['AffectedIncome', 'AffectedAssets',
                                                   'HowCoped0', 'HowCoped1', 'HowCoped2']:
            s = s.drop(columns=[col], errors='ignore')
    s = s.set_index(target_idx)

with open('updated_ids.json', 'r') as f:
    updated_ids = json.load(f)

s = id_walk(s, updated_ids, hh_index='i')

to_parquet(s, '../var/shocks.parquet')
