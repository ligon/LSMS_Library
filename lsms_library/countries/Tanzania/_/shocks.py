#!/usr/bin/env python
from lsms_library import Country
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
import json

# GH #1287: declarations belong to logical waves; readers use physical folders.
country = Country('Tanzania', preload_panel_ids=False)
declared_folders = {
    wave.wave_folder
    for wave in (country[year] for year in country.waves)
    if 'shocks' in wave.data_scheme
}

s = {}
for t in Waves.keys():
    if t not in declared_folders:
        continue
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
        except Exception as exc:
            load_error = exc
            continue
    if not loaded:
        # Both existing paths failed for a declared producer (ledger #1287 §4).
        raise RuntimeError(
            f'Tanzania/shocks: could not load declared folder {t}; tried {candidates}'
        ) from load_error

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
