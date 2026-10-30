#!/usr/bin/env python
"""Concatenate Tanzania life_satisfaction (Section G subjective welfare) across
waves and apply panel-id walking.

Each wave-level script writes a ``(t, i, Domain)`` parquet with the head's
domain-satisfaction ratings.  The 2008-15 folder's parquet already carries all
four NPS rounds as distinct ``t`` values; the 2019-20 / 2020-21 folders carry
one wave each.  We load by folder (Waves.keys()), concatenate, then id_walk to
harmonize household ids across waves.
"""
from lsms_library import Country
from lsms_library.local_tools import to_parquet, get_dataframe
from lsms_library.paths import data_root

import sys
sys.path.append('../../_/')
import pandas as pd
from tanzania import Waves, id_walk
import json

# GH #1287: declarations belong to logical waves; readers use physical folders.
country = Country('Tanzania', preload_panel_ids=False)
declared_folders = {
    wave.wave_folder
    for wave in (country[year] for year in country.waves)
    if 'life_satisfaction' in wave.data_scheme
}

s = {}
for t in Waves.keys():
    if t not in declared_folders:
        continue
    candidates = [
        str(data_root('Tanzania') / t / '_' / 'life_satisfaction.parquet'),
        '../' + t + '/_/life_satisfaction.parquet',
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
            f'Tanzania/life_satisfaction: could not load declared folder {t}; tried {candidates}'
        ) from load_error

if not s:
    raise RuntimeError('No life_satisfaction data found for any wave.')

s = pd.concat(s.values())

# Ensure index uses 'i' (not 'j')
if 'j' in s.index.names and 'i' not in s.index.names:
    s.index = s.index.rename({'j': 'i'})

target_idx = ['t', 'i', 'Domain']
if list(s.index.names) != target_idx:
    s = s.reset_index()
    for col in list(s.columns):
        if col not in target_idx and col != 'Satisfaction':
            s = s.drop(columns=[col], errors='ignore')
    s = s.set_index(target_idx)

with open('updated_ids.json', 'r') as f:
    updated_ids = json.load(f)

s = id_walk(s, updated_ids, hh_index='i')

to_parquet(s, '../var/life_satisfaction.parquet')
