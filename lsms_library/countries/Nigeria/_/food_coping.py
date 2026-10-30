"""Concatenate wave-level food_coping for Nigeria (#332, Family B).

Each wired wave's ``Nigeria/<wave>/_/food_coping.py`` writes a parquet
indexed by ``(t, i, Strategy)`` with an integer ``Days`` column (0-7).
This script concatenates them into the country-level table.

Only W1-W3 (2010-11/2012-13/2015-16) carry the GHS section-9 coping
day-count battery; undeclared folders are skipped using native/YAML metadata
(W4+ section 9 switched to FIES, wired separately).  No id_walk is
applied: Nigeria GHS-Panel hhid is stable across waves.  `v` is
intentionally absent; the framework joins it from sample() at API time.
"""
import pandas as pd

from lsms_library import Country

from lsms_library.local_tools import get_dataframe, to_parquet
from nigeria import Waves

country = Country('Nigeria', preload_panel_ids=False)
declared_folders = set()
for quarter in country.waves:
    wave = country[quarter]
    if 'food_coping' in wave.data_scheme:
        declared_folders.add(wave.wave_folder)

pieces = []
for t in Waves.keys():
    if t not in declared_folders:
        continue
    fn = f'../{t}/_/food_coping.parquet'
    df = get_dataframe(fn)
    pieces.append(df)

assert pieces, "food_coping: no wave-level parquets found"

p = pd.concat(pieces)

to_parquet(p, '../var/food_coping.parquet')
