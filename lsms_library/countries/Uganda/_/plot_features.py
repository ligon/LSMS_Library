"""Concatenate wave-level plot_features data for Uganda (GH #167 Phase 1).

Each wave's ``Uganda/<wave>/_/plot_features.py`` produces a parquet
with index ``(t, i, plot_id)`` and the canonical columns from
``data_info.yml`` (Area, AreaUnit, Tenure, TenureSystem, SoilType,
Irrigated, Latitude, Longitude).  This script concatenates them and
applies cross-wave id_walk so the household index uses the panel
canonical id scheme.
"""
import json

import pandas as pd

from lsms_library import Country
from lsms_library.local_tools import get_dataframe, to_parquet
from uganda import id_walk


pieces = []
country = Country('Uganda')
for t in country.waves:
    wave = country[t]
    # GH #1287; ledger §§2–4: absence is a declaration, not a read failure.
    # Wave.data_scheme includes both YAML and native script declarations.
    if 'plot_features' not in wave.data_scheme:
        continue
    fn = f'../{wave.wave_folder}/_/plot_features.parquet'
    try:
        df = get_dataframe(fn)
    except Exception as error:
        raise RuntimeError(
            f'Uganda/{t}: declared plot_features failed to load {fn}; '
            'refusing a partial country table.'
        ) from error
    pieces.append(df)

assert pieces, "plot_features: no wave-level parquets found"

p = pd.concat(pieces)

updated_ids = json.load(open('updated_ids.json'))
p = id_walk(p, updated_ids)

to_parquet(p, '../var/plot_features.parquet')
