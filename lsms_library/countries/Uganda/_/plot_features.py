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

from lsms_library.local_tools import get_dataframe, to_parquet
from uganda import Waves, id_walk


pieces = []
for t in Waves.keys():
    fn = f'../{t}/_/plot_features.parquet'
    try:
        df = get_dataframe(fn)
    except Exception:
        # Wave not yet wired for plot_features (either the .py
        # script doesn't exist or the parquet hasn't been built).
        # DVC raises PathMissingError here, not FileNotFoundError.
        continue
    pieces.append(df)

assert pieces, "plot_features: no wave-level parquets found"

p = pd.concat(pieces)

updated_ids = json.load(open('updated_ids.json'))
p = id_walk(p, updated_ids)

to_parquet(p, '../var/plot_features.parquet')
