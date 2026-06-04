"""Concatenate wave-level plot_features data for Tanzania NPS (GH #167).

Each buildable wave's ``Tanzania/<wave>/_/plot_features.py`` produces a
parquet with index ``(t, i, plot_id)`` and the canonical columns from
data_info.yml (Area, AreaUnit, Tenure, TenureSystem, SoilType,
Irrigated).  This script concatenates the per-wave parquets and applies
cross-wave id_walk so the household index uses the panel canonical id
scheme.

Only 2019-20 (NPS-SDD Extended Panel) and 2020-21 (NPS Y5 Refresh
Panel) are buildable: the 2008-15 multi-round folder has no agriculture
source file on disk, so those four rounds are deferred.
"""
import json

import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet
from tanzania import id_walk


WAVES = ['2019-20', '2020-21']

pieces = []
for t in WAVES:
    fn = f'../{t}/_/plot_features.parquet'
    try:
        df = get_dataframe(fn)
    except Exception:
        # Wave not yet wired / parquet not built.  DVC raises
        # PathMissingError here, not FileNotFoundError.
        continue
    pieces.append(df)

assert pieces, "plot_features: no wave-level parquets found"

p = pd.concat(pieces)

updated_ids = json.load(open('updated_ids.json'))
p = id_walk(p, updated_ids, hh_index='i')

to_parquet(p, '../var/plot_features.parquet')
