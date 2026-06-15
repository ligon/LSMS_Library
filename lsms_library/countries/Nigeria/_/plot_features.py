"""Concatenate wave-level plot_features for Nigeria (GH #167).

Each wave's ``Nigeria/<wave>/_/plot_features.py`` writes a parquet
indexed by ``(t, i, plot_id)`` with the canonical columns (Area,
AreaUnit, Tenure, TenureSystem, SoilType, Irrigated).  This script
concatenates them into the country-level table.

No id_walk is applied: Nigeria GHS-Panel household ids (hhid) are
stable across waves -- the panel_ids feature maps previous_i = hhid --
so there is no per-wave remapping dict.  This mirrors Nigeria's
food_acquired country-level concatenator.  `v` is intentionally absent;
the framework joins it from sample() at API time.
"""
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet
from nigeria import Waves

pieces = []
for t in Waves.keys():
    fn = f'../{t}/_/plot_features.parquet'
    try:
        df = get_dataframe(fn)
    except Exception:
        # Wave not wired (script absent or parquet not yet built).
        # DVC raises PathMissingError here, not FileNotFoundError.
        continue
    pieces.append(df)

assert pieces, "plot_features: no wave-level parquets found"

p = pd.concat(pieces)

to_parquet(p, '../var/plot_features.parquet')
