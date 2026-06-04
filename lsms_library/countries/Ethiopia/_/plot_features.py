"""Concatenate wave-level plot_features data for Ethiopia (GH #167).

Each wave's ``Ethiopia/<wave>/_/plot_features.py`` produces a parquet
with index ``(t, i, plot_id)`` and the canonical plot_features columns
(Area, AreaUnit, Tenure, TenureSystem, SoilType, Irrigated).  This
script concatenates them across the five ESS waves and applies the
cross-wave ``id_walk`` so the household index uses the panel canonical
id scheme (the same conversion ``sample()`` receives at API time, which
keeps the ``_join_v_from_sample`` join aligned).

``id_walk`` is idempotent (it sets ``attrs['id_converted']``), so the
framework's ``_finalize_result`` will skip re-applying it.  The wave
parquets emit the wave-native household id that matches ``sample().i``
(``household_id2`` for W2/W3, ``household_id`` elsewhere) — see the
per-wave scripts and the CONTENTS.org plot_features section.

GPS Latitude/Longitude are NOT emitted: the public ESS plot GPS is
100% redacted to '**CONFIDENTIAL**'.
"""
import json

import pandas as pd

from lsms_library.local_tools import get_dataframe, id_walk, to_parquet
from ethiopia import Waves


pieces = []
for t in Waves.keys():
    fn = f'../{t}/_/plot_features.parquet'
    try:
        df = get_dataframe(fn)
    except Exception:
        # Wave not yet wired / parquet not built (DVC raises
        # PathMissingError here, not FileNotFoundError).
        continue
    pieces.append(df)

assert pieces, "plot_features: no wave-level parquets found"

p = pd.concat(pieces)

updated_ids = json.load(open('updated_ids.json'))
p = id_walk(p, updated_ids)

to_parquet(p, '../var/plot_features.parquet')
