#!/usr/bin/env python
"""Concatenate wave-level crop_production data for Ethiopia (GAP 1).

Each wave's ``Ethiopia/<wave>/_/crop_production.py`` produces a parquet
with index ``(t, i, plot_id, j, u)`` and the canonical crop_production
columns (Quantity, Quantity_sold, Value_sold, planting_month,
harvest_month, intercropped, perennial).  This script concatenates them
across the five ESS waves and applies the cross-wave ``id_walk`` so the
household index uses the panel canonical id scheme (the same conversion
``sample()`` receives at API time, keeping the ``_join_v_from_sample``
join aligned).

``id_walk`` is idempotent (it sets ``attrs['id_converted']``), so the
framework's ``_finalize_result`` will skip re-applying it.  The wave
parquets emit the wave-native household id that matches ``sample().i``
(``household_id2`` for W2/W3, ``household_id`` elsewhere) — see the
per-wave scripts and the plot_features CONTENTS.org section.
"""
import json

import pandas as pd

from lsms_library.local_tools import get_dataframe, id_walk, to_parquet
from ethiopia import Waves


pieces = []
for t in Waves.keys():
    fn = f'../{t}/_/crop_production.parquet'
    try:
        df = get_dataframe(fn)
    except Exception:
        # Wave not yet built / parquet absent (DVC raises PathMissingError).
        continue
    pieces.append(df)

assert pieces, "crop_production: no wave-level parquets found"

p = pd.concat(pieces)

updated_ids = json.load(open('updated_ids.json'))
p = id_walk(p, updated_ids)

to_parquet(p, '../var/crop_production.parquet')
