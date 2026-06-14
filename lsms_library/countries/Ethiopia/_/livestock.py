#!/usr/bin/env python
"""Concatenate wave-level livestock data for Ethiopia (GAP 4).

Each wave's ``Ethiopia/<wave>/_/livestock.py`` produces a parquet with
index ``(t, i, animal)`` and the canonical livestock columns (HeadCount,
HeadAcquired, HeadSold, Value).  This script concatenates them across the
five ESS waves and applies the cross-wave ``id_walk`` so the household
index uses the panel canonical id scheme (the same conversion ``sample()``
receives at API time -- though livestock is in the framework ``_no_v_join``
set, so no ``v`` is joined; ``id_walk`` keeps ``i`` aligned with the panel
for cross-feature joins / the WB livestock-binary cross-check).

``id_walk`` is idempotent (it sets ``attrs['id_converted']``), so the
framework's ``_finalize_result`` skips re-applying it.  The wave parquets
emit the wave-native household id that matches ``sample().i``
(``household_id2`` for W2/W3, ``household_id`` elsewhere) -- identical to
plot_features / crop_production / plot_inputs.

Grain is ``(t, i, animal)`` -- NO ``v`` level (livestock is per-household,
not per-cluster; the framework ``_no_v_join`` set already covers it).
"""
import json

import pandas as pd

from lsms_library.local_tools import get_dataframe, id_walk, to_parquet
from ethiopia import Waves


pieces = []
for t in Waves.keys():
    fn = f'../{t}/_/livestock.parquet'
    try:
        df = get_dataframe(fn)
    except Exception:
        # Wave not yet built / parquet absent (DVC raises PathMissingError).
        continue
    pieces.append(df)

assert pieces, "livestock: no wave-level parquets found"

p = pd.concat(pieces)

updated_ids = json.load(open('updated_ids.json'))
p = id_walk(p, updated_ids)

to_parquet(p, '../var/livestock.parquet')
