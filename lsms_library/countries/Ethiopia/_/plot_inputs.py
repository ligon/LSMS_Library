#!/usr/bin/env python
"""Concatenate wave-level plot_inputs data for Ethiopia (GAP 2).

Each wave's ``Ethiopia/<wave>/_/plot_inputs.py`` produces a parquet with
index ``(t, i, plot_id, input, j)`` and the canonical plot_inputs columns
(Quantity, u, Purchased, Quantity_purchased, Improved).  This script
concatenates them across the five ESS waves and applies the cross-wave
``id_walk`` so the household index uses the panel canonical id scheme (the
same conversion ``sample()`` receives at API time, keeping the
``_join_v_from_sample`` join aligned).

``id_walk`` is idempotent (it sets ``attrs['id_converted']``), so the
framework's ``_finalize_result`` will skip re-applying it.  The wave
parquets emit the wave-native household id that matches ``sample().i``
(``household_id2`` for W2/W3, ``household_id`` elsewhere) -- identical to
plot_features / crop_production, so the three features join on
``(t, i, plot_id)`` (and plot_inputs.j joins crop_production.j on the
shared harmonize_crop labels for crop-specific seed / pesticide rows).
"""
import json

import pandas as pd

from lsms_library.local_tools import get_dataframe, id_walk, to_parquet
from ethiopia import Waves


pieces = []
for t in Waves.keys():
    fn = f'../{t}/_/plot_inputs.parquet'
    try:
        df = get_dataframe(fn)
    except Exception:
        # Wave not yet built / parquet absent (DVC raises PathMissingError).
        continue
    pieces.append(df)

assert pieces, "plot_inputs: no wave-level parquets found"

p = pd.concat(pieces)

updated_ids = json.load(open('updated_ids.json'))
p = id_walk(p, updated_ids)

to_parquet(p, '../var/plot_inputs.parquet')
