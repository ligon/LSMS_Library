"""Concatenate wave-level plot_inputs data for Uganda (GAP 2).

Each wave's ``Uganda/<wave>/_/plot_inputs.py`` produces a parquet indexed
``(t, i, plot, input, j, season)`` with the REPORTED input columns (Quantity,
u, Purchased, Quantity_purchased, Improved).  This script concatenates them
across waves and applies cross-wave id_walk so the household index uses the
panel canonical id scheme.

Source: AGSEC3A/3B (fertilizer / pesticide) + AGSEC4A (seed).  2005-06 is
intentionally absent: it has no plot-input module (the UNPS agriculture
panel starts at wave 1 = 2009-10).
"""
import json

import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet
from uganda import Waves, id_walk


pieces = []
for t in Waves.keys():
    fn = f'../{t}/_/plot_inputs.parquet'
    try:
        df = get_dataframe(fn)
    except Exception:
        # Wave not wired for plot_inputs (no .py / parquet, e.g. 2005-06).
        # DVC raises PathMissingError here, not FileNotFoundError.
        continue
    pieces.append(df)

assert pieces, "plot_inputs: no wave-level parquets found"

p = pd.concat(pieces)

updated_ids = json.load(open('updated_ids.json'))
p = id_walk(p, updated_ids)

to_parquet(p, '../var/plot_inputs.parquet')
