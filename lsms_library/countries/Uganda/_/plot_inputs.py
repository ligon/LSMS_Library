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

from lsms_library import Country
from lsms_library.local_tools import get_dataframe, to_parquet
from uganda import id_walk


pieces = []
country = Country('Uganda')
for t in country.waves:
    wave = country[t]
    # GH #1287; ledger §§2–4: absence is declared, not inferred from a read.
    if 'plot_inputs' not in wave.data_scheme:
        continue
    fn = f'../{wave.wave_folder}/_/plot_inputs.parquet'
    try:
        df = get_dataframe(fn)
    except Exception as error:
        raise RuntimeError(
            f'Uganda/{t}: declared plot_inputs failed to load {fn}; '
            'refusing a partial country table.'
        ) from error
    pieces.append(df)

assert pieces, "plot_inputs: no wave-level parquets found"

p = pd.concat(pieces)

updated_ids = json.load(open('updated_ids.json'))
p = id_walk(p, updated_ids)

to_parquet(p, '../var/plot_inputs.parquet')
