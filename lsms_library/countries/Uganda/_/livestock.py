"""Concatenate wave-level livestock data for Uganda (GAP 4).

Each wave's ``Uganda/<wave>/_/livestock.py`` produces a parquet indexed
``(t, i, animal)`` with the REPORTED livestock columns (HeadCount,
HeadAcquired, HeadSold, Value).  This script concatenates them across waves
and applies cross-wave id_walk so the household index uses the panel
canonical id scheme.

Source: the UNPS livestock roster (AGSEC6A/6B/6C) that the WB code reads
only to collapse to a single engaged-livestock binary.  2005-06 is
intentionally absent: it has no AGSEC6C poultry roster in the canonical
form and the UNPS agriculture panel the WB harmonise effort tracks starts
at wave 1 = 2009-10.
"""
import json

import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet
from uganda import Waves, id_walk


pieces = []
for t in Waves.keys():
    fn = f'../{t}/_/livestock.parquet'
    try:
        df = get_dataframe(fn)
    except Exception:
        # Wave not wired for livestock (no .py / parquet, e.g. 2005-06).
        # DVC raises PathMissingError here, not FileNotFoundError.
        continue
    pieces.append(df)

assert pieces, "livestock: no wave-level parquets found"

p = pd.concat(pieces)

updated_ids = json.load(open('updated_ids.json'))
p = id_walk(p, updated_ids)

to_parquet(p, '../var/livestock.parquet')
