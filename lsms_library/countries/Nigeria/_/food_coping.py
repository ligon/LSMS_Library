"""Concatenate wave-level food_coping for Nigeria (#332, Family B).

Each wired wave's ``Nigeria/<wave>/_/food_coping.py`` writes a parquet
indexed by ``(t, i, Strategy)`` with an integer ``Days`` column (0-7).
This script concatenates them into the country-level table.

Only W1-W3 (2010-11/2012-13/2015-16) carry the GHS section-9 coping
day-count battery; waves without a food_coping.py script are skipped
(W4+ section 9 switched to FIES, wired separately).  No id_walk is
applied: Nigeria GHS-Panel hhid is stable across waves.  `v` is
intentionally absent; the framework joins it from sample() at API time.
"""
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet
from nigeria import Waves

pieces = []
for t in Waves.keys():
    fn = f'../{t}/_/food_coping.parquet'
    try:
        df = get_dataframe(fn)
    except Exception:
        # Wave not wired (script absent or parquet not yet built).
        # DVC raises PathMissingError here, not FileNotFoundError.
        continue
    pieces.append(df)

assert pieces, "food_coping: no wave-level parquets found"

p = pd.concat(pieces)

to_parquet(p, '../var/food_coping.parquet')
