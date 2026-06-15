#!/usr/bin/env python
"""Concatenate wave-level anthropometry data for Ethiopia (GAP 5).

Each wave's ``Ethiopia/<wave>/_/anthropometry.py`` produces a parquet with
index ``(t, i, pid)`` and the canonical reported columns (Weight, Height,
MUAC, Age_months, Sex).  This script concatenates them across the five ESS
waves and applies the cross-wave ``id_walk`` so the household index uses the
panel canonical id scheme -- the same conversion ``sample()`` receives at
API time.  ``v`` is then auto-joined from ``sample()`` by the framework
(``_join_v_from_sample`` in country.py) at query time: do NOT bake it in.

anthropometry is INDIVIDUAL-level (one row per measured child), so the
household id ``i`` carries the framework panel conversion exactly as
household_roster does (W2/W3 emit household_id2 to match the roster's i/pid;
W1/W4/W5 emit household_id).  ``id_walk`` is idempotent (it sets
``attrs['id_converted']``) so ``_finalize_result`` skips re-applying it.

This is a NEW, separate feature from ``nutrition`` (which is nutrient
*intake*, a different construct).  No z-scores (haz06/waz06/whz06/bmiz06) or
wasting/stunting are stored here -- those are WHO-2006-reference transforms
computed at query time, never data-layer columns.
"""
import json

import pandas as pd

from lsms_library.local_tools import get_dataframe, id_walk, to_parquet
from ethiopia import Waves


pieces = []
for t in Waves.keys():
    fn = f'../{t}/_/anthropometry.parquet'
    try:
        df = get_dataframe(fn)
    except Exception:
        # Wave not yet built / parquet absent (DVC raises PathMissingError).
        continue
    pieces.append(df)

assert pieces, "anthropometry: no wave-level parquets found"

p = pd.concat(pieces)

updated_ids = json.load(open('updated_ids.json'))
p = id_walk(p, updated_ids)

to_parquet(p, '../var/anthropometry.parquet')
