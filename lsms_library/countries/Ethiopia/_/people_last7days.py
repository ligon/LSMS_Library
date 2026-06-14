#!/usr/bin/env python
"""Concatenate wave-level people_last7days data for Ethiopia (GAP 3.1).

Each wave's ``Ethiopia/<wave>/_/people_last7days.py`` produces a parquet
with index ``(t, i, pid)`` and the canonical people_last7days columns
(farm_work, SOB_work, wage_work, farm_hrs, SB_hrs, wage_hrs, industry,
working_age).  This script concatenates them across the five ESS waves and
applies the cross-wave ``id_walk`` so the household index uses the panel
canonical id scheme (the same conversion ``sample()`` receives at API time;
people_last7days is household-linked and NOT in ``_no_v_join``, so v
auto-joins from sample()).

``id_walk`` is idempotent (it sets ``attrs['id_converted']``), so the
framework's ``_finalize_result`` skips re-applying it.  The wave parquets
emit the wave-native household / individual id that matches the household
roster (``household_id2`` / ``individual_id2`` for W2/W3, else
``household_id`` / ``individual_id``), so people_last7days joins the roster
on ``(t, i, pid)``.
"""
import json

import pandas as pd

from lsms_library.local_tools import get_dataframe, id_walk, to_parquet
from ethiopia import Waves


pieces = []
for t in Waves.keys():
    fn = f'../{t}/_/people_last7days.parquet'
    try:
        df = get_dataframe(fn)
    except Exception:
        # Wave not yet built / parquet absent (DVC raises PathMissingError).
        continue
    pieces.append(df)

assert pieces, "people_last7days: no wave-level parquets found"

p = pd.concat(pieces)

updated_ids = json.load(open('updated_ids.json'))
p = id_walk(p, updated_ids)

to_parquet(p, '../var/people_last7days.parquet')
