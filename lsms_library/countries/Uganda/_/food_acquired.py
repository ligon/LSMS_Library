"""Concatenate wave-level food_acquired data for Uganda.

Wave-level scripts (each ``Uganda/<wave>/_/food_acquired.py``) call
``uganda.food_acquired_to_canonical()`` and produce canonical-form
parquets with index ``[t, i, j, u, s]`` (``s in {'purchased', 'inkind',
'produced'}``) and columns ``[Quantity, Expenditure, Price]`` (Phase 3
of GH #169).  This script just concatenates them across waves and
applies cross-wave id_walk.

The pre-Phase-3 implementation expected wide-form wave parquets and
did ``df['t'] = t`` followed by ``groupby(['i','t','j','u']).sum()``;
that broke once the wave-level reshape moved ``t`` into the index
(pandas 2.x raises ``ValueError: 't' is both an index level and a
column label``).  Replaced 2026-05-08 to mirror the Ethiopia fix from
PR #242.
"""
import json

import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet
from uganda import Waves, id_walk


p = []
for t in Waves.keys():
    df = get_dataframe('../' + t + '/_/food_acquired.parquet').squeeze()
    # Wave parquet already has canonical index [t, i, j, u, s] and
    # columns [Quantity, Expenditure, Price] from
    # food_acquired_to_canonical().
    p.append(df)

p = pd.concat(p)

updated_ids = json.load(open('updated_ids.json'))
p = id_walk(p, updated_ids)

to_parquet(p, '../var/food_acquired.parquet')
