"""Concatenate wave-level anthropometry data for Tanzania NPS
(parity-loop GAP 5).

anthropometry is a NEW item-level feature: reported body measures (Weight,
Height, MUAC) at the individual grain (t, i, pid).  It is DISTINCT from our
``nutrition`` feature (nutrient intake).  The WHO/2006 z-scores
(haz06 / waz06 / whz06 / bmiz06) and the wasting/stunting flags are a
query-time TRANSFORM over these raw measures -- never stored here.

Each buildable wave's ``Tanzania/<wave>/_/anthropometry.py`` writes a parquet
indexed ``(t, i, pid)`` with columns Weight / Height / MUAC.  The 2008-15
multi-round folder's parquet carries all four NPS rounds (2008-09, 2010-11,
2012-13, 2014-15) from the harmonised section-V panel file ``upd4_hh_v.dta``;
the 2019-20 and 2020-21 folders carry one wave each (HH_SEC_V / hh_sec_v).

We load by folder (Waves.keys()), concatenate, then id_walk to harmonize
household ids across waves (pid is left untouched -- id_walk renames only the
``i`` level).  ``anthropometry`` is individual-level; the framework joins the
cluster ``v`` from sample() at API time (it is NOT in the _no_v_join set).
"""
import json

import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet
from lsms_library.paths import data_root
from tanzania import Waves, id_walk
import warnings

pieces = {}
for t in Waves.keys():
    candidates = [
        str(data_root('Tanzania') / t / '_' / 'anthropometry.parquet'),
        '../' + t + '/_/anthropometry.parquet',
    ]
    for path in candidates:
        try:
            pieces[t] = get_dataframe(path)
            break
        except (FileNotFoundError, Exception):
            continue
    if t not in pieces:
        warnings.warn(f'Could not load anthropometry for {t}')

assert pieces, "anthropometry: no wave-level parquets found"

p = pd.concat(pieces.values())

# Ensure the canonical (t, i, pid) index.
target_idx = ['t', 'i', 'pid']
if list(p.index.names) != target_idx:
    p = p.reset_index()
    p = p.set_index(target_idx)

updated_ids = json.load(open('updated_ids.json'))
p = id_walk(p, updated_ids, hh_index='i')

# GH #637 key-soundness review -- key SOUND, collapse is dead code.
# Every wave parquet is already (t, i, pid)-unique (see each wave script's own
# note), the folders contribute disjoint t values, and id_walk renames only the
# `i` level -- measured on a cold build it produces 0 collisions here.  So the
# concatenation cannot introduce a duplicate and .first() is never called.
if not p.index.is_unique:
    p = p.groupby(level=p.index.names).first()

to_parquet(p, '../var/anthropometry.parquet')
