"""Concatenate wave-level individual_education data for Tanzania NPS (GH #171).

Each buildable wave's ``Tanzania/<wave>/_/individual_education.py`` writes a
parquet indexed ``(t, i, pid)`` whose ``Educational Attainment`` column already
holds the CANONICAL ordinal label -- the wave scripts call
``tanzania.harmonize_education_labels`` to map the raw NPS "highest grade
completed" codes (hc_07 / hh_c07: PP, ADULT, D1..D8, F1..F6, 'O'+COURSE,
DIPLOMA, U1..U5&+, ...) onto the canonical vocabulary via the
``harmonize_education`` table in ``_/categorical_mapping.org``.  Harmonization
lives in the wave scripts (not here) because the framework concatenates the
per-wave parquets directly when they exist; this country-level aggregator only
runs as the no-per-wave-parquet fallback.

The 2008-15 multi-round folder parquet carries all four NPS rounds
(2008-09 .. 2014-15) from upd4_hh_c.dta; 2019-20 and 2020-21 carry one wave
each.  We load by folder (Waves.keys()), concatenate, then id_walk to harmonize
household ids across waves (pid untouched -- id_walk renames only ``i``).
individual_education is individual-level; the framework joins the cluster ``v``
from sample() at API time (canonical index (t, v, i, pid)).
"""
import json
import warnings

import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet
from lsms_library.paths import data_root
from tanzania import Waves, id_walk

pieces = {}
for t in Waves.keys():
    candidates = [
        str(data_root('Tanzania') / t / '_' / 'individual_education.parquet'),
        '../' + t + '/_/individual_education.parquet',
    ]
    for path in candidates:
        try:
            pieces[t] = get_dataframe(path)
            break
        except (FileNotFoundError, Exception):
            continue
    if t not in pieces:
        warnings.warn(f'Could not load individual_education for {t}')

assert pieces, "individual_education: no wave-level parquets found"

p = pd.concat(pieces.values())

# Ensure the canonical (t, i, pid) index.
target_idx = ['t', 'i', 'pid']
if list(p.index.names) != target_idx:
    p = p.reset_index()
    p = p.set_index(target_idx)

updated_ids = json.load(open('updated_ids.json'))
p = id_walk(p, updated_ids, hh_index='i')

if not p.index.is_unique:
    p = p.groupby(level=p.index.names).first()

to_parquet(p, '../var/individual_education.parquet')
