"""Concatenate wave-level livestock for Niger (GAP 4, item-level).

Each wave's ``Niger/<wave>/_/livestock.py`` writes a parquet indexed by
``(t, i, animal)`` with the reported per-species columns (HeadCount,
HeadAcquired, HeadSold).  This script concatenates the four waves
(2011-12, 2014-15, 2018-19, 2021-22), each of which has a livestock
roster.  ``v`` is NOT baked in: 'livestock' is in the framework
``_no_v_join`` set, so the household-level rows carry no cluster level and
the framework does not join one from ``sample()``.

As in the crop_production / plot_inputs siblings, this does NOT apply
``id_walk`` here; the framework runs it in ``_finalize_result`` on every
read.  Each wave's ``_finish_livestock`` already sums the ECVMA animal
sub-type lines (bœuf/vache/... -> Cattle) onto one row per (household,
canonical species), so the ``(t, i, animal)`` index is UNIQUE within each
wave and the concatenation stays unique across waves (the four ``t`` values
are disjoint).  No rows are lost to the framework's canonical-index de-dup.
"""
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet


WAVES = ['2011-12', '2014-15', '2018-19', '2021-22']

pieces = []
for t in WAVES:
    fn = f'../{t}/_/livestock.parquet'
    try:
        df = get_dataframe(fn)
    except Exception:
        # Wave not wired for livestock (no .py script / no parquet).
        # DVC raises PathMissingError here, not FileNotFoundError.
        continue
    pieces.append(df)

assert pieces, 'livestock: no wave-level parquets found'

p = pd.concat(pieces)

to_parquet(p, '../var/livestock.parquet')
