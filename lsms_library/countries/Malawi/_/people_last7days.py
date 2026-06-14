"""Concatenate wave-level people_last7days parquets for Malawi (GAP 3,
individual grain).

Each buildable wave's ``Malawi/<wave>/_/people_last7days.py`` produces a
parquet indexed (t, i, pid) with the canonical per-individual 7-day columns
(farm_work, SOB_work, wage_work, farm_hrs, SB_hrs, wage_hrs, wage_industry,
working_age).  This script concatenates them.  Cross-wave id_walk (panel-id
chaining) and the join of the cluster id ``v`` are applied by the framework
at API time in _finalize_result.

Only the four IHS3+ IHPS waves are buildable (2010-11, 2013-14, 2016-17,
2019-20) -- the waves with a standard hh_mod_e labor module.  2004-05
(IHS2) is DEFERRED: its labor module predates the 7-day activity battery
this feature reads.
"""
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet


WAVES = ['2010-11', '2013-14', '2016-17', '2019-20']

pieces = []
for t in WAVES:
    fn = f'../{t}/_/people_last7days.parquet'
    try:
        df = get_dataframe(fn)
    except Exception:
        # Wave not yet wired / parquet not built (DVC raises
        # PathMissingError here, not FileNotFoundError).
        continue
    pieces.append(df)

assert pieces, "people_last7days: no wave-level parquets found"

p = pd.concat(pieces)

to_parquet(p, '../var/people_last7days.parquet')
