"""Concatenate wave-level livestock parquets for Malawi (GAP 4).

Each buildable wave's ``Malawi/<wave>/_/livestock.py`` produces a parquet
indexed (t, i, animal) with the canonical item-level columns (HeadCount,
HeadAcquired, HeadSold, Value).  This script concatenates them.  Cross-wave
id_walk (panel-id chaining) is applied by the framework at API time in
_finalize_result.  ``v`` is NOT joined: 'livestock' is in the framework
_no_v_join set (a household-level holding, not a cluster-keyed roster), so
the grain stays (t, i, animal) with no cluster level.

Only the four IHS3+ IHPS waves are buildable (2010-11, 2013-14, 2016-17,
2019-20) -- the same waves as crop_production / plot_inputs / plot_features,
and the World Bank Plotcrop panel's waves 1-4.  2004-05 (IHS2) is DEFERRED:
its agricultural file is household-level aggregated, with no Module R
livestock roster.
"""
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet


WAVES = ['2010-11', '2013-14', '2016-17', '2019-20']

pieces = []
for t in WAVES:
    fn = f'../{t}/_/livestock.parquet'
    try:
        df = get_dataframe(fn)
    except Exception:
        # Wave not yet wired / parquet not built (DVC raises
        # PathMissingError here, not FileNotFoundError).
        continue
    pieces.append(df)

assert pieces, "livestock: no wave-level parquets found"

p = pd.concat(pieces)

to_parquet(p, '../var/livestock.parquet')
