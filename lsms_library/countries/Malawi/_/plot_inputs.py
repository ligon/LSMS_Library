"""Concatenate wave-level plot_inputs parquets for Malawi (GAP 2).

Each buildable wave's ``Malawi/<wave>/_/plot_inputs.py`` produces a parquet
indexed (t, i, plot, input, crop, u) with the canonical item-level columns
(Quantity, Quantity_purchased, Purchased, Improved).  This script
concatenates them.  Cross-wave id_walk (panel-id chaining) and the join of
the cluster id ``v`` are applied by the framework at API time in
_finalize_result.

Only the four IHS3+ IHPS waves are buildable (2010-11, 2013-14, 2016-17,
2019-20) -- the same waves as crop_production / plot_features.  2004-05
(IHS2) is DEFERRED: its agricultural file is household-level aggregated,
with no plot-level input roster.
"""
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet


WAVES = ['2010-11', '2013-14', '2016-17', '2019-20']

pieces = []
for t in WAVES:
    fn = f'../{t}/_/plot_inputs.parquet'
    try:
        df = get_dataframe(fn)
    except Exception:
        # Wave not yet wired / parquet not built (DVC raises
        # PathMissingError here, not FileNotFoundError).
        continue
    pieces.append(df)

assert pieces, "plot_inputs: no wave-level parquets found"

p = pd.concat(pieces)

to_parquet(p, '../var/plot_inputs.parquet')
