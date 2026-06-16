"""Concatenate wave-level crop_production parquets for Malawi (GAP 1).

Each buildable wave's ``Malawi/<wave>/_/crop_production.py`` produces a
parquet indexed (t, i, plot, crop) with the canonical item-level columns
(Quantity, u, Quantity_sold, Value_sold, planting_month, harvest_month,
intercropped, perennial).  This script concatenates them.  Cross-wave
id_walk (panel-id chaining) and the join of the cluster id ``v`` are
applied by the framework at API time in _finalize_result.

Only the four IHS3+ IHPS waves are buildable (2010-11, 2013-14, 2016-17,
2019-20) -- these are the World Bank Plotcrop panel's waves 1-4.  2004-05
(IHS2) is DEFERRED: its agricultural file is household-level aggregated
(wide crop columns), with no plot-crop roster.
"""
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet


WAVES = ['2010-11', '2013-14', '2016-17', '2019-20']

pieces = []
for t in WAVES:
    fn = f'../{t}/_/crop_production.parquet'
    try:
        df = get_dataframe(fn)
    except Exception:
        # Wave not yet wired / parquet not built (DVC raises
        # PathMissingError here, not FileNotFoundError).
        continue
    pieces.append(df)

assert pieces, "crop_production: no wave-level parquets found"

p = pd.concat(pieces)

# Move u into the index (canonical (t, i, plot, crop, u)).  u is a grouping
# key, not a free column: a plot-crop measured in two units must be two
# distinct rows, not one survivor of a .first() collapse.  See
# SkunkWorks/grain_aggregation_policy.org.  Wave parquets carry u as a
# column; append it here so the country parquet's on-disk index matches the
# declared schema.
if 'u' in p.columns:
    p = p.set_index('u', append=True)

to_parquet(p, '../var/crop_production.parquet')
