"""Concatenate wave-level plot_features parquets for Malawi (GH #167).

Each buildable wave's ``Malawi/<wave>/_/plot_features.py`` produces a
parquet indexed (t, i, plot_id) with the canonical columns.  This script
concatenates them.  Cross-wave id_walk (panel-id chaining) and the join
of the cluster id ``v`` are applied by the framework at API time in
_finalize_result, so -- unlike Uganda -- no id_walk is run here (Malawi
has no country-level updated_ids.json; panel linkage flows from the
``panel_ids`` table).

2004-05 (IHS2) is DEFERRED: it has no standard plot roster.
"""
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet


# Only the four buildable IHS3+ waves; 2004-05 deferred.
WAVES = ['2010-11', '2013-14', '2016-17', '2019-20']

pieces = []
for t in WAVES:
    fn = f'../{t}/_/plot_features.parquet'
    try:
        df = get_dataframe(fn)
    except Exception:
        # Wave not yet wired / parquet not built.  DVC raises
        # PathMissingError here, not FileNotFoundError.
        continue
    pieces.append(df)

assert pieces, "plot_features: no wave-level parquets found"

p = pd.concat(pieces)

to_parquet(p, '../var/plot_features.parquet')
