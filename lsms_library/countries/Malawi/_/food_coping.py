"""Concatenate wave-level food_coping parquets for Malawi (GH #332).

Each buildable wave's ``Malawi/<wave>/_/food_coping.py`` produces a
parquet indexed (t, i, Strategy) with the integer ``Days`` column.  This
script concatenates them.  Cross-wave id_walk (panel-id chaining) and the
join of the cluster id ``v`` are applied by the framework at API time in
_finalize_result -- as for plot_features, no id_walk is run here.

2004-05 (IHS2) is ABSENT: its Module H is a 3-day food-consumption recall
(item-level), not the rCSI coping battery introduced in IHS3.
"""
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet


# Only the four buildable IHS3+ / IHPS waves carry Module H rCSI items.
WAVES = ['2010-11', '2013-14', '2016-17', '2019-20']

pieces = []
for t in WAVES:
    fn = f'../{t}/_/food_coping.parquet'
    try:
        df = get_dataframe(fn)
    except Exception:
        # Wave not yet wired / parquet not built.  DVC raises
        # PathMissingError here, not FileNotFoundError.
        continue
    pieces.append(df)

assert pieces, "food_coping: no wave-level parquets found"

p = pd.concat(pieces)

to_parquet(p, '../var/food_coping.parquet')
