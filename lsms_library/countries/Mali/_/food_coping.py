"""Concatenate wave-level food_coping (rCSI day-counts) for Mali (#332).

Each wave's ``Mali/<wave>/_/food_coping.py`` writes a parquet indexed by
``(t, i, Strategy)`` with column ``Days`` (0-7, days in the last 7 the
household used the coping strategy).  Only the EACI 2014-15 wave carries
the Section-17 rCSI battery; the EHCVM waves (2018-19, 2021-22) use the
FAO FIES instrument instead (wired as ``food_security``) and have no
day-count coping block.  This single-wave concatenator mirrors the
plot_features pattern for consistency and future-proofing.

``v`` is NOT baked in — the framework joins it from ``sample()`` at API
time.  ``id_walk`` is likewise left to ``_finalize_result`` (cached
parquets store pre-transformation data).
"""
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet

WAVES = ['2014-15', '2017-18', '2018-19', '2021-22']

pieces = []
for t in WAVES:
    fn = f'../{t}/_/food_coping.parquet'
    try:
        df = get_dataframe(fn)
    except Exception:
        # Wave not wired for food_coping (no .py script / no parquet).
        # DVC raises PathMissingError here, not FileNotFoundError.
        continue
    pieces.append(df)

assert pieces, "food_coping: no wave-level parquets found"

p = pd.concat(pieces)

assert p.index.is_unique, "Non-unique (t, i, Strategy) after concat"

to_parquet(p, '../var/food_coping.parquet')
