"""Concatenate wave-level crop_production for Mali (GAP 1; parity loop).

Each EACI wave's ``Mali/<wave>/_/crop_production.py`` writes a parquet
indexed by ``(t, i, plot, crop)`` with the reported item-level harvest
columns (Quantity, u, Quantity_sold, Value_sold, planting_month,
harvest_month, intercropped, perennial).  The crop/harvest module lives in
the EACI waves only (2014-15 EACI14, 2017-18 EACI17); the EHCVM waves
(2018-19, 2021-22) carry no harvest block, so they contribute nothing.

``v`` is NOT baked in — the framework joins it from ``sample()`` at API
time.  ``id_walk`` is left to ``_finalize_result`` (cached parquets store
pre-transformation data), matching the plot_features / food_coping pattern.
"""
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet

WAVES = ['2014-15', '2017-18', '2018-19', '2021-22']

pieces = []
for t in WAVES:
    fn = f'../{t}/_/crop_production.parquet'
    try:
        df = get_dataframe(fn)
    except Exception:
        # Wave not wired (no .py script / no parquet); DVC raises
        # PathMissingError here, not FileNotFoundError.
        continue
    pieces.append(df)

assert pieces, "crop_production: no wave-level parquets found"

p = pd.concat(pieces)

assert p.index.is_unique, "Non-unique (t, i, plot, crop) after concat"

to_parquet(p, '../var/crop_production.parquet')
