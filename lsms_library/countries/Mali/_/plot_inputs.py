"""Concatenate wave-level plot_inputs for Mali (GAP 2; parity loop).

Each EACI wave's ``Mali/<wave>/_/plot_inputs.py`` writes a parquet indexed
by ``(t, i, plot, input, crop)`` with the reported item-level input columns
(Quantity, u, Purchased, Quantity_purchased, Improved).  The crop/input
module lives in the EACI waves only (2014-15 EACI14, 2017-18 EACI17); the
EHCVM waves (2018-19, 2021-22) carry no agriculture-input block, so they
contribute nothing — the same wave split as crop_production.

``v`` is NOT baked in — the framework joins it from ``sample()`` at API
time.  ``id_walk`` is left to ``_finalize_result`` (cached parquets store
pre-transformation data), matching the crop_production / plot_features /
food_coping pattern.
"""
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet

WAVES = ['2014-15', '2017-18', '2018-19', '2021-22']

pieces = []
for t in WAVES:
    fn = f'../{t}/_/plot_inputs.parquet'
    try:
        df = get_dataframe(fn)
    except Exception:
        # Wave not wired (no .py script / no parquet); DVC raises
        # PathMissingError here, not FileNotFoundError.
        continue
    pieces.append(df)

assert pieces, "plot_inputs: no wave-level parquets found"

p = pd.concat(pieces)

assert p.index.is_unique, "Non-unique (t, i, plot, input, crop) after concat"

to_parquet(p, '../var/plot_inputs.parquet')
